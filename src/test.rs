use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use serde::{Deserialize, Serialize, ser::SerializeStruct};
use tokio::{
    net::UdpSocket,
    sync::{mpsc, oneshot},
};

use crate::{
    client::Client,
    error::DiaError,
    sequencer::Sequencer,
    util::{open_multicast_reader, open_multicast_writer},
};

#[derive(Clone, Serialize, Deserialize)]
struct TestMessage {
    msg: String,
}

static FAIL_NEXT_SERIALIZATION: AtomicBool = AtomicBool::new(false);

#[derive(Deserialize)]
struct FailsOnceMessage {
    msg: String,
}

impl Serialize for FailsOnceMessage {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if FAIL_NEXT_SERIALIZATION.swap(false, Ordering::SeqCst) {
            return Err(serde::ser::Error::custom(
                "intentional serialization failure",
            ));
        }

        let mut state = serializer.serialize_struct("FailsOnceMessage", 1)?;
        state.serialize_field("msg", &self.msg)?;
        state.end()
    }
}

async fn relay_first_packet_away_from_client_one(
    source: UdpSocket,
    client_one_writer: UdpSocket,
    client_one_addr: std::net::SocketAddr,
    client_two_writer: UdpSocket,
    client_two_addr: std::net::SocketAddr,
    first_packet_forwarded: oneshot::Sender<()>,
) -> Result<(), DiaError> {
    let mut buf = [0u8; 1024];

    let (first_len, _) = source.recv_from(&mut buf).await?;
    client_two_writer
        .send_to(&buf[..first_len], client_two_addr)
        .await?;
    let _ = first_packet_forwarded.send(());

    let (second_len, _) = source.recv_from(&mut buf).await?;
    client_one_writer
        .send_to(&buf[..second_len], client_one_addr)
        .await?;
    client_two_writer
        .send_to(&buf[..second_len], client_two_addr)
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_basic() -> Result<(), DiaError> {
    let propose_addr = "239.0.1.1:6000".parse().unwrap();
    let consensus_addr = "239.0.1.2:6000".parse().unwrap();

    let sequencer = Sequencer::bind(propose_addr, consensus_addr).await?;
    tokio::spawn(sequencer.run());

    let (sender, receiver) =
        Client::<TestMessage>::bind_split(propose_addr, consensus_addr).await?;
    let (tx, mut rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        receiver
            .listen(move |message| {
                let _ = tx.send((message.id(), message.payload.msg));
            })
            .await
    });

    let msg = String::from("pooper");
    let sent_id = sender.send(TestMessage { msg: msg.clone() }).await?;
    let (received_id, received) = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("round-trip timed out")
        .expect("channel closed");

    assert_eq!(received_id, sent_id);
    assert_eq!(received, msg);

    Ok(())
}

#[tokio::test]
async fn test_concurrent_sends_from_one_client() -> Result<(), DiaError> {
    let propose_addr = "239.0.1.5:6000".parse().unwrap();
    let consensus_addr = "239.0.1.6:6000".parse().unwrap();
    let sequencer = Sequencer::bind(propose_addr, consensus_addr).await?;
    tokio::spawn(sequencer.run());

    let (sender, receiver) =
        Client::<TestMessage>::bind_split(propose_addr, consensus_addr).await?;
    let (tx, mut rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        receiver
            .listen(move |message| {
                let _ = tx.send((message.id(), message.payload.msg));
            })
            .await
    });

    let (first, second) = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(
            sender.send(TestMessage {
                msg: "first".into(),
            }),
            sender.send(TestMessage {
                msg: "second".into(),
            })
        )
    })
    .await
    .expect("concurrent sends did not finish");
    let first = first?;
    let second = second?;

    let mut received = Vec::new();
    for _ in 0..2 {
        received.push(
            tokio::time::timeout(Duration::from_secs(2), rx.recv())
                .await
                .expect("message was not sequenced")
                .expect("receiver stopped"),
        );
    }
    let mut received_ids = received.iter().map(|(id, _)| *id).collect::<Vec<_>>();
    received_ids.sort_by_key(|id| id.op_id);
    let mut sent_ids = vec![first, second];
    sent_ids.sort_by_key(|id| id.op_id);
    assert_eq!(received_ids, sent_ids);
    let mut payloads = received
        .into_iter()
        .map(|(_, payload)| payload)
        .collect::<Vec<_>>();
    payloads.sort();
    assert_eq!(payloads, ["first", "second"]);

    Ok(())
}

#[tokio::test]
async fn test_sender_recovers_after_serialization_failure() -> Result<(), DiaError> {
    let propose_addr = "239.0.1.7:6000".parse().unwrap();
    let consensus_addr = "239.0.1.8:6000".parse().unwrap();
    let sequencer = Sequencer::bind(propose_addr, consensus_addr).await?;
    tokio::spawn(sequencer.run());

    let (sender, receiver) =
        Client::<FailsOnceMessage>::bind_split(propose_addr, consensus_addr).await?;
    let (tx, mut rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        receiver
            .listen(move |message| {
                let _ = tx.send(message.id());
            })
            .await
    });

    FAIL_NEXT_SERIALIZATION.store(true, Ordering::SeqCst);
    assert!(matches!(
        sender
            .send(FailsOnceMessage {
                msg: "fails".into(),
            })
            .await,
        Err(DiaError::Serialization(_))
    ));

    let sent_id = tokio::time::timeout(
        Duration::from_secs(2),
        sender.send(FailsOnceMessage {
            msg: "succeeds".into(),
        }),
    )
    .await
    .expect("send remained blocked after serialization failure")?;
    let received_id = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("successful send was not sequenced")
        .expect("receiver stopped");
    assert_eq!(received_id, sent_id);

    Ok(())
}

#[tokio::test]
async fn test_client_repairs_a_missing_consensus_message() -> Result<(), DiaError> {
    let propose_addr = "239.0.1.9:6000".parse().unwrap();
    let sequencer_consensus_addr = "239.0.1.10:6000".parse().unwrap();
    let client_one_consensus_addr = "239.0.1.11:6000".parse().unwrap();
    let client_two_consensus_addr = "239.0.1.12:6000".parse().unwrap();

    let sequencer = Sequencer::bind(propose_addr, sequencer_consensus_addr).await?;
    tokio::spawn(sequencer.run());

    // This is a network-only fault injector: it does not inspect or construct
    // Dia packets. It drops the first sequencer datagram only for client one.
    let relay_source = open_multicast_reader(sequencer_consensus_addr)?;
    let client_one_writer = open_multicast_writer(client_one_consensus_addr).await?;
    let client_two_writer = open_multicast_writer(client_two_consensus_addr).await?;
    let (first_packet_forwarded_tx, first_packet_forwarded_rx) = oneshot::channel();
    let relay = tokio::spawn(relay_first_packet_away_from_client_one(
        relay_source,
        client_one_writer,
        client_one_consensus_addr,
        client_two_writer,
        client_two_consensus_addr,
        first_packet_forwarded_tx,
    ));

    let (client_one_sender, client_one_receiver) =
        Client::<TestMessage>::bind_split(propose_addr, client_one_consensus_addr).await?;
    let (client_one_history_tx, mut client_one_history_rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        client_one_receiver
            .listen(move |message| {
                let _ = client_one_history_tx.send((message.header.seq_id, message.payload.msg));
            })
            .await
    });

    let (client_two_sender, client_two_receiver) =
        Client::<TestMessage>::bind_split(propose_addr, client_two_consensus_addr).await?;
    tokio::spawn(async move { client_two_receiver.listen(|_| {}).await });

    let _client_one_send = tokio::spawn(async move {
        client_one_sender
            .send(TestMessage {
                msg: "operation one".into(),
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), first_packet_forwarded_rx)
        .await
        .expect("sequencer did not accept client one's operation")
        .expect("relay stopped before forwarding the first packet");

    tokio::time::timeout(
        Duration::from_secs(2),
        client_two_sender.send(TestMessage {
            msg: "operation two".into(),
        }),
    )
    .await
    .expect("client two's operation was not sequenced")?;
    relay.await.expect("relay task panicked")?;

    let client_one_history = tokio::time::timeout(Duration::from_secs(2), async {
        let mut history = Vec::new();
        for _ in 0..2 {
            history.push(
                client_one_history_rx
                    .recv()
                    .await
                    .expect("client one's receiver stopped"),
            );
        }
        history
    })
    .await
    .expect("client one did not repair the missing consensus message");
    assert_eq!(
        client_one_history,
        vec![
            (0, "operation one".to_string()),
            (1, "operation two".to_string()),
        ]
    );

    Ok(())
}

#[tokio::test]
async fn test_concurrent() -> Result<(), DiaError> {
    let propose_addr = "239.0.1.3:6000".parse().unwrap();
    let consensus_addr = "239.0.1.4:6000".parse().unwrap();

    let sequencer = Sequencer::bind(propose_addr, consensus_addr).await?;
    tokio::spawn(sequencer.run());

    const NUM_NODES: usize = 10;
    const MSGS_PER_NODE: usize = 15;
    const TOTAL: usize = NUM_NODES * MSGS_PER_NODE;

    let mut writers = Vec::with_capacity(NUM_NODES);
    let mut receivers = Vec::with_capacity(NUM_NODES);
    for node_idx in 0..NUM_NODES {
        let (tx, rx) = mpsc::unbounded_channel();
        let (sender, receiver) =
            Client::<TestMessage>::bind_split(propose_addr, consensus_addr).await?;
        tokio::spawn(async move {
            receiver
                .listen(move |message| {
                    let _ = tx.send((message.header.seq_id, message.payload.msg));
                })
                .await
        });
        writers.push(tokio::spawn(async move {
            for msg_idx in 0..MSGS_PER_NODE {
                sender
                    .send(TestMessage {
                        msg: format!("n{}_m{}", node_idx, msg_idx),
                    })
                    .await?;
            }
            Ok::<_, DiaError>(())
        }));
        receivers.push(rx);
    }
    for w in writers {
        w.await.unwrap()?;
    }

    let mut observed: Vec<Vec<(u64, String)>> = Vec::with_capacity(NUM_NODES);
    for (node_idx, mut rx) in receivers.into_iter().enumerate() {
        let order = tokio::time::timeout(Duration::from_secs(5), async {
            let mut v = Vec::with_capacity(TOTAL);
            for _ in 0..TOTAL {
                v.push(rx.recv().await.expect("channel closed"));
            }
            v
        })
        .await
        .unwrap_or_else(|_| panic!("node {} timed out waiting for consensus messages", node_idx));
        observed.push(order);
    }

    let reference = &observed[0];
    for (i, order) in observed.iter().enumerate().skip(1) {
        assert_eq!(
            order, reference,
            "node {} saw different order than node 0",
            i
        );
    }

    let seq_ids = reference
        .iter()
        .map(|(seq_id, _)| *seq_id)
        .collect::<Vec<_>>();
    assert_eq!(
        seq_ids,
        (0..TOTAL as u64).collect::<Vec<_>>(),
        "sequencer assigned non-monotonic sequence ids"
    );

    let mut sorted = reference
        .iter()
        .map(|(_, message)| message.clone())
        .collect::<Vec<_>>();
    sorted.sort();
    let mut expected: Vec<String> = (0..NUM_NODES)
        .flat_map(|n| (0..MSGS_PER_NODE).map(move |m| format!("n{}_m{}", n, m)))
        .collect();
    expected.sort();
    assert_eq!(
        sorted, expected,
        "consensus stream missing or duplicating messages"
    );

    Ok(())
}
