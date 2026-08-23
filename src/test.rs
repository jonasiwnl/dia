use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use serde::{Deserialize, Serialize, ser::SerializeStruct};
use tokio::sync::mpsc;

use crate::{
    client::Client,
    error::DiaError,
    network::testing::{NetworkChange, Node, SimulatedNetwork},
    sequencer::Sequencer,
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

#[tokio::test]
async fn test_pending_send_fails_when_consensus_receiver_fails() {
    let propose_addr = "239.0.1.9:6000".parse().unwrap();
    let consensus_addr = "239.0.1.10:6000".parse().unwrap();
    let network = SimulatedNetwork::new();
    let transport = network.client(1);

    let client = Client::<TestMessage>::from_transport(
        propose_addr,
        consensus_addr,
        transport.proposal_sender(),
        transport.consensus_receiver(),
    );
    let (sender, receiver) = client.split();
    let listener = tokio::spawn(async move { receiver.listen(|_| {}).await });

    let send = tokio::spawn(async move {
        sender
            .send(TestMessage {
                msg: "will fail".into(),
            })
            .await
    });
    network
        .wait_for_transmission(Node::Client(1), Node::Sequencer)
        .await;
    network.apply(NetworkChange::fail_inbound(
        Node::Client(1),
        std::io::ErrorKind::ConnectionReset,
    ));

    let result = tokio::time::timeout(Duration::from_secs(1), send)
        .await
        .expect("send hung after an injected receiver failure")
        .expect("send task panicked");
    assert!(
        matches!(result, Err(DiaError::Network(ref error)) if error.kind() == std::io::ErrorKind::ConnectionReset)
    );

    assert!(matches!(listener.await.unwrap(), Err(DiaError::Network(_))));
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
                let _ = tx.send((message.header.msg_id, message.payload.msg));
            })
            .await
    });

    let msg = String::from("pooper");
    sender.send(TestMessage { msg: msg.clone() }).await?;
    let (received_id, received) = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("round-trip timed out")
        .expect("channel closed");

    assert!(!received_id.is_nil());
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
                let _ = tx.send((message.header.msg_id, message.payload.msg));
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
    first?;
    second?;

    let mut received = Vec::new();
    for _ in 0..2 {
        received.push(
            tokio::time::timeout(Duration::from_secs(2), rx.recv())
                .await
                .expect("message was not sequenced")
                .expect("receiver stopped"),
        );
    }
    let received_ids = received.iter().map(|(id, _)| *id).collect::<Vec<_>>();
    assert_eq!(received_ids.len(), 2);
    assert_ne!(received_ids[0], received_ids[1]);
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
                let _ = tx.send(message.header.msg_id);
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

    tokio::time::timeout(
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
    assert!(!received_id.is_nil());

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
                    let _ = tx.send((message.header.seq_num, message.payload.msg));
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
