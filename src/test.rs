use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::{client::Client, error::DiaError, sequencer::Sequencer};

#[derive(Serialize, Deserialize)]
struct TestMessage {
    msg: String,
}

#[tokio::test]
async fn test_basic() -> Result<(), DiaError> {
    let propose_addr = "239.0.1.1:6000".parse().unwrap();
    let consensus_addr = "239.0.1.2:6000".parse().unwrap();

    let sequencer = Sequencer::bind(propose_addr, consensus_addr).await?;
    tokio::spawn(sequencer.run());

    let (tx, mut rx) = mpsc::unbounded_channel();
    let (sender, receiver) =
        Client::<TestMessage>::bind_split(propose_addr, consensus_addr).await?;
    tokio::spawn(async move {
        receiver
            .listen(move |message| {
                let _ = tx.send(message.payload.msg);
            })
            .await
    });

    let msg = String::from("pooper");
    sender.send(TestMessage { msg: msg.clone() }).await?;

    let received = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("round-trip timed out")
        .expect("channel closed");

    assert_eq!(received, msg);
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

    let mut senders = Vec::with_capacity(NUM_NODES);
    let mut receivers = Vec::with_capacity(NUM_NODES);
    for _ in 0..NUM_NODES {
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
        senders.push(sender);
        receivers.push(rx);
    }

    let mut writers = Vec::with_capacity(NUM_NODES);
    for (node_idx, sender) in senders.into_iter().enumerate() {
        writers.push(tokio::spawn(async move {
            for msg_idx in 0..MSGS_PER_NODE {
                sender
                    .send(TestMessage {
                        msg: format!("n{}_m{}", node_idx, msg_idx),
                    })
                    .await?;
            }
            Ok::<(), DiaError>(())
        }));
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
