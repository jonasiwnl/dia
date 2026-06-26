use std::sync::Arc;
use std::time::Duration;

use serde::{Serialize, Deserialize};
use tokio::sync::mpsc::{self, UnboundedSender};

use crate::{client::{Client, ProcessMessage}, error::DiaError, sequencer::Sequencer};

#[derive(Serialize, Deserialize)]
struct TestMessage {
    msg: String,
}

struct TestMessageProcessor {
    tx: UnboundedSender<String>,
}

impl ProcessMessage for TestMessageProcessor {
    type Message = TestMessage;

    fn create_message(&self, data: String) -> Self::Message {
        TestMessage { msg: data }
    }

    fn message_handler(&self, message: Self::Message) {
        let _ = self.tx.send(message.msg);
    }
}

#[tokio::test]
async fn test_basic() -> Result<(), DiaError> {
    let propose_addr = "239.0.1.1:6000".parse().unwrap();
    let consensus_addr = "239.0.1.2:6000".parse().unwrap();

    let sequencer = Sequencer::bind(propose_addr, consensus_addr).await?;
    tokio::spawn(sequencer.run());

    let (tx, mut rx) = mpsc::unbounded_channel();
    let client = Arc::new(
        Client::bind(propose_addr, consensus_addr, TestMessageProcessor { tx }).await?
    );
    let listener = Arc::clone(&client);
    tokio::spawn(async move { listener.listen().await });

    let msg = String::from("pooper");
    client.write_message(TestMessage { msg: msg.clone() }).await?;

    let _ = rx.recv().await;
    let received = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("round-trip timed out")
        .expect("channel closed");

    assert_eq!(received, msg);
    Ok(())
}
