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

    let sequencer = Sequencer::new(propose_addr, consensus_addr);
    tokio::spawn(async move { sequencer.start().await });

    let (tx, mut rx) = mpsc::unbounded_channel();
    let client = Arc::new(Client::new(
        propose_addr,
        consensus_addr,
        TestMessageProcessor { tx },
    ));
    let listener = Arc::clone(&client);
    tokio::spawn(async move { listener.listen().await });

    let msg = String::from("pooper");

    let received = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            client.write_message(TestMessage { msg: msg.clone() }).await?;
            let _ = rx.recv().await;
            if let Ok(Some(s)) =
                tokio::time::timeout(Duration::from_millis(200), rx.recv()).await
            {
                return Ok::<String, DiaError>(s);
            }
        }
    })
    .await
    .expect("round-trip timed out")?;

    assert_eq!(received, msg);
    Ok(())
}
