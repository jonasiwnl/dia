use std::sync::{Arc, Mutex};

use serde::{Serialize, Deserialize};

use crate::{client::{Client, ProcessMessage}, error::DiaError, sequencer::Sequencer};

#[derive(Serialize, Deserialize)]
struct TestMessage {
    msg: String,
}

struct TestMessageProcessor {
    recv: Mutex<String>,
}

impl TestMessageProcessor {
    fn new() -> Self {
        Self{ recv: Mutex::from(String::from("")) }
    }
}

impl ProcessMessage for TestMessageProcessor {
    type Message = TestMessage;

    fn create_message(&self, data: String) -> Self::Message {
        TestMessage{msg: data}
    }

    fn message_handler(&self, message: Self::Message) {
        let mut guard = self.recv.lock().unwrap();
        *guard = message.msg;
    }
}

#[tokio::test]
async fn test_basic() -> Result<(), DiaError> {
    let propose_addr = "239.0.1.1:6000".parse().unwrap();
    let consensus_addr = "239.0.1.2:6000".parse().unwrap();

    let sequencer = Sequencer::new(propose_addr, consensus_addr);
    tokio::spawn(async move { sequencer.start().await });
    let client = Arc::new(Client::new(propose_addr, consensus_addr, TestMessageProcessor::new()));
    let listener = Arc::clone(&client);
    tokio::spawn(async move { listener.listen().await });

    // TODO: race condition
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let msg = String::from("pooper");
    client.write_message(TestMessage{msg: msg.clone()}).await?;

    // TODO: race condition
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let recv_msg = client.processor.recv.lock().unwrap();
    assert_eq!(*recv_msg, msg);

    // TODO: race condition
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    Ok(())
}
