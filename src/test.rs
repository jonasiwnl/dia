use std::sync::Mutex;

use crate::client::{Client, ProcessMessage};

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

#[test]
fn test_basic() {
    let client = Client::new(TestMessageProcessor::new());
    let msg = String::from("pooper");
    client.write_message(TestMessage{msg: msg.clone()});
    let recv_msg = client.processor.recv.lock().unwrap();
    assert_eq!(*recv_msg, msg);
}
