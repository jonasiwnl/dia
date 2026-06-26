use std::net::SocketAddr;

use serde::{Serialize, de::DeserializeOwned};
use tokio::net::UdpSocket;

use crate::error::DiaError;

pub trait ProcessMessage {
    type Message: Serialize + DeserializeOwned + Send + 'static;
    fn create_message(&self, data: String) -> Self::Message;
    fn message_handler(&self, message: Self::Message);
}

pub struct Client<MessageProcessor> {
    outbound: SocketAddr,
    events: SocketAddr,
    pub processor: MessageProcessor,
}

impl<MessageProcessor: ProcessMessage> Client<MessageProcessor> {
    pub fn new(outbound: SocketAddr, events: SocketAddr, processor: MessageProcessor) -> Self {
        Self {
            outbound,
            events,
            processor,
        }
    }

    // Sends a message to the sequencer, blocks until it receives a response
    pub async fn write_message(&self, message: <MessageProcessor as ProcessMessage>::Message) -> Result<(), DiaError> {
        let socket = UdpSocket::bind("0.0.0.0:0").await?;

        match self.outbound {
            SocketAddr::V4(v4) => (*v4.ip(), v4.port()),
            SocketAddr::V6(_) => panic!("[client] IPv6 multicast requires a different setup"),
        };

        // This may need to be increased with more nodes
        socket.set_multicast_ttl_v4(1)?;
        socket.set_multicast_loop_v4(true)?;

        let bytes_sent = socket.send_to(&bincode::serialize(&message)?, self.outbound).await?;
        eprintln!("[client] successfully broadcasted {} bytes to multicast topic {}", bytes_sent, self.outbound);

        self.processor.message_handler(message);

        Ok(())
    }
}
