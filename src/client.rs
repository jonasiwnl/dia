use std::net::SocketAddr;

use serde::{Serialize, de::DeserializeOwned};

use crate::{error::DiaError, sequencer::SequencerHeader, util::{open_multicast_reader, open_multicast_writer}};

pub trait ProcessMessage {
    type Message: Serialize + DeserializeOwned + Send + 'static;
    fn create_message(&self, data: String) -> Self::Message;
    fn message_handler(&self, message: Self::Message);
}

pub struct Client<MessageProcessor> {
    propose_addr: SocketAddr,
    consensus_addr: SocketAddr,
    pub processor: MessageProcessor,
}

impl<MessageProcessor: ProcessMessage> Client<MessageProcessor> {
    pub fn new(propose_addr: SocketAddr, consensus_addr: SocketAddr, processor: MessageProcessor) -> Self {
        Self {
            propose_addr,
            consensus_addr,
            processor,
        }
    }

    // Sends a message to the sequencer, blocks until it receives a response
    pub async fn write_message(&self, message: <MessageProcessor as ProcessMessage>::Message) -> Result<(), DiaError> {
        let socket = open_multicast_writer(self.propose_addr).await?;

        let bytes_sent = socket.send_to(&bincode::serialize(&message)?, self.propose_addr).await?;
        eprintln!("[client] successfully broadcasted {} bytes to multicast topic {}", bytes_sent, self.propose_addr);

        self.processor.message_handler(message);

        Ok(())
    }

    pub async fn listen(&self) -> Result<(), DiaError> {
        let socket = open_multicast_reader(self.consensus_addr)?;

        eprintln!("[client] listening on: {}", self.consensus_addr);
        let mut buf = [0u8; 1024];

        loop {
            match socket.recv_from(&mut buf).await {
                Ok((amt, src)) => {
                    eprintln!("[client] received {} bytes from {}", amt, src);
                    let (header_bytes, payload) = buf[..amt].split_at(std::mem::size_of::<SequencerHeader>());
                    let header: SequencerHeader = bincode::deserialize(header_bytes)?;
                    let message = bincode::deserialize::<MessageProcessor::Message>(payload)?;
                    self.processor.message_handler(message);
                }
                Err(e) => eprintln!("[client] recv error: {:?}", e),
            }
        }
    }
}
