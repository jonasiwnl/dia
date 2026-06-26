use std::net::{Ipv4Addr, SocketAddr};

use serde::{Serialize, de::DeserializeOwned};
use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;

use crate::{error::DiaError, sequencer::SequencerHeader};

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

    pub async fn listen(&self) -> Result<(), DiaError> {
        let multicast_ip = match self.events {
            SocketAddr::V4(v4) => *v4.ip(),
            SocketAddr::V6(_) => panic!("IPv6 not supported"),
        };
        let bind_addr: SocketAddr = format!("0.0.0.0:{}", self.events.port()).parse().unwrap();
        let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        socket.set_reuse_address(true)?;
        socket.set_reuse_port(true)?;
        socket.bind(&bind_addr.into())?;
        socket.join_multicast_v4(&multicast_ip, &Ipv4Addr::UNSPECIFIED)?;
        let socket: std::net::UdpSocket = socket.into();
        socket.set_nonblocking(true)?;
        let socket = UdpSocket::from_std(socket)?;

        eprintln!("[client] listening on: {}", self.events);
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
