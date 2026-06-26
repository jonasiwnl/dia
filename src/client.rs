use std::net::SocketAddr;
use tokio::net::UdpSocket;

pub trait ProcessMessage {
    type Message;
    fn create_message(&self, data: String) -> Self::Message;
    fn message_handler(&self, message: Self::Message);
}

pub struct Client<MessageProcessor> {
    outbound: SocketAddr,
    pub processor: MessageProcessor,
}

impl<MessageProcessor: ProcessMessage> Client<MessageProcessor> {
    pub fn new(outbound: SocketAddr, processor: MessageProcessor) -> Self {
        Self {
            outbound,
            processor,
        }
    }

    // Sends a message to the sequencer, blocks until it receives a response
    pub async fn write_message(&self, message: <MessageProcessor as ProcessMessage>::Message) -> std::io::Result<()> {
         let socket = UdpSocket::bind("0.0.0.0:0").await?;

        // 2. Configure Multicast Time-To-Live (TTL)
        // 1 = Local subnet only (prevents packets from leaving your local network)
        // If you are using a virtual network or Docker cluster, you might need to increase this.
        let (multicast_ip, _) = match self.outbound {
            SocketAddr::V4(v4) => (*v4.ip(), v4.port()),
            SocketAddr::V6(_) => panic!("IPv6 multicast requires a different setup"),
        };
        
        // Set the TTL so the packet routes correctly as multicast
        socket.set_multicast_ttl_v4(1)?;

        // 3. Send the raw bytes straight to the multicast SocketAddr
        let bytes_sent = socket.send_to(message.as_bytes(), self.outbound).await?;
        eprintln!("Successfully broadcasted {} bytes to multicast topic {}", bytes_sent, self.outbound);

        self.processor.message_handler(message);

        Ok(())
    }
}
