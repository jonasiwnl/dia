use std::net::SocketAddr;
use tokio::net::UdpSocket;

pub struct Sequencer {
    inbound: SocketAddr,
    outbound: SocketAddr,
}

impl Sequencer {
    pub fn new(inbound: SocketAddr, outbound: SocketAddr) -> Self {
        Self{ inbound, outbound }
    }

    pub async fn start(&self) -> std::io::Result<()> {
        let socket = UdpSocket::bind(self.inbound).await?;
        eprintln!("Constantly listening synchronously on a background thread: {}", self.inbound);
        let mut buf = [0u8; 1024];

        loop {
            match socket.recv_from(&mut buf).await {
                Ok((amt, src)) => {
                    let payload = String::from_utf8_lossy(&buf[..amt]);
                    eprintln!("Received {} bytes from {}: {}", amt, src, payload);
                }
                Err(e) => {
                    eprintln!("Async read error: {:?}", e);
                }
            }
        }
    }
}
