use std::net::{Ipv4Addr, SocketAddr};

use tokio::net::UdpSocket;
use socket2::{Domain, Protocol, Socket, Type};

pub struct Sequencer {
    inbound: SocketAddr,
    outbound: SocketAddr,
}

impl Sequencer {
    pub fn new(inbound: SocketAddr, outbound: SocketAddr) -> Self {
        Self{ inbound, outbound }
    }

    pub async fn start(&self) -> std::io::Result<()> {
        let multicast_ip = match self.inbound {
            SocketAddr::V4(v4) => *v4.ip(),
            SocketAddr::V6(_) => panic!("IPv6 not supported"),
        };
        let bind_addr: SocketAddr = format!("0.0.0.0:{}", self.inbound.port()).parse().unwrap();
        let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        socket.set_reuse_address(true)?;
        socket.bind(&bind_addr.into())?;
        socket.join_multicast_v4(&multicast_ip, &Ipv4Addr::UNSPECIFIED)?;
        let socket: std::net::UdpSocket = socket.into();
        socket.set_nonblocking(true)?;
        let socket = UdpSocket::from_std(socket)?;

        eprintln!("[sequencer] listening on: {}", self.inbound);
        let mut buf = [0u8; 1024];

        loop {
            match socket.recv_from(&mut buf).await {
                Ok((amt, src)) => {
                    let payload = String::from_utf8_lossy(&buf[..amt]);
                    eprintln!("[sequencer] received {} bytes from {}: {}", amt, src, payload);
                }
                Err(e) => {
                    eprintln!("[sequencer] socket read error: {:?}", e);
                }
            }
        }
    }
}
