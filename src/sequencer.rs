use std::{net::{Ipv4Addr, SocketAddr}, time::{SystemTime, UNIX_EPOCH}};

use serde::{Serialize, Deserialize};
use tokio::net::UdpSocket;
use socket2::{Domain, Protocol, Socket, Type};

use crate::error::DiaError;

pub struct Sequencer {
    inbound: SocketAddr,
    outbound: SocketAddr,
}

#[derive(Serialize, Deserialize)]
struct SequencerHeader {
    seq_id: u64,
    timestamp_ns: u64,
}

impl Sequencer {
    pub fn new(inbound: SocketAddr, outbound: SocketAddr) -> Self {
        Self{ inbound, outbound }
    }

    pub async fn start(&self) -> Result<(), DiaError> {
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

        match self.outbound {
            SocketAddr::V4(v4) => (*v4.ip(), v4.port()),
            SocketAddr::V6(_) => panic!("[sequencer] IPv6 multicast requires a different setup"),
        };
        let out_socket = UdpSocket::bind("0.0.0.0:0").await?;
        out_socket.set_multicast_loop_v4(true)?;
        out_socket.set_multicast_ttl_v4(1)?;

        eprintln!("[sequencer] listening on: {}", self.inbound);
        let mut buf = [0u8; 1024];
        let mut seq_id = 0;

        loop {
            match socket.recv_from(&mut buf).await {
                Ok((amt, src)) => {
                    let payload = String::from_utf8_lossy(&buf[..amt]);
                    eprintln!("[sequencer] received {} bytes from {}: {}", amt, src, payload);

                    let header = SequencerHeader{ seq_id, timestamp_ns: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64 };
                    seq_id += 1;
                    let header_bytes = bincode::serialize(&header)?;
                    let mut stamped = header_bytes;
                    stamped.extend_from_slice(&buf[..amt]);

                    let bytes_sent = out_socket.send_to(&stamped, self.outbound).await?;
                    eprintln!("[sequencer] successfully broadcasted {} bytes to multicast topic {}", bytes_sent, self.outbound);
                }
                Err(e) => {
                    eprintln!("[sequencer] socket read error: {:?}", e);
                }
            }
        }
    }
}
