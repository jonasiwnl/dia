use std::net::{Ipv4Addr, SocketAddr};

use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;

use crate::error::DiaError;

pub fn open_multicast_reader(addr: SocketAddr) -> Result<UdpSocket, DiaError> {
    let multicast_ip = match addr {
        SocketAddr::V4(v4) => *v4.ip(),
        SocketAddr::V6(_) => panic!("IPv6 not supported"),
    };
    let bind_addr: SocketAddr = format!("0.0.0.0:{}", addr.port()).parse().unwrap();
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_address(true)?;
    socket.set_reuse_port(true)?;
    socket.bind(&bind_addr.into())?;
    socket.join_multicast_v4(&multicast_ip, &Ipv4Addr::UNSPECIFIED)?;
    let socket: std::net::UdpSocket = socket.into();
    socket.set_nonblocking(true)?;
    return Ok(UdpSocket::from_std(socket)?);
}

pub async fn open_multicast_writer(addr: SocketAddr) -> Result<UdpSocket, DiaError> {
    match addr {
        SocketAddr::V4(v4) => (*v4.ip(), v4.port()),
        SocketAddr::V6(_) => panic!("[sequencer] IPv6 multicast requires a different setup"),
    };
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    socket.set_multicast_loop_v4(true)?;
    socket.set_multicast_ttl_v4(1)?;
    return Ok(socket);
}
