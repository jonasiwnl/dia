use std::{
    future::Future,
    io,
    net::{Ipv4Addr, SocketAddr},
    pin::Pin,
};

use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;

use crate::error::DiaError;

pub const MAX_DATAGRAM_SIZE: usize = 65_535;

pub type NetworkFuture<'a, T> = Pin<Box<dyn Future<Output = io::Result<T>> + Send + 'a>>;

pub struct ReceivedDatagram {
    pub bytes: Vec<u8>,
    pub src: SocketAddr,
}

/// An outbound datagram endpoint. Implement this in tests to inject faults.
pub trait DatagramSender: Send + Sync {
    fn send(&self, packet: Vec<u8>) -> NetworkFuture<'_, usize>;
}

/// An inbound datagram endpoint. Implement this in tests to inject faults.
pub trait DatagramReceiver: Send + Sync {
    fn recv(&self) -> NetworkFuture<'_, ReceivedDatagram>;
}

pub struct UdpMulticastSender {
    socket: UdpSocket,
    destination: SocketAddr,
}

pub struct UdpMulticastReceiver {
    socket: UdpSocket,
}

impl UdpMulticastSender {
    pub async fn bind(destination: SocketAddr) -> Result<Self, DiaError> {
        match destination {
            SocketAddr::V4(v4) => (*v4.ip(), v4.port()),
            SocketAddr::V6(_) => panic!("IPv6 multicast requires a different setup"),
        };
        let socket = UdpSocket::bind("0.0.0.0:0").await?;
        socket.set_multicast_loop_v4(true)?;
        socket.set_multicast_ttl_v4(1)?;
        Ok(Self {
            socket,
            destination,
        })
    }
}

impl DatagramSender for UdpMulticastSender {
    fn send(&self, packet: Vec<u8>) -> NetworkFuture<'_, usize> {
        Box::pin(async move { self.socket.send_to(&packet, self.destination).await })
    }
}

impl UdpMulticastReceiver {
    pub fn bind(addr: SocketAddr) -> Result<Self, DiaError> {
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
        Ok(Self {
            socket: UdpSocket::from_std(socket)?,
        })
    }
}

impl DatagramReceiver for UdpMulticastReceiver {
    fn recv(&self) -> NetworkFuture<'_, ReceivedDatagram> {
        Box::pin(async move {
            let mut bytes: Vec<u8> = vec![0; MAX_DATAGRAM_SIZE];
            let (len, src) = self.socket.recv_from(&mut bytes).await?;
            bytes.truncate(len);
            Ok(ReceivedDatagram { bytes, src })
        })
    }
}

#[cfg(test)]
pub mod testing {
    use std::{
        collections::HashMap,
        io::{self, ErrorKind},
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::{Arc, Mutex as StdMutex},
    };

    use tokio::sync::{Mutex, mpsc};

    use super::{DatagramReceiver, DatagramSender, NetworkFuture, ReceivedDatagram};

    #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
    pub enum Node {
        Client(u64),
        Sequencer,
    }

    #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
    struct Edge {
        from: Node,
        to: Node,
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum LinkState {
        Up,
        Drop,
        Disconnect(ErrorKind),
        RejectSend(ErrorKind),
    }

    /// A change to one part of the simulated network graph.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub enum NetworkChange {
        FailInbound { node: Node, error_kind: ErrorKind },
        RestoreInbound { node: Node },
        FailOutbound { node: Node, error_kind: ErrorKind },
        RestoreOutbound { node: Node },
        DropLink { from: Node, to: Node },
        RestoreLink { from: Node, to: Node },
    }

    impl NetworkChange {
        pub fn fail_inbound(node: Node, error_kind: ErrorKind) -> Self {
            Self::FailInbound { node, error_kind }
        }

        pub fn restore_inbound(node: Node) -> Self {
            Self::RestoreInbound { node }
        }

        pub fn fail_outbound(node: Node, error_kind: ErrorKind) -> Self {
            Self::FailOutbound { node, error_kind }
        }

        pub fn restore_outbound(node: Node) -> Self {
            Self::RestoreOutbound { node }
        }

        pub fn drop_link(from: Node, to: Node) -> Self {
            Self::DropLink { from, to }
        }

        pub fn restore_link(from: Node, to: Node) -> Self {
            Self::RestoreLink { from, to }
        }
    }

    /// A deterministic graph of logical proposal and consensus links.
    ///
    /// All links start healthy. `apply` records only the requested override,
    /// leaving every other node and link unchanged.
    pub struct SimulatedNetwork {
        inner: Arc<Inner>,
    }

    impl SimulatedNetwork {
        pub fn new() -> Self {
            let (transmission_tx, transmission_rx) = mpsc::unbounded_channel();
            Self {
                inner: Arc::new(Inner {
                    state: StdMutex::new(NetworkState::default()),
                    transmission_tx,
                    transmission_rx: Mutex::new(transmission_rx),
                }),
            }
        }

        pub fn client(&self, id: u64) -> ClientTransport {
            let node = Node::Client(id);
            let receiver = self.register(node);
            ClientTransport {
                proposal_sender: Arc::new(SimulatedSender {
                    inner: Arc::clone(&self.inner),
                    from: node,
                    destination: Destination::Node(Node::Sequencer),
                }),
                consensus_receiver: receiver,
            }
        }

        pub fn sequencer(&self) -> SequencerTransport {
            let receiver = self.register(Node::Sequencer);
            SequencerTransport {
                proposal_receiver: receiver,
                consensus_sender: Arc::new(SimulatedSender {
                    inner: Arc::clone(&self.inner),
                    from: Node::Sequencer,
                    destination: Destination::AllClients,
                }),
            }
        }

        pub fn apply(&self, change: NetworkChange) {
            let wake_receiver = {
                let mut state = self.inner.state.lock().expect("network state poisoned");
                match change {
                    NetworkChange::FailInbound { node, error_kind } => {
                        state
                            .inbound
                            .insert(node, LinkState::Disconnect(error_kind));
                        state
                            .endpoints
                            .get(&node)
                            .cloned()
                            .map(|tx| (tx, error_kind))
                    }
                    NetworkChange::RestoreInbound { node } => {
                        state.inbound.remove(&node);
                        None
                    }
                    NetworkChange::FailOutbound { node, error_kind } => {
                        state
                            .outbound
                            .insert(node, LinkState::RejectSend(error_kind));
                        None
                    }
                    NetworkChange::RestoreOutbound { node } => {
                        state.outbound.remove(&node);
                        None
                    }
                    NetworkChange::DropLink { from, to } => {
                        state.links.insert(Edge { from, to }, LinkState::Drop);
                        None
                    }
                    NetworkChange::RestoreLink { from, to } => {
                        state.links.remove(&Edge { from, to });
                        None
                    }
                }
            };
            if let Some((receiver, error_kind)) = wake_receiver {
                let _ = receiver.send(Err(error_kind));
            }
        }

        pub async fn wait_for_transmission(&self, from: Node, to: Node) {
            let expected = Edge { from, to };
            let actual = self
                .inner
                .transmission_rx
                .lock()
                .await
                .recv()
                .await
                .expect("network stopped recording transmissions");
            assert_eq!(actual, expected, "unexpected network transmission");
        }

        fn register(&self, node: Node) -> Arc<dyn DatagramReceiver> {
            let (tx, rx) = mpsc::unbounded_channel();
            let mut state = self.inner.state.lock().expect("network state poisoned");
            assert!(
                state.endpoints.insert(node, tx).is_none(),
                "node {node:?} is already registered"
            );
            Arc::new(SimulatedReceiver {
                inner: Arc::clone(&self.inner),
                node,
                rx: Mutex::new(rx),
            })
        }
    }

    impl Default for SimulatedNetwork {
        fn default() -> Self {
            Self::new()
        }
    }

    pub struct ClientTransport {
        proposal_sender: Arc<dyn DatagramSender>,
        consensus_receiver: Arc<dyn DatagramReceiver>,
    }

    impl ClientTransport {
        pub fn proposal_sender(&self) -> Arc<dyn DatagramSender> {
            Arc::clone(&self.proposal_sender)
        }

        pub fn consensus_receiver(&self) -> Arc<dyn DatagramReceiver> {
            Arc::clone(&self.consensus_receiver)
        }
    }

    pub struct SequencerTransport {
        proposal_receiver: Arc<dyn DatagramReceiver>,
        consensus_sender: Arc<dyn DatagramSender>,
    }

    impl SequencerTransport {
        pub fn proposal_receiver(&self) -> Arc<dyn DatagramReceiver> {
            Arc::clone(&self.proposal_receiver)
        }

        pub fn consensus_sender(&self) -> Arc<dyn DatagramSender> {
            Arc::clone(&self.consensus_sender)
        }
    }

    struct Inner {
        state: StdMutex<NetworkState>,
        transmission_tx: mpsc::UnboundedSender<Edge>,
        transmission_rx: Mutex<mpsc::UnboundedReceiver<Edge>>,
    }

    #[derive(Default)]
    struct NetworkState {
        endpoints: HashMap<Node, mpsc::UnboundedSender<Result<ReceivedDatagram, ErrorKind>>>,
        inbound: HashMap<Node, LinkState>,
        outbound: HashMap<Node, LinkState>,
        links: HashMap<Edge, LinkState>,
    }

    #[derive(Clone, Copy)]
    enum Destination {
        Node(Node),
        AllClients,
    }

    struct SimulatedSender {
        inner: Arc<Inner>,
        from: Node,
        destination: Destination,
    }

    impl DatagramSender for SimulatedSender {
        fn send(&self, datagram: Vec<u8>) -> NetworkFuture<'_, usize> {
            Box::pin(async move {
                let datagram_len = datagram.len();
                let destinations = match self.destination {
                    Destination::Node(node) => vec![node],
                    Destination::AllClients => {
                        let state = self.inner.state.lock().expect("network state poisoned");
                        state
                            .endpoints
                            .keys()
                            .filter_map(|node| matches!(*node, Node::Client(_)).then_some(*node))
                            .collect()
                    }
                };

                for to in destinations {
                    let edge = Edge {
                        from: self.from,
                        to,
                    };
                    let _ = self.inner.transmission_tx.send(edge);
                    let (link_state, receiver) = {
                        let state = self.inner.state.lock().expect("network state poisoned");
                        (
                            resolve_link_state(&state, edge),
                            state.endpoints.get(&to).cloned(),
                        )
                    };
                    match link_state {
                        LinkState::Up => {
                            if let Some(receiver) = receiver {
                                let _ = receiver.send(Ok(ReceivedDatagram {
                                    bytes: datagram.clone(),
                                    src: simulated_address(self.from),
                                }));
                            }
                        }
                        LinkState::Drop => {}
                        LinkState::Disconnect(error_kind) | LinkState::RejectSend(error_kind) => {
                            return Err(network_error(error_kind));
                        }
                    }
                }
                Ok(datagram_len)
            })
        }
    }

    struct SimulatedReceiver {
        inner: Arc<Inner>,
        node: Node,
        rx: Mutex<mpsc::UnboundedReceiver<Result<ReceivedDatagram, ErrorKind>>>,
    }

    impl DatagramReceiver for SimulatedReceiver {
        fn recv(&self) -> NetworkFuture<'_, ReceivedDatagram> {
            Box::pin(async move {
                let inbound_state = {
                    let state = self.inner.state.lock().expect("network state poisoned");
                    state.inbound.get(&self.node).copied()
                };
                if let Some(LinkState::Disconnect(error_kind)) = inbound_state {
                    return Err(network_error(error_kind));
                }

                match self.rx.lock().await.recv().await {
                    Some(Ok(datagram)) => Ok(datagram),
                    Some(Err(error_kind)) => Err(network_error(error_kind)),
                    None => Err(network_error(ErrorKind::BrokenPipe)),
                }
            })
        }
    }

    fn resolve_link_state(state: &NetworkState, edge: Edge) -> LinkState {
        state
            .links
            .get(&edge)
            .or_else(|| state.outbound.get(&edge.from))
            .or_else(|| state.inbound.get(&edge.to))
            .copied()
            .unwrap_or(LinkState::Up)
    }

    fn network_error(error_kind: ErrorKind) -> io::Error {
        io::Error::new(error_kind, "injected network failure")
    }

    fn simulated_address(node: Node) -> SocketAddr {
        let host = match node {
            Node::Client(id) => id as u8,
            Node::Sequencer => 254,
        };
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, host)), 0)
    }
}
