use std::{marker::PhantomData, net::SocketAddr, sync::Arc};

use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::{Mutex, oneshot};
use uuid::Uuid;

use crate::{
    error::DiaError,
    network::{DatagramReceiver, DatagramSender, UdpMulticastReceiver, UdpMulticastSender},
    types::{SequencedMessage, SequencerHeader},
};

pub struct Client<Message> {
    sender: ClientSender<Message>,
    receiver: ClientReceiver<Message>,
}

struct ClientState {
    pending: Option<(Uuid, oneshot::Sender<()>)>,
}

pub struct ClientSender<Message> {
    propose_addr: SocketAddr,
    send_lock: Mutex<()>,
    network: Arc<dyn DatagramSender>,
    state: Arc<Mutex<ClientState>>,
    _message: PhantomData<Message>,
}

pub struct ClientReceiver<Message> {
    consensus_addr: SocketAddr,
    network: Arc<dyn DatagramReceiver>,
    state: Arc<Mutex<ClientState>>,
    _message: PhantomData<Message>,
}

#[derive(serde::Serialize)]
pub struct IdentifiedMessage<Message> {
    pub msg_id: uuid::Uuid,
    pub payload: Message,
}

impl<Message> Client<Message> {
    pub fn from_transport(
        propose_addr: SocketAddr,
        consensus_addr: SocketAddr,
        proposal_sender: Arc<dyn DatagramSender>,
        consensus_receiver: Arc<dyn DatagramReceiver>,
    ) -> Self {
        let state = Arc::new(Mutex::new(ClientState { pending: None }));
        let receiver = ClientReceiver {
            consensus_addr,
            network: consensus_receiver,
            state: Arc::clone(&state),
            _message: PhantomData,
        };
        let sender = ClientSender {
            propose_addr,
            send_lock: Mutex::new(()),
            network: proposal_sender,
            state,
            _message: PhantomData,
        };
        Self { sender, receiver }
    }
}

impl<Message> Client<Message>
where
    Message: Serialize + DeserializeOwned,
{
    pub async fn bind(
        propose_addr: SocketAddr,
        consensus_addr: SocketAddr,
    ) -> Result<Self, DiaError> {
        Ok(Self::from_transport(
            propose_addr,
            consensus_addr,
            Arc::new(UdpMulticastSender::bind(propose_addr).await?),
            Arc::new(UdpMulticastReceiver::bind(consensus_addr)?),
        ))
    }

    pub async fn bind_split(
        propose_addr: SocketAddr,
        consensus_addr: SocketAddr,
    ) -> Result<(ClientSender<Message>, ClientReceiver<Message>), DiaError> {
        Ok(Self::bind(propose_addr, consensus_addr).await?.split())
    }

    pub fn split(self) -> (ClientSender<Message>, ClientReceiver<Message>) {
        (self.sender, self.receiver)
    }

    pub async fn send(&self, message: Message) -> Result<(), DiaError> {
        self.sender.send(message).await
    }

    pub async fn recv(&self) -> Result<SequencedMessage<Message>, DiaError> {
        self.receiver.recv().await
    }

    pub async fn listen<Handler>(&self, handler: Handler) -> Result<(), DiaError>
    where
        Handler: FnMut(SequencedMessage<Message>),
    {
        self.receiver.listen(handler).await
    }
}

impl<Message> ClientSender<Message>
where
    Message: Serialize,
{
    // Thread safe, blocking send
    pub async fn send(&self, message: Message) -> Result<(), DiaError> {
        let _send_guard = self.send_lock.lock().await;

        let msg_id = Uuid::new_v4();
        let payload = bincode::serialize(&message)?;
        let (completion_tx, completion_rx) = oneshot::channel();
        {
            let mut state = self.state.lock().await;
            debug_assert!(state.pending.is_none());
            state.pending = Some((msg_id, completion_tx));
        }

        let mut proposal = Vec::with_capacity(16 + payload.len());
        proposal.extend_from_slice(msg_id.as_bytes());
        proposal.extend_from_slice(&payload);
        let bytes_sent = match self.network.send(proposal).await {
            Ok(bytes_sent) => bytes_sent,
            Err(err) => {
                self.state.lock().await.pending = None;
                return Err(err.into());
            }
        };
        eprintln!(
            "[client] successfully broadcasted {} bytes to multicast topic {}",
            bytes_sent, self.propose_addr
        );

        completion_rx
            .await
            .map_err(|_| DiaError::ClientReceiverDropped)
    }
}

impl<Message> ClientReceiver<Message>
where
    Message: DeserializeOwned,
{
    pub async fn recv(&self) -> Result<SequencedMessage<Message>, DiaError> {
        match self.network.recv().await {
            Ok(packet) => {
                let amt = packet.bytes.len();
                let src = packet.src;
                eprintln!("[client] received {} bytes from {}", amt, src);
                let header_len = SequencerHeader::encoded_len()?;
                if amt < header_len {
                    return Err(DiaError::MalformedPacket {
                        expected: header_len,
                        actual: amt,
                    });
                }

                let (header_bytes, payload) = packet.bytes.split_at(header_len);
                let header = bincode::deserialize(header_bytes)?;
                let payload = bincode::deserialize(payload)?;
                Ok(SequencedMessage { header, payload })
            }
            Err(e) => {
                eprintln!("[client] recv error: {:?}", e);
                Err(DiaError::Network(e))
            }
        }
    }

    pub async fn listen<Handler>(&self, mut handler: Handler) -> Result<(), DiaError>
    where
        Handler: FnMut(SequencedMessage<Message>),
    {
        eprintln!("[client] listening on: {}", self.consensus_addr);
        loop {
            let msg = match self.recv().await {
                Ok(msg) => msg,
                Err(err) => {
                    // Dropping the sender wakes the local send that is awaiting it.
                    self.state.lock().await.pending = None;
                    return Err(err);
                }
            };
            let completion_tx = {
                let mut state = self.state.lock().await;
                if state
                    .pending
                    .as_ref()
                    .is_some_and(|(msg_id, _)| *msg_id == msg.header.msg_id)
                {
                    state.pending.take().map(|(_, completion_tx)| completion_tx)
                } else {
                    None
                }
            };
            if let Some(completion_tx) = completion_tx {
                let _ = completion_tx.send(());
            }
            handler(msg);
        }
    }
}
