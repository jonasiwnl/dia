use std::{marker::PhantomData, net::SocketAddr};

use serde::{Serialize, de::DeserializeOwned};
use tokio::net::UdpSocket;

use crate::{
    error::DiaError,
    sequencer::SequencerHeader,
    util::{open_multicast_reader, open_multicast_writer},
};

pub struct Client<Message> {
    sender: ClientSender<Message>,
    receiver: ClientReceiver<Message>,
}

pub struct ClientSender<Message> {
    propose_addr: SocketAddr,
    socket: UdpSocket,
    _message: PhantomData<Message>,
}

pub struct ClientReceiver<Message> {
    consensus_addr: SocketAddr,
    socket: UdpSocket,
    _message: PhantomData<Message>,
}

pub struct SequencedMessage<Message> {
    pub header: SequencerHeader,
    pub payload: Message,
}

impl<Message> Client<Message>
where
    Message: Serialize + DeserializeOwned,
{
    pub async fn bind(
        propose_addr: SocketAddr,
        consensus_addr: SocketAddr,
    ) -> Result<Self, DiaError> {
        let receiver = ClientReceiver {
            consensus_addr,
            socket: open_multicast_reader(consensus_addr)?,
            _message: PhantomData,
        };
        let sender = ClientSender {
            propose_addr,
            socket: open_multicast_writer(propose_addr).await?,
            _message: PhantomData,
        };
        Ok(Self { sender, receiver })
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
    pub async fn send(&self, message: Message) -> Result<(), DiaError> {
        let bytes_sent = self
            .socket
            .send_to(&bincode::serialize(&message)?, self.propose_addr)
            .await?;
        eprintln!(
            "[client] successfully broadcasted {} bytes to multicast topic {}",
            bytes_sent, self.propose_addr
        );
        Ok(())
    }
}

impl<Message> ClientReceiver<Message>
where
    Message: DeserializeOwned,
{
    pub async fn recv(&self) -> Result<SequencedMessage<Message>, DiaError> {
        // TODO: adjust buf length, or maybe a param
        let mut buf = [0u8; 1024];

        match self.socket.recv_from(&mut buf).await {
            Ok((amt, src)) => {
                eprintln!("[client] received {} bytes from {}", amt, src);
                let header_len = std::mem::size_of::<SequencerHeader>();
                if amt < header_len {
                    return Err(DiaError::MalformedPacket {
                        expected: header_len,
                        actual: amt,
                    });
                }

                let (header_bytes, payload) = buf[..amt].split_at(header_len);
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
            handler(self.recv().await?);
        }
    }
}
