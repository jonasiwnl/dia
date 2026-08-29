use std::{marker::PhantomData, net::SocketAddr, sync::Arc};

use redb::Database;
use serde::de::DeserializeOwned;

use crate::{
    db::store_message,
    error::DiaError,
    network::{DatagramReceiver, UdpMulticastReceiver, recv_consensus_message},
    types::SequencedMessage,
};

pub struct RepairService<Message> {
    db_client: Database,
    consensus_addr: SocketAddr,
    network: Arc<dyn DatagramReceiver>,
    _message: PhantomData<Message>,
}

impl<Message> RepairService<Message>
// TODO: should only be for listen/recv
where
    Message: DeserializeOwned,
{
    pub fn from_transport(
        db_client: Database,
        consensus_addr: SocketAddr,
        consensus_receiver: Arc<dyn DatagramReceiver>,
    ) -> Self {
        Self {
            db_client,
            consensus_addr,
            network: consensus_receiver,
            _message: PhantomData,
        }
    }

    pub fn bind(db_client: Database, consensus_addr: SocketAddr) -> Result<Self, DiaError> {
        Ok(Self::from_transport(
            db_client,
            consensus_addr,
            Arc::new(UdpMulticastReceiver::bind(consensus_addr)?),
        ))
    }

    pub async fn recv(&self) -> Result<SequencedMessage<Message>, DiaError> {
        recv_consensus_message(self.network.as_ref()).await
    }

    pub async fn listen(&self) -> Result<(), DiaError> {
        eprintln!("[repair] listening on: {}", self.consensus_addr);
        loop {
            let msg = self.recv().await?;
            store_message(&self.db_client, msg.header.seq_num, &[])?;
        }
    }
}
