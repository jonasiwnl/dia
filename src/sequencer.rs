use std::{
    net::SocketAddr,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use tokio::sync::mpsc;
use uuid::Uuid;

use crate::{
    error::DiaError,
    network::{DatagramReceiver, DatagramSender, UdpMulticastReceiver, UdpMulticastSender},
    types::SequencerHeader,
};

pub struct Sequencer {
    propose_addr: SocketAddr,
    consensus_addr: SocketAddr,
    reader: Arc<dyn DatagramReceiver>,
    writer: Arc<dyn DatagramSender>,
}

struct ReceivedPacket {
    bytes: Vec<u8>,
    src: SocketAddr,
}

const MESSAGE_ID_LEN: usize = 16;

impl Sequencer {
    pub fn from_transport(
        propose_addr: SocketAddr,
        consensus_addr: SocketAddr,
        proposal_receiver: Arc<dyn DatagramReceiver>,
        consensus_sender: Arc<dyn DatagramSender>,
    ) -> Self {
        Self {
            propose_addr,
            consensus_addr,
            reader: proposal_receiver,
            writer: consensus_sender,
        }
    }
}

impl Sequencer {
    pub async fn bind(
        propose_addr: SocketAddr,
        consensus_addr: SocketAddr,
    ) -> Result<Self, DiaError> {
        Ok(Self::from_transport(
            propose_addr,
            consensus_addr,
            Arc::new(UdpMulticastReceiver::bind(propose_addr)?),
            Arc::new(UdpMulticastSender::bind(consensus_addr).await?),
        ))
    }

    async fn sink(
        consensus_addr: SocketAddr,
        writer: Arc<dyn DatagramSender>,
        mut rx: mpsc::Receiver<ReceivedPacket>,
    ) -> Result<(), DiaError> {
        let mut seq_num = 0;
        let header_len = SequencerHeader::encoded_len()?;

        while let Some(packet) = rx.recv().await {
            if packet.bytes.len() < MESSAGE_ID_LEN {
                return Err(DiaError::MalformedPacket {
                    expected: MESSAGE_ID_LEN,
                    actual: packet.bytes.len(),
                });
            }

            // TODO: this should be debug only code
            let payload = String::from_utf8_lossy(&packet.bytes[MESSAGE_ID_LEN..]);
            eprintln!(
                "[sequencer] received {} bytes from {}: {}",
                packet.bytes.len(),
                packet.src,
                payload
            );

            let mut id_buf = [0u8; 16];
            id_buf.copy_from_slice(&packet.bytes[..MESSAGE_ID_LEN]);
            let msg_id = Uuid::from_bytes(id_buf);

            // Stamp the packet with a sequence number and timestamp
            let header = SequencerHeader {
                msg_id,
                seq_num,
                timestamp_ns: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64,
            };
            seq_num += 1;
            let header_bytes = bincode::serialize(&header)?;
            debug_assert_eq!(header_bytes.len(), header_len);
            let mut stamped = Vec::with_capacity(header_len + packet.bytes.len() - MESSAGE_ID_LEN);
            stamped.extend_from_slice(&header_bytes);
            stamped.extend_from_slice(&packet.bytes[MESSAGE_ID_LEN..]);

            // Multicast the stamped packet back
            let bytes_sent = writer.send(stamped).await?;
            eprintln!(
                "[sequencer] successfully broadcasted {} bytes to multicast topic {}",
                bytes_sent, consensus_addr
            );
        }

        Ok(())
    }

    pub async fn run(self) -> Result<(), DiaError> {
        let Sequencer {
            propose_addr,
            consensus_addr,
            reader,
            writer,
        } = self;
        eprintln!("[sequencer] listening on: {}", propose_addr);
        // TODO / PARAM: q size
        let (sink_tx, sink_rx) = mpsc::channel::<ReceivedPacket>(50);

        tokio::spawn(async move {
            if let Err(e) = Self::sink(consensus_addr, writer, sink_rx).await {
                eprintln!("[sequencer] sink error: {:?}", e);
            }
        });

        loop {
            match reader.recv().await {
                Ok(packet) => {
                    let packet = ReceivedPacket {
                        bytes: packet.bytes,
                        src: packet.src,
                    };
                    if sink_tx.send(packet).await.is_err() {
                        eprintln!("[sequencer] sink closed");
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("[sequencer] socket read error: {:?}", e);
                }
            }
        }

        Ok(())
    }
}
