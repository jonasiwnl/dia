use std::{
    net::SocketAddr,
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use tokio::{net::UdpSocket, sync::mpsc};
use uuid::Uuid;

use crate::{
    error::DiaError,
    util::{open_multicast_reader, open_multicast_writer},
};

pub struct Sequencer {
    propose_addr: SocketAddr,
    consensus_addr: SocketAddr,
    reader_socket: UdpSocket,
    writer_socket: UdpSocket,
}

struct ReceivedPacket {
    buf: Vec<u8>,
    payload_len: usize,
    src: SocketAddr,
}

#[derive(Serialize, Deserialize)]
pub struct SequencerHeader {
    pub msg_id: Uuid,
    pub seq_num: u64,
    pub timestamp_ns: u64,
}

impl Sequencer {
    pub async fn bind(
        propose_addr: SocketAddr,
        consensus_addr: SocketAddr,
    ) -> Result<Self, DiaError> {
        let reader_socket = open_multicast_reader(propose_addr)?;
        let writer_socket = open_multicast_writer(consensus_addr).await?;
        Ok(Self {
            propose_addr,
            consensus_addr,
            reader_socket,
            writer_socket,
        })
    }

    async fn sink(
        consensus_addr: SocketAddr,
        writer_socket: UdpSocket,
        mut rx: mpsc::Receiver<ReceivedPacket>,
        free_buf_tx: mpsc::Sender<Vec<u8>>,
    ) -> Result<(), DiaError> {
        let mut seq_num = 0;
        let header_len = std::mem::size_of::<SequencerHeader>();

        while let Some(mut packet) = rx.recv().await {
            // TODO: this should be debug only code
            let payload = String::from_utf8_lossy(&packet.buf[header_len..][..packet.payload_len]);
            eprintln!(
                "[sequencer] received {} bytes from {}: {}",
                packet.payload_len, packet.src, payload
            );

            let mut id_buf = [0u8; 16];
            id_buf.copy_from_slice(&packet.buf[..16]);
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
            packet.buf[..header_len].copy_from_slice(&header_bytes);

            // Multicast the stamped packet back
            let stamped_len = header_len + packet.payload_len;
            let bytes_sent = writer_socket
                .send_to(&packet.buf[..stamped_len], consensus_addr)
                .await?;
            eprintln!(
                "[sequencer] successfully broadcasted {} bytes to multicast topic {}",
                bytes_sent, consensus_addr
            );

            let _ = free_buf_tx.try_send(packet.buf);
        }

        Ok(())
    }

    pub async fn run(self) -> Result<(), DiaError> {
        let Sequencer {
            propose_addr,
            consensus_addr,
            reader_socket,
            writer_socket,
        } = self;
        eprintln!("[sequencer] listening on: {}", propose_addr);
        // TODO / PARAM
        let payload_capacity = 1024;
        let header_len = std::mem::size_of::<SequencerHeader>();
        let buffer_len = header_len + payload_capacity;
        let mut buf = vec![0u8; buffer_len];
        // TODO / PARAM: q size
        let (sink_tx, sink_rx) = mpsc::channel::<ReceivedPacket>(50);
        let (free_buf_tx, mut free_buf_rx) = mpsc::channel::<Vec<u8>>(50);

        tokio::spawn(async move {
            if let Err(e) = Self::sink(consensus_addr, writer_socket, sink_rx, free_buf_tx).await {
                eprintln!("[sequencer] sink error: {:?}", e);
            }
        });

        loop {
            match reader_socket.recv_from(&mut buf[header_len..]).await {
                Ok((amt, src)) => {
                    let packet = ReceivedPacket {
                        buf,
                        payload_len: amt,
                        src,
                    };
                    if sink_tx.send(packet).await.is_err() {
                        eprintln!("[sequencer] sink closed");
                        break;
                    }

                    buf = free_buf_rx
                        .try_recv()
                        .unwrap_or_else(|_| vec![0u8; buffer_len]);
                }
                Err(e) => {
                    eprintln!("[sequencer] socket read error: {:?}", e);
                }
            }
        }

        Ok(())
    }
}
