use std::{net::SocketAddr, time::{SystemTime, UNIX_EPOCH}};

use serde::{Serialize, Deserialize};

use crate::{error::DiaError, util::{open_multicast_reader, open_multicast_writer}};

pub struct Sequencer {
    propose_addr: SocketAddr,
    consensus_addr: SocketAddr,
}

#[derive(Serialize, Deserialize)]
pub struct SequencerHeader {
    seq_id: u64,
    timestamp_ns: u64,
}

impl Sequencer {
    pub fn new(propose_addr: SocketAddr, consensus_addr: SocketAddr) -> Self {
        Self{ propose_addr, consensus_addr }
    }

    pub async fn start(&self) -> Result<(), DiaError> {
        let reader_socket = open_multicast_reader(self.propose_addr)?;
        let writer_socket = open_multicast_writer(self.consensus_addr).await?;

        eprintln!("[sequencer] listening on: {}", self.propose_addr);
        let mut buf = [0u8; 1024];
        let mut seq_id = 0;

        loop {
            match reader_socket.recv_from(&mut buf).await {
                Ok((amt, src)) => {
                    let payload = String::from_utf8_lossy(&buf[..amt]);
                    eprintln!("[sequencer] received {} bytes from {}: {}", amt, src, payload);

                    let header = SequencerHeader{ seq_id, timestamp_ns: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64 };
                    seq_id += 1;
                    let header_bytes = bincode::serialize(&header)?;
                    let mut stamped = header_bytes;
                    stamped.extend_from_slice(&buf[..amt]);

                    let bytes_sent = writer_socket.send_to(&stamped, self.consensus_addr).await?;
                    eprintln!("[sequencer] successfully broadcasted {} bytes to multicast topic {}", bytes_sent, self.consensus_addr);
                }
                Err(e) => {
                    eprintln!("[sequencer] socket read error: {:?}", e);
                }
            }
        }
    }
}
