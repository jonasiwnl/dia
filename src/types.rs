use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::DiaError;

pub struct SequencedMessage<Message> {
    pub header: SequencerHeader,
    pub payload: Message,
}

#[derive(Serialize, Deserialize)]
pub struct SequencerHeader {
    pub msg_id: Uuid,
    pub seq_num: u64,
    pub timestamp_ns: u64,
}

impl SequencerHeader {
    pub fn encoded_len() -> Result<usize, DiaError> {
        Ok(bincode::serialized_size(&Self {
            msg_id: Uuid::nil(),
            seq_num: 0,
            timestamp_ns: 0,
        })? as usize)
    }
}
