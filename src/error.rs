use thiserror::Error;

#[derive(Error, Debug)]
pub enum DiaError {
    #[error("Failed to parse incoming network packet: {0}")]
    Serialization(#[from] bincode::Error),

    #[error("I/O failure: {0}")]
    Network(#[from] std::io::Error),
}
