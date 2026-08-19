/// Errors raised by host-galaxy association.
#[derive(thiserror::Error, Debug)]
pub enum HostError {
    #[error("no candidates found within search radius")]
    NoCandidates,

    #[error("invalid galaxy shape parameters: {0}")]
    InvalidShape(String),
}
