use fabro_types::BlobHash;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("SlateDB error: {0}")]
    Slate(#[from] slatedb::Error),
    #[error("Object store error: {0}")]
    ObjectStore(#[from] object_store::Error),
    #[error("Serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("SQLite error: {0}")]
    Sqlite(#[from] sqlx::Error),
    #[error("stored blob {blob_hash} has bytes that conflict with its hash")]
    BlobHashConflict { blob_hash: BlobHash },
    #[error("stored blob data does not match requested hash {blob_hash}")]
    BlobIntegrity { blob_hash: BlobHash },
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Invalid event payload: {0}")]
    InvalidEvent(String),
    #[error("event rejected by run projection: {source}")]
    EventRejected {
        #[source]
        source: Box<Self>,
    },
    #[error("Run not found: {0}")]
    RunNotFound(String),
    #[error("Session not found: {0}")]
    SessionNotFound(String),
    #[error("Session already exists: {0}")]
    SessionAlreadyExists(String),
    #[error("run store is read-only")]
    ReadOnly,
    #[error("event sequence limit of {max_seq} reached")]
    EventSequenceExhausted { max_seq: u32 },
    #[error("invalid key segment: {segment:?}")]
    InvalidKeySegment { segment: String },
    #[error("failed to parse key: {0}")]
    KeyParse(String),
    #[error("stored run summary {run_id} has inconsistent field {field}")]
    RunSummaryMismatch {
        run_id: String,
        field:  &'static str,
    },
    #[error(transparent)]
    InvalidTransition(#[from] fabro_types::InvalidTransition),
    #[error("{0}")]
    Other(String),
}
