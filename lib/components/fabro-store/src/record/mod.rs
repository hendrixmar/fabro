//! Internal typed key/value helpers for simple SlateDB-backed records.
//!
//! The split of responsibility is:
//! - [`Record`]: declares the key prefix, id type, and codec for one persisted
//!   type.
//! - [`RecordId`]: converts the typed id to and from key segments.
//! - [`Repository`]: performs the generic get/put/delete/scan operations.
//!
//! Production stores no longer read or write SlateDB records; this module is
//! compiled only for tests that model the retired Slate layout and goes away
//! with the remaining compatibility bridges.

mod codec;
mod record_id;
mod repository;

#[cfg(test)]
pub(crate) use codec::JsonCodec;
pub(crate) use codec::{Codec, MarkerCodec, RawBytesCodec};
pub(crate) use repository::Repository;

use crate::Result;

pub(crate) trait Record: Sized + Send + Sync + 'static {
    type Id: RecordId;
    type Codec: Codec<Self>;

    const PREFIX: &'static str;

    #[cfg(test)]
    fn id(&self) -> Self::Id;
}

pub(crate) trait RecordId: Sized {
    fn key_segments(&self) -> Vec<String>;

    fn from_key_segments(segs: &[&str]) -> Result<Self>;
}
