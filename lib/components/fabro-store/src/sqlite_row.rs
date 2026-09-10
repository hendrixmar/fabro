//! Shared decoding helpers for columns the SQLite-backed stores have in
//! common. `record` names the stored domain type (e.g. "auth session") so
//! corruption errors say which table failed without repeating the schema.

use chrono::{DateTime, Utc};
use fabro_types::IdpIdentity;
use sqlx::Row as _;
use sqlx::sqlite::SqliteRow;

use crate::{Error, Result};

pub(crate) fn identity_from_row(row: &SqliteRow, record: &'static str) -> Result<IdpIdentity> {
    IdpIdentity::new(
        row.try_get::<String, _>("identity_issuer")?,
        row.try_get::<String, _>("identity_subject")?,
    )
    .map_err(|source| Error::InvalidStoredIdentity { record, source })
}

pub(crate) fn timestamp_from_row(
    row: &SqliteRow,
    record: &'static str,
    field: &'static str,
) -> Result<DateTime<Utc>> {
    let value: i64 = row.try_get(field)?;
    DateTime::from_timestamp_millis(value).ok_or(Error::InvalidStoredTimestamp {
        record,
        field,
        value,
    })
}
