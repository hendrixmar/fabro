//! Pre-activation backup and integrity helpers shared by the SQLite
//! activation bridges.
//!
//! Every activation snapshots the live database to a private, integrity
//! checked backup file before importing legacy data, and re-validates any
//! backup it finds on a later start. This module owns that mechanism so the
//! blob and run-history bridges cannot drift apart.

use std::path::{Path, PathBuf};

use futures_util::TryStreamExt as _;
use sqlx::Connection as _;
use sqlx::sqlite::{SqliteConnectOptions, SqliteConnection};
use tokio::fs;
use tokio::task::{JoinError, spawn_blocking};
use tracing::debug;

const STAGING_SUFFIX: &str = ".tmp";

#[derive(Debug, thiserror::Error)]
pub(crate) enum BackupError {
    #[error("reading activation backup metadata at {path}")]
    Metadata {
        path:   PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("activation backup is not a regular file at {path}")]
    NotRegular { path: PathBuf },
    #[error("activation backup permissions are not private at {path}")]
    NotPrivate { path: PathBuf },
    #[error("opening or checking activation backup integrity at {path}")]
    Integrity {
        path:   PathBuf,
        #[source]
        source: sqlx::Error,
    },
    #[error("activation backup integrity check did not return exactly one ok result at {path}")]
    IntegrityFailed { path: PathBuf },
    #[error("staging the pre-activation SQLite backup")]
    Stage(#[source] fabro_db::SnapshotStagingError),
    #[error("joining the activation backup publication task")]
    JoinPublication(#[source] JoinError),
    #[error("publishing the activation backup at {path} without overwriting")]
    Publish {
        path:   PathBuf,
        #[source]
        source: std::io::Error,
    },
}

pub(crate) async fn backup_exists(path: &Path) -> Result<bool, BackupError> {
    match fs::metadata(path).await {
        Ok(_) => Ok(true),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(BackupError::Metadata {
            path: path.to_path_buf(),
            source,
        }),
    }
}

/// Snapshots `pool` to `backup_path` without overwriting an existing file.
///
/// The snapshot is staged beside the target, validated, and then published
/// with an atomic no-clobber rename. A backup that another process published
/// concurrently is validated in place instead.
pub(crate) async fn create_backup(
    pool: &sqlx::SqlitePool,
    backup_path: &Path,
) -> Result<(), BackupError> {
    let staging_path = fabro_db::append_to_path(backup_path, STAGING_SUFFIX);
    fabro_db::write_snapshot_to_staging(pool, &staging_path)
        .await
        .map_err(BackupError::Stage)?;
    validate_backup(&staging_path).await?;

    let publish_staging = staging_path.clone();
    let publish_backup = backup_path.to_path_buf();
    let already_exists = spawn_blocking(move || {
        let staging = tempfile::TempPath::from_path(publish_staging);
        match staging.persist_noclobber(&publish_backup) {
            Ok(()) => {
                // Make the rename's directory entry durable: the retained
                // backup is the documented rollback artifact, so it must not
                // vanish in a crash after the import has already committed.
                fabro_db::sync_parent_directory(&publish_backup)?;
                Ok(false)
            }
            Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => Ok(true),
            Err(error) => Err(error.error),
        }
    })
    .await
    .map_err(BackupError::JoinPublication)?
    .map_err(|source| BackupError::Publish {
        path: backup_path.to_path_buf(),
        source,
    })?;

    // The staging copy was validated just before the atomic rename, so only a
    // concurrently published file still needs its own validation.
    if already_exists {
        debug!(
            backup_path = %backup_path.display(),
            "Reusing concurrently published SQLite activation backup"
        );
        validate_backup(backup_path).await?;
    }
    Ok(())
}

/// Requires `path` to be a private regular file holding a SQLite database
/// whose `PRAGMA integrity_check` passes.
pub(crate) async fn validate_backup(path: &Path) -> Result<(), BackupError> {
    let metadata = fs::symlink_metadata(path)
        .await
        .map_err(|source| BackupError::Metadata {
            path: path.to_path_buf(),
            source,
        })?;
    if !metadata.is_file() {
        return Err(BackupError::NotRegular {
            path: path.to_path_buf(),
        });
    }
    validate_private_permissions(path, &metadata)?;

    let options = SqliteConnectOptions::new()
        .filename(path)
        .read_only(true)
        .immutable(true)
        .create_if_missing(false);
    let mut connection = SqliteConnection::connect_with(&options)
        .await
        .map_err(|source| BackupError::Integrity {
            path: path.to_path_buf(),
            source,
        })?;
    let ok = integrity_check_is_ok(&mut connection)
        .await
        .map_err(|source| BackupError::Integrity {
            path: path.to_path_buf(),
            source,
        })?;
    if !ok {
        return Err(BackupError::IntegrityFailed {
            path: path.to_path_buf(),
        });
    }
    Ok(())
}

/// Returns whether `PRAGMA integrity_check` reports exactly one `ok` row.
pub(crate) async fn integrity_check_is_ok<'a, E>(executor: E) -> Result<bool, sqlx::Error>
where
    E: sqlx::Executor<'a, Database = sqlx::Sqlite>,
{
    let mut rows = sqlx::query_scalar::<_, String>("PRAGMA integrity_check").fetch(executor);
    let first = rows.try_next().await?;
    let second = rows.try_next().await?;
    Ok(first.as_deref() == Some("ok") && second.is_none())
}

#[cfg(unix)]
fn validate_private_permissions(
    path: &Path,
    metadata: &std::fs::Metadata,
) -> Result<(), BackupError> {
    use std::os::unix::fs::PermissionsExt as _;

    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(BackupError::NotPrivate {
            path: path.to_path_buf(),
        });
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_private_permissions(
    _path: &Path,
    _metadata: &std::fs::Metadata,
) -> Result<(), BackupError> {
    Ok(())
}
