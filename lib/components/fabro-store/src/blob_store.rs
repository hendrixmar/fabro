#[cfg(test)]
use std::sync::Arc;

use bytes::Bytes;
use fabro_types::BlobHash;
use sqlx::SqlitePool;

#[cfg(test)]
use crate::record::Repository;
#[cfg(test)]
use crate::record::{RawBytesCodec, Record};
use crate::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Blob(pub Bytes);

impl AsRef<[u8]> for Blob {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl From<Bytes> for Blob {
    fn from(value: Bytes) -> Self {
        Self(value)
    }
}

#[cfg(test)]
impl Record for Blob {
    type Id = BlobHash;
    type Codec = RawBytesCodec;

    const PREFIX: &'static str = "blobs/sha256";

    #[cfg(test)]
    fn id(&self) -> Self::Id {
        BlobHash::new(&self.0)
    }
}

/// Temporary backend split for compatibility tests.
///
/// Production compiles only the SQLite arm. The Slate arm remains test-only
/// while the startup import bridge is supported, so tests can exercise the
/// old-source boundary directly. Delete the Slate arm (and this enum) with the
/// separately authorized compatibility cleanup, then inline SQLite into
/// [`BlobStore`].
enum BlobBackend {
    #[cfg(test)]
    Slate(Repository<Blob>),
    Sqlite(SqlitePool),
}

pub struct BlobStore {
    backend: BlobBackend,
}

impl std::fmt::Debug for BlobStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let backend = match &self.backend {
            #[cfg(test)]
            BlobBackend::Slate(_) => "slate",
            BlobBackend::Sqlite(_) => "sqlite",
        };
        f.debug_struct("BlobStore")
            .field("backend", &backend)
            .finish_non_exhaustive()
    }
}

impl BlobStore {
    /// Creates a blob store backed by a SQLite pool whose migrations have run.
    #[must_use]
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            backend: BlobBackend::Sqlite(pool),
        }
    }

    #[cfg(test)]
    pub(crate) fn from_slate(db: Arc<slatedb::Db>) -> Self {
        Self {
            backend: BlobBackend::Slate(Repository::new(db)),
        }
    }

    pub async fn write(&self, bytes: &[u8]) -> Result<BlobHash> {
        match &self.backend {
            #[cfg(test)]
            BlobBackend::Slate(repo) => {
                let blob = Blob(Bytes::copy_from_slice(bytes));
                let id = blob.id();
                repo.put(&blob).await?;
                Ok(id)
            }
            BlobBackend::Sqlite(pool) => {
                let blob_hash = BlobHash::new(bytes);
                let result = sqlx::query(
                    "INSERT INTO blobs (hash, data) VALUES (?, ?) \
                     ON CONFLICT(hash) DO NOTHING",
                )
                .bind(blob_hash.to_string())
                .bind(bytes)
                .execute(pool)
                .await?;

                if result.rows_affected() == 1 {
                    return Ok(blob_hash);
                }

                let stored: Vec<u8> = sqlx::query_scalar("SELECT data FROM blobs WHERE hash = ?")
                    .bind(blob_hash.to_string())
                    .fetch_one(pool)
                    .await?;
                if stored == bytes {
                    Ok(blob_hash)
                } else {
                    Err(Error::BlobHashConflict { blob_hash })
                }
            }
        }
    }

    pub async fn read(&self, blob_hash: &BlobHash) -> Result<Option<Bytes>> {
        match &self.backend {
            #[cfg(test)]
            BlobBackend::Slate(repo) => Ok(repo.get(blob_hash).await?.map(|blob| blob.0)),
            BlobBackend::Sqlite(pool) => {
                let stored: Option<Vec<u8>> =
                    sqlx::query_scalar("SELECT data FROM blobs WHERE hash = ?")
                        .bind(blob_hash.to_string())
                        .fetch_optional(pool)
                        .await?;
                let Some(stored) = stored else {
                    return Ok(None);
                };
                if BlobHash::new(&stored) != *blob_hash {
                    return Err(Error::BlobIntegrity {
                        blob_hash: *blob_hash,
                    });
                }
                Ok(Some(Bytes::from(stored)))
            }
        }
    }

    pub async fn exists(&self, blob_hash: &BlobHash) -> Result<bool> {
        match &self.backend {
            #[cfg(test)]
            BlobBackend::Slate(repo) => repo.exists(blob_hash).await,
            BlobBackend::Sqlite(pool) => {
                let exists: bool =
                    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM blobs WHERE hash = ?)")
                        .bind(blob_hash.to_string())
                        .fetch_one(pool)
                        .await?;
                Ok(exists)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use fabro_types::BlobHash;
    use object_store::memory::InMemory;

    use super::BlobStore;
    use crate::Error;
    use crate::keys::SlateKey;

    type TestResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;

    async fn slate_store() -> Arc<BlobStore> {
        let raw_db = Arc::new(
            slatedb::Db::open("blob-store-tests", Arc::new(InMemory::new()))
                .await
                .unwrap(),
        );
        Arc::new(BlobStore::from_slate(raw_db))
    }

    async fn raw_slate_store(name: &str) -> (Arc<slatedb::Db>, BlobStore) {
        let raw_db = Arc::new(
            slatedb::Db::open(name, Arc::new(InMemory::new()))
                .await
                .unwrap(),
        );
        let store = BlobStore::from_slate(raw_db.clone());
        (raw_db, store)
    }

    async fn sqlite_store() -> TestResult<(tempfile::TempDir, fabro_db::Database, BlobStore)> {
        let dir = tempfile::tempdir()?;
        let database = fabro_db::Database::connect(dir.path().join("fabro.sqlite3")).await?;
        database.migrate().await?;
        let store = BlobStore::new(database.clone_pool());
        Ok((dir, database, store))
    }

    #[tokio::test]
    async fn slate_writes_reads_and_checks_existence() {
        let store = slate_store().await;
        let bytes = b"hello world";
        let id = store.write(bytes).await.unwrap();

        assert_eq!(
            store.read(&id).await.unwrap(),
            Some(Bytes::from_static(bytes))
        );
        assert_eq!(store.write(bytes).await.unwrap(), id);
        assert!(store.exists(&id).await.unwrap());
        assert!(!store.exists(&BlobHash::new(b"missing")).await.unwrap());
    }

    #[tokio::test]
    async fn slate_empty_blobs_round_trip() {
        let store = slate_store().await;
        let id = store.write(b"").await.unwrap();

        assert_eq!(store.read(&id).await.unwrap(), Some(Bytes::new()));
    }

    #[tokio::test]
    async fn raw_slate_db_reads_exact_blob_bytes() {
        let (raw_db, store) = raw_slate_store("blob-store-tests").await;
        let bytes = b"{\"ok\":true}";
        let id = store.write(bytes).await.unwrap();

        let saved = raw_db
            .get(SlateKey::new("blobs").with("sha256").with(id))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(saved.as_ref(), bytes);
    }

    #[tokio::test]
    async fn sqlite_writes_reads_and_checks_existence() -> TestResult<()> {
        let (_dir, database, store) = sqlite_store().await?;
        let store = Arc::new(store);

        let binary = [0_u8, 0xff, 0x80, b'a'];
        let (first_write, concurrent_write) =
            tokio::join!(store.write(&binary), store.write(&binary));
        let binary_hash = first_write?;
        assert_eq!(concurrent_write?, binary_hash);
        let empty_hash = store.write(b"").await?;

        assert_eq!(store.write(&binary).await?, binary_hash);
        assert_eq!(
            store.read(&binary_hash).await?,
            Some(Bytes::copy_from_slice(&binary))
        );
        assert_eq!(store.read(&empty_hash).await?, Some(Bytes::new()));
        assert!(store.exists(&binary_hash).await?);
        let missing_hash = BlobHash::new(b"missing");
        assert_eq!(store.read(&missing_hash).await?, None);
        assert!(!store.exists(&missing_hash).await?);

        let row_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM blobs")
            .fetch_one(database.pool())
            .await?;
        assert_eq!(row_count, 2);
        Ok(())
    }

    #[tokio::test]
    async fn sqlite_write_rejects_conflicting_stored_bytes() -> TestResult<()> {
        let (_dir, database, store) = sqlite_store().await?;
        let expected = b"expected";
        let blob_hash = BlobHash::new(expected);
        sqlx::query("INSERT INTO blobs (hash, data) VALUES (?, ?)")
            .bind(blob_hash.to_string())
            .bind(b"different".as_slice())
            .execute(database.pool())
            .await?;

        let error = store
            .write(expected)
            .await
            .expect_err("conflicting bytes should fail");
        assert!(
            matches!(error, Error::BlobHashConflict { blob_hash: value } if value == blob_hash)
        );
        Ok(())
    }

    #[tokio::test]
    async fn sqlite_read_rejects_bytes_that_do_not_match_hash() -> TestResult<()> {
        let (_dir, database, store) = sqlite_store().await?;
        let blob_hash = BlobHash::new(b"expected");
        sqlx::query("INSERT INTO blobs (hash, data) VALUES (?, ?)")
            .bind(blob_hash.to_string())
            .bind(b"different".as_slice())
            .execute(database.pool())
            .await?;

        let error = store
            .read(&blob_hash)
            .await
            .expect_err("mismatched stored bytes should fail");
        assert!(matches!(error, Error::BlobIntegrity { blob_hash: value } if value == blob_hash));
        Ok(())
    }
}
