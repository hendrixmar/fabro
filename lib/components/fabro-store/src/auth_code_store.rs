//! SQLite-backed storage for pending CLI authorizations.
//!
//! The raw authorization code is a one-time bearer credential. It remains at
//! the HTTP boundary and is hashed before every database operation; the
//! stored domain type owns only the approved authorization the code unlocks.

use chrono::{DateTime, Utc};
use fabro_types::IdpIdentity;
use sha2::{Digest as _, Sha256};
use sqlx::sqlite::SqliteRow;
use sqlx::{Row as _, SqlitePool};

use crate::{Result, sqlite_row};

const RECORD_NAME: &str = "pending CLI authorization";

/// Approved identity and OAuth context waiting for a CLI code exchange.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingCliAuthorization {
    pub identity:       IdpIdentity,
    pub login:          String,
    pub name:           String,
    pub email:          String,
    pub avatar_url:     String,
    pub code_challenge: String,
    pub redirect_uri:   String,
    pub expires_at:     DateTime<Utc>,
}

/// Issues, consumes, and expires pending CLI authorizations in SQLite.
pub struct AuthCodeStore {
    pool: SqlitePool,
}

impl std::fmt::Debug for AuthCodeStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthCodeStore").finish_non_exhaustive()
    }
}

impl AuthCodeStore {
    #[must_use]
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Persist a pending authorization under the SHA-256 digest of `code`.
    pub async fn issue(&self, code: &str, pending: &PendingCliAuthorization) -> Result<()> {
        let code_hash = hash_code(code);
        sqlx::query(
            r"
INSERT INTO oauth_authorization_codes (
    code_hash, identity_issuer, identity_subject, login, name, email,
    avatar_url, code_challenge, redirect_uri, expires_at_ms
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
",
        )
        .bind(code_hash.as_slice())
        .bind(pending.identity.issuer())
        .bind(pending.identity.subject())
        .bind(&pending.login)
        .bind(&pending.name)
        .bind(&pending.email)
        .bind(&pending.avatar_url)
        .bind(&pending.code_challenge)
        .bind(&pending.redirect_uri)
        .bind(pending.expires_at.timestamp_millis())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Atomically remove the authorization for `code` and return it if live.
    ///
    /// Expiry is checked after deletion so every exchange attempt burns a
    /// matching code, including an expired one.
    pub async fn consume(
        &self,
        code: &str,
        now: DateTime<Utc>,
    ) -> Result<Option<PendingCliAuthorization>> {
        let code_hash = hash_code(code);
        let row = sqlx::query(
            r"
DELETE FROM oauth_authorization_codes
WHERE code_hash = ?
RETURNING identity_issuer, identity_subject, login, name, email, avatar_url,
          code_challenge, redirect_uri, expires_at_ms
",
        )
        .bind(code_hash.as_slice())
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };
        let pending = pending_from_row(&row)?;
        if pending.expires_at <= now {
            return Ok(None);
        }
        Ok(Some(pending))
    }

    /// Delete authorizations expiring at or before `cutoff`.
    pub async fn gc_expired(&self, cutoff: DateTime<Utc>) -> Result<u64> {
        let result = sqlx::query("DELETE FROM oauth_authorization_codes WHERE expires_at_ms <= ?")
            .bind(cutoff.timestamp_millis())
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }

    /// Close the shared pool to exercise storage-failure paths in consumers.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn test_close(&self) {
        self.pool.close().await;
    }
}

fn hash_code(code: &str) -> [u8; 32] {
    Sha256::digest(code.as_bytes()).into()
}

fn pending_from_row(row: &SqliteRow) -> Result<PendingCliAuthorization> {
    Ok(PendingCliAuthorization {
        identity:       sqlite_row::identity_from_row(row, RECORD_NAME)?,
        login:          row.try_get("login")?,
        name:           row.try_get("name")?,
        email:          row.try_get("email")?,
        avatar_url:     row.try_get("avatar_url")?,
        code_challenge: row.try_get("code_challenge")?,
        redirect_uri:   row.try_get("redirect_uri")?,
        expires_at:     sqlite_row::timestamp_from_row(row, RECORD_NAME, "expires_at_ms")?,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::{Duration, Utc};
    use fabro_types::IdpIdentity;
    use sha2::{Digest as _, Sha256};
    use tokio::fs;
    use tokio::task::JoinSet;

    use super::{AuthCodeStore, PendingCliAuthorization};
    use crate::{Error, test_support};

    fn pending(expires_at: chrono::DateTime<Utc>) -> PendingCliAuthorization {
        PendingCliAuthorization {
            identity: IdpIdentity::new("https://github.com", "12345").unwrap(),
            login: "octocat".to_string(),
            name: "The Octocat".to_string(),
            email: "octocat@example.com".to_string(),
            avatar_url: "https://example.com/octocat.png".to_string(),
            code_challenge: "challenge".to_string(),
            redirect_uri: "http://127.0.0.1:4444/callback".to_string(),
            expires_at,
        }
    }

    fn now() -> chrono::DateTime<Utc> {
        chrono::DateTime::from_timestamp_millis(Utc::now().timestamp_millis()).unwrap()
    }

    #[tokio::test]
    async fn issue_and_consume_round_trips_once() {
        let (_directory, store) = test_support::sqlite_auth_code_store().await;
        let now = now();
        let expected = pending(now + Duration::seconds(60));
        store.issue("one-time-code", &expected).await.unwrap();

        assert_eq!(
            store.consume("one-time-code", now).await.unwrap(),
            Some(expected)
        );
        assert!(store.consume("one-time-code", now).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn concurrent_consume_has_one_winner_across_store_instances() {
        let (_directory, store) = test_support::sqlite_auth_code_store().await;
        let now = now();
        store
            .issue("contended-code", &pending(now + Duration::seconds(60)))
            .await
            .unwrap();
        let stores = [
            Arc::new(AuthCodeStore::new(store.pool.clone())),
            Arc::new(AuthCodeStore::new(store.pool.clone())),
        ];

        let mut tasks = JoinSet::new();
        for index in 0..16 {
            let store = Arc::clone(&stores[index % stores.len()]);
            tasks.spawn(async move {
                store
                    .consume("contended-code", now)
                    .await
                    .unwrap()
                    .is_some()
            });
        }

        let mut winners = 0;
        while let Some(result) = tasks.join_next().await {
            if result.unwrap() {
                winners += 1;
            }
        }
        assert_eq!(winners, 1);
    }

    #[tokio::test]
    async fn expired_consume_deletes_the_row() {
        let (_directory, store) = test_support::sqlite_auth_code_store().await;
        let now = now();
        store
            .issue("expired-code", &pending(now - Duration::seconds(1)))
            .await
            .unwrap();

        assert!(store.consume("expired-code", now).await.unwrap().is_none());
        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM oauth_authorization_codes")
            .fetch_one(&store.pool)
            .await
            .unwrap();
        assert_eq!(rows, 0);
    }

    #[tokio::test]
    async fn gc_removes_only_rows_at_or_before_cutoff() {
        let (_directory, store) = test_support::sqlite_auth_code_store().await;
        let now = now();
        for (code, expiry) in [
            ("before", now - Duration::seconds(1)),
            ("at", now),
            ("after", now + Duration::seconds(1)),
        ] {
            store.issue(code, &pending(expiry)).await.unwrap();
        }

        assert_eq!(store.gc_expired(now).await.unwrap(), 2);
        assert!(store.consume("before", now).await.unwrap().is_none());
        assert!(store.consume("at", now).await.unwrap().is_none());
        assert!(store.consume("after", now).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn survives_reopening_the_sqlite_pool() {
        let (directory, store) = test_support::sqlite_auth_code_store().await;
        let now = now();
        let expected = pending(now + Duration::seconds(60));
        store.issue("durable-code", &expected).await.unwrap();
        store.pool.close().await;

        let database = fabro_db::Database::connect(directory.path().join("fabro.sqlite3"))
            .await
            .unwrap();
        database.migrate().await.unwrap();
        let reopened = AuthCodeStore::new(database.clone_pool());
        assert_eq!(
            reopened.consume("durable-code", now).await.unwrap(),
            Some(expected)
        );
    }

    #[tokio::test]
    async fn duplicate_hash_fails_without_overwriting() {
        let (_directory, store) = test_support::sqlite_auth_code_store().await;
        let now = now();
        let first = pending(now + Duration::seconds(60));
        let mut second = pending(now + Duration::seconds(120));
        second.login = "different-login".to_string();
        store.issue("duplicate-code", &first).await.unwrap();

        assert!(store.issue("duplicate-code", &second).await.is_err());
        assert_eq!(
            store.consume("duplicate-code", now).await.unwrap(),
            Some(first)
        );
    }

    #[tokio::test]
    async fn errors_do_not_expose_sensitive_fields() {
        let (_directory, store) = test_support::sqlite_auth_code_store().await;
        let raw_code = "raw-authorization-code";
        let entry = pending(now() + Duration::seconds(60));
        store.issue(raw_code, &entry).await.unwrap();

        let err = store.issue(raw_code, &entry).await.unwrap_err();
        let rendered = err.to_string();
        let code_hash = hex::encode(super::hash_code(raw_code));
        for sensitive in [
            raw_code,
            code_hash.as_str(),
            entry.code_challenge.as_str(),
            entry.redirect_uri.as_str(),
            entry.login.as_str(),
            entry.name.as_str(),
            entry.email.as_str(),
            entry.avatar_url.as_str(),
        ] {
            assert!(
                !rendered.contains(sensitive),
                "storage error exposed sensitive field {sensitive:?}: {rendered}"
            );
        }
    }

    #[tokio::test]
    async fn persistence_contains_hash_but_not_raw_code() {
        let (directory, store) = test_support::sqlite_auth_code_store().await;
        let raw_code = "raw-authorization-code-that-must-never-be-persisted";
        store
            .issue(raw_code, &pending(Utc::now() + Duration::seconds(60)))
            .await
            .unwrap();

        let persisted_hash: Vec<u8> =
            sqlx::query_scalar("SELECT code_hash FROM oauth_authorization_codes")
                .fetch_one(&store.pool)
                .await
                .unwrap();
        let expected_hash: [u8; 32] = Sha256::digest(raw_code.as_bytes()).into();
        assert_eq!(persisted_hash, expected_hash);
        sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
            .execute(&store.pool)
            .await
            .unwrap();
        store.pool.close().await;
        let bytes = fs::read(directory.path().join("fabro.sqlite3"))
            .await
            .unwrap();
        assert!(
            !bytes
                .windows(raw_code.len())
                .any(|window| window == raw_code.as_bytes())
        );
    }

    #[tokio::test]
    async fn invalid_stored_timestamp_is_typed() {
        let (_directory, store) = test_support::sqlite_auth_code_store().await;
        store
            .issue(
                "invalid-timestamp-code",
                &pending(Utc::now() + Duration::seconds(60)),
            )
            .await
            .unwrap();
        sqlx::query("UPDATE oauth_authorization_codes SET expires_at_ms = ?")
            .bind(i64::MAX)
            .execute(&store.pool)
            .await
            .unwrap();

        let err = store
            .consume("invalid-timestamp-code", Utc::now())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidStoredTimestamp {
            record: "pending CLI authorization",
            field:  "expires_at_ms",
            value:  i64::MAX,
        }));
    }
}
