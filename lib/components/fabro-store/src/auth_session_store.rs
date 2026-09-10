//! SQLite-backed storage for CLI auth sessions and their refresh tokens.
//!
//! A session is a rotation chain. The chain owns the identity and profile;
//! each token in it owns only its own lifetime. Splitting them that way is
//! what makes every operation here an indexed query rather than a scan over
//! every token ever issued.

use chrono::{DateTime, Utc};
use fabro_types::IdpIdentity;
use sqlx::sqlite::SqliteRow;
use sqlx::{Row as _, SqlitePool};
use uuid::Uuid;

use crate::{Error, Result, sqlite_row};

const RECORD_NAME: &str = "auth session";

/// A CLI auth session: one rotation chain, owned by one identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthSessionRecord {
    pub id:           Uuid,
    pub identity:     IdpIdentity,
    pub login:        String,
    pub name:         String,
    pub email:        String,
    pub avatar_url:   String,
    pub user_agent:   String,
    pub created_at:   DateTime<Utc>,
    pub last_used_at: DateTime<Utc>,
}

/// Token-specific facts needed to open a session with its first refresh
/// token. The store derives the owning session and unused state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitialRefreshToken {
    pub token_hash: [u8; 32],
    pub issued_at:  DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

/// Complete refresh-token row stored in SQLite.
struct RefreshTokenRecord {
    token_hash: [u8; 32],
    session_id: Uuid,
    issued_at:  DateTime<Utc>,
    expires_at: DateTime<Utc>,
    used_at:    Option<DateTime<Utc>>,
}

/// A session with a spendable token, as returned by the session listing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveCliSession {
    pub session:    AuthSessionRecord,
    /// Expiry of the session's live token.
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RotateOutcome {
    /// The presented token was spent and its successor issued.
    Rotated(AuthSessionRecord),
    /// The presented token had already been rotated away, so its session was
    /// revoked in the same transaction.
    ReplayedAndRevoked(AuthSessionRecord),
    Expired,
    NotFound,
}

pub struct AuthSessionStore {
    pool: SqlitePool,
}

impl std::fmt::Debug for AuthSessionStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthSessionStore").finish_non_exhaustive()
    }
}

const SELECT_SESSION_BY_TOKEN_SQL: &str = r"
SELECT s.id, s.identity_issuer, s.identity_subject, s.login, s.name, s.email,
       s.avatar_url, s.user_agent, s.created_at_ms, s.last_used_at_ms
FROM auth_sessions s
JOIN refresh_tokens t ON t.session_id = s.id
WHERE t.token_hash = ?
";

const SELECT_ACTIVE_SESSIONS_SQL: &str = r"
SELECT s.id, s.identity_issuer, s.identity_subject, s.login, s.name, s.email,
       s.avatar_url, s.user_agent, s.created_at_ms, s.last_used_at_ms,
       t.expires_at_ms
FROM auth_sessions s
JOIN refresh_tokens t ON t.session_id = s.id AND t.used_at_ms IS NULL
WHERE s.identity_issuer = ? AND s.identity_subject = ? AND t.expires_at_ms > ?
ORDER BY s.last_used_at_ms DESC
";

const SELECT_SESSION_BY_ID_SQL: &str = r"
SELECT s.id, s.identity_issuer, s.identity_subject, s.login, s.name, s.email,
       s.avatar_url, s.user_agent, s.created_at_ms, s.last_used_at_ms
FROM auth_sessions s
WHERE s.id = ?
";

impl AuthSessionStore {
    #[must_use]
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Open a new session with its first refresh token.
    pub async fn create_session(
        &self,
        session: &AuthSessionRecord,
        token: &InitialRefreshToken,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            r"
INSERT INTO auth_sessions (
    id, identity_issuer, identity_subject, login, name, email, avatar_url,
    user_agent, created_at_ms, last_used_at_ms
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
",
        )
        .bind(session.id.to_string())
        .bind(session.identity.issuer())
        .bind(session.identity.subject())
        .bind(&session.login)
        .bind(&session.name)
        .bind(&session.email)
        .bind(&session.avatar_url)
        .bind(&session.user_agent)
        .bind(session.created_at.timestamp_millis())
        .bind(session.last_used_at.timestamp_millis())
        .execute(&mut *tx)
        .await?;
        insert_token(&mut tx, &RefreshTokenRecord {
            token_hash: token.token_hash,
            session_id: session.id,
            issued_at:  token.issued_at,
            expires_at: token.expires_at,
            used_at:    None,
        })
        .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Look up the session a token belongs to, spent or not.
    pub async fn find_session_by_token_hash(
        &self,
        token_hash: &[u8; 32],
    ) -> Result<Option<AuthSessionRecord>> {
        sqlx::query(SELECT_SESSION_BY_TOKEN_SQL)
            .bind(token_hash.as_slice())
            .fetch_optional(&self.pool)
            .await?
            .as_ref()
            .map(session_from_row)
            .transpose()
    }

    /// Sessions belonging to `identity` that still hold a spendable token.
    ///
    /// The partial unique index guarantees at most one live token per
    /// session, so this joins one row per session rather than grouping
    /// candidates.
    pub async fn active_cli_sessions(
        &self,
        identity: &IdpIdentity,
        now: DateTime<Utc>,
    ) -> Result<Vec<ActiveCliSession>> {
        sqlx::query(SELECT_ACTIVE_SESSIONS_SQL)
            .bind(identity.issuer())
            .bind(identity.subject())
            .bind(now.timestamp_millis())
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(|row| {
                let expires_at =
                    sqlite_row::timestamp_from_row(&row, RECORD_NAME, "expires_at_ms")?;
                Ok(ActiveCliSession {
                    session: session_from_row(&row)?,
                    expires_at,
                })
            })
            .collect()
    }

    /// Spend `presented_hash` and issue `new_token_hash` in its place.
    ///
    /// The claiming UPDATE is the transaction's first statement, so SQLite
    /// takes the write lock before anything is read. A concurrent caller
    /// blocks on it, then observes `used_at_ms` already set and revokes the
    /// session before returning [`RotateOutcome::ReplayedAndRevoked`], with no
    /// application mutex involved.
    pub async fn rotate(
        &self,
        presented_hash: &[u8; 32],
        new_token_hash: &[u8; 32],
        new_expires_at: DateTime<Utc>,
        user_agent: &str,
        now: DateTime<Utc>,
    ) -> Result<RotateOutcome> {
        let now_ms = now.timestamp_millis();
        let mut tx = self.pool.begin().await?;

        let claimed: Option<String> = sqlx::query_scalar(
            r"
UPDATE refresh_tokens SET used_at_ms = ?
WHERE token_hash = ? AND used_at_ms IS NULL AND expires_at_ms > ?
RETURNING session_id
",
        )
        .bind(now_ms)
        .bind(presented_hash.as_slice())
        .bind(now_ms)
        .fetch_optional(&mut *tx)
        .await?;

        let Some(session_id) = claimed else {
            // Cold path: the claim failed, so read once more to say why.
            let existing: Option<(Option<i64>, i64)> = sqlx::query_as(
                "SELECT used_at_ms, expires_at_ms FROM refresh_tokens WHERE token_hash = ?",
            )
            .bind(presented_hash.as_slice())
            .fetch_optional(&mut *tx)
            .await?;
            // Expiry is checked before reuse so an expired token reports as
            // expired even if it had already been rotated away, matching the
            // ordering callers rely on: only a live replay revokes the chain.
            let outcome = match existing {
                None => RotateOutcome::NotFound,
                Some((used_at_ms, expires_at_ms)) => {
                    if expires_at_ms <= now_ms {
                        RotateOutcome::Expired
                    } else if used_at_ms.is_some() {
                        let session = load_session(&mut tx, presented_hash).await?;
                        if let Some(session) = session {
                            sqlx::query("DELETE FROM auth_sessions WHERE id = ?")
                                .bind(session.id.to_string())
                                .execute(&mut *tx)
                                .await?;
                            RotateOutcome::ReplayedAndRevoked(session)
                        } else {
                            RotateOutcome::NotFound
                        }
                    } else {
                        // Unreachable: a live, unexpired token would have been
                        // claimed by the UPDATE above, in this transaction.
                        RotateOutcome::NotFound
                    }
                }
            };
            tx.commit().await?;
            return Ok(outcome);
        };

        let session_id = parse_uuid(&session_id)?;
        insert_token(&mut tx, &RefreshTokenRecord {
            token_hash: *new_token_hash,
            session_id,
            issued_at: now,
            expires_at: new_expires_at,
            used_at: None,
        })
        .await?;
        sqlx::query("UPDATE auth_sessions SET last_used_at_ms = ?, user_agent = ? WHERE id = ?")
            .bind(now_ms)
            .bind(user_agent)
            .bind(session_id.to_string())
            .execute(&mut *tx)
            .await?;

        let row = sqlx::query(SELECT_SESSION_BY_ID_SQL)
            .bind(session_id.to_string())
            .fetch_one(&mut *tx)
            .await?;
        let session = session_from_row(&row)?;
        tx.commit().await?;
        Ok(RotateOutcome::Rotated(session))
    }

    /// Revoke a session outright. Its tokens go with it via `ON DELETE
    /// CASCADE`.
    pub async fn delete_session(&self, session_id: Uuid) -> Result<()> {
        sqlx::query("DELETE FROM auth_sessions WHERE id = ?")
            .bind(session_id.to_string())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Revoke a session on behalf of its owner, but only while it is still
    /// usable. Returns the number of sessions deleted (0 or 1), so a caller
    /// can distinguish "revoked" from "no such live session".
    pub async fn delete_active_session_for_identity(
        &self,
        identity: &IdpIdentity,
        session_id: Uuid,
        now: DateTime<Utc>,
    ) -> Result<u64> {
        let deleted = sqlx::query(
            r"
DELETE FROM auth_sessions
WHERE id = ? AND identity_issuer = ? AND identity_subject = ?
  AND EXISTS (
      SELECT 1 FROM refresh_tokens t
      WHERE t.session_id = auth_sessions.id
        AND t.used_at_ms IS NULL
        AND t.expires_at_ms > ?
  )
",
        )
        .bind(session_id.to_string())
        .bind(identity.issuer())
        .bind(identity.subject())
        .bind(now.timestamp_millis())
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(deleted)
    }

    /// Drop expired tokens, then any session left without one. Returns the
    /// number of tokens removed.
    pub async fn gc_expired(&self, cutoff: DateTime<Utc>) -> Result<u64> {
        let mut tx = self.pool.begin().await?;
        let tokens = sqlx::query("DELETE FROM refresh_tokens WHERE expires_at_ms <= ?")
            .bind(cutoff.timestamp_millis())
            .execute(&mut *tx)
            .await?
            .rows_affected();
        sqlx::query(
            r"
DELETE FROM auth_sessions
WHERE NOT EXISTS (SELECT 1 FROM refresh_tokens t WHERE t.session_id = auth_sessions.id)
",
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(tokens)
    }
}

async fn insert_token(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    token: &RefreshTokenRecord,
) -> Result<()> {
    sqlx::query(
        r"
INSERT INTO refresh_tokens (token_hash, session_id, issued_at_ms, expires_at_ms, used_at_ms)
VALUES (?, ?, ?, ?, ?)
",
    )
    .bind(token.token_hash.as_slice())
    .bind(token.session_id.to_string())
    .bind(token.issued_at.timestamp_millis())
    .bind(token.expires_at.timestamp_millis())
    .bind(token.used_at.map(|used_at| used_at.timestamp_millis()))
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn load_session(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    token_hash: &[u8; 32],
) -> Result<Option<AuthSessionRecord>> {
    sqlx::query(SELECT_SESSION_BY_TOKEN_SQL)
        .bind(token_hash.as_slice())
        .fetch_optional(&mut **tx)
        .await?
        .as_ref()
        .map(session_from_row)
        .transpose()
}

fn session_from_row(row: &SqliteRow) -> Result<AuthSessionRecord> {
    Ok(AuthSessionRecord {
        id:           parse_uuid(&row.try_get::<String, _>("id")?)?,
        identity:     sqlite_row::identity_from_row(row, RECORD_NAME)?,
        login:        row.try_get("login")?,
        name:         row.try_get("name")?,
        email:        row.try_get("email")?,
        avatar_url:   row.try_get("avatar_url")?,
        user_agent:   row.try_get("user_agent")?,
        created_at:   sqlite_row::timestamp_from_row(row, RECORD_NAME, "created_at_ms")?,
        last_used_at: sqlite_row::timestamp_from_row(row, RECORD_NAME, "last_used_at_ms")?,
    })
}

fn parse_uuid(value: &str) -> Result<Uuid> {
    Uuid::parse_str(value)
        .map_err(|err| Error::Other(format!("stored auth session has an invalid id: {err}")))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::{Duration, Utc};
    use fabro_types::IdpIdentity;
    use tokio::task::JoinSet;
    use uuid::Uuid;

    use super::{AuthSessionRecord, AuthSessionStore, InitialRefreshToken, RotateOutcome};
    use crate::test_support::sqlite_auth_session_store;

    fn identity(subject: &str) -> IdpIdentity {
        IdpIdentity::new("https://github.com", subject).unwrap()
    }

    fn session(id: Uuid, subject: &str) -> AuthSessionRecord {
        let now = Utc::now();
        AuthSessionRecord {
            id,
            identity: identity(subject),
            login: "octocat".to_string(),
            name: "The Octocat".to_string(),
            email: "octocat@example.com".to_string(),
            avatar_url: "https://example.com/octocat.png".to_string(),
            user_agent: "fabro-cli/0.3".to_string(),
            created_at: now,
            last_used_at: now,
        }
    }

    /// Tokens are issued an hour back so that a fixture with a negative
    /// `expires_in` is still a coherent row: issued in the past, expired since.
    fn token(hash: [u8; 32], expires_in: Duration) -> InitialRefreshToken {
        let now = Utc::now();
        InitialRefreshToken {
            token_hash: hash,
            issued_at:  now - Duration::hours(1),
            expires_at: now + expires_in,
        }
    }

    async fn open_session(
        store: &AuthSessionStore,
        subject: &str,
        hash: [u8; 32],
        expires_in: Duration,
    ) -> Uuid {
        let id = Uuid::new_v4();
        store
            .create_session(&session(id, subject), &token(hash, expires_in))
            .await
            .unwrap();
        id
    }

    #[tokio::test]
    async fn create_session_round_trips_through_its_token() {
        let (_dir, store) = sqlite_auth_session_store().await;
        let id = open_session(&store, "12345", [1_u8; 32], Duration::days(30)).await;

        let found = store
            .find_session_by_token_hash(&[1_u8; 32])
            .await
            .unwrap()
            .expect("session should be found by its token");
        assert_eq!(found.id, id);
        assert_eq!(found.identity, identity("12345"));
        assert_eq!(found.login, "octocat");
        assert_eq!(found.avatar_url, "https://example.com/octocat.png");

        let (token_session_id, used_at_ms): (String, Option<i64>) = sqlx::query_as(
            "SELECT session_id, used_at_ms FROM refresh_tokens WHERE token_hash = ?",
        )
        .bind([1_u8; 32].as_slice())
        .fetch_one(&store.pool)
        .await
        .unwrap();
        assert_eq!(token_session_id, id.to_string());
        assert_eq!(used_at_ms, None);

        assert!(
            store
                .find_session_by_token_hash(&[9_u8; 32])
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn rotate_spends_the_presented_token_and_issues_its_successor() {
        let (_dir, store) = sqlite_auth_session_store().await;
        let id = open_session(&store, "12345", [1_u8; 32], Duration::days(30)).await;
        let now = Utc::now() + Duration::seconds(1);

        let outcome = store
            .rotate(
                &[1_u8; 32],
                &[2_u8; 32],
                now + Duration::days(30),
                "fabro-cli/0.4",
                now,
            )
            .await
            .unwrap();
        let RotateOutcome::Rotated(rotated) = outcome else {
            panic!("expected rotation, got {outcome:?}");
        };
        assert_eq!(rotated.id, id);
        // The rotation refreshes the session's user agent and last-used time,
        // while created_at stays the true start of the chain.
        assert_eq!(rotated.user_agent, "fabro-cli/0.4");
        assert_eq!(
            rotated.last_used_at.timestamp_millis(),
            now.timestamp_millis()
        );
        assert!(rotated.created_at < rotated.last_used_at);

        // Both tokens still resolve to the session; only the new one is live.
        assert!(
            store
                .find_session_by_token_hash(&[1_u8; 32])
                .await
                .unwrap()
                .is_some()
        );
        let active = store
            .active_cli_sessions(&identity("12345"), now)
            .await
            .unwrap();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].session.id, id);
    }

    #[tokio::test]
    async fn rotate_revokes_session_when_a_spent_token_is_replayed() {
        let (_dir, store) = sqlite_auth_session_store().await;
        let id = open_session(&store, "12345", [1_u8; 32], Duration::days(30)).await;
        let now = Utc::now();

        store
            .rotate(
                &[1_u8; 32],
                &[2_u8; 32],
                now + Duration::days(30),
                "ua",
                now,
            )
            .await
            .unwrap();
        let replay = store
            .rotate(
                &[1_u8; 32],
                &[3_u8; 32],
                now + Duration::days(30),
                "ua",
                now,
            )
            .await
            .unwrap();

        let RotateOutcome::ReplayedAndRevoked(session) = replay else {
            panic!("expected replay revocation, got {replay:?}");
        };
        assert_eq!(session.id, id);
        for hash in [[1_u8; 32], [2_u8; 32]] {
            assert!(
                store
                    .find_session_by_token_hash(&hash)
                    .await
                    .unwrap()
                    .is_none(),
                "replay revocation should delete every token in the session"
            );
        }
    }

    #[tokio::test]
    async fn rotate_returns_error_when_replay_revocation_fails() {
        use std::error::Error as _;

        let (_dir, store) = sqlite_auth_session_store().await;
        open_session(&store, "12345", [1_u8; 32], Duration::days(30)).await;
        let now = Utc::now();

        store
            .rotate(
                &[1_u8; 32],
                &[2_u8; 32],
                now + Duration::days(30),
                "ua",
                now,
            )
            .await
            .unwrap();
        sqlx::query(
            r"
CREATE TRIGGER reject_auth_session_delete
BEFORE DELETE ON auth_sessions
BEGIN
    SELECT RAISE(ABORT, 'auth session delete rejected');
END
",
        )
        .execute(&store.pool)
        .await
        .unwrap();

        let error = store
            .rotate(
                &[1_u8; 32],
                &[3_u8; 32],
                now + Duration::days(30),
                "ua",
                now,
            )
            .await
            .expect_err("failed replay revocation should return an error");
        assert!(matches!(error, crate::Error::Sqlite(_)));
        assert!(error.source().is_some());
        for hash in [[1_u8; 32], [2_u8; 32]] {
            assert!(
                store
                    .find_session_by_token_hash(&hash)
                    .await
                    .unwrap()
                    .is_some(),
                "failed replay revocation should leave the session intact"
            );
        }
    }

    #[tokio::test]
    async fn rotate_reports_expiry_before_reuse() {
        let (_dir, store) = sqlite_auth_session_store().await;
        let now = Utc::now();
        open_session(&store, "12345", [1_u8; 32], Duration::seconds(30)).await;

        // An unknown token is simply absent.
        assert_eq!(
            store
                .rotate(
                    &[9_u8; 32],
                    &[8_u8; 32],
                    now + Duration::days(30),
                    "ua",
                    now
                )
                .await
                .unwrap(),
            RotateOutcome::NotFound
        );

        // Live but past its expiry.
        let later = now + Duration::seconds(60);
        assert_eq!(
            store
                .rotate(
                    &[1_u8; 32],
                    &[2_u8; 32],
                    later + Duration::days(30),
                    "ua",
                    later
                )
                .await
                .unwrap(),
            RotateOutcome::Expired
        );

        // Spend it while still valid, then let it expire: expiry wins over
        // reuse, so a stale retry does not read as a live replay.
        store
            .rotate(
                &[1_u8; 32],
                &[2_u8; 32],
                now + Duration::seconds(30),
                "ua",
                now,
            )
            .await
            .unwrap();
        assert_eq!(
            store
                .rotate(
                    &[1_u8; 32],
                    &[4_u8; 32],
                    later + Duration::days(30),
                    "ua",
                    later
                )
                .await
                .unwrap(),
            RotateOutcome::Expired
        );
    }

    #[tokio::test]
    async fn active_cli_sessions_lists_only_live_sessions_owned_by_the_identity() {
        let (_dir, store) = sqlite_auth_session_store().await;
        let now = Utc::now();
        let live = open_session(&store, "12345", [1_u8; 32], Duration::days(30)).await;
        let expired = open_session(&store, "12345", [2_u8; 32], Duration::seconds(-1)).await;
        let other = open_session(&store, "67890", [3_u8; 32], Duration::days(30)).await;

        let active = store
            .active_cli_sessions(&identity("12345"), now)
            .await
            .unwrap();
        let ids: Vec<Uuid> = active.iter().map(|entry| entry.session.id).collect();
        assert_eq!(ids, vec![live], "expired={expired}, other-identity={other}");
        assert!(active[0].expires_at > now);

        // Once the successor token expires in turn, the session drops off the
        // list: the spent predecessor does not keep it alive.
        store
            .rotate(
                &[1_u8; 32],
                &[4_u8; 32],
                now + Duration::seconds(30),
                "ua",
                now,
            )
            .await
            .unwrap();
        assert!(
            store
                .active_cli_sessions(&identity("12345"), now + Duration::seconds(60))
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn delete_session_removes_its_tokens() {
        let (_dir, store) = sqlite_auth_session_store().await;
        let now = Utc::now();
        let id = open_session(&store, "12345", [1_u8; 32], Duration::days(30)).await;
        store
            .rotate(
                &[1_u8; 32],
                &[2_u8; 32],
                now + Duration::days(30),
                "ua",
                now,
            )
            .await
            .unwrap();

        store.delete_session(id).await.unwrap();

        for hash in [[1_u8; 32], [2_u8; 32]] {
            assert!(
                store
                    .find_session_by_token_hash(&hash)
                    .await
                    .unwrap()
                    .is_none(),
                "cascade should remove every token in the chain"
            );
        }
    }

    #[tokio::test]
    async fn delete_active_session_for_identity_requires_a_live_owned_session() {
        let (_dir, store) = sqlite_auth_session_store().await;
        let now = Utc::now();
        let owned = open_session(&store, "12345", [1_u8; 32], Duration::days(30)).await;
        let expired = open_session(&store, "12345", [2_u8; 32], Duration::seconds(-1)).await;

        // Another identity cannot revoke it.
        assert_eq!(
            store
                .delete_active_session_for_identity(&identity("67890"), owned, now)
                .await
                .unwrap(),
            0
        );
        // Neither can the owner, once nothing in it is spendable.
        assert_eq!(
            store
                .delete_active_session_for_identity(&identity("12345"), expired, now)
                .await
                .unwrap(),
            0
        );
        // An unknown session is a no-op rather than an error.
        assert_eq!(
            store
                .delete_active_session_for_identity(&identity("12345"), Uuid::new_v4(), now)
                .await
                .unwrap(),
            0
        );

        assert_eq!(
            store
                .delete_active_session_for_identity(&identity("12345"), owned, now)
                .await
                .unwrap(),
            1
        );
        assert!(
            store
                .find_session_by_token_hash(&[1_u8; 32])
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .find_session_by_token_hash(&[2_u8; 32])
                .await
                .unwrap()
                .is_some(),
            "revoking one session must not touch another"
        );
    }

    #[tokio::test]
    async fn gc_expired_drops_expired_tokens_and_the_sessions_left_empty() {
        let (_dir, store) = sqlite_auth_session_store().await;
        let now = Utc::now();
        let live = open_session(&store, "12345", [1_u8; 32], Duration::days(30)).await;
        open_session(&store, "12345", [2_u8; 32], Duration::seconds(-1)).await;

        assert_eq!(store.gc_expired(now).await.unwrap(), 1);
        assert!(
            store
                .find_session_by_token_hash(&[2_u8; 32])
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store
                .find_session_by_token_hash(&[1_u8; 32])
                .await
                .unwrap()
                .map(|session| session.id),
            Some(live)
        );
        // Idempotent: a second sweep finds nothing left to remove.
        assert_eq!(store.gc_expired(now).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn concurrent_rotation_lets_exactly_one_caller_win() {
        let (_dir, store) = sqlite_auth_session_store().await;
        let store = Arc::new(store);
        let now = Utc::now();
        open_session(&store, "12345", [0_u8; 32], Duration::days(30)).await;

        let mut tasks = JoinSet::new();
        for index in 1..=8_u8 {
            let store = Arc::clone(&store);
            tasks.spawn(async move {
                store
                    .rotate(
                        &[0_u8; 32],
                        &[index; 32],
                        now + Duration::days(30),
                        "ua",
                        now,
                    )
                    .await
                    .unwrap()
            });
        }

        let mut rotated = 0;
        let mut replayed_and_revoked = 0;
        let mut not_found = 0;
        while let Some(outcome) = tasks.join_next().await {
            match outcome.unwrap() {
                RotateOutcome::Rotated(_) => rotated += 1,
                RotateOutcome::ReplayedAndRevoked(_) => replayed_and_revoked += 1,
                RotateOutcome::NotFound => not_found += 1,
                other @ RotateOutcome::Expired => panic!("unexpected outcome {other:?}"),
            }
        }
        // SQLite's write lock serialises the claiming UPDATE, so the losers
        // cannot race past replay detection. The first loser revokes the
        // session, and later callers then find no token rows.
        assert_eq!(rotated, 1);
        assert_eq!(replayed_and_revoked, 1);
        assert_eq!(not_found, 6);
    }
}
