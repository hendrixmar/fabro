-- A CLI auth session is a rotation chain: the identity and profile are facts
-- about the chain, not about any single token in it. Keeping them here means a
-- chain has exactly one owner, and `created_at_ms` is the real start of the
-- session rather than the newest token's issue time.
CREATE TABLE auth_sessions (
    id                TEXT PRIMARY KEY NOT NULL,
    identity_issuer   TEXT NOT NULL,
    identity_subject  TEXT NOT NULL,
    login             TEXT NOT NULL,
    name              TEXT NOT NULL,
    email             TEXT NOT NULL,
    avatar_url        TEXT NOT NULL DEFAULT '',
    user_agent        TEXT NOT NULL DEFAULT '',
    created_at_ms     INTEGER NOT NULL,
    last_used_at_ms   INTEGER NOT NULL,
    CHECK (length(id) = 36),
    CHECK (length(identity_issuer) > 0),
    CHECK (length(identity_subject) > 0)
);

CREATE INDEX auth_sessions_by_identity
    ON auth_sessions (identity_issuer, identity_subject, last_used_at_ms DESC);

-- Rotated tokens are retained until they expire so a replayed token is still
-- recognisable as one that existed, rather than indistinguishable from a
-- forgery. `used_at_ms IS NULL` marks the one token that can still be spent.
CREATE TABLE refresh_tokens (
    token_hash    BLOB PRIMARY KEY NOT NULL,
    session_id    TEXT NOT NULL REFERENCES auth_sessions(id) ON DELETE CASCADE,
    issued_at_ms  INTEGER NOT NULL,
    expires_at_ms INTEGER NOT NULL,
    used_at_ms    INTEGER,
    CHECK (length(token_hash) = 32),
    CHECK (expires_at_ms > issued_at_ms)
);

-- Deliberately no ordering CHECK between a session's timestamps and its
-- tokens'. Rotation stamps `now` from the process clock against rows written
-- by an earlier request, so an NTP step backwards would turn a harmless clock
-- anomaly into refresh failing outright for every affected session.

-- Rotation marks the presented token used before issuing its successor, so a
-- chain can only ever hold one live token. Enforcing it here turns an implicit
-- code convention into a constraint, and lets the session listing find the
-- live token by index instead of grouping candidates in memory.
CREATE UNIQUE INDEX refresh_tokens_one_live_per_session
    ON refresh_tokens (session_id) WHERE used_at_ms IS NULL;

CREATE INDEX refresh_tokens_by_expiry ON refresh_tokens (expires_at_ms);
