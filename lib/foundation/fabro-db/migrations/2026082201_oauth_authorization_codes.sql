-- A pending CLI authorization is short-lived application state. Its raw
-- bearer code stays at the HTTP boundary; SQLite receives only its SHA-256
-- digest so a database disclosure cannot reveal an exchangeable code.
CREATE TABLE oauth_authorization_codes (
    code_hash          BLOB PRIMARY KEY NOT NULL,
    identity_issuer    TEXT NOT NULL,
    identity_subject   TEXT NOT NULL,
    login              TEXT NOT NULL,
    name               TEXT NOT NULL,
    email              TEXT NOT NULL,
    avatar_url         TEXT NOT NULL DEFAULT '',
    code_challenge     TEXT NOT NULL,
    redirect_uri       TEXT NOT NULL,
    expires_at_ms      INTEGER NOT NULL,

    CHECK (length(code_hash) = 32),
    CHECK (length(identity_issuer) > 0),
    CHECK (length(identity_subject) > 0)
);

CREATE INDEX oauth_authorization_codes_by_expiry
    ON oauth_authorization_codes (expires_at_ms);
