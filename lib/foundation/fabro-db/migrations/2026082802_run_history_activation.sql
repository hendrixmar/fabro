CREATE TABLE legacy_run_history_activation (
    singleton INTEGER PRIMARY KEY NOT NULL,
    source_fingerprint BLOB NOT NULL,
    source_runs INTEGER NOT NULL,
    source_events INTEGER NOT NULL,
    activated_at_ms INTEGER NOT NULL,

    CHECK (singleton = 1),
    CHECK (length(source_fingerprint) = 32),
    CHECK (source_runs >= 0),
    CHECK (source_events >= 0),
    CHECK (activated_at_ms >= 0)
);

CREATE TABLE legacy_run_history_deletions (
    run_id TEXT PRIMARY KEY NOT NULL,
    deleted_at_ms INTEGER NOT NULL,

    CHECK (deleted_at_ms >= 0)
);
