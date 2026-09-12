-- The durable conversation of an Ask Fabro session: the coding agent's
-- session record as JSON, keyed by session id. Written after every turn and
-- read back to resume the session on its recorded model.
CREATE TABLE run_session_records (
    session_id TEXT PRIMARY KEY NOT NULL,
    run_id TEXT NOT NULL,
    record_json TEXT NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    CHECK (json_valid(record_json))
);

CREATE INDEX run_session_records_by_run
ON run_session_records(run_id);
