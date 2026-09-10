CREATE TABLE run_events (
    run_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    event_name TEXT NOT NULL,
    node_id TEXT,
    stage_id TEXT,
    session_id TEXT,
    event_json TEXT NOT NULL,
    PRIMARY KEY (run_id, seq),
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE,
    CHECK (seq BETWEEN 1 AND 999999),
    CHECK (json_valid(event_json))
);

CREATE INDEX run_events_by_stage
ON run_events(run_id, stage_id, seq)
WHERE stage_id IS NOT NULL;

CREATE INDEX run_events_by_legacy_node
ON run_events(run_id, node_id, seq)
WHERE stage_id IS NULL AND node_id IS NOT NULL;

CREATE INDEX run_events_by_session
ON run_events(run_id, session_id, seq)
WHERE session_id IS NOT NULL
  AND event_name GLOB 'run.session.*';

CREATE INDEX run_events_by_pull_request_creation_request
ON run_events(run_id, seq)
WHERE event_name = 'pull_request.creation_requested';
