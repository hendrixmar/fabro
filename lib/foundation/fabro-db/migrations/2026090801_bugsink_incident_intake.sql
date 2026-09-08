CREATE TABLE bugsink_deliveries (
    origin TEXT NOT NULL CHECK(length(origin) > 0),
    project_id INTEGER NOT NULL CHECK(typeof(project_id) = 'integer' AND project_id >= 0),
    body_digest TEXT NOT NULL CHECK(length(body_digest) = 64 AND body_digest NOT GLOB '*[^0-9a-f]*'),
    issue_id TEXT NOT NULL,
    reason TEXT NOT NULL CHECK(reason IN ('NEW','REGRESSED','UNMUTED','TEST')),
    received_ms INTEGER NOT NULL,
    PRIMARY KEY(origin, project_id, body_digest)
);

CREATE TABLE bugsink_incidents (
    incident_key TEXT PRIMARY KEY NOT NULL,
    origin TEXT NOT NULL CHECK(length(origin) > 0),
    project_id INTEGER NOT NULL CHECK(typeof(project_id) = 'integer' AND project_id >= 0),
    issue_id TEXT NOT NULL,
    observation_key TEXT,
    observed_event TEXT,
    is_resolved INTEGER CHECK(is_resolved IN (0, 1)),
    is_muted INTEGER CHECK(is_muted IN (0, 1)),
    episode INTEGER NOT NULL DEFAULT 1 CHECK(episode >= 1),
    requested_generation INTEGER NOT NULL DEFAULT 0 CHECK(typeof(requested_generation) = 'integer' AND requested_generation >= 0),
    applied_generation INTEGER NOT NULL DEFAULT 0 CHECK(applied_generation >= 0 AND applied_generation <= requested_generation),
    refresh_generation INTEGER CHECK(refresh_generation > applied_generation AND refresh_generation <= requested_generation),
    alert_reason TEXT CHECK(alert_reason IN ('NEW','REGRESSED','UNMUTED')),
    read_attempts INTEGER NOT NULL DEFAULT 0 CHECK(read_attempts BETWEEN 0 AND 5),
    next_read_ms INTEGER,
    parked_reason TEXT,
    UNIQUE(origin, project_id, issue_id)
);

CREATE TABLE bugsink_runs (
    run_id TEXT PRIMARY KEY NOT NULL,
    incident_key TEXT NOT NULL REFERENCES bugsink_incidents(incident_key),
    observation_key TEXT NOT NULL,
    episode INTEGER NOT NULL CHECK(episode >= 1),
    attempt INTEGER NOT NULL CHECK(attempt >= 1),
    authorized_by TEXT CHECK(authorized_by IS NULL OR length(trim(authorized_by)) > 0),
    state TEXT NOT NULL CHECK(state IN ('reserved','creating','submitted','active','succeeded','failed','uncertain')),
    source_revision TEXT NOT NULL CHECK(length(source_revision) > 0),
    manifest_digest TEXT,
    failure_class TEXT,
    UNIQUE(incident_key, observation_key, attempt),
    CHECK(attempt <= 2 OR authorized_by IS NOT NULL)
);

-- Uncertainty is not a lease: only reconciliation/operator action releases this slot.
CREATE UNIQUE INDEX bugsink_one_active_run ON bugsink_runs((1))
    WHERE state IN ('reserved','creating','submitted','active','uncertain');

CREATE TABLE bugsink_scans (
    origin TEXT NOT NULL CHECK(length(origin) > 0),
    project_id INTEGER NOT NULL CHECK(typeof(project_id) = 'integer' AND project_id >= 0),
    baseline_started_ms INTEGER,
    baseline_complete INTEGER NOT NULL DEFAULT 0 CHECK(baseline_complete IN (0, 1)),
    cursor TEXT,
    scan_started_ms INTEGER,
    next_scan_ms INTEGER,
    PRIMARY KEY(origin, project_id)
);
