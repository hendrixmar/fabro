-- no-transaction
-- Open the environment provider column to any sandbox-driver kind name.
-- SQLite cannot drop a CHECK constraint, so the table is rebuilt following
-- the documented procedure: foreign keys off, copy, swap, verify, on again.
-- `automations.environment_id` references this table by name; the drop and
-- rename leave that reference pointing at the rebuilt table.
PRAGMA foreign_keys = OFF;

BEGIN;

CREATE TABLE environments_new (
    id TEXT PRIMARY KEY NOT NULL,
    revision TEXT NOT NULL,
    provider TEXT NOT NULL,
    cwd TEXT,
    image_docker TEXT,
    image_dockerfile_inline TEXT,
    resources_cpu INTEGER,
    resources_memory TEXT,
    resources_disk TEXT,
    network_mode TEXT NOT NULL,
    network_allow_json TEXT NOT NULL DEFAULT '[]',
    lifecycle_preserve INTEGER NOT NULL,
    lifecycle_stop_on_terminal INTEGER NOT NULL,
    lifecycle_auto_stop TEXT,
    labels_json TEXT NOT NULL DEFAULT '{}',
    env_json TEXT NOT NULL DEFAULT '{}',
    CHECK (length(id) BETWEEN 1 AND 63),
    CHECK (substr(id, 1, 1) GLOB '[a-z0-9]'),
    CHECK (id NOT GLOB '*[^a-z0-9-]*'),
    CHECK (id <> 'local'),
    CHECK (length(revision) = 64),
    CHECK (revision NOT GLOB '*[^0-9a-f]*'),
    CHECK (length(provider) BETWEEN 1 AND 64),
    CHECK (substr(provider, 1, 1) GLOB '[a-z0-9]'),
    CHECK (substr(provider, -1, 1) GLOB '[a-z0-9]'),
    CHECK (provider NOT GLOB '*[^a-z0-9-]*'),
    CHECK (network_mode IN ('allow_all', 'block', 'cidr_allow_list')),
    CHECK (lifecycle_preserve IN (0, 1)),
    CHECK (lifecycle_stop_on_terminal IN (0, 1)),
    CHECK (json_valid(network_allow_json)),
    CHECK (json_valid(labels_json)),
    CHECK (json_valid(env_json))
);

INSERT INTO environments_new SELECT * FROM environments;

DROP TABLE environments;

ALTER TABLE environments_new RENAME TO environments;

PRAGMA foreign_key_check;

COMMIT;

PRAGMA foreign_keys = ON;
