-- Native projects: an existing GitHub repository connected to this Fabro
-- server. Repository identity is the immutable numeric GitHub repository ID
-- (stored as a decimal string because it does not fit a JavaScript number);
-- the slug and default branch are cached metadata refreshed by the server.
CREATE TABLE projects (
    id TEXT PRIMARY KEY NOT NULL,
    revision TEXT NOT NULL,
    name TEXT NOT NULL,
    github_repository_id TEXT NOT NULL,
    repository TEXT NOT NULL,
    repository_key TEXT NOT NULL,
    default_branch TEXT NOT NULL,
    intake_binding_id TEXT,
    CHECK (length(id) BETWEEN 1 AND 63),
    CHECK (substr(id, 1, 1) GLOB '[a-z0-9]'),
    CHECK (id NOT GLOB '*[^a-z0-9-]*'),
    CHECK (length(revision) = 64),
    CHECK (revision NOT GLOB '*[^0-9a-f]*'),
    CHECK (length(trim(name)) > 0),
    CHECK (
        length(github_repository_id) BETWEEN 1 AND 20
        AND github_repository_id NOT GLOB '*[^0-9]*'
    ),
    CHECK (length(repository) BETWEEN 3 AND 140),
    CHECK (repository_key = lower(repository)),
    CHECK (length(default_branch) BETWEEN 1 AND 255),
    CHECK (
        intake_binding_id IS NULL
        OR length(trim(intake_binding_id)) BETWEEN 1 AND 63
    )
);

CREATE UNIQUE INDEX projects_github_repository_id_idx
ON projects(github_repository_id);

CREATE UNIQUE INDEX projects_repository_key_idx
ON projects(repository_key);

CREATE UNIQUE INDEX projects_intake_binding_id_idx
ON projects(intake_binding_id)
WHERE intake_binding_id IS NOT NULL;

-- Automation scope. A null `project_id` keeps the existing global meaning; a
-- project id makes the definition a concrete instance owned by that project.
-- `available_to_projects` is only meaningful on global definitions and makes
-- one selectable as a project automation source; it never activates a trigger.
ALTER TABLE automations
ADD COLUMN project_id TEXT
REFERENCES projects(id) ON DELETE RESTRICT;

ALTER TABLE automations
ADD COLUMN available_to_projects INTEGER NOT NULL DEFAULT 0
CHECK (available_to_projects IN (0, 1));

CREATE INDEX automations_project_id_idx
ON automations(project_id);

CREATE TRIGGER automation_scope_insert
BEFORE INSERT ON automations
WHEN NEW.available_to_projects = 1 AND NEW.project_id IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'available_to_projects is only valid on global automations');
END;

CREATE TRIGGER automation_scope_update
BEFORE UPDATE OF project_id, available_to_projects ON automations
WHEN NEW.available_to_projects = 1 AND NEW.project_id IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'available_to_projects is only valid on global automations');
END;
