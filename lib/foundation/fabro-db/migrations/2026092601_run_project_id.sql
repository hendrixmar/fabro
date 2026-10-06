-- Owning project of a run: decided once, when the run's row is first
-- written (server-verified automation label, then the parent's project, then
-- a repository match), and never moved by a later event. Null means
-- unassigned. No foreign key: run history must survive project changes.
ALTER TABLE runs ADD COLUMN project_id TEXT;

CREATE INDEX runs_by_project ON runs(project_id, created_at_ms DESC, id DESC);
