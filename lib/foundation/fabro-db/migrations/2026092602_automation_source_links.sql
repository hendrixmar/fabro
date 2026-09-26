-- A project automation that links a global definition. The link owns its
-- target, environment and triggers; its workflow and workflow source are
-- read from the global on every load, so edits to the global reach every
-- project. RESTRICT keeps a linked global from being deleted.
ALTER TABLE automations
ADD COLUMN source_automation_id TEXT
REFERENCES automations(id) ON DELETE RESTRICT;

CREATE INDEX automations_source_automation_id_idx
ON automations(source_automation_id);

CREATE TRIGGER automation_link_insert
BEFORE INSERT ON automations
WHEN NEW.source_automation_id IS NOT NULL AND NEW.project_id IS NULL
BEGIN
    SELECT RAISE(ABORT, 'source_automation_id is only valid on project automations');
END;

CREATE TRIGGER automation_link_update
BEFORE UPDATE OF project_id ON automations
WHEN NEW.source_automation_id IS NOT NULL AND NEW.project_id IS NULL
BEGIN
    SELECT RAISE(ABORT, 'source_automation_id is only valid on project automations');
END;
