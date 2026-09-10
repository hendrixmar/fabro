ALTER TABLE automations
ADD COLUMN environment_id TEXT
REFERENCES environments(id) ON DELETE RESTRICT;

ALTER TABLE automations
ADD COLUMN last_error TEXT;

CREATE INDEX automations_environment_id_idx
ON automations(environment_id);
