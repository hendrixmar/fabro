ALTER TABLE automations ADD COLUMN workflow_source_repository TEXT
    CHECK (
        workflow_source_repository IS NULL
        OR length(workflow_source_repository) BETWEEN 3 AND 140
    );

ALTER TABLE automations ADD COLUMN workflow_source_branch TEXT
    CHECK (
        workflow_source_branch IS NULL
        OR length(workflow_source_branch) BETWEEN 1 AND 255
    );

ALTER TABLE automations ADD COLUMN workflow_source_tag TEXT
    CHECK (
        workflow_source_tag IS NULL
        OR length(workflow_source_tag) BETWEEN 1 AND 255
    );

ALTER TABLE automations ADD COLUMN workflow_source_sha TEXT
    CHECK (
        workflow_source_sha IS NULL
        OR (
            length(workflow_source_sha) = 40
            AND workflow_source_sha NOT GLOB '*[^0-9a-f]*'
        )
    );

CREATE TRIGGER automation_workflow_source_all_or_none_insert
BEFORE INSERT ON automations
WHEN
    (NEW.workflow_source_repository IS NULL)
    + (NEW.workflow_source_branch IS NULL) NOT IN (0, 2)
    OR (
        NEW.workflow_source_repository IS NULL
        AND (NEW.workflow_source_tag IS NOT NULL OR NEW.workflow_source_sha IS NOT NULL)
    )
BEGIN
    SELECT RAISE(ABORT, 'automation workflow source requires repository and branch together');
END;

CREATE TRIGGER automation_workflow_source_all_or_none_update
BEFORE UPDATE OF
    workflow_source_repository,
    workflow_source_branch,
    workflow_source_tag,
    workflow_source_sha
ON automations
WHEN
    (NEW.workflow_source_repository IS NULL)
    + (NEW.workflow_source_branch IS NULL) NOT IN (0, 2)
    OR (
        NEW.workflow_source_repository IS NULL
        AND (NEW.workflow_source_tag IS NOT NULL OR NEW.workflow_source_sha IS NOT NULL)
    )
BEGIN
    SELECT RAISE(ABORT, 'automation workflow source requires repository and branch together');
END;
