CREATE TEMP TABLE automation_target_migration_candidates (
    id TEXT PRIMARY KEY NOT NULL,
    legacy_ref TEXT NOT NULL,
    branch TEXT NOT NULL,
    tag TEXT,
    sha TEXT
);

CREATE TEMP TRIGGER reject_unsupported_automation_target
BEFORE INSERT ON automation_target_migration_candidates
WHEN
    length(NEW.branch) NOT BETWEEN 1 AND 255
    OR NEW.branch != trim(NEW.branch)
    OR substr(NEW.branch, 1, 1) IN ('/', '-', '.')
    OR substr(NEW.branch, -1, 1) IN ('/', '.')
    OR NEW.branch = '@'
    OR instr(NEW.branch, '..') > 0
    OR instr(NEW.branch, '//') > 0
    OR instr(NEW.branch, '@{') > 0
    OR NEW.branch GLOB '*[^A-Za-z0-9/._-]*'
    OR NEW.branch GLOB '*/.*'
    OR NEW.branch GLOB '*.lock'
    OR NEW.branch GLOB '*.lock/*'
    OR NEW.branch = 'HEAD'
    OR NEW.branch GLOB 'refs/*'
    OR NEW.branch GLOB 'tags/*'
    OR NEW.branch GLOB 'heads/*'
    OR (length(NEW.branch) = 40 AND NEW.branch NOT GLOB '*[^0-9A-Fa-f]*')
    OR (
        NEW.tag IS NOT NULL
        AND (
            length(NEW.tag) NOT BETWEEN 1 AND 255
            OR NEW.tag != trim(NEW.tag)
            OR substr(NEW.tag, 1, 1) IN ('/', '-', '.')
            OR substr(NEW.tag, -1, 1) IN ('/', '.')
            OR NEW.tag = '@'
            OR instr(NEW.tag, '..') > 0
            OR instr(NEW.tag, '//') > 0
            OR instr(NEW.tag, '@{') > 0
            OR NEW.tag GLOB '*[^A-Za-z0-9/._-]*'
            OR NEW.tag GLOB '*/.*'
            OR NEW.tag GLOB '*.lock'
            OR NEW.tag GLOB '*.lock/*'
            OR NEW.tag = 'HEAD'
            OR NEW.tag GLOB 'refs/*'
            OR NEW.tag GLOB 'tags/*'
            OR (length(NEW.tag) = 40 AND NEW.tag NOT GLOB '*[^0-9A-Fa-f]*')
        )
    )
BEGIN
    SELECT RAISE(
        ABORT,
        'cannot migrate automations.target_ref: unsupported legacy selector; edit it to a branch, supported heads/tags selector, HEAD, or 40-hex SHA and restart'
    );
END;

INSERT INTO automation_target_migration_candidates (id, legacy_ref, branch, tag, sha)
SELECT
    id,
    target_ref,
    CASE
        WHEN length(target_ref) = 40 AND target_ref NOT GLOB '*[^0-9A-Fa-f]*' THEN 'main'
        WHEN target_ref GLOB 'refs/tags/?*' THEN 'main'
        WHEN target_ref GLOB 'tags/?*' THEN 'main'
        WHEN target_ref GLOB 'refs/heads/?*' THEN substr(target_ref, 12)
        WHEN target_ref GLOB 'heads/?*' THEN substr(target_ref, 7)
        WHEN target_ref = 'HEAD' THEN 'main'
        ELSE target_ref
    END,
    CASE
        WHEN target_ref GLOB 'refs/tags/?*' THEN substr(target_ref, 11)
        WHEN target_ref GLOB 'tags/?*' THEN substr(target_ref, 6)
        ELSE NULL
    END,
    CASE
        WHEN length(target_ref) = 40 AND target_ref NOT GLOB '*[^0-9A-Fa-f]*' THEN lower(target_ref)
        ELSE NULL
    END
FROM automations;

DROP TRIGGER reject_unsupported_automation_target;

ALTER TABLE automations RENAME COLUMN target_ref TO target_branch;
ALTER TABLE automations ADD COLUMN target_tag TEXT
    CHECK (target_tag IS NULL OR length(target_tag) BETWEEN 1 AND 255);
ALTER TABLE automations ADD COLUMN target_sha TEXT
    CHECK (
        target_sha IS NULL
        OR (
            length(target_sha) = 40
            AND target_sha NOT GLOB '*[^0-9a-f]*'
        )
    );

UPDATE automations
SET
    target_branch = candidates.branch,
    target_tag = candidates.tag,
    target_sha = candidates.sha
FROM automation_target_migration_candidates AS candidates
WHERE candidates.id = automations.id;

DROP TABLE automation_target_migration_candidates;
