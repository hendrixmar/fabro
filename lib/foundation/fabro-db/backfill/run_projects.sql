-- Tag runs persisted before runs.project_id existed. First match wins:
-- the server-owned fabro_project_id label, the legacy `project` label (a
-- project id or its intake binding id), the parent's project, then the
-- project whose repository the run targeted. Only unassigned rows that
-- resolve to a project change, so a second run is a no-op.
WITH RECURSIVE
own(id, parent_id, project_id) AS (
    SELECT
        r.id,
        r.parent_id,
        COALESCE(
            (SELECT p.id FROM projects AS p
             WHERE p.id = json_extract(r.summary_json, '$.labels.fabro_project_id')),
            (SELECT p.id FROM projects AS p
             WHERE json_extract(r.summary_json, '$.labels.project') IN (p.id, p.intake_binding_id)
             ORDER BY p.id LIMIT 1)
        )
    FROM runs AS r
),
repo(id, project_id) AS (
    SELECT r.id, p.id
    FROM runs AS r
    JOIN projects AS p ON p.repository_key = lower(r.repository_name)
),
tagged(id, project_id) AS (
    SELECT o.id, COALESCE(o.project_id, (SELECT repo.project_id FROM repo WHERE repo.id = o.id))
    FROM own AS o
    WHERE o.parent_id IS NULL OR o.parent_id NOT IN (SELECT id FROM runs)
    UNION
    SELECT o.id, COALESCE(o.project_id, t.project_id, (SELECT repo.project_id FROM repo WHERE repo.id = o.id))
    FROM own AS o
    JOIN tagged AS t ON o.parent_id = t.id
)
UPDATE runs
SET project_id = (SELECT tagged.project_id FROM tagged WHERE tagged.id = runs.id)
WHERE project_id IS NULL
  AND id IN (SELECT tagged.id FROM tagged WHERE tagged.project_id IS NOT NULL);
