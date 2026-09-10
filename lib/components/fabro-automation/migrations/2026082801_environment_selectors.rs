//! Assigns a safe environment selector to automations created before the
//! selector column existed. Remove this compatibility migration after
//! 2026-11-28, once supported upgrades no longer span that release.

use fabro_db::DbPool;
use tracing::info;

use crate::{AutomationReplace, AutomationStore, AutomationStoreError};

pub(crate) const REMOVAL_DEADLINE: &str = "2026-11-28";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvironmentSelectorBackfillReport {
    pub updated_rows:   usize,
    pub environment_id: Option<String>,
}

/// Backfill incomplete automations only when selection is unambiguous:
/// prefer a clone-compatible `default`, otherwise use the sole compatible
/// environment. Zero or multiple candidates remain incomplete for an operator
/// to resolve in the UI.
pub async fn backfill_environment_selectors(
    pool: &DbPool,
) -> Result<EnvironmentSelectorBackfillReport, AutomationStoreError> {
    let has_incomplete =
        sqlx::query("SELECT 1 FROM automations WHERE environment_id IS NULL LIMIT 1")
            .fetch_optional(pool)
            .await?
            .is_some();
    if !has_incomplete {
        return Ok(EnvironmentSelectorBackfillReport {
            updated_rows:   0,
            environment_id: None,
        });
    }

    let compatible_ids = sqlx::query_scalar::<_, String>(
        "SELECT id FROM environments WHERE provider IN ('docker', 'daytona') ORDER BY id",
    )
    .fetch_all(pool)
    .await?;
    let environment_id = compatible_ids
        .iter()
        .find(|id| id.as_str() == "default")
        .cloned()
        .or_else(|| (compatible_ids.len() == 1).then(|| compatible_ids[0].clone()));

    let Some(environment_id) = environment_id else {
        return Ok(EnvironmentSelectorBackfillReport {
            updated_rows:   0,
            environment_id: None,
        });
    };

    let store = AutomationStore::new(pool.clone());
    let incomplete = store
        .list()
        .await?
        .into_iter()
        .filter(|automation| automation.environment_id.is_none())
        .collect::<Vec<_>>();
    for automation in &incomplete {
        store
            .replace(&automation.id, &automation.revision, AutomationReplace {
                name:            automation.name.clone(),
                description:     automation.description.clone(),
                environment_id:  Some(environment_id.clone()),
                target:          automation.target.clone(),
                workflow:        automation.workflow.clone(),
                workflow_source: automation.workflow_source.clone(),
                triggers:        automation.triggers.clone(),
            })
            .await?;
    }

    if !incomplete.is_empty() {
        info!(
            updated_rows = incomplete.len(),
            environment_id,
            removal_deadline = REMOVAL_DEADLINE,
            "Backfilled automation environment selectors"
        );
    }

    Ok(EnvironmentSelectorBackfillReport {
        updated_rows:   incomplete.len(),
        environment_id: Some(environment_id),
    })
}
