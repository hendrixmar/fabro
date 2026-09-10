use std::collections::HashMap;

use fabro_model::Catalog;

use crate::{BilledTokenCounts, ModelRef, RunProjection, RunTiming, StageSummary, StageTiming};

#[derive(Debug, Clone, PartialEq)]
pub struct ProjectionBillingStage {
    pub node_id: String,
    pub billing: BilledTokenCounts,
    /// Per-node timing summed across every visit of that node within this
    /// projection. `wall_time_ms`, `inference_time_ms`, `tool_time_ms`, and
    /// `active_time_ms` are all summed in lockstep.
    pub timing:  StageTiming,
    pub model:   Option<ModelRef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionBillingByModel {
    pub model:   ModelRef,
    pub stages:  i64,
    pub billing: BilledTokenCounts,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ProjectionBillingRollup {
    pub stages:             Vec<ProjectionBillingStage>,
    pub totals:             BilledTokenCounts,
    pub by_model:           Vec<ProjectionBillingByModel>,
    /// Run-level timing summed across every stage visit. `wall_time_ms` is
    /// the sum of stage visit wall times (not the run clock duration).
    pub timing:             RunTiming,
    pub billed_visit_count: usize,
}

impl ProjectionBillingRollup {
    #[must_use]
    pub fn billing_if_present(&self) -> Option<BilledTokenCounts> {
        (self.billed_visit_count > 0).then(|| self.totals.clone())
    }

    /// Reconstruct the conclusion's per-node summaries from checkpoint and
    /// stage events. Repeated visits share one row, ordered by the node's
    /// first stage event.
    #[must_use]
    pub fn conclusion_stages(&self, projection: &RunProjection) -> (Vec<StageSummary>, u32) {
        let projection_order = stage_projection_order(projection);
        // Looping workflows revisit nodes; `completed_nodes` accumulates duplicates
        // while the other checkpoint maps are keyed by node_id. Dedupe to one row
        // per node so the stages table matches the deduped billing total.
        if let Some(cp) = projection.current_checkpoint() {
            let billing_by_node = self
                .stages
                .iter()
                .map(|stage| (stage.node_id.as_str(), stage))
                .collect::<HashMap<_, _>>();
            let mut stage_rows = Vec::new();
            let mut seen = std::collections::HashSet::new();
            let mut retries_sum: u32 = 0;
            let mut stage_order = Vec::new();

            for (original_checkpoint_order, node_id) in cp.completed_nodes.iter().enumerate() {
                if !seen.insert(node_id.as_str()) {
                    continue;
                }
                stage_order.push((original_checkpoint_order, node_id.as_str()));
            }
            let mut extra_node_outcomes = cp
                .node_outcomes
                .keys()
                .filter(|node_id| !seen.contains(node_id.as_str()))
                .map(String::as_str)
                .collect::<Vec<_>>();
            extra_node_outcomes.sort_unstable();
            let extra_offset = stage_order.len();
            for (extra_index, node_id) in extra_node_outcomes.into_iter().enumerate() {
                seen.insert(node_id);
                stage_order.push((extra_offset + extra_index, node_id));
            }

            for (original_checkpoint_order, node_id) in stage_order {
                let retries = cp
                    .node_retries
                    .get(node_id)
                    .copied()
                    .unwrap_or(1)
                    .saturating_sub(1);
                retries_sum += retries;
                let billing = billing_by_node.get(node_id);

                let summary = StageSummary {
                    stage_id: node_id.to_string(),
                    stage_label: node_id.to_string(),
                    timing: billing.map_or_else(StageTiming::default, |stage| stage.timing),
                    billing_usd_micros: billing.and_then(|stage| stage.billing.total_usd_micros),
                    retries,
                };
                stage_rows.push((
                    projection_order.get(node_id).copied().unwrap_or(u32::MAX),
                    original_checkpoint_order,
                    summary,
                ));
            }
            stage_rows.sort_by(|left, right| {
                left.0
                    .cmp(&right.0)
                    .then_with(|| left.1.cmp(&right.1))
                    .then_with(|| left.2.stage_id.cmp(&right.2.stage_id))
            });
            let stages = stage_rows
                .into_iter()
                .map(|(_, _, summary)| summary)
                .collect();
            (stages, retries_sum)
        } else {
            (vec![], 0)
        }
    }
}

#[must_use]
pub fn billing_rollup_from_projection(
    projection: &RunProjection,
    catalog: Option<&Catalog>,
) -> ProjectionBillingRollup {
    let mut stage_indices = HashMap::<String, usize>::new();
    let mut stages = Vec::<ProjectionBillingStage>::new();
    let mut by_model = HashMap::<ModelRef, ProjectionBillingByModel>::new();
    let mut totals = BilledTokenCounts::default();
    let mut run_timing = RunTiming::default();
    let mut billed_visit_count = 0_usize;

    for (stage_id, stage) in projection.iter_stages() {
        if projection.is_boundary_stage(stage_id.node_id()) {
            continue;
        }
        let usage = stage.billed_usage(catalog);
        let usage = usage.as_ref();
        if stage.completion.is_none() && stage.timing.is_none() && usage.is_zero() {
            continue;
        }

        let node_id = stage_id.node_id();
        let index = *stage_indices.entry(node_id.to_string()).or_insert_with(|| {
            let index = stages.len();
            stages.push(ProjectionBillingStage {
                node_id: node_id.to_string(),
                billing: BilledTokenCounts::default(),
                timing:  StageTiming::default(),
                model:   None,
            });
            index
        });
        let row = &mut stages[index];

        if let Some(timing) = stage.timing {
            row.timing = row.timing.saturating_add(&timing);
            run_timing = run_timing.saturating_add(&RunTiming::from(timing));
        }

        if !usage.is_zero() {
            billed_visit_count += 1;
            row.billing.add_counts(usage);
            totals.add_counts(usage);

            if let Some(model) = &stage.model {
                row.model = Some(model.clone());
                let model_entry =
                    by_model
                        .entry(model.clone())
                        .or_insert_with(|| ProjectionBillingByModel {
                            model:   model.clone(),
                            stages:  0,
                            billing: BilledTokenCounts::default(),
                        });
                model_entry.stages += 1;
                model_entry.billing.add_counts(usage);
            }
        }
    }

    let mut by_model = by_model.into_values().collect::<Vec<_>>();
    by_model.sort_by(|left, right| {
        let left_provider = left.model.provider.to_string();
        let right_provider = right.model.provider.to_string();
        left_provider
            .cmp(&right_provider)
            .then_with(|| left.model.model_id.cmp(&right.model.model_id))
            .then_with(|| {
                left.model
                    .speed
                    .map(<&'static str>::from)
                    .cmp(&right.model.speed.map(<&'static str>::from))
            })
    });

    ProjectionBillingRollup {
        stages,
        totals,
        by_model,
        timing: run_timing,
        billed_visit_count,
    }
}

fn stage_projection_order(state: &RunProjection) -> HashMap<String, u32> {
    let mut order = HashMap::new();
    for (stage_id, stage) in state.iter_stages() {
        order
            .entry(stage_id.node_id().to_string())
            .and_modify(|first_seq: &mut u32| {
                *first_seq = (*first_seq).min(stage.first_event_seq.get());
            })
            .or_insert_with(|| stage.first_event_seq.get());
    }
    order
}
