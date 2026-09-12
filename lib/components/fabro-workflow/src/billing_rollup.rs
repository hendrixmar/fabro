pub use fabro_types::billing_rollup::{
    ProjectionBillingByModel, ProjectionBillingRollup, ProjectionBillingStage,
    billing_rollup_from_projection,
};

#[cfg(test)]
mod tests {
    use fabro_types::{
        AttrValue, BilledTokenCounts, Graph, ModelRef, Node, RunProjection, RunSpec,
        StageCompletion, StageOutcome, first_event_seq, test_support,
    };
    use lithos_llm::catalog::{ModelId, builtin};

    use super::billing_rollup_from_projection;
    use crate::test_support::test_usage;

    fn test_projection() -> RunProjection {
        RunProjection::new(
            "Test run".to_string(),
            run_spec_with_boundary_nodes(),
            chrono::Utc::now(),
        )
    }

    #[test]
    fn rollup_groups_stage_rows_by_node_and_sums_retry_visit_usage() {
        let mut projection = test_projection();
        let failed_usage = test_usage("gpt-old", 100, 10);
        let success_usage = test_usage("gpt-new", 200, 20);
        let first = projection.stage_entry("verify", 1, first_event_seq(1));
        first.timing = Some(fabro_types::StageTiming::wall_only(1200));
        first.usage = BilledTokenCounts::from_billed_usage(std::slice::from_ref(&failed_usage));
        first.model = Some(failed_usage.model().clone());
        first.completion = Some(StageCompletion {
            outcome:        StageOutcome::Failed {
                retry_requested: true,
            },
            notes:          None,
            failure_reason: Some("try again".to_string()),
            timestamp:      chrono::Utc::now(),
        });
        let second = projection.stage_entry("verify", 2, first_event_seq(2));
        second.timing = Some(fabro_types::StageTiming::wall_only(800));
        second.usage = BilledTokenCounts::from_billed_usage(std::slice::from_ref(&success_usage));
        second.model = Some(success_usage.model().clone());
        second.completion = Some(StageCompletion {
            outcome:        StageOutcome::Succeeded,
            notes:          None,
            failure_reason: None,
            timestamp:      chrono::Utc::now(),
        });

        let rollup = billing_rollup_from_projection(&projection);

        assert_eq!(rollup.stages.len(), 1);
        assert_eq!(rollup.stages[0].node_id, "verify");
        assert_eq!(
            rollup.stages[0]
                .model
                .as_ref()
                .map(|model| model.model_id.as_str()),
            Some("gpt-new")
        );
        assert_eq!(rollup.stages[0].timing.wall_time_ms, 2000);
        assert_eq!(rollup.stages[0].billing.input_tokens, 300);
        assert_eq!(rollup.stages[0].billing.output_tokens, 30);
        assert_eq!(rollup.stages[0].billing.total_usd_micros, Some(330));

        assert_eq!(rollup.timing.wall_time_ms, 2000);
        assert_eq!(rollup.totals.input_tokens, 300);
        assert_eq!(rollup.totals.output_tokens, 30);
        assert_eq!(rollup.totals.total_usd_micros, Some(330));
        assert_eq!(rollup.billed_visit_count, 2);

        assert_eq!(rollup.by_model.len(), 2);
        assert_eq!(rollup.by_model[0].model.model_id.as_str(), "gpt-new");
        assert_eq!(rollup.by_model[0].stages, 1);
        assert_eq!(rollup.by_model[0].billing.input_tokens, 200);
        assert_eq!(rollup.by_model[1].model.model_id.as_str(), "gpt-old");
        assert_eq!(rollup.by_model[1].stages, 1);
        assert_eq!(rollup.by_model[1].billing.input_tokens, 100);
    }

    #[test]
    fn rollup_includes_completed_non_llm_stage_rows_with_zero_billing() {
        let mut projection = test_projection();
        let stage = projection.stage_entry("build", 1, first_event_seq(1));
        stage.timing = Some(fabro_types::StageTiming::wall_only(25));
        stage.completion = Some(StageCompletion {
            outcome:        StageOutcome::Succeeded,
            notes:          None,
            failure_reason: None,
            timestamp:      chrono::Utc::now(),
        });

        let rollup = billing_rollup_from_projection(&projection);

        assert_eq!(rollup.stages.len(), 1);
        assert_eq!(rollup.stages[0].node_id, "build");
        assert_eq!(rollup.stages[0].timing.wall_time_ms, 25);
        assert!(rollup.stages[0].model.is_none());
        assert_eq!(rollup.stages[0].billing.input_tokens, 0);
        assert_eq!(rollup.timing.wall_time_ms, 25);
        assert!(rollup.by_model.is_empty());
        assert!(rollup.billing_if_present().is_none());
    }

    #[test]
    fn rollup_excludes_workflow_boundary_stage_rows() {
        let mut projection = test_projection();
        projection.spec = run_spec_with_boundary_nodes();
        let start = projection.stage_entry("start", 1, first_event_seq(1));
        start.timing = Some(fabro_types::StageTiming::wall_only(25));
        start.completion = Some(StageCompletion {
            outcome:        StageOutcome::Succeeded,
            notes:          None,
            failure_reason: None,
            timestamp:      chrono::Utc::now(),
        });
        let exit = projection.stage_entry("exit", 1, first_event_seq(2));
        exit.timing = Some(fabro_types::StageTiming::wall_only(7));
        exit.completion = Some(StageCompletion {
            outcome:        StageOutcome::Succeeded,
            notes:          None,
            failure_reason: None,
            timestamp:      chrono::Utc::now(),
        });

        let rollup = billing_rollup_from_projection(&projection);

        assert_eq!(rollup.stages.len(), 0);
        assert_eq!(rollup.timing.wall_time_ms, 0);
    }

    #[test]
    fn rollup_keeps_in_flight_stage_usage_unpriced() {
        let mut projection = test_projection();
        let model = ModelRef::new(builtin::openai(), ModelId::new("gpt-5.4"));
        let stage = projection.stage_entry("agent", 1, first_event_seq(1));
        stage.started_at = Some(chrono::Utc::now());
        stage.usage = BilledTokenCounts {
            input_tokens: 500_000,
            output_tokens: 125_000,
            total_tokens: 625_000,
            ..BilledTokenCounts::default()
        };
        stage.model = Some(model.clone());

        let rollup = billing_rollup_from_projection(&projection);

        // The rollup keeps the shape of what the events recorded. Costs come
        // from the events themselves; an in-flight stage that has recorded no
        // cost yet stays unpriced rather than being re-estimated here.
        assert_eq!(rollup.stages.len(), 1);
        assert_eq!(rollup.stages[0].node_id, "agent");
        assert_eq!(rollup.stages[0].billing.total_usd_micros, None);
        assert_eq!(rollup.stages[0].billing.input_tokens, 500_000);
        assert_eq!(rollup.totals.total_usd_micros, None);
        assert_eq!(rollup.by_model.len(), 1);
        assert_eq!(rollup.by_model[0].billing.input_tokens, 500_000);
    }

    fn run_spec_with_boundary_nodes() -> RunSpec {
        let mut graph = Graph::new("test");
        graph.nodes.insert("start".to_string(), {
            let mut node = Node::new("start");
            node.attrs.insert(
                "shape".to_string(),
                AttrValue::String("Mdiamond".to_string()),
            );
            node
        });
        graph.nodes.insert("exit".to_string(), {
            let mut node = Node::new("exit");
            node.attrs.insert(
                "shape".to_string(),
                AttrValue::String("Msquare".to_string()),
            );
            node
        });

        RunSpec {
            graph,
            ..test_support::test_run_spec()
        }
    }
}
