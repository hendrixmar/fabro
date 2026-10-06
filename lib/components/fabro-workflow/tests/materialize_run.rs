use fabro_graphviz::graph::Graph;
use fabro_graphviz::parser;
use fabro_llm::test_support::test_catalog;
use fabro_types::WorkflowSettings;
use fabro_types::settings::InterpString;
use fabro_types::settings::run::{PullRequestSettings, RunGoal, RunModelSettings, RunNamespace};
use fabro_workflow::run_materialization::materialize_run;
use lithos_llm::catalog::builtin;

fn graph(source: &str) -> Graph {
    parser::parse(source).expect("graph should parse")
}

#[test]
fn materialize_run_applies_graph_and_catalog_defaults() {
    let source = r#"digraph Test {
        graph [goal="Build feature"]
        start [shape=Mdiamond]
        work  [prompt="Do work"]
        exit  [shape=Msquare]
        start -> work -> exit
    }"#;

    let settings = WorkflowSettings {
        run: RunNamespace {
            model: RunModelSettings {
                name: Some("sonnet".to_string()),
                ..RunModelSettings::default()
            },
            pull_request: Some(PullRequestSettings {
                enabled: false,
                ..PullRequestSettings::default()
            }),
            ..RunNamespace::default()
        },
        ..WorkflowSettings::default()
    };

    let materialized = materialize_run(settings, &graph(source), &test_catalog(), &[
        builtin::anthropic(),
    ])
    .unwrap();
    let resolved = &materialized.run;

    assert_eq!(resolved.model.name.as_deref(), Some("claude-sonnet-5"));
    assert_eq!(resolved.model.provider.as_deref(), Some("anthropic"));
    assert_eq!(
        materialized.run.goal.as_ref(),
        Some(&RunGoal::Inline(InterpString::parse("Build feature")))
    );
    assert!(resolved.pull_request.is_none());
}

#[test]
fn materialize_run_uses_configured_provider_defaults() {
    let source = r#"digraph Test {
        graph [goal="Build feature"]
        start [shape=Mdiamond]
        work  [prompt="Do work"]
        exit  [shape=Msquare]
        start -> work -> exit
    }"#;

    let materialized = materialize_run(
        WorkflowSettings::default(),
        &graph(source),
        &test_catalog(),
        &[builtin::openai()],
    )
    .unwrap();
    let resolved = &materialized.run;

    assert_eq!(resolved.model.provider.as_deref(), Some("openai"));
}

#[test]
fn materialize_command_only_run_without_providers_has_no_model_defaults() {
    let source = r#"digraph Test {
        start [shape=Mdiamond]
        command [shape=parallelogram, script="true"]
        exit [shape=Msquare]
        start -> command -> exit
    }"#;

    let materialized = materialize_run(
        WorkflowSettings::default(),
        &graph(source),
        &test_catalog(),
        &[],
    )
    .unwrap();

    assert_eq!(materialized.run.model.name, None);
    assert_eq!(materialized.run.model.provider, None);
}

#[test]
fn materialize_acp_only_run_preserves_opaque_model_metadata_without_providers() {
    let source = r#"digraph Test {
        graph [default_model="harness-default", default_provider="external-harness"]
        start [shape=Mdiamond]
        work [backend="acp", prompt="Do work", model="harness-only-model", provider="harness", acp.command="native-agent"]
        exit [shape=Msquare]
        start -> work -> exit
    }"#;
    let graph = graph(source);

    let materialized =
        materialize_run(WorkflowSettings::default(), &graph, &test_catalog(), &[]).unwrap();

    assert_eq!(materialized.run.model.name, None);
    assert_eq!(materialized.run.model.provider, None);
    assert_eq!(graph.nodes["work"].model(), Some("harness-only-model"));
    assert_eq!(graph.nodes["work"].provider(), Some("harness"));
    assert_eq!(
        graph.nodes["work"]
            .attrs
            .get("acp.command")
            .and_then(|value| value.as_str()),
        Some("native-agent")
    );
}

#[test]
fn materialize_api_and_mixed_runs_still_require_an_eligible_model_provider() {
    let api_graph = r#"digraph Test {
        start [shape=Mdiamond]
        work [prompt="Do work"]
        exit [shape=Msquare]
        start -> work -> exit
    }"#;
    let mixed_graph = r#"digraph Test {
        start [shape=Mdiamond]
        external [backend="acp", prompt="Do work", model="harness-only-model"]
        api [prompt="Do API work", model="gpt-5.4"]
        exit [shape=Msquare]
        start -> external -> api -> exit
    }"#;

    for source in [api_graph, mixed_graph] {
        assert!(
            materialize_run(
                WorkflowSettings::default(),
                &graph(source),
                &test_catalog(),
                &[]
            )
            .is_err()
        );
    }
}
