#![expect(
    clippy::disallowed_methods,
    reason = "tests assert the raw template source"
)]

use std::collections::BTreeMap;

use fabro_types::SandboxProviderKind;
use fabro_types::settings::InterpString;
use fabro_types::settings::cli::{OutputFormat, OutputVerbosity};
use fabro_types::settings::server::LogDestination;

use crate::{Combine, SettingsLayer, StringOrSplice};

fn parse(input: &str) -> SettingsLayer {
    input
        .parse::<SettingsLayer>()
        .expect("fixture should parse")
}

#[test]
fn run_inputs_replace_wholesale() {
    let lower = parse(
        r#"
[run.inputs]
a = "lower"
b = "lower"
"#,
    );
    let higher = parse(
        r#"
[run.inputs]
a = "higher"
"#,
    );
    let merged = higher.combine(lower);
    let inputs = merged.run.unwrap().inputs.unwrap();
    assert_eq!(inputs.len(), 1);
    assert_eq!(inputs.get("a"), Some(&toml::Value::String("higher".into())));
    assert!(!inputs.contains_key("b"), "lower key should be gone");
}

#[test]
fn run_environment_env_merges_sticky() {
    let lower = parse(
        r#"
[run.environment.env]
A = "lower-a"
B = "lower-b"
"#,
    );
    let higher = parse(
        r#"
[run.environment.env]
A = "higher-a"
C = "higher-c"
"#,
    );
    let merged = higher.combine(lower);
    let environment = merged.run.unwrap().environment.unwrap();
    assert_eq!(environment.env.len(), 3);
    assert_eq!(
        environment
            .env
            .get("A")
            .map(InterpString::as_source)
            .as_deref(),
        Some("higher-a")
    );
    assert_eq!(
        environment
            .env
            .get("B")
            .map(InterpString::as_source)
            .as_deref(),
        Some("lower-b")
    );
}

#[test]
fn run_prepare_steps_replaces_whole_list() {
    let lower = parse(
        r#"
[[run.prepare.steps]]
script = "lower-1"

[[run.prepare.steps]]
script = "lower-2"
"#,
    );
    let higher = parse(
        r#"
[[run.prepare.steps]]
script = "higher-1"
"#,
    );
    let merged = higher.combine(lower);
    let steps = merged.run.unwrap().prepare.unwrap().steps;
    assert_eq!(steps.len(), 1);
}

#[test]
fn run_model_fallbacks_splice_inserts_inherited() {
    let lower = parse(
        r#"
[run.model.fallbacks]
"kimi-k3" = ["openrouter:moonshotai/kimi-k3", "gpt-terra"]
"gpt-sol" = ["claude-opus"]
"#,
    );
    let higher = parse(
        r#"
[run.model.fallbacks]
"kimi-k3" = ["anthropic", "..."]
"claude-fable" = ["gpt-sol", "claude-opus"]
"#,
    );
    let merged = higher.combine(lower);
    let fallbacks = merged.run.unwrap().model.unwrap().fallbacks;
    assert_eq!(
        serde_json::to_value(fallbacks.get("kimi-k3").unwrap()).unwrap(),
        serde_json::json!(["anthropic", "openrouter:moonshotai/kimi-k3", "gpt-terra"])
    );
    assert_eq!(
        serde_json::to_value(fallbacks.get("gpt-sol").unwrap()).unwrap(),
        serde_json::json!(["claude-opus"])
    );
    assert_eq!(
        serde_json::to_value(fallbacks.get("claude-fable").unwrap()).unwrap(),
        serde_json::json!(["gpt-sol", "claude-opus"])
    );
}

#[test]
fn run_model_fallbacks_rejects_the_removed_array_shape() {
    let error = r#"
[run.model]
fallbacks = ["anthropic"]
"#
    .parse::<SettingsLayer>()
    .expect_err("the global fallback array must stay unsupported");

    assert!(
        error.to_string().contains("invalid type: sequence"),
        "unexpected parse error: {error}"
    );
}

#[test]
fn hooks_replace_by_id() {
    let lower = parse(
        r#"
[[run.hooks]]
id = "shared"
event = "run_start"
script = "lower-script"
"#,
    );
    let higher = parse(
        r#"
[[run.hooks]]
id = "shared"
event = "run_start"
script = "higher-script"
"#,
    );
    let merged = higher.combine(lower);
    let hooks = merged.run.unwrap().hooks;
    assert_eq!(hooks.len(), 1);
    assert_eq!(
        hooks[0]
            .script
            .as_ref()
            .map(InterpString::as_source)
            .as_deref(),
        Some("higher-script")
    );
}

#[test]
fn anonymous_hooks_append_after_merged_inherited() {
    let lower = parse(
        r#"
[[run.hooks]]
event = "run_start"
script = "lower-anon"
"#,
    );
    let higher = parse(
        r#"
[[run.hooks]]
event = "run_complete"
script = "higher-anon"
"#,
    );
    let merged = higher.combine(lower);
    let hooks = merged.run.unwrap().hooks;
    assert_eq!(hooks.len(), 2);
    assert_eq!(
        hooks[0]
            .script
            .as_ref()
            .map(InterpString::as_source)
            .as_deref(),
        Some("lower-anon")
    );
    assert_eq!(
        hooks[1]
            .script
            .as_ref()
            .map(InterpString::as_source)
            .as_deref(),
        Some("higher-anon")
    );
}

#[test]
fn notification_route_events_splice() {
    let lower = parse(
        r#"
[run.notifications.ops]
enabled = true
provider = "slack"
events = ["run.failed"]
"#,
    );
    let higher = parse(
        r#"
[run.notifications.ops]
events = ["...", "run.completed"]
"#,
    );
    let merged = higher.combine(lower);
    let run = merged.run.unwrap();
    let route = &run.notifications["ops"];
    assert_eq!(route.enabled, Some(true));
    assert_eq!(route.provider.as_deref(), Some("slack"));
    assert_eq!(route.events, vec![
        StringOrSplice::Value("run.failed".to_string()),
        StringOrSplice::Value("run.completed".to_string()),
    ]);
}

#[test]
fn project_metadata_replaces_wholesale() {
    let lower = parse(
        r#"
[project.metadata]
a = "1"
b = "2"
"#,
    );
    let higher = parse(
        r#"
[project.metadata]
a = "replaced"
"#,
    );
    let merged = higher.combine(lower);
    let meta = merged.project.unwrap().metadata;
    assert_eq!(meta.len(), 1);
    assert_eq!(meta.get("a"), Some(&"replaced".to_string()));
}

#[test]
fn cli_output_merges_by_field() {
    let lower = parse(
        r#"
[cli.output]
format = "text"
verbosity = "normal"
"#,
    );
    let higher = parse(
        r#"
[cli.output]
verbosity = "verbose"
"#,
    );

    let merged = higher.combine(lower);
    let output = merged.cli.unwrap().output.unwrap();
    assert_eq!(output.format, Some(OutputFormat::Text));
    assert_eq!(output.verbosity, Some(OutputVerbosity::Verbose));
}

#[test]
fn cli_updates_merges_by_field() {
    let lower = parse(
        r"
[cli.updates]
check = true
",
    );
    let higher = parse(
        r#"
[cli.logging]
level = "debug"
"#,
    );

    let merged = higher.combine(lower);
    let updates = merged.cli.unwrap().updates.unwrap();
    assert_eq!(updates.check, Some(true));
}

#[test]
fn server_logging_merges_by_field() {
    let lower = parse(
        r#"
[server.logging]
level = "warn"
"#,
    );
    let higher = parse(
        r#"
[server.logging]
destination = "stdout"
"#,
    );

    let merged = higher.combine(lower);
    let logging = merged.server.unwrap().logging.unwrap();
    assert_eq!(
        logging.level.as_ref().map(fabro_config::LogFilter::as_str),
        Some("warn")
    );
    assert_eq!(logging.destination, Some(LogDestination::Stdout));
}

#[test]
fn server_sandbox_providers_merge_by_kind_and_sparse_field() {
    let lower = parse(
        r#"
[server.auth]
methods = ["dev-token"]

[server.sandbox.providers.local]
enabled = false

[server.sandbox.providers.daytona]
enabled = true

[server.sandbox.providers.e2b]
path = "/opt/fabro/plugins/e2b"
sha256 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
args = ["--lower"]
inherit_env = ["PATH"]
env = { LOWER = "lower" }

[server.sandbox.providers.other-plugin]
dev = true
"#,
    );
    let higher = parse(
        r#"
[server.sandbox.providers.daytona]
enabled = false

[server.sandbox.providers.e2b]
enabled = false
dev = true
args = []
inherit_env = []
env = { HIGHER = "higher" }
"#,
    );

    let merged = higher.combine(lower);
    let settings = crate::ServerSettingsBuilder::from_layer(&merged)
        .expect("merged server settings should resolve");
    let providers = settings.server.sandbox.providers;
    assert!(!providers.is_enabled(&SandboxProviderKind::LOCAL));
    assert!(!providers.is_enabled(&SandboxProviderKind::DAYTONA));
    assert!(providers.is_enabled(&SandboxProviderKind::DOCKER));
    assert!(providers.is_enabled(&SandboxProviderKind::try_new("other-plugin").unwrap()));

    let kind = SandboxProviderKind::try_new("e2b").unwrap();
    assert!(!providers.is_enabled(&kind));
    let plugin = providers.get(&kind).unwrap().plugin.as_ref().unwrap();
    assert_eq!(plugin.path.as_deref(), Some("/opt/fabro/plugins/e2b"));
    assert_eq!(
        plugin.sha256.as_deref(),
        Some("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
    );
    assert!(plugin.dev);
    assert!(plugin.args.is_empty());
    assert!(plugin.inherit_env.is_empty());
    assert_eq!(
        plugin.env,
        BTreeMap::from([("HIGHER".to_string(), "higher".to_string())])
    );
    assert_eq!(providers.enabled_plugins().count(), 1);

    let empty = parse(
        r"
[server.sandbox.providers]
",
    );
    assert_eq!(
        crate::ServerSettingsBuilder::from_layer(&empty.combine(merged))
            .expect("an empty provider map should preserve fallback policies")
            .server
            .sandbox
            .providers,
        providers
    );
}

#[test]
fn whole_replace_option_subtable_does_not_inherit_fallback_fields() {
    let lower = parse(
        r#"
[server.artifacts.s3]
bucket = "lower-bucket"
region = "us-east-1"
"#,
    );
    let higher = parse(
        r#"
[server.artifacts.s3]
bucket = "higher-bucket"
"#,
    );

    let merged = higher.combine(lower);
    let s3 = merged.server.unwrap().artifacts.unwrap().s3.unwrap();
    assert_eq!(s3.bucket, Some("higher-bucket".to_string()));
    assert_eq!(s3.region, None);
}
