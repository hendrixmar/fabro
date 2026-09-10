use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result, anyhow};
use fabro_config::{
    CliLayer, CliOutputLayer, RunGoalLayer, RunLayer, parse_input_overrides, parse_labels,
};
use fabro_manifest::{RunOverrideInput, build_run_overrides};
use fabro_types::RunIntentArgs;
use fabro_types::settings::cli::OutputVerbosity;
use fabro_types::settings::interp::InterpString;
use tokio::fs;

use crate::args::{PreflightArgs, RunArgs};

#[derive(Clone, Debug, Default)]
pub(crate) struct ManifestSettingsOverrides {
    pub(crate) run:             Option<RunLayer>,
    pub(crate) cli:             Option<CliLayer>,
    pub(crate) input_overrides: HashMap<String, toml::Value>,
}

#[derive(Debug)]
pub(super) struct PreparedIntentOverrides {
    pub(super) intent_args: RunIntentArgs,
    pub(super) goal:        Option<String>,
}

fn sparse_flag(value: bool) -> Option<bool> {
    value.then_some(true)
}

fn cli_layer_for_verbose(verbose: bool) -> Option<CliLayer> {
    verbose.then(|| CliLayer {
        output: Some(CliOutputLayer {
            verbosity: Some(OutputVerbosity::Verbose),
            ..CliOutputLayer::default()
        }),
        ..CliLayer::default()
    })
}

/// Build the `run.goal` override from the `--goal` / `--goal-file` args.
///
/// The two are mutually exclusive at the clap level; this helper assumes
/// at most one is set and returns an error if that invariant is violated.
///
/// CLI-supplied file paths are anchored at `cwd` (where the user invoked
/// the command), matching standard Unix CLI-flag conventions.
fn goal_layer_from_args(
    goal: Option<&str>,
    goal_file: Option<&Path>,
    cwd: &Path,
) -> Result<Option<RunGoalLayer>> {
    match (goal, goal_file) {
        (Some(_), Some(_)) => Err(anyhow!(
            "--goal and --goal-file are mutually exclusive; use exactly one"
        )),
        (Some(text), None) => Ok(Some(RunGoalLayer::Inline(InterpString::parse(text)))),
        (None, Some(path)) => {
            let absolute = if path.is_absolute() {
                path.to_path_buf()
            } else {
                cwd.join(path)
            };
            Ok(Some(RunGoalLayer::File {
                file: InterpString::parse(&absolute.to_string_lossy()),
            }))
        }
        (None, None) => Ok(None),
    }
}

fn current_dir_or_dot() -> PathBuf {
    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}

async fn intent_goal_from_args(
    goal: Option<&str>,
    goal_file: Option<&Path>,
    cwd: &Path,
) -> Result<Option<String>> {
    match (goal, goal_file) {
        (Some(_), Some(_)) => Err(anyhow!(
            "--goal and --goal-file are mutually exclusive; use exactly one"
        )),
        (Some(text), None) => Ok(Some(text.to_owned())),
        (None, Some(path)) => {
            let absolute = if path.is_absolute() {
                path.to_path_buf()
            } else {
                cwd.join(path)
            };
            Ok(Some(fs::read_to_string(&absolute).await.with_context(
                || format!("failed to read goal file {}", absolute.display()),
            )?))
        }
        (None, None) => Ok(None),
    }
}

pub(super) async fn prepare_intent_overrides(
    args: &RunArgs,
    cwd: &Path,
) -> Result<PreparedIntentOverrides> {
    let goal = intent_goal_from_args(args.goal.as_deref(), args.goal_file.as_deref(), cwd).await?;
    let input_overrides = parse_input_overrides(&args.inputs.values)?;
    let inputs = input_overrides
        .iter()
        .map(|(key, value)| {
            let value = fabro_types::toml_scalar_to_json_value(value)
                .map_err(anyhow::Error::new)
                .with_context(|| format!("failed to convert input override `{key}`"))?;
            Ok((key.clone(), value))
        })
        .collect::<Result<HashMap<_, _>>>()?;
    let labels = parse_labels(&args.label);
    let dry_run = sparse_flag(args.dry_run);
    let auto_approve = sparse_flag(args.auto_approve);
    let preserve_sandbox = sparse_flag(args.preserve_sandbox);
    Ok(PreparedIntentOverrides {
        intent_args: RunIntentArgs {
            model: args.model.clone(),
            provider: args.provider.clone(),
            inputs,
            labels,
            dry_run,
            auto_approve,
            preserve_sandbox,
        },
        goal,
    })
}

pub(crate) fn preflight_args_overrides(args: &PreflightArgs) -> Result<ManifestSettingsOverrides> {
    let cwd = current_dir_or_dot();
    let goal = goal_layer_from_args(args.goal.as_deref(), args.goal_file.as_deref(), &cwd)?;
    let mut run = build_run_overrides(RunOverrideInput {
        goal:             None,
        model:            args.model.as_deref(),
        provider:         args.provider.as_deref(),
        environment:      args.environment.as_deref(),
        preserve_sandbox: None,
        dry_run:          None,
        auto_approve:     None,
        labels:           HashMap::new(),
    });
    run.goal = goal;

    Ok(ManifestSettingsOverrides {
        run:             Some(run),
        cli:             cli_layer_for_verbose(args.verbose),
        input_overrides: parse_input_overrides(&args.inputs.values)?,
    })
}

#[cfg(test)]
#[expect(
    clippy::disallowed_methods,
    reason = "tests assert the raw template source"
)]
mod tests {
    use super::*;
    use crate::args::{InputOverrideArgs, ServerTargetArgs};

    fn run_args() -> RunArgs {
        RunArgs {
            target:           ServerTargetArgs::default(),
            inputs:           InputOverrideArgs::default(),
            workflow:         Some(PathBuf::from("workflow.fabro")),
            dry_run:          false,
            auto_approve:     false,
            goal:             None,
            goal_file:        None,
            model:            None,
            provider:         None,
            verbose:          false,
            environment:      None,
            label:            Vec::new(),
            parent:           None,
            preserve_sandbox: false,
            detach:           false,
        }
    }

    #[tokio::test]
    async fn intent_overrides_preserve_typed_values_and_sparse_flags() {
        let mut args = run_args();
        args.inputs.values = vec![
            "string=hello".to_string(),
            "boolean=true".to_string(),
            "integer=42".to_string(),
            "float=1.25".to_string(),
        ];
        args.goal = Some("Ship it".to_string());
        args.model = Some("gpt-5".to_string());
        args.provider = Some("openai".to_string());
        args.environment = Some("cloud".to_string());
        args.label = vec!["team=cli".to_string()];
        args.dry_run = true;
        args.auto_approve = true;
        args.preserve_sandbox = true;
        args.verbose = true;

        let PreparedIntentOverrides { intent_args, goal } =
            prepare_intent_overrides(&args, Path::new("/caller"))
                .await
                .unwrap();

        assert_eq!(goal.as_deref(), Some("Ship it"));
        assert_eq!(
            intent_args.inputs,
            HashMap::from([
                ("string".to_string(), serde_json::json!("hello")),
                ("boolean".to_string(), serde_json::json!(true)),
                ("integer".to_string(), serde_json::json!(42)),
                ("float".to_string(), serde_json::json!(1.25)),
            ])
        );
        assert_eq!(intent_args.model.as_deref(), Some("gpt-5"));
        assert_eq!(intent_args.provider.as_deref(), Some("openai"));
        assert_eq!(intent_args.labels.get("team"), Some(&"cli".to_string()));
        assert_eq!(intent_args.dry_run, Some(true));
        assert_eq!(intent_args.auto_approve, Some(true));
        assert_eq!(intent_args.preserve_sandbox, Some(true));
        assert!(
            !serde_json::to_value(&intent_args)
                .unwrap()
                .as_object()
                .unwrap()
                .contains_key("verbose")
        );
    }

    #[tokio::test]
    async fn intent_overrides_leave_false_flags_absent() {
        let prepared = prepare_intent_overrides(&run_args(), Path::new("/caller"))
            .await
            .unwrap();

        assert_eq!(prepared.intent_args.dry_run, None);
        assert_eq!(prepared.intent_args.auto_approve, None);
        assert_eq!(prepared.intent_args.preserve_sandbox, None);
    }

    #[tokio::test]
    async fn intent_goal_files_are_read_by_value_from_relative_and_absolute_paths() {
        let dir = tempfile::tempdir().unwrap();
        let relative = PathBuf::from("goals/task.md");
        fs::create_dir_all(dir.path().join("goals")).await.unwrap();
        fs::write(dir.path().join(&relative), "Goal from file")
            .await
            .unwrap();

        for goal_file in [relative, dir.path().join("goals/task.md")] {
            let mut args = run_args();
            args.goal_file = Some(goal_file);
            let PreparedIntentOverrides {
                intent_args: _,
                goal,
            } = prepare_intent_overrides(&args, dir.path()).await.unwrap();

            assert_eq!(goal.as_deref(), Some("Goal from file"));
        }
    }

    #[tokio::test]
    async fn intent_goal_file_read_errors_preserve_the_resolved_path_and_source() {
        let mut args = run_args();
        args.goal_file = Some(PathBuf::from("missing.md"));

        let error = prepare_intent_overrides(&args, Path::new("/caller"))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("/caller/missing.md"));
        assert!(error.source().is_some());
    }

    #[tokio::test]
    async fn intent_goal_and_goal_file_together_are_rejected_defensively() {
        let mut args = run_args();
        args.goal = Some("inline".to_string());
        args.goal_file = Some(PathBuf::from("goal.md"));

        let error = prepare_intent_overrides(&args, Path::new("/caller"))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("mutually exclusive"));
    }

    #[tokio::test]
    async fn intent_overrides_reject_non_finite_float_with_input_key() {
        let mut args = run_args();
        args.inputs.values = vec!["temperature=nan".to_string()];

        let error = prepare_intent_overrides(&args, Path::new("/caller"))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("temperature"));
        assert!(format!("{error:#}").contains("finite"));
        assert!(error.chain().any(|cause| {
            cause
                .downcast_ref::<fabro_types::TomlScalarToJsonError>()
                .is_some()
        }));
    }

    #[test]
    fn goal_and_goal_file_together_is_rejected() {
        let err = goal_layer_from_args(
            Some("inline text"),
            Some(Path::new("goal.md")),
            Path::new("/tmp"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("mutually exclusive"));
    }

    #[test]
    fn goal_file_is_anchored_at_cwd_when_relative() {
        let layer =
            goal_layer_from_args(None, Some(Path::new("prompts/goal.md")), Path::new("/cwd"))
                .unwrap()
                .expect("should build a goal layer");
        let RunGoalLayer::File { file } = layer else {
            panic!("expected file variant");
        };
        assert_eq!(file.as_source(), "/cwd/prompts/goal.md");
    }

    #[test]
    fn absolute_goal_file_is_preserved() {
        let layer = goal_layer_from_args(None, Some(Path::new("/abs/goal.md")), Path::new("/cwd"))
            .unwrap()
            .expect("should build a goal layer");
        let RunGoalLayer::File { file } = layer else {
            panic!("expected file variant");
        };
        assert_eq!(file.as_source(), "/abs/goal.md");
    }

    #[test]
    fn inline_goal_builds_inline_variant() {
        let layer = goal_layer_from_args(Some("inline goal"), None, Path::new("/cwd"))
            .unwrap()
            .expect("should build a goal layer");
        assert!(matches!(layer, RunGoalLayer::Inline(_)));
    }

    #[test]
    fn empty_args_produce_no_goal_layer() {
        assert!(
            goal_layer_from_args(None, None, Path::new("/cwd"))
                .unwrap()
                .is_none()
        );
    }
}
