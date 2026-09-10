use fabro_model::Catalog;
use fabro_validate::LintRule;

use super::types::{Transformed, Validated};

/// VALIDATE phase: run lint rules against the transformed graph.
///
/// Catalog-backed rules (model and provider availability) run only when
/// `catalog` is `Some`. Offline callers pass `None` so a workflow naming a
/// server-owned model is left for the server to judge.
///
/// **Infallible.** Always returns `Validated` with diagnostics. Caller decides
/// whether to fail via `validated.raise_on_errors()`.
pub fn validate(
    transformed: Transformed,
    catalog: Option<&Catalog>,
    extra_rules: &[&dyn LintRule],
) -> Validated {
    let Transformed {
        graph,
        source,
        mut diagnostics,
    } = transformed;
    diagnostics.extend(match catalog {
        Some(catalog) => fabro_validate::validate_with_catalog(&graph, catalog, extra_rules),
        None => fabro_validate::validate(&graph, extra_rules),
    });
    Validated::new(graph, source, diagnostics)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::pipeline::parse::parse;
    use crate::pipeline::transform;
    use crate::pipeline::types::TransformOptions;

    fn transform_options() -> TransformOptions {
        TransformOptions {
            current_dir:       None,
            file_resolver:     None,
            template_context:  fabro_template::TemplateContext::new(),
            source_name:       None,
            render_mode:       crate::operations::RenderMode::Strict,
            custom_transforms: vec![],
            model_resolution:  None,
        }
    }

    fn run_pipeline(dot: &str) -> Validated {
        let parsed = parse(dot).unwrap();
        let transformed = transform::transform(parsed, &transform_options()).unwrap();
        validate(transformed, None, &[])
    }

    #[test]
    fn validate_valid_graph() {
        let dot = r#"digraph Test {
            graph [goal="Build feature"]
            start [shape=Mdiamond]
            exit  [shape=Msquare]
            start -> exit
        }"#;
        let validated = run_pipeline(dot);
        assert!(!validated.has_errors());
        assert!(validated.raise_on_errors().is_ok());
    }

    #[test]
    fn validate_missing_start_node() {
        let dot = r#"digraph Test {
            graph [goal="Test"]
            work [label="Work"]
        }"#;
        let validated = run_pipeline(dot);
        assert!(validated.has_errors());
        assert!(validated.raise_on_errors().is_err());
    }

    #[test]
    fn validate_into_parts() {
        let dot = r#"digraph Test {
            graph [goal="Build feature"]
            start [shape=Mdiamond]
            exit  [shape=Msquare]
            start -> exit
        }"#;
        let validated = run_pipeline(dot);
        let (graph, source, diagnostics) = validated.into_parts();
        assert_eq!(graph.name, "Test");
        assert_eq!(source, dot);
        assert!(
            diagnostics
                .iter()
                .all(|d| d.severity != fabro_validate::Severity::Error)
        );
    }

    #[test]
    fn validate_diagnostics_accessible_before_raise() {
        let dot = r#"digraph Test {
            graph [goal="Test"]
            work [label="Work"]
        }"#;
        let validated = run_pipeline(dot);
        // Can read diagnostics before raising
        let diags = validated.diagnostics();
        assert!(!diags.is_empty());
        // Then raise
        assert!(validated.raise_on_errors().is_err());
    }

    #[test]
    fn validates_fully_rendered_model_stylesheet_syntax() {
        let dot = r#"digraph Test {
            graph [model_stylesheet="* { {{ inputs.declaration }} }"]
            start [shape=Mdiamond]
            exit [shape=Msquare]
            start -> exit
        }"#;
        let transformed = transform::transform(parse(dot).unwrap(), &TransformOptions {
            template_context: fabro_template::TemplateContext::new().with_inputs(HashMap::from([
                (
                    "declaration".to_string(),
                    toml::Value::String("garbage garbage".to_string()),
                ),
            ])),
            source_name: Some("workflow.fabro".to_string()),
            render_mode: crate::operations::RenderMode::Structural,
            ..transform_options()
        })
        .unwrap();
        let validated = validate(transformed, None, &[]);

        assert!(
            validated
                .diagnostics()
                .iter()
                .any(|diagnostic| diagnostic.rule == "stylesheet_syntax"),
            "{:?}",
            validated.diagnostics()
        );
    }

    #[test]
    fn unresolved_model_stylesheet_skips_stylesheet_rules() {
        let dot = r#"digraph Test {
            graph [model_stylesheet="* { model: {{ vars.MODEL }}; }"]
            start [shape=Mdiamond]
            exit [shape=Msquare]
            start -> exit
        }"#;
        let transformed = transform::transform(parse(dot).unwrap(), &TransformOptions {
            source_name: Some("workflow.fabro".to_string()),
            render_mode: crate::operations::RenderMode::Structural,
            ..transform_options()
        })
        .unwrap();
        let validated = validate(transformed, None, &[]);

        assert!(validated.diagnostics().iter().any(|diagnostic| {
            diagnostic.rule == "template_undefined_variable"
                && diagnostic.message.contains("vars.MODEL")
        }));
        assert!(validated.diagnostics().iter().all(|diagnostic| {
            diagnostic.rule != "stylesheet_syntax" && diagnostic.rule != "stylesheet_model_known"
        }));
    }
}
