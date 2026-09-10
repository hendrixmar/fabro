//! Static file references in workflow graphs.
//!
//! Workflow graphs name other files through a fixed attribute vocabulary
//! (`import`, `stack.child_workflow`, `@`-prefixed `prompt`/`output_schema`
//! values, the graph `goal`, and inline root template fields such as
//! `model_stylesheet`). File references are *static*: they may not contain
//! template syntax, because they are resolved before template rendering.
//!
//! [`visit_graph_references`] is the one walker over that vocabulary. The
//! manifest bundler and workflow-version validation both consume it, so a new
//! reference-bearing attribute is added here once instead of drifting between
//! per-crate walkers.

use fabro_types::graph::{
    AttributeScope, Graph, GraphReferenceKind, ReferenceKind, reference_kind_for_attribute,
};

use crate::contains_template_syntax;

/// A static file reference that unexpectedly contains template syntax.
#[derive(Debug, thiserror::Error)]
#[error("templates are not supported in {kind}s: {value}")]
pub struct StaticReferenceError {
    kind:  ReferenceKind,
    value: String,
}

impl StaticReferenceError {
    #[must_use]
    pub fn new(kind: ReferenceKind, value: impl Into<String>) -> Self {
        Self {
            kind,
            value: value.into(),
        }
    }

    #[must_use]
    pub fn kind(&self) -> ReferenceKind {
        self.kind
    }

    #[must_use]
    pub fn value(&self) -> &str {
        &self.value
    }
}

/// Reject static file references (imports, child workflows, `@` file values)
/// that contain template syntax.
pub fn validate_static_reference(
    value: &str,
    kind: ReferenceKind,
) -> Result<(), StaticReferenceError> {
    if contains_template_syntax(value) {
        return Err(StaticReferenceError::new(kind, value));
    }
    Ok(())
}

/// One file reference or inline template discovered in a workflow graph.
///
/// `@` prefixes are already stripped from file references; inline variants
/// carry template content that the consumer should feed to template-dependency
/// discovery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GraphReference<'graph> {
    /// `graph [goal="@<reference>"]`.
    GoalFile { reference: &'graph str },
    /// A non-`@` graph `goal`: inline template content.
    GoalInline { content: &'graph str },
    /// The entrypoint graph's inline `model_stylesheet` template content.
    ///
    /// Emitted only when the walked graph is [`GraphPosition::Entrypoint`];
    /// imported stylesheets are ignored at runtime, so they are never
    /// template roots.
    ModelStylesheetInline { content: &'graph str },
    /// `node [import="<reference>"]` — another graph file to walk.
    Import { reference: &'graph str },
    /// `node [stack.child_workflow="<reference>"]`.
    ChildWorkflow { reference: &'graph str },
    /// `node [<key>="@<reference>"]` for file-inlined attributes
    /// (`prompt`, `output_schema`).
    FileInline {
        key:       &'graph str,
        reference: &'graph str,
    },
    /// A non-`@` node prompt: inline template content.
    InlinePrompt { content: &'graph str },
}

/// Error from [`visit_graph_references`].
#[derive(Debug, thiserror::Error)]
pub enum GraphReferenceError<E> {
    #[error(transparent)]
    StaticReference(StaticReferenceError),
    #[error(transparent)]
    Visit(E),
}

/// Whether the walked graph is the workflow's entrypoint or was reached
/// through an `import`/`stack.child_workflow` reference.
///
/// Position-dependent reference semantics (today: `model_stylesheet` is a
/// template root only on the entrypoint) live in the walker, so every
/// consumer applies the same rule.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GraphPosition {
    Entrypoint,
    Imported,
}

/// Walk every static file reference and inline template in one parsed graph,
/// validating that file references are template-free before emitting them.
///
/// The walker covers a single graph; recursion into `Import` targets and
/// resolution of references against a file source are the consumer's job.
/// `position` tells the walker whether this graph is the workflow entrypoint,
/// which gates position-dependent references such as `model_stylesheet`.
pub fn visit_graph_references<'graph, E>(
    graph: &'graph Graph,
    position: GraphPosition,
    mut visit: impl FnMut(GraphReference<'graph>) -> Result<(), E>,
) -> Result<(), GraphReferenceError<E>> {
    let goal = graph.goal();
    if !goal.is_empty() {
        if let Some(reference) = goal.strip_prefix('@') {
            validate_static_reference(reference, ReferenceKind::GraphGoalFile)
                .map_err(GraphReferenceError::StaticReference)?;
            visit(GraphReference::GoalFile { reference }).map_err(GraphReferenceError::Visit)?;
        } else {
            visit(GraphReference::GoalInline { content: goal })
                .map_err(GraphReferenceError::Visit)?;
        }
    }

    let model_stylesheet = graph.model_stylesheet();
    if position == GraphPosition::Entrypoint && !model_stylesheet.is_empty() {
        visit(GraphReference::ModelStylesheetInline {
            content: model_stylesheet,
        })
        .map_err(GraphReferenceError::Visit)?;
    }

    for node in graph.nodes.values() {
        for (key, value) in &node.attrs {
            let Some(value) = value.as_str() else {
                continue;
            };
            let Some(kind) = reference_kind_for_attribute(AttributeScope::Node, key, value) else {
                continue;
            };
            let reference = match kind {
                GraphReferenceKind::Import | GraphReferenceKind::ChildWorkflow => value,
                // Classification only yields these kinds for `@` values.
                GraphReferenceKind::FileInline | GraphReferenceKind::GraphGoalFile => value
                    .strip_prefix('@')
                    .expect("file reference classification requires a leading '@'"),
            };
            validate_static_reference(reference, kind.into())
                .map_err(GraphReferenceError::StaticReference)?;
            let event = match kind {
                GraphReferenceKind::Import => GraphReference::Import { reference },
                GraphReferenceKind::ChildWorkflow => GraphReference::ChildWorkflow { reference },
                GraphReferenceKind::FileInline => GraphReference::FileInline { key, reference },
                GraphReferenceKind::GraphGoalFile => GraphReference::GoalFile { reference },
            };
            visit(event).map_err(GraphReferenceError::Visit)?;
        }

        if let Some(prompt) = node.prompt().filter(|prompt| !prompt.starts_with('@')) {
            visit(GraphReference::InlinePrompt { content: prompt })
                .map_err(GraphReferenceError::Visit)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use fabro_types::graph::{AttrValue, Graph, Node, ReferenceKind};

    use super::{GraphPosition, GraphReference, GraphReferenceError, validate_static_reference};

    #[test]
    fn static_reference_rejects_template_syntax() {
        let error = validate_static_reference(
            "@schemas/{{ inputs.schema }}.json",
            ReferenceKind::FileInline,
        )
        .unwrap_err();

        assert_eq!(error.kind(), ReferenceKind::FileInline);
        assert_eq!(error.value(), "@schemas/{{ inputs.schema }}.json");
        assert!(
            error
                .to_string()
                .contains("templates are not supported in file inline references"),
            "unexpected error: {error}",
        );
        assert!(
            validate_static_reference("@schemas/result.json", ReferenceKind::FileInline).is_ok()
        );
    }

    fn node_with(id: &str, attrs: &[(&str, &str)]) -> Node {
        let mut node = Node::new(id);
        for (key, value) in attrs {
            node.attrs
                .insert((*key).to_string(), AttrValue::String((*value).to_string()));
        }
        node
    }

    #[test]
    fn visits_every_reference_kind_once() {
        let mut graph = Graph::new("test");
        graph.attrs.insert(
            "goal".to_string(),
            AttrValue::String("@goal.md".to_string()),
        );
        graph.attrs.insert(
            "model_stylesheet".to_string(),
            AttrValue::String("{% include 'styles.partial' %}".to_string()),
        );
        for node in [
            node_with("imported", &[("import", "graphs/child.fabro")]),
            node_with("child", &[("stack.child_workflow", "children/check.fabro")]),
            node_with("file_prompt", &[("prompt", "@prompts/task.md")]),
            node_with("inline", &[("prompt", "Do the {{ thing }}")]),
        ] {
            graph.nodes.insert(node.id.clone(), node);
        }

        let mut seen = BTreeSet::new();
        super::visit_graph_references(
            &graph,
            GraphPosition::Entrypoint,
            |reference| -> Result<(), std::convert::Infallible> {
                seen.insert(match reference {
                    GraphReference::GoalFile { reference } => format!("goal-file:{reference}"),
                    GraphReference::GoalInline { content } => format!("goal-inline:{content}"),
                    GraphReference::ModelStylesheetInline { content } => {
                        format!("stylesheet-inline:{content}")
                    }
                    GraphReference::Import { reference } => format!("import:{reference}"),
                    GraphReference::ChildWorkflow { reference } => format!("child:{reference}"),
                    GraphReference::FileInline { key, reference } => {
                        format!("file:{key}:{reference}")
                    }
                    GraphReference::InlinePrompt { content } => format!("inline:{content}"),
                });
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(
            seen,
            BTreeSet::from([
                "goal-file:goal.md".to_string(),
                "import:graphs/child.fabro".to_string(),
                "child:children/check.fabro".to_string(),
                "file:prompt:prompts/task.md".to_string(),
                "inline:Do the {{ thing }}".to_string(),
                "stylesheet-inline:{% include 'styles.partial' %}".to_string(),
            ])
        );
    }

    #[test]
    fn imported_graphs_do_not_emit_model_stylesheet() {
        let mut graph = Graph::new("test");
        graph.attrs.insert(
            "model_stylesheet".to_string(),
            AttrValue::String("* { reasoning_effort: low; }".to_string()),
        );

        super::visit_graph_references(
            &graph,
            GraphPosition::Imported,
            |reference| -> Result<(), std::convert::Infallible> {
                panic!("imported graph emitted {reference:?}")
            },
        )
        .unwrap();
    }

    #[test]
    fn rejects_template_syntax_in_references_before_visiting() {
        let mut graph = Graph::new("test");
        graph.nodes.insert(
            "imported".to_string(),
            node_with("imported", &[("import", "graphs/{{ name }}.fabro")]),
        );

        let error = super::visit_graph_references(
            &graph,
            GraphPosition::Entrypoint,
            |_| -> Result<(), std::convert::Infallible> {
                panic!("references with template syntax must not be visited")
            },
        )
        .unwrap_err();
        assert!(matches!(error, GraphReferenceError::StaticReference(_)));
    }
}
