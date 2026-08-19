use fabro_graphviz::graph::Graph;

use crate::{Diagnostic, LintRule, Severity};

pub(super) fn rule() -> Box<dyn LintRule> {
    Box::new(Rule)
}

struct Rule;

/// Harnesses with a first-class engine translation.
const KNOWN_HARNESSES: &[&str] = &["codex", "omp"];

impl LintRule for Rule {
    fn name(&self) -> &'static str {
        "harness_valid"
    }

    fn apply(&self, graph: &Graph) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();
        for node in graph.nodes.values() {
            let Some(harness) = node.harness_attr() else {
                continue;
            };

            if node.backend() != Some("acp") {
                diagnostics.push(Diagnostic {
                    rule: self.name().to_string(),
                    severity: Severity::Error,
                    message: format!(
                        "harness=\"{harness}\" requires backend=\"acp\"; harness selection is \
                         only meaningful for ACP-backed agent nodes"
                    ),
                    node_id: Some(node.id.clone()),
                    edge: None,
                    fix: Some("Set backend=\"acp\" or remove the harness attribute".to_string()),

                    ..Diagnostic::default()
                });
                continue;
            }

            if !KNOWN_HARNESSES.contains(&harness) {
                diagnostics.push(Diagnostic {
                    rule: self.name().to_string(),
                    severity: Severity::Error,
                    message: format!(
                        "unknown harness \"{harness}\"; only {} have engine-side model/effort \
                         translation",
                        KNOWN_HARNESSES.join(", ")
                    ),
                    node_id: Some(node.id.clone()),
                    edge: None,
                    fix: Some(format!(
                        "Use acp.command/acp.config to launch other harnesses; known values: {}",
                        KNOWN_HARNESSES.join(", ")
                    )),

                    ..Diagnostic::default()
                });
            }
        }
        diagnostics
    }
}


#[cfg(test)]
mod tests {
    use fabro_graphviz::graph::AttrValue;

    use super::{KNOWN_HARNESSES, Rule};
    use crate::rules::test_support::{minimal_graph, node_with_attrs};
    use crate::{LintRule, Severity};

    #[test]
    fn harness_valid_passes_without_harness_attr() {
        let graph = minimal_graph();
        assert!(Rule.apply(&graph).is_empty());
    }

    #[test]
    fn harness_valid_accepts_known_harnesses_on_acp_nodes() {
        for harness in KNOWN_HARNESSES {
            let mut graph = minimal_graph();
            graph.nodes.insert(
                "work".to_string(),
                node_with_attrs(
                    "work",
                    &[
                        ("backend", "acp"),
                        ("harness", harness),
                        ("acp.command", "codex-acp"),
                    ],
                ),
            );
            assert!(Rule.apply(&graph).is_empty(), "harness: {harness}");
        }
    }

    #[test]
    fn harness_valid_rejects_harness_without_acp_backend() {
        let mut graph = minimal_graph();
        graph.nodes.insert(
            "work".to_string(),
            node_with_attrs("work", &[("harness", "codex")]),
        );

        let diagnostics = Rule.apply(&graph);
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].severity, Severity::Error);
        assert!(
            diagnostics[0]
                .message
                .contains("harness=\"codex\" requires backend=\"acp\"")
        );
    }

    #[test]
    fn harness_valid_rejects_unknown_harness_suggesting_acp_command() {
        let mut graph = minimal_graph();
        graph.nodes.insert(
            "work".to_string(),
            node_with_attrs(
                "work",
                &[("backend", "acp"), ("harness", "claude")],
            ),
        );

        let diagnostics = Rule.apply(&graph);
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].severity, Severity::Error);
        assert!(diagnostics[0].message.contains("unknown harness \"claude\""));
        assert!(
            diagnostics[0]
                .fix
                .as_deref()
                .unwrap()
                .contains("acp.command")
        );
    }

    #[test]
    fn harness_valid_ignores_non_string_attr_values() {
        let mut graph = minimal_graph();
        let mut node = fabro_graphviz::graph::Node::new("work");
        node.attrs
            .insert("harness".to_string(), AttrValue::Integer(1));
        graph.nodes.insert("work".to_string(), node);

        assert!(Rule.apply(&graph).is_empty());
    }
}
