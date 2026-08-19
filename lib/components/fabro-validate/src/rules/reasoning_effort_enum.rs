use std::str::FromStr;

use fabro_graphviz::graph::Graph;
use fabro_model::ReasoningEffort;

use crate::{Diagnostic, LintRule, Severity};

pub(super) fn rule() -> Box<dyn LintRule> {
    Box::new(Rule)
}

struct Rule;

impl LintRule for Rule {
    fn name(&self) -> &'static str {
        "reasoning_effort_enum"
    }

    fn apply(&self, graph: &Graph) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();
        for node in graph.nodes.values() {
            let Some(effort) = node.reasoning_effort_attr() else {
                continue;
            };
            if ReasoningEffort::from_str(effort).is_ok() {
                continue;
            }
            diagnostics.push(Diagnostic {
                rule: self.name().to_string(),
                severity: Severity::Error,
                message: format!(
                    "invalid reasoning_effort \"{effort}\"; expected one of: {}",
                    ReasoningEffort::variants()
                        .iter()
                        .map(|variant| variant.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                node_id: Some(node.id.clone()),
                edge: None,
                fix: Some("Use one of: low, medium, high, xhigh, max".to_string()),

                ..Diagnostic::default()
            });
        }
        diagnostics
    }
}

#[cfg(test)]
mod tests {
    use super::Rule;
    use crate::rules::test_support::{minimal_graph, node_with_attrs};
    use crate::{LintRule, Severity};

    #[test]
    fn reasoning_effort_enum_passes_without_attr() {
        let graph = minimal_graph();
        assert!(Rule.apply(&graph).is_empty());
    }

    #[test]
    fn reasoning_effort_enum_accepts_all_variants() {
        for effort in ["low", "medium", "high", "xhigh", "max"] {
            let mut graph = minimal_graph();
            graph.nodes.insert(
                "work".to_string(),
                node_with_attrs("work", &[("reasoning_effort", effort)]),
            );
            assert!(Rule.apply(&graph).is_empty(), "effort: {effort}");
        }
    }

    #[test]
    fn reasoning_effort_enum_rejects_banana() {
        let mut graph = minimal_graph();
        graph.nodes.insert(
            "work".to_string(),
            node_with_attrs("work", &[("reasoning_effort", "banana")]),
        );

        let diagnostics = Rule.apply(&graph);
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].severity, Severity::Error);
        assert!(
            diagnostics[0]
                .message
                .contains("invalid reasoning_effort \"banana\"")
        );
        assert!(diagnostics[0].message.contains("xhigh"));
        assert!(
            diagnostics[0]
                .fix
                .as_deref()
                .unwrap()
                .contains("low, medium, high, xhigh, max")
        );
    }
}
