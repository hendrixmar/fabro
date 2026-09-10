use fabro_graphviz::graph::{AttrValue, Graph};
use fabro_types::OnFailure;

use crate::{Diagnostic, LintRule, Severity};

pub(super) fn rule() -> Box<dyn LintRule> {
    Box::new(Rule)
}

struct Rule;

fn invalid_value_diagnostic(
    rule: &str,
    node_id: Option<&str>,
    value: &AttrValue,
) -> Option<Diagnostic> {
    let message = match value {
        AttrValue::String(value) if value.parse::<OnFailure>().is_ok() => return None,
        AttrValue::String(value) => match node_id {
            Some(node_id) => format!("Node '{node_id}' has invalid on_failure value '{value}'"),
            None => format!("Graph has invalid on_failure value '{value}'"),
        },
        _ => match node_id {
            Some(node_id) => {
                format!("Node '{node_id}' attribute 'on_failure' must be a string")
            }
            None => "Graph attribute 'on_failure' must be a string".to_string(),
        },
    };

    Some(Diagnostic {
        rule: rule.to_string(),
        severity: Severity::Error,
        message,
        node_id: node_id.map(str::to_string),
        fix: Some(format!("Use one of: {}", OnFailure::expected_values())),
        ..Diagnostic::default()
    })
}

impl LintRule for Rule {
    fn name(&self) -> &'static str {
        "on_failure_valid"
    }

    fn apply(&self, graph: &Graph) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();

        if let Some(value) = graph.attrs.get("on_failure") {
            diagnostics.extend(invalid_value_diagnostic(self.name(), None, value));
        }

        let mut node_values: Vec<_> = graph
            .nodes
            .iter()
            .filter_map(|(node_id, node)| {
                node.attrs
                    .get("on_failure")
                    .map(|value| (node_id.as_str(), value))
            })
            .collect();
        node_values.sort_unstable_by_key(|(node_id, _)| *node_id);
        for (node_id, value) in node_values {
            diagnostics.extend(invalid_value_diagnostic(self.name(), Some(node_id), value));
        }

        for edge in &graph.edges {
            if edge.attrs.contains_key("on_failure") {
                diagnostics.push(Diagnostic {
                    rule: self.name().to_string(),
                    severity: Severity::Warning,
                    message: format!(
                        "Edge '{} -> {}' sets 'on_failure', which has no effect on edges",
                        edge.from, edge.to
                    ),
                    fix: Some("Set 'on_failure' on the graph or the source node".to_string()),
                    edge: Some((edge.from.clone(), edge.to.clone())),
                    ..Diagnostic::default()
                });
            }
        }

        diagnostics
    }
}

#[cfg(test)]
mod tests {
    use fabro_graphviz::graph::{AttrValue, Edge};

    use super::Rule;
    use crate::rules::test_support::{minimal_graph, node_with_attrs};
    use crate::{LintRule, Severity};

    #[test]
    fn accepts_absent_and_supported_graph_values() {
        let mut graph = minimal_graph();
        assert!(Rule.apply(&graph).is_empty());

        for value in ["route", "exit", "succeed"] {
            graph.attrs.insert(
                "on_failure".to_string(),
                AttrValue::String(value.to_string()),
            );
            assert!(Rule.apply(&graph).is_empty());
        }
    }

    #[test]
    fn accepts_supported_node_values() {
        let mut graph = minimal_graph();
        for value in ["route", "exit", "succeed"] {
            graph.nodes.insert(
                "work".to_string(),
                node_with_attrs("work", &[("on_failure", value)]),
            );
            assert!(Rule.apply(&graph).is_empty());
        }
    }

    #[test]
    fn rejects_unsupported_graph_value() {
        let mut graph = minimal_graph();
        graph.attrs.insert(
            "on_failure".to_string(),
            AttrValue::String("stop".to_string()),
        );

        let diagnostics = Rule.apply(&graph);

        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].severity, Severity::Error);
        assert_eq!(
            diagnostics[0].message,
            "Graph has invalid on_failure value 'stop'"
        );
        assert_eq!(
            diagnostics[0].fix.as_deref(),
            Some("Use one of: route, exit, succeed")
        );
    }

    #[test]
    fn rejects_non_string_graph_value() {
        let mut graph = minimal_graph();
        graph
            .attrs
            .insert("on_failure".to_string(), AttrValue::Boolean(true));

        let diagnostics = Rule.apply(&graph);

        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].severity, Severity::Error);
        assert_eq!(
            diagnostics[0].message,
            "Graph attribute 'on_failure' must be a string"
        );
    }

    #[test]
    fn rejects_unsupported_node_value() {
        let mut graph = minimal_graph();
        graph.nodes.insert(
            "work".to_string(),
            node_with_attrs("work", &[("on_failure", "stop")]),
        );

        let diagnostics = Rule.apply(&graph);

        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].severity, Severity::Error);
        assert_eq!(
            diagnostics[0].message,
            "Node 'work' has invalid on_failure value 'stop'"
        );
        assert_eq!(diagnostics[0].node_id.as_deref(), Some("work"));
        assert_eq!(
            diagnostics[0].fix.as_deref(),
            Some("Use one of: route, exit, succeed")
        );
    }

    #[test]
    fn rejects_non_string_node_value() {
        let mut graph = minimal_graph();
        let mut node = node_with_attrs("work", &[]);
        node.attrs
            .insert("on_failure".to_string(), AttrValue::Boolean(true));
        graph.nodes.insert("work".to_string(), node);

        let diagnostics = Rule.apply(&graph);

        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].severity, Severity::Error);
        assert_eq!(
            diagnostics[0].message,
            "Node 'work' attribute 'on_failure' must be a string"
        );
    }

    #[test]
    fn warns_for_edge_placement() {
        let mut graph = minimal_graph();
        let mut edge = Edge::new("start", "work");
        edge.attrs.insert(
            "on_failure".to_string(),
            AttrValue::String("exit".to_string()),
        );
        graph.edges.push(edge);

        let diagnostics = Rule.apply(&graph);

        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].severity, Severity::Warning);
        assert_eq!(
            diagnostics[0].message,
            "Edge 'start -> work' sets 'on_failure', which has no effect on edges"
        );
        assert_eq!(
            diagnostics[0].fix.as_deref(),
            Some("Set 'on_failure' on the graph or the source node")
        );
        assert_eq!(
            diagnostics[0].edge,
            Some(("start".to_string(), "work".to_string()))
        );
    }
}
