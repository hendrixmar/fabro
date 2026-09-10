use fabro_graphviz::graph::Graph;

use crate::{Diagnostic, LintRule, Severity};

pub(super) fn rule() -> Box<dyn LintRule> {
    Box::new(Rule)
}

/// `auto_status=true` is the deprecated spelling of `on_failure="succeed"`.
/// The runtime still honors it as an alias; this rule points workflows at the
/// explicit policy.
struct Rule;

impl LintRule for Rule {
    fn name(&self) -> &'static str {
        "auto_status_deprecated"
    }

    fn apply(&self, graph: &Graph) -> Vec<Diagnostic> {
        let mut nodes: Vec<_> = graph
            .nodes
            .values()
            .filter(|node| node.attrs.contains_key("auto_status"))
            .collect();
        nodes.sort_unstable_by(|a, b| a.id.cmp(&b.id));

        nodes
            .into_iter()
            .map(|node| {
                let (message, fix) = if node.attrs.contains_key("on_failure") {
                    (
                        format!(
                            "Node '{}' sets deprecated 'auto_status', which is ignored because \
                             'on_failure' is set",
                            node.id
                        ),
                        "Remove 'auto_status'",
                    )
                } else if node.auto_status() {
                    (
                        format!("Node '{}' sets deprecated 'auto_status=true'", node.id),
                        "Use on_failure=\"succeed\" instead",
                    )
                } else {
                    (
                        format!(
                            "Node '{}' sets deprecated 'auto_status', which has no effect \
                             unless it is true",
                            node.id
                        ),
                        "Remove 'auto_status'",
                    )
                };
                Diagnostic {
                    rule: self.name().to_string(),
                    severity: Severity::Warning,
                    message,
                    node_id: Some(node.id.clone()),
                    fix: Some(fix.to_string()),
                    ..Diagnostic::default()
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use fabro_graphviz::graph::{AttrValue, Graph};

    use super::Rule;
    use crate::rules::test_support::{minimal_graph, node_with_attrs};
    use crate::{LintRule, Severity};

    fn graph_with_auto_status(value: AttrValue, on_failure: Option<&str>) -> Graph {
        let mut graph = minimal_graph();
        let mut node = match on_failure {
            Some(policy) => node_with_attrs("work", &[("on_failure", policy)]),
            None => node_with_attrs("work", &[]),
        };
        node.attrs.insert("auto_status".to_string(), value);
        graph.nodes.insert("work".to_string(), node);
        graph
    }

    #[test]
    fn accepts_graphs_without_auto_status() {
        let mut graph = minimal_graph();
        graph.nodes.insert(
            "work".to_string(),
            node_with_attrs("work", &[("on_failure", "succeed")]),
        );
        assert!(Rule.apply(&graph).is_empty());
    }

    #[test]
    fn warns_for_auto_status_true_and_suggests_succeed_policy() {
        let graph = graph_with_auto_status(AttrValue::Boolean(true), None);

        let diagnostics = Rule.apply(&graph);

        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].severity, Severity::Warning);
        assert_eq!(
            diagnostics[0].message,
            "Node 'work' sets deprecated 'auto_status=true'"
        );
        assert_eq!(diagnostics[0].node_id.as_deref(), Some("work"));
        assert_eq!(
            diagnostics[0].fix.as_deref(),
            Some("Use on_failure=\"succeed\" instead")
        );
    }

    #[test]
    fn warns_that_auto_status_is_ignored_when_on_failure_is_set() {
        let graph = graph_with_auto_status(AttrValue::Boolean(true), Some("exit"));

        let diagnostics = Rule.apply(&graph);

        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].severity, Severity::Warning);
        assert_eq!(
            diagnostics[0].message,
            "Node 'work' sets deprecated 'auto_status', which is ignored because 'on_failure' is set"
        );
        assert_eq!(diagnostics[0].fix.as_deref(), Some("Remove 'auto_status'"));
    }

    #[test]
    fn warns_for_auto_status_false() {
        let graph = graph_with_auto_status(AttrValue::Boolean(false), None);

        let diagnostics = Rule.apply(&graph);

        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].severity, Severity::Warning);
        assert_eq!(
            diagnostics[0].message,
            "Node 'work' sets deprecated 'auto_status', which has no effect unless it is true"
        );
        assert_eq!(diagnostics[0].fix.as_deref(), Some("Remove 'auto_status'"));
    }

    #[test]
    fn reports_nodes_in_id_order() {
        let mut graph = minimal_graph();
        for id in ["zeta", "alpha"] {
            let mut node = node_with_attrs(id, &[]);
            node.attrs
                .insert("auto_status".to_string(), AttrValue::Boolean(true));
            graph.nodes.insert(id.to_string(), node);
        }

        let ids: Vec<_> = Rule
            .apply(&graph)
            .into_iter()
            .map(|diagnostic| diagnostic.node_id.unwrap())
            .collect();

        assert_eq!(ids, ["alpha", "zeta"]);
    }
}
