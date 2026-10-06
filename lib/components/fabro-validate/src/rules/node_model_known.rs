use fabro_graphviz::graph::{Graph, node_needs_api_backend};
use fabro_llm::lithos_catalog::Catalog;

use super::model_support::{check_model_known, check_provider_known};
use crate::{Diagnostic, LintRule};

pub(super) fn rule(catalog: &Catalog) -> Box<dyn LintRule + '_> {
    Box::new(Rule { catalog })
}

struct Rule<'a> {
    catalog: &'a Catalog,
}

impl LintRule for Rule<'_> {
    fn name(&self) -> &'static str {
        "node_model_known"
    }

    fn apply(&self, graph: &Graph) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();
        for node in graph.nodes.values() {
            if !node_needs_api_backend(node) {
                continue;
            }
            let context = format!("on node '{}'", node.id);
            let node_id = Some(node.id.clone());
            if let Some(model) = node.model() {
                if let Some(d) =
                    check_model_known(self.name(), self.catalog, model, &context, node_id.clone())
                {
                    diagnostics.push(d);
                }
            }
            if let Some(provider) = node.provider() {
                if let Some(d) = check_provider_known(
                    self.name(),
                    self.catalog,
                    provider,
                    &context,
                    node_id.clone(),
                ) {
                    diagnostics.push(d);
                }
            }
        }
        diagnostics
    }
}

#[cfg(test)]
mod tests {
    use fabro_graphviz::graph::{AttrValue, Node};
    use fabro_llm::test_support::test_catalog;

    use super::Rule;
    use crate::rules::test_support::minimal_graph;
    use crate::{LintRule, Severity};

    #[test]
    fn node_model_known_rule_valid_model() {
        let mut g = minimal_graph();
        let mut node = Node::new("work");
        node.attrs.insert(
            "model".to_string(),
            AttrValue::String("claude-sonnet-4.5".to_string()),
        );
        g.nodes.insert("work".to_string(), node);
        let catalog = test_catalog();
        let rule = Rule { catalog: &catalog };
        let d = rule.apply(&g);
        assert!(d.is_empty());
    }

    #[test]
    fn node_model_known_rule_unknown_model() {
        let mut g = minimal_graph();
        let mut node = Node::new("work");
        node.attrs.insert(
            "model".to_string(),
            AttrValue::String("nonexistent-model-xyz".to_string()),
        );
        g.nodes.insert("work".to_string(), node);
        let catalog = test_catalog();
        let rule = Rule { catalog: &catalog };
        let d = rule.apply(&g);
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].severity, Severity::Warning);
        assert!(d[0].message.contains("nonexistent-model-xyz"));
        assert_eq!(d[0].node_id.as_deref(), Some("work"));
    }

    #[test]
    fn node_model_known_rule_alias() {
        let mut g = minimal_graph();
        let mut node = Node::new("work");
        node.attrs
            .insert("model".to_string(), AttrValue::String("opus".to_string()));
        g.nodes.insert("work".to_string(), node);
        let catalog = test_catalog();
        let rule = Rule { catalog: &catalog };
        let d = rule.apply(&g);
        assert!(d.is_empty());
    }

    #[test]
    fn node_model_known_rule_unknown_provider() {
        let mut g = minimal_graph();
        let mut node = Node::new("work");
        node.attrs.insert(
            "provider".to_string(),
            AttrValue::String("nonexistent-provider".to_string()),
        );
        g.nodes.insert("work".to_string(), node);
        let catalog = test_catalog();
        let rule = Rule { catalog: &catalog };
        let d = rule.apply(&g);
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].severity, Severity::Warning);
        assert!(d[0].message.contains("nonexistent-provider"));
        assert_eq!(d[0].node_id.as_deref(), Some("work"));
    }

    #[test]
    fn node_model_known_rule_no_model_no_provider() {
        let g = minimal_graph();
        let catalog = test_catalog();
        let rule = Rule { catalog: &catalog };
        let d = rule.apply(&g);
        assert!(d.is_empty());
    }
    #[test]
    fn node_model_known_rule_preserves_opaque_acp_model_and_provider_hints() {
        let mut g = minimal_graph();
        let mut node = Node::new("external");
        for (key, value) in [
            ("backend", "acp"),
            ("model", "harness-only-model"),
            ("provider", "external-harness"),
        ] {
            node.attrs
                .insert(key.to_string(), AttrValue::String(value.to_string()));
        }
        g.nodes.insert(node.id.clone(), node);

        let catalog = test_catalog();
        let rule = Rule { catalog: &catalog };
        assert!(rule.apply(&g).is_empty());
    }

    #[test]
    fn node_model_known_rule_keeps_api_checks_strict_in_mixed_graphs() {
        let mut g = minimal_graph();
        let mut acp = Node::new("external");
        acp.attrs
            .insert("backend".to_string(), AttrValue::String("acp".to_string()));
        acp.attrs.insert(
            "model".to_string(),
            AttrValue::String("harness-only-model".to_string()),
        );
        acp.attrs.insert(
            "provider".to_string(),
            AttrValue::String("external-harness".to_string()),
        );
        g.nodes.insert(acp.id.clone(), acp);
        let mut api = Node::new("api");
        api.attrs.insert(
            "model".to_string(),
            AttrValue::String("nonexistent-model-xyz".to_string()),
        );
        api.attrs.insert(
            "provider".to_string(),
            AttrValue::String("nonexistent-provider".to_string()),
        );
        g.nodes.insert(api.id.clone(), api);

        let catalog = test_catalog();
        let rule = Rule { catalog: &catalog };
        let diagnostics = rule.apply(&g);

        assert_eq!(diagnostics.len(), 2);
        assert!(
            diagnostics
                .iter()
                .all(|diagnostic| diagnostic.node_id.as_deref() == Some("api"))
        );
    }
}
