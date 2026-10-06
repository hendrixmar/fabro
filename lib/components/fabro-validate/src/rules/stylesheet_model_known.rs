use fabro_graphviz::graph::{Graph, node_needs_api_backend};
use fabro_graphviz::stylesheet::{Selector, parse_stylesheet};
use fabro_llm::lithos_catalog::Catalog;

use super::model_support::{check_model_known, check_provider_known};
use crate::{Diagnostic, LintRule};

pub(super) fn rule(catalog: &Catalog) -> Box<dyn LintRule + '_> {
    Box::new(Rule { catalog })
}

struct Rule<'a> {
    catalog: &'a Catalog,
}

impl Rule<'_> {
    fn selector_label(selector: &Selector) -> String {
        match selector {
            Selector::Universal => "*".to_string(),
            Selector::Shape(s) => s.clone(),
            Selector::Class(c) => format!(".{c}"),
            Selector::Id(id) => format!("#{id}"),
        }
    }
}

impl LintRule for Rule<'_> {
    fn name(&self) -> &'static str {
        "stylesheet_model_known"
    }

    fn apply(&self, graph: &Graph) -> Vec<Diagnostic> {
        let stylesheet_str = graph.model_stylesheet();
        if stylesheet_str.is_empty() {
            return Vec::new();
        }
        let Ok(stylesheet) = parse_stylesheet(stylesheet_str) else {
            return Vec::new(); // syntax errors caught by stylesheet_syntax rule
        };

        let mut diagnostics = Vec::new();
        for rule in &stylesheet.rules {
            if !graph.nodes.iter().any(|(node_id, node)| {
                rule.selector.matches_node(node_id, node) && node_needs_api_backend(node)
            }) {
                continue;
            }
            let label = Self::selector_label(&rule.selector);
            for decl in &rule.declarations {
                let context = format!("in stylesheet rule '{label}'");
                match decl.property.as_str() {
                    "model" => {
                        if let Some(d) = check_model_known(
                            self.name(),
                            self.catalog,
                            &decl.value,
                            &context,
                            None,
                        ) {
                            diagnostics.push(d);
                        }
                    }
                    "provider" => {
                        if let Some(d) = check_provider_known(
                            self.name(),
                            self.catalog,
                            &decl.value,
                            &context,
                            None,
                        ) {
                            diagnostics.push(d);
                        }
                    }
                    _ => {}
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
    fn stylesheet_model_known_rule_valid() {
        let mut graph = minimal_graph();
        let api = Node::new("api");
        graph.nodes.insert(api.id.clone(), api);
        graph.attrs.insert(
            "model_stylesheet".to_string(),
            AttrValue::String("* { model: claude-sonnet-4.5; provider: anthropic; }".to_string()),
        );
        let catalog = test_catalog();
        let rule = Rule { catalog: &catalog };
        assert!(rule.apply(&graph).is_empty());
    }

    #[test]
    fn stylesheet_model_known_rule_unknown_model() {
        let mut graph = minimal_graph();
        let opus = Node::new("opus");
        graph.nodes.insert(opus.id.clone(), opus);
        graph.attrs.insert(
            "model_stylesheet".to_string(),
            AttrValue::String("#opus { model: claude-opus-4-5; }".to_string()),
        );
        let catalog = test_catalog();
        let rule = Rule { catalog: &catalog };
        let diagnostics = rule.apply(&graph);
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].severity, Severity::Warning);
        assert!(diagnostics[0].message.contains("claude-opus-4-5"));
        assert!(diagnostics[0].message.contains("#opus"));
    }

    #[test]
    fn stylesheet_model_known_rule_unknown_provider() {
        let mut graph = minimal_graph();
        let api = Node::new("api");
        graph.nodes.insert(api.id.clone(), api);
        graph.attrs.insert(
            "model_stylesheet".to_string(),
            AttrValue::String("* { provider: nonexistent-provider; }".to_string()),
        );
        let catalog = test_catalog();
        let rule = Rule { catalog: &catalog };
        let diagnostics = rule.apply(&graph);
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].severity, Severity::Warning);
        assert!(diagnostics[0].message.contains("nonexistent-provider"));
    }

    #[test]
    fn stylesheet_model_known_rule_alias() {
        let mut graph = minimal_graph();
        let api = Node::new("api");
        graph.nodes.insert(api.id.clone(), api);
        graph.attrs.insert(
            "model_stylesheet".to_string(),
            AttrValue::String("* { model: opus; }".to_string()),
        );
        let catalog = test_catalog();
        let rule = Rule { catalog: &catalog };
        assert!(rule.apply(&graph).is_empty());
    }

    #[test]
    fn acp_only_stylesheet_model_and_provider_hints_are_opaque() {
        let mut graph = minimal_graph();
        let mut external = Node::new("external");
        external
            .attrs
            .insert("backend".to_string(), AttrValue::String("acp".to_string()));
        graph.nodes.insert(external.id.clone(), external);
        graph.attrs.insert(
            "model_stylesheet".to_string(),
            AttrValue::String(
                "#external { model: harness-only-model; provider: external-harness; }".to_string(),
            ),
        );

        let catalog = test_catalog();
        let rule = Rule { catalog: &catalog };
        assert!(rule.apply(&graph).is_empty());
    }

    #[test]
    fn mixed_graph_validates_only_stylesheet_models_used_by_api_nodes() {
        let mut graph = minimal_graph();
        let mut external = Node::new("external");
        external
            .attrs
            .insert("backend".to_string(), AttrValue::String("acp".to_string()));
        graph.nodes.insert(external.id.clone(), external);
        let api = Node::new("api");
        graph.nodes.insert(api.id.clone(), api);
        graph.attrs.insert(
            "model_stylesheet".to_string(),
            AttrValue::String(
                "#external { model: harness-only-model; provider: external-harness; } \
                 #api { model: missing-api-model; provider: missing-api-provider; }"
                    .to_string(),
            ),
        );

        let catalog = test_catalog();
        let rule = Rule { catalog: &catalog };
        let diagnostics = rule.apply(&graph);

        assert_eq!(diagnostics.len(), 2);
        assert!(diagnostics.iter().all(|diagnostic| {
            diagnostic.message.contains("missing-api-")
                && !diagnostic.message.contains("harness-only-model")
                && !diagnostic.message.contains("external-harness")
        }));
    }

    #[test]
    fn stylesheet_model_known_rule_no_stylesheet() {
        let graph = minimal_graph();
        let catalog = test_catalog();
        let rule = Rule { catalog: &catalog };
        assert!(rule.apply(&graph).is_empty());
    }
}
