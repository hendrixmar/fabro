use std::path::PathBuf;

use fabro_graphviz::graph::Graph;
use fabro_util::Home;

use crate::{Diagnostic, LintRule, Severity};

pub(super) fn rule() -> Box<dyn LintRule> {
    Box::new(Rule {
        skills_root: Home::from_env().skills_dir(),
    })
}

struct Rule {
    skills_root: PathBuf,
}

impl LintRule for Rule {
    fn name(&self) -> &'static str {
        "skills_known"
    }

    fn apply(&self, graph: &Graph) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();
        for node in graph.nodes.values() {
            let Some(names) = node.skills_attr() else {
                continue;
            };
            for name in names {
                if self.skills_root.join(&name).is_dir() {
                    continue;
                }
                diagnostics.push(Diagnostic {
                    rule: self.name().to_string(),
                    severity: Severity::Warning,
                    message: format!(
                        "skill '{name}' not in engine skill library ({}); repo-local skills \
                         cannot be checked at validate time",
                        self.skills_root.display()
                    ),
                    node_id: Some(node.id.clone()),
                    edge: None,
                    fix: Some(format!(
                        "Seed the skill with: cp -r <skill> {}/",
                        self.skills_root.join(&name).display()
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
    use std::path::PathBuf;

    use super::Rule;
    use crate::rules::test_support::{minimal_graph, node_with_attrs};
    use crate::{LintRule, Severity};

    fn temp_skills_root(tag: &str, skill_names: &[&str]) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "fabro-skills-known-{tag}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("create temp skills root");
        for name in skill_names {
            std::fs::create_dir_all(root.join(name)).expect("create skill dir");
            std::fs::write(root.join(name).join("SKILL.md"), "# skill\n").expect("write SKILL.md");
        }
        root
    }

    #[test]
    fn skills_known_passes_without_skills_attr() {
        let graph = minimal_graph();
        let rule = Rule {
            skills_root: PathBuf::from("/nonexistent"),
        };
        assert!(rule.apply(&graph).is_empty());
    }

    #[test]
    fn skills_known_accepts_seeded_skills() {
        let root = temp_skills_root("seeded", &["tdd", "diagnosing-bugs"]);
        let mut graph = minimal_graph();
        graph.nodes.insert(
            "build".to_string(),
            node_with_attrs("build", &[("skills", "tdd, diagnosing-bugs")]),
        );

        let rule = Rule { skills_root: root.clone() };
        assert!(rule.apply(&graph).is_empty());
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn skills_known_warns_on_missing_skill() {
        let root = temp_skills_root("missing", &["tdd"]);
        let mut graph = minimal_graph();
        graph.nodes.insert(
            "build".to_string(),
            node_with_attrs("build", &[("skills", "tdd,no-such-skill")]),
        );

        let rule = Rule { skills_root: root.clone() };
        let diagnostics = rule.apply(&graph);
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].severity, Severity::Warning);
        assert!(
            diagnostics[0]
                .message
                .contains("skill 'no-such-skill' not in engine skill library")
        );
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn skills_known_ignores_empty_entries() {
        let root = temp_skills_root("empty-entries", &["tdd"]);
        let mut graph = minimal_graph();
        graph.nodes.insert(
            "build".to_string(),
            node_with_attrs("build", &[("skills", "tdd,, ")]),
        );

        let rule = Rule { skills_root: root.clone() };
        assert!(rule.apply(&graph).is_empty());
        std::fs::remove_dir_all(&root).ok();
    }
}
