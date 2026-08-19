use std::collections::HashMap;

use fabro_acp::AcpToolKind;
use fabro_types::{AgentToolCategory, AgentToolSource, AgentToolSummary};

const OMP_BUILTINS: &[(&str, &str, AgentToolCategory)] = &[
    (
        "ast_edit",
        "Structural codemod via ast-grep patterns; preview-staged before write.",
        AgentToolCategory::Write,
    ),
    (
        "ast_grep",
        "Structural code search via ast-grep patterns.",
        AgentToolCategory::Read,
    ),
    (
        "bash",
        "Run shell commands in a persistent session.",
        AgentToolCategory::Shell,
    ),
    (
        "browser",
        "Drive a real Chromium tab via Puppeteer.",
        AgentToolCategory::Other,
    ),
    (
        "debug",
        "DAP-driven breakpoints, stepping, and locals inspection.",
        AgentToolCategory::Other,
    ),
    (
        "edit",
        "Line-anchored patches verified against file snapshots.",
        AgentToolCategory::Write,
    ),
    (
        "eval",
        "Run Python or JavaScript cells in a persistent kernel.",
        AgentToolCategory::Shell,
    ),
    (
        "find",
        "Fast file-name lookup by glob.",
        AgentToolCategory::Read,
    ),
    (
        "generate_image",
        "Structured image generation.",
        AgentToolCategory::Other,
    ),
    (
        "github",
        "GitHub repository, pull request, search, and Actions operations.",
        AgentToolCategory::Other,
    ),
    (
        "inspect_image",
        "Inspect a local image with a vision model.",
        AgentToolCategory::Read,
    ),
    (
        "irc",
        "Short messages between peer agents.",
        AgentToolCategory::Subagent,
    ),
    (
        "job",
        "List, wait on, or cancel background jobs.",
        AgentToolCategory::Subagent,
    ),
    (
        "lsp",
        "Language-server navigation, refactoring, actions, and diagnostics.",
        AgentToolCategory::Read,
    ),
    (
        "read",
        "Read files, directories, archives, data, documents, images, and URLs.",
        AgentToolCategory::Read,
    ),
    (
        "recipe",
        "Run a target from the project task runner.",
        AgentToolCategory::Shell,
    ),
    (
        "report_tool_issue",
        "Report unexpected tool behavior for QA tracking.",
        AgentToolCategory::Other,
    ),
    (
        "resolve",
        "Apply or discard a pending preview action.",
        AgentToolCategory::Write,
    ),
    (
        "search",
        "Regex content search across project data.",
        AgentToolCategory::Read,
    ),
    (
        "task",
        "Spawn parallel subagents.",
        AgentToolCategory::Subagent,
    ),
    (
        "todo",
        "Track phased tasks.",
        AgentToolCategory::Other,
    ),
    (
        "web_search",
        "Run a web search through the configured provider.",
        AgentToolCategory::Read,
    ),
    (
        "write",
        "Create or overwrite files and supported data targets.",
        AgentToolCategory::Write,
    ),
];

pub(super) struct AcpObservedTool<'a> {
    pub(super) title:     &'a str,
    pub(super) kind:      AcpToolKind,
    pub(super) raw_input: &'a serde_json::Value,
}

pub(super) struct AcpToolInventory {
    harness:    Option<String>,
    tools:      Vec<AgentToolSummary>,
    name_index: HashMap<String, usize>,
}

impl AcpToolInventory {
    pub(super) fn for_harness(harness: Option<&str>) -> Self {
        let capacity = if harness == Some("omp") {
            OMP_BUILTINS.len()
        } else {
            0
        };
        let mut inventory = Self {
            harness: harness.map(str::to_owned),
            tools: Vec::with_capacity(capacity),
            name_index: HashMap::with_capacity(capacity),
        };

        if harness == Some("omp") {
            for &(name, description, category) in OMP_BUILTINS {
                inventory.append(AgentToolSummary {
                    name: name.to_owned(),
                    description: description.to_owned(),
                    source: AgentToolSource::Native,
                    category,
                    invoked: false,
                });
            }
        }

        inventory
    }

    pub(super) fn snapshot(&self) -> Vec<AgentToolSummary> {
        self.tools.clone()
    }

    pub(super) fn observe(&mut self, observed: AcpObservedTool<'_>) -> bool {
        let name = canonical_name(self.harness.as_deref(), &observed);
        if let Some(index) = self.name_index.get(&name).copied() {
            let tool = &mut self.tools[index];
            if tool.invoked {
                return false;
            }
            tool.invoked = true;
            return true;
        }

        let category = category_for_kind(observed.kind);
        self.append(AgentToolSummary {
            name,
            description: "Observed ACP tool".to_owned(),
            source: AgentToolSource::Native,
            category,
            invoked: true,
        });
        true
    }

    fn append(&mut self, tool: AgentToolSummary) {
        let index = self.tools.len();
        self.name_index.insert(tool.name.clone(), index);
        self.tools.push(tool);
    }
}

fn canonical_name(harness: Option<&str>, observed: &AcpObservedTool<'_>) -> String {
    let lower = observed.title.trim().to_ascii_lowercase();
    if harness == Some("omp") {
        return match observed.kind {
            AcpToolKind::Read => "read".to_owned(),
            AcpToolKind::Search => "search".to_owned(),
            AcpToolKind::Execute => "bash".to_owned(),
            AcpToolKind::Edit | AcpToolKind::Delete | AcpToolKind::Move => "edit".to_owned(),
            AcpToolKind::Fetch => "web_search".to_owned(),
            AcpToolKind::Think
                if identifies_todo_or_plan(&lower)
                    || raw_input_identifies_todo_or_plan(observed.raw_input) =>
            {
                "todo".to_owned()
            }
            _ => normalize_extension_name(&lower),
        };
    }
    if lower.starts_with("read file ") {
        "read_file".into()
    } else if lower == "list files" || lower.starts_with("list files ") {
        "list_files".into()
    } else if matches!(observed.kind, AcpToolKind::Execute) {
        "shell".into()
    } else {
        normalize_extension_name(&lower)
    }
}

fn identifies_todo_or_plan(text: &str) -> bool {
    text.split(|character: char| !character.is_ascii_alphanumeric())
        .any(|word| word.eq_ignore_ascii_case("todo") || word.eq_ignore_ascii_case("plan"))
}

fn raw_input_identifies_todo_or_plan(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::String(text) => identifies_todo_or_plan(text),
        serde_json::Value::Array(items) => {
            items.iter().any(raw_input_identifies_todo_or_plan)
        }
        serde_json::Value::Object(fields) => fields.iter().any(|(key, value)| {
            identifies_todo_or_plan(key) || raw_input_identifies_todo_or_plan(value)
        }),
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => false,
    }
}

fn normalize_extension_name(name: &str) -> String {
    let mut normalized = String::with_capacity(name.len());
    let mut separated = false;
    for character in name.chars() {
        if character.is_ascii_alphanumeric() {
            if separated && !normalized.is_empty() {
                normalized.push('_');
            }
            normalized.push(character.to_ascii_lowercase());
            separated = false;
        } else if !normalized.is_empty() {
            separated = true;
        }
    }

    if normalized.is_empty() {
        "tool".to_owned()
    } else {
        normalized
    }
}

fn category_for_kind(kind: AcpToolKind) -> AgentToolCategory {
    match kind {
        AcpToolKind::Read | AcpToolKind::Search | AcpToolKind::Fetch => AgentToolCategory::Read,
        AcpToolKind::Edit | AcpToolKind::Delete | AcpToolKind::Move => AgentToolCategory::Write,
        AcpToolKind::Execute => AgentToolCategory::Shell,
        AcpToolKind::Think | AcpToolKind::SwitchMode | AcpToolKind::Other => {
            AgentToolCategory::Other
        }
    }
}

#[cfg(test)]
mod tests {
    use fabro_acp::AcpToolKind;
    use fabro_types::{AgentToolCategory, AgentToolSource};

    use super::{AcpObservedTool, AcpToolInventory};

    #[test]
    fn omp_catalog_contains_exact_documented_builtin_names() {
        let inventory = AcpToolInventory::for_harness(Some("omp"));
        let snapshot = inventory.snapshot();
        let actual = snapshot
            .iter()
            .map(|tool| {
                (
                    tool.name.as_str(),
                    tool.description.as_str(),
                    tool.category,
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(
            actual,
            vec![
                (
                    "ast_edit",
                    "Structural codemod via ast-grep patterns; preview-staged before write.",
                    AgentToolCategory::Write,
                ),
                (
                    "ast_grep",
                    "Structural code search via ast-grep patterns.",
                    AgentToolCategory::Read,
                ),
                (
                    "bash",
                    "Run shell commands in a persistent session.",
                    AgentToolCategory::Shell,
                ),
                (
                    "browser",
                    "Drive a real Chromium tab via Puppeteer.",
                    AgentToolCategory::Other,
                ),
                (
                    "debug",
                    "DAP-driven breakpoints, stepping, and locals inspection.",
                    AgentToolCategory::Other,
                ),
                (
                    "edit",
                    "Line-anchored patches verified against file snapshots.",
                    AgentToolCategory::Write,
                ),
                (
                    "eval",
                    "Run Python or JavaScript cells in a persistent kernel.",
                    AgentToolCategory::Shell,
                ),
                (
                    "find",
                    "Fast file-name lookup by glob.",
                    AgentToolCategory::Read,
                ),
                (
                    "generate_image",
                    "Structured image generation.",
                    AgentToolCategory::Other,
                ),
                (
                    "github",
                    "GitHub repository, pull request, search, and Actions operations.",
                    AgentToolCategory::Other,
                ),
                (
                    "inspect_image",
                    "Inspect a local image with a vision model.",
                    AgentToolCategory::Read,
                ),
                (
                    "irc",
                    "Short messages between peer agents.",
                    AgentToolCategory::Subagent,
                ),
                (
                    "job",
                    "List, wait on, or cancel background jobs.",
                    AgentToolCategory::Subagent,
                ),
                (
                    "lsp",
                    "Language-server navigation, refactoring, actions, and diagnostics.",
                    AgentToolCategory::Read,
                ),
                (
                    "read",
                    "Read files, directories, archives, data, documents, images, and URLs.",
                    AgentToolCategory::Read,
                ),
                (
                    "recipe",
                    "Run a target from the project task runner.",
                    AgentToolCategory::Shell,
                ),
                (
                    "report_tool_issue",
                    "Report unexpected tool behavior for QA tracking.",
                    AgentToolCategory::Other,
                ),
                (
                    "resolve",
                    "Apply or discard a pending preview action.",
                    AgentToolCategory::Write,
                ),
                (
                    "search",
                    "Regex content search across project data.",
                    AgentToolCategory::Read,
                ),
                (
                    "task",
                    "Spawn parallel subagents.",
                    AgentToolCategory::Subagent,
                ),
                (
                    "todo",
                    "Track phased tasks.",
                    AgentToolCategory::Other,
                ),
                (
                    "web_search",
                    "Run a web search through the configured provider.",
                    AgentToolCategory::Read,
                ),
                (
                    "write",
                    "Create or overwrite files and supported data targets.",
                    AgentToolCategory::Write,
                ),
            ]
        );
        assert!(snapshot.iter().all(|tool| {
            tool.source == AgentToolSource::Native && !tool.invoked
        }));
    }

    #[test]
    fn codex_read_titles_collapse_to_one_canonical_tool() {
        let mut inventory = AcpToolInventory::for_harness(Some("codex"));
        assert!(inventory.snapshot().is_empty());
        assert!(inventory.observe(observed("Read file '/a.rs'", AcpToolKind::Read)));
        assert!(!inventory.observe(observed("Read file '/b.rs'", AcpToolKind::Read)));
        assert_eq!(
            inventory
                .snapshot()
                .iter()
                .map(|tool| tool.name.as_str())
                .collect::<Vec<_>>(),
            ["read_file"]
        );
    }

    #[test]
    fn codex_list_and_execute_titles_use_canonical_names() {
        let mut inventory = AcpToolInventory::for_harness(Some("codex"));
        assert!(inventory.observe(observed("List files in src", AcpToolKind::Read)));
        assert!(inventory.observe(observed("$ cargo check", AcpToolKind::Execute)));
        assert_eq!(
            inventory
                .snapshot()
                .into_iter()
                .map(|tool| tool.name)
                .collect::<Vec<_>>(),
            ["list_files", "shell"]
        );
    }

    #[test]
    fn omp_observation_marks_catalog_entry_and_appends_plugin() {
        let mut inventory = AcpToolInventory::for_harness(Some("omp"));
        assert!(inventory.observe(observed("read_small_file", AcpToolKind::Read)));
        assert!(
            inventory
                .snapshot()
                .iter()
                .find(|tool| tool.name == "read")
                .unwrap()
                .invoked
        );

        assert!(inventory.observe(observed("my_plugin_action", AcpToolKind::Other)));
        assert!(inventory.snapshot().iter().any(|tool| {
            tool.name == "my_plugin_action" && tool.invoked
        }));
    }

    #[test]
    fn omp_think_title_marks_official_todo_entry() {
        let mut inventory = AcpToolInventory::for_harness(Some("omp"));

        assert!(inventory.observe(observed("Plan next steps", AcpToolKind::Think)));

        let snapshot = inventory.snapshot();
        assert!(snapshot.iter().find(|tool| tool.name == "todo").unwrap().invoked);
        assert!(!snapshot.iter().any(|tool| tool.name == "plan_next_steps"));
    }

    #[test]
    fn omp_think_raw_input_marks_official_todo_entry() {
        let mut inventory = AcpToolInventory::for_harness(Some("omp"));
        let raw_input = serde_json::json!({"operation": "update_todo", "items": []});

        assert!(inventory.observe(AcpObservedTool {
            title: "Think",
            kind: AcpToolKind::Think,
            raw_input: &raw_input,
        }));

        let snapshot = inventory.snapshot();
        assert!(snapshot.iter().find(|tool| tool.name == "todo").unwrap().invoked);
        assert!(!snapshot.iter().any(|tool| tool.name == "think"));
    }

    #[test]
    fn omp_unrelated_think_appends_observed_extension() {
        let mut inventory = AcpToolInventory::for_harness(Some("omp"));
        let raw_input = serde_json::json!({"question": "Which approach is safer?"});

        assert!(inventory.observe(AcpObservedTool {
            title: "Analyze architecture",
            kind: AcpToolKind::Think,
            raw_input: &raw_input,
        }));

        let snapshot = inventory.snapshot();
        assert!(!snapshot.iter().find(|tool| tool.name == "todo").unwrap().invoked);
        assert!(snapshot.iter().any(|tool| {
            tool.name == "analyze_architecture" && tool.invoked
        }));
    }

    #[test]
    fn unknown_titles_normalize_to_stable_snake_case_names() {
        let mut inventory = AcpToolInventory::for_harness(Some("codex"));
        assert!(inventory.observe(observed(
            "  Deploy!!! Stage @ PROD  ",
            AcpToolKind::Other,
        )));
        assert!(!inventory.observe(observed(
            "deploy stage -- prod",
            AcpToolKind::Other,
        )));
        assert!(inventory.observe(observed(" !!! ", AcpToolKind::Other)));

        let snapshot = inventory.snapshot();
        assert_eq!(
            snapshot
                .iter()
                .map(|tool| tool.name.as_str())
                .collect::<Vec<_>>(),
            ["deploy_stage_prod", "tool"]
        );
        assert!(snapshot.iter().all(|tool| {
            tool.description == "Observed ACP tool"
                && tool.source == AgentToolSource::Native
                && tool.invoked
        }));
    }

    #[test]
    fn unknown_tool_category_is_derived_from_structured_kind() {
        let cases = [
            (AcpToolKind::Read, AgentToolCategory::Read),
            (AcpToolKind::Search, AgentToolCategory::Read),
            (AcpToolKind::Fetch, AgentToolCategory::Read),
            (AcpToolKind::Edit, AgentToolCategory::Write),
            (AcpToolKind::Delete, AgentToolCategory::Write),
            (AcpToolKind::Move, AgentToolCategory::Write),
            (AcpToolKind::Execute, AgentToolCategory::Shell),
            (AcpToolKind::Think, AgentToolCategory::Other),
            (AcpToolKind::SwitchMode, AgentToolCategory::Other),
            (AcpToolKind::Other, AgentToolCategory::Other),
        ];

        for (kind, expected) in cases {
            let mut inventory = AcpToolInventory::for_harness(Some("codex"));
            assert!(inventory.observe(observed("Mystery action", kind)));
            assert_eq!(inventory.snapshot()[0].category, expected);
        }
    }

    #[test]
    fn observe_returns_true_only_for_inventory_changes() {
        let mut inventory = AcpToolInventory::for_harness(Some("omp"));
        assert!(inventory.observe(observed("read once", AcpToolKind::Read)));
        assert!(!inventory.observe(observed("read twice", AcpToolKind::Read)));
        assert!(inventory.observe(observed("Plugin action", AcpToolKind::Other)));
        assert!(!inventory.observe(observed("plugin-action", AcpToolKind::Other)));
    }

    fn observed(title: &str, kind: AcpToolKind) -> AcpObservedTool<'_> {
        AcpObservedTool {
            title,
            kind,
            raw_input: &serde_json::Value::Null,
        }
    }
}
