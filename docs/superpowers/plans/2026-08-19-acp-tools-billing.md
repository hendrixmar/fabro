# Canonical ACP Tools and Exact Billing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give OMP a documented available-tool catalog, group argument-specific ACP calls under stable identities, and surface exact codex/OMP token billing in Fabro's existing Tools and Billing UI.

**Architecture:** `fabro-acp` preserves structured ACP tool kinds and complete `PromptResponse.usage` behind its existing turn interface. `fabro-workflow` owns harness-specific catalog/canonicalization and translates exact external usage into the existing billing model; the run projection and React UI remain ACP-agnostic.

**Tech Stack:** Rust 2024, agent-client-protocol 0.11.1 with `unstable_session_usage`, Tokio, serde, Fabro run events/projections, React/Bun UI tests.

**Spec:** `docs/superpowers/specs/2026-08-19-acp-tools-billing-design.md`

## Global Constraints

- OMP baseline is the 23 built-ins documented at <https://omp.sh/docs/tools>; it is not represented as the exact live registry.
- Codex remains observed-only; do not invent a static catalog.
- Timeline titles remain unchanged; only dropdown identities are canonicalized.
- Exact per-turn ACP usage is authoritative for token buckets.
- `usage_update.used/size` is context occupancy, never billing.
- USD appears only when reported by the harness; missing cost remains `None`, never zero.
- Reuse `agent.tools.available`, `CodergenResult` billing, stage projection, and existing UI.
- Every production change follows RED → verify failure → GREEN → verify pass → commit.

---

## File structure

- Create `lib/components/fabro-workflow/src/handler/llm/acp_tools.rs`: OMP catalog, canonical identities, deduplication, categories, invoked state.
- Modify `lib/components/fabro-workflow/src/handler/llm/mod.rs`: declare the private `acp_tools` module.
- Modify `lib/components/fabro-workflow/src/handler/llm/acp.rs`: emit initial/live inventory snapshots and translate exact ACP usage.
- Modify `lib/components/fabro-acp/src/session.rs`: preserve structured tool kind, complete prompt responses, usage updates, and aggregate usage.
- Modify `lib/components/fabro-acp/src/lib.rs`: export ACP observation/usage types.
- Modify `lib/components/fabro-acp/src/test_support.rs`: fake-agent usage and cumulative-cost responses.
- Modify `lib/foundation/fabro-model/src/billing.rs`: add `ModelBillingFacts::Reported`.
- Modify `lib/components/fabro-workflow/src/outcome.rs`: construct reported external usage without catalog pricing.
- Modify `apps/fabro-web/app/routes/run-billing.test.tsx`: prove ACP stage rows render exact tokens and unknown cost correctly.
- Modify `apps/fabro-web/app/components/stage-insights-sidebar.test.tsx`: prove catalog/invoked counts render correctly.

---

### Task 1: Preserve structured ACP tool kind

**Files:**
- Modify: `lib/components/fabro-acp/src/session.rs:30-156`
- Modify: `lib/components/fabro-acp/src/lib.rs:18-20`
- Test: `lib/components/fabro-acp/src/session.rs` in-module tests

**Interfaces:**
- Produces:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcpToolKind {
    Read,
    Edit,
    Delete,
    Move,
    Search,
    Execute,
    Think,
    Fetch,
    SwitchMode,
    Other,
}
```

- Extends `AcpSessionActivity::ToolStarted` with `kind: AcpToolKind`.
- Later tasks consume `AcpToolKind`; no workflow module imports ACP SDK schema types.

- [ ] **Step 1: Write the failing tool-kind conversion test**

Add to `session.rs` tests:

```rust
#[test]
fn tool_call_preserves_structured_kind() {
    let mut tracked = HashMap::new();
    let started = SessionUpdate::ToolCall(
        ToolCall::new("call-read", "Read file /tmp/a.rs")
            .kind(ToolKind::Read)
            .raw_input(serde_json::json!({"path": "/tmp/a.rs"})),
    );

    let events = convert_session_update(&started, &mut tracked);
    assert!(matches!(
        &events[0],
        AcpSessionActivity::ToolStarted { kind: AcpToolKind::Read, .. }
    ));
}
```

Import `ToolKind` and `AcpToolKind` in the test module.

- [ ] **Step 2: Run the test to verify RED**

Run:

```bash
cargo test -p fabro-acp tool_call_preserves_structured_kind
```

Expected: compile failure because `AcpToolKind` and `ToolStarted.kind` do not exist.

- [ ] **Step 3: Implement the minimal kind adapter**

In `session.rs`, add `AcpToolKind` and a private conversion:

```rust
impl From<ToolKind> for AcpToolKind {
    fn from(value: ToolKind) -> Self {
        match value {
            ToolKind::Read => Self::Read,
            ToolKind::Edit => Self::Edit,
            ToolKind::Delete => Self::Delete,
            ToolKind::Move => Self::Move,
            ToolKind::Search => Self::Search,
            ToolKind::Execute => Self::Execute,
            ToolKind::Think => Self::Think,
            ToolKind::Fetch => Self::Fetch,
            ToolKind::SwitchMode => Self::SwitchMode,
            _ => Self::Other,
        }
    }
}
```

Store `kind: AcpToolKind` in `TrackedToolCall`. Populate it from `ToolCall.kind`. On a `ToolCallUpdate`, update the tracked value when `fields.kind` is present, otherwise retain the existing value. Include it in every `ToolStarted` activity.

Export `AcpToolKind` from `fabro-acp/src/lib.rs`.

- [ ] **Step 4: Run focused and crate tests to verify GREEN**

```bash
cargo test -p fabro-acp tool_call_preserves_structured_kind
cargo test -p fabro-acp
```

Expected: all fabro-acp tests pass.

- [ ] **Step 5: Commit**

```bash
git add lib/components/fabro-acp/src/session.rs lib/components/fabro-acp/src/lib.rs
git commit -m "feat(acp): preserve structured tool kinds"
```

---

### Task 2: Implement canonical catalog-backed ACP tool inventory

**Files:**
- Create: `lib/components/fabro-workflow/src/handler/llm/acp_tools.rs`
- Modify: `lib/components/fabro-workflow/src/handler/llm/mod.rs`
- Test: `lib/components/fabro-workflow/src/handler/llm/acp_tools.rs` in-module tests

**Interfaces:**
- Consumes: `fabro_acp::AcpToolKind` from Task 1.
- Produces:

```rust
pub struct AcpObservedTool<'a> {
    pub title: &'a str,
    pub kind: AcpToolKind,
    pub raw_input: &'a serde_json::Value,
}

pub struct AcpToolInventory { /* private map + deterministic order */ }

impl AcpToolInventory {
    pub fn for_harness(harness: Option<&str>) -> Self;
    pub fn snapshot(&self) -> Vec<AgentToolSummary>;
    pub fn observe(&mut self, observed: AcpObservedTool<'_>) -> bool;
}
```

- `observe` returns `true` only when a new entry is appended or `invoked` changes from false to true.

- [ ] **Step 1: Write failing OMP catalog tests**

Create `acp_tools.rs` with tests first:

```rust
#[test]
fn omp_catalog_contains_exact_documented_builtin_names() {
    let inventory = AcpToolInventory::for_harness(Some("omp"));
    let names = inventory.snapshot().into_iter().map(|tool| tool.name).collect::<Vec<_>>();
    assert_eq!(names, vec![
        "ast_edit", "ast_grep", "bash", "browser", "debug", "edit", "eval", "find",
        "generate_image", "github", "inspect_image", "irc", "job", "lsp", "read",
        "recipe", "report_tool_issue", "resolve", "search", "task", "todo", "web_search",
        "write",
    ]);
    assert!(inventory.snapshot().iter().all(|tool| !tool.invoked));
}
```

```rust
#[test]
fn codex_read_titles_collapse_to_one_canonical_tool() {
    let mut inventory = AcpToolInventory::for_harness(Some("codex"));
    assert!(inventory.observe(observed("Read file '/a.rs'", AcpToolKind::Read)));
    assert!(!inventory.observe(observed("Read file '/b.rs'", AcpToolKind::Read)));
    assert_eq!(inventory.snapshot().iter().map(|tool| tool.name.as_str()).collect::<Vec<_>>(), ["read_file"]);
}
```

```rust
#[test]
fn omp_observation_marks_catalog_entry_and_appends_plugin() {
    let mut inventory = AcpToolInventory::for_harness(Some("omp"));
    assert!(inventory.observe(observed("read_small_file", AcpToolKind::Read)));
    assert!(inventory.snapshot().iter().find(|tool| tool.name == "read").unwrap().invoked);

    assert!(inventory.observe(observed("my_plugin_action", AcpToolKind::Other)));
    assert!(inventory.snapshot().iter().any(|tool| tool.name == "my_plugin_action" && tool.invoked));
}
```

Test helper:

```rust
fn observed(title: &str, kind: AcpToolKind) -> AcpObservedTool<'_> {
    AcpObservedTool { title, kind, raw_input: &serde_json::Value::Null }
}
```

- [ ] **Step 2: Run tests to verify RED**

```bash
cargo test -p fabro-workflow --lib acp_tools
```

Expected: compile failure because the module/types do not exist.

- [ ] **Step 3: Implement the OMP catalog and canonicalizer**

Declare `mod acp_tools;` in `llm/mod.rs`.

Use a private ordered vector plus name index (or a `BTreeMap` plus explicit catalog order). Each official entry must carry its official summary and category. Use these exact tuples:

```rust
const OMP_BUILTINS: &[(&str, &str, AgentToolCategory)] = &[
    ("ast_edit", "Structural codemod via ast-grep patterns; preview-staged before write.", AgentToolCategory::Write),
    ("ast_grep", "Structural code search via ast-grep patterns.", AgentToolCategory::Read),
    ("bash", "Run shell commands in a persistent session.", AgentToolCategory::Shell),
    ("browser", "Drive a real Chromium tab via Puppeteer.", AgentToolCategory::Other),
    ("debug", "DAP-driven breakpoints, stepping, and locals inspection.", AgentToolCategory::Other),
    ("edit", "Line-anchored patches verified against file snapshots.", AgentToolCategory::Write),
    ("eval", "Run Python or JavaScript cells in a persistent kernel.", AgentToolCategory::Shell),
    ("find", "Fast file-name lookup by glob.", AgentToolCategory::Read),
    ("generate_image", "Structured image generation.", AgentToolCategory::Other),
    ("github", "GitHub repository, pull request, search, and Actions operations.", AgentToolCategory::Other),
    ("inspect_image", "Inspect a local image with a vision model.", AgentToolCategory::Read),
    ("irc", "Short messages between peer agents.", AgentToolCategory::Subagent),
    ("job", "List, wait on, or cancel background jobs.", AgentToolCategory::Subagent),
    ("lsp", "Language-server navigation, refactoring, actions, and diagnostics.", AgentToolCategory::Read),
    ("read", "Read files, directories, archives, data, documents, images, and URLs.", AgentToolCategory::Read),
    ("recipe", "Run a target from the project task runner.", AgentToolCategory::Shell),
    ("report_tool_issue", "Report unexpected tool behavior for QA tracking.", AgentToolCategory::Other),
    ("resolve", "Apply or discard a pending preview action.", AgentToolCategory::Write),
    ("search", "Regex content search across project data.", AgentToolCategory::Read),
    ("task", "Spawn parallel subagents.", AgentToolCategory::Subagent),
    ("todo", "Track phased tasks.", AgentToolCategory::Other),
    ("web_search", "Run a web search through the configured provider.", AgentToolCategory::Read),
    ("write", "Create or overwrite files and supported data targets.", AgentToolCategory::Write),
];
```

Canonicalization rules:

```rust
fn canonical_name(harness: Option<&str>, observed: &AcpObservedTool<'_>) -> String {
    let lower = observed.title.trim().to_ascii_lowercase();
    if harness == Some("omp") {
        return match observed.kind {
            AcpToolKind::Read => "read",
            AcpToolKind::Search => "search",
            AcpToolKind::Execute => "bash",
            AcpToolKind::Edit | AcpToolKind::Delete | AcpToolKind::Move => "edit",
            AcpToolKind::Fetch => "web_search",
            _ => normalize_extension_name(&lower),
        }.to_string();
    }
    if lower.starts_with("read file ") { "read_file".into() }
    else if lower == "list files" || lower.starts_with("list files ") { "list_files".into() }
    else if matches!(observed.kind, AcpToolKind::Execute) { "shell".into() }
    else { normalize_extension_name(&lower) }
}
```

`normalize_extension_name` lowercases, replaces non-alphanumeric runs with `_`, trims `_`, and falls back to `tool` when empty.

Use `AgentToolSource::Native`; unknown entries get description `"Observed ACP tool"` and category derived from `AcpToolKind`.

- [ ] **Step 4: Run inventory tests to verify GREEN**

```bash
cargo test -p fabro-workflow --lib acp_tools
```

Expected: all inventory tests pass.

- [ ] **Step 5: Commit**

```bash
git add lib/components/fabro-workflow/src/handler/llm/acp_tools.rs lib/components/fabro-workflow/src/handler/llm/mod.rs
git commit -m "feat: add canonical ACP tool inventory"
```

---

### Task 3: Wire catalog and canonical snapshots into ACP stage events

**Files:**
- Modify: `lib/components/fabro-workflow/src/handler/llm/acp.rs:250-330,832-925`
- Test: `lib/components/fabro-workflow/src/handler/llm/acp.rs` in-module tests

**Interfaces:**
- Consumes `AcpToolInventory` and `AcpObservedTool` from Task 2.
- Produces existing `Event::AgentToolsAvailable`; no event schema change.

- [ ] **Step 1: Replace the current exact-title test with failing canonical/catalog tests**

Update `acp_observed_tools_populate_tools_available_snapshots` into two tests.

OMP initial/live test:

```rust
#[test]
fn omp_acp_tools_emit_catalog_then_invoked_snapshot() {
    let (emitter, event_rx, callback) = callback_harness("omp");
    let initial = next_tools_snapshot(&event_rx);
    assert_eq!(initial.tools.len(), 23);
    assert_eq!(initial.tools.iter().filter(|tool| tool.invoked).count(), 0);

    callback(AcpSessionActivity::ToolStarted {
        tool_call_id: "read-1".into(),
        tool_name: "read one file".into(),
        title: "read one file".into(),
        kind: AcpToolKind::Read,
        raw_input: serde_json::json!({"path": "a.rs"}),
    });
    let updated = next_tools_snapshot(&event_rx);
    assert_eq!(updated.tools.iter().filter(|tool| tool.invoked).map(|tool| tool.name.as_str()).collect::<Vec<_>>(), ["read"]);
}
```

Codex dedup test:

```rust
#[test]
fn codex_acp_tools_group_argument_specific_titles() {
    let (_emitter, event_rx, callback) = callback_harness("codex");
    callback(tool_started("r1", "Read file '/a.rs'", AcpToolKind::Read));
    callback(tool_started("r2", "Read file '/b.rs'", AcpToolKind::Read));
    let snapshots = event_rx.try_iter().filter_map(to_tools_props).collect::<Vec<_>>();
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0].tools[0].name, "read_file");
}
```

`callback_harness` creates `session_activity_callback(..., Some(harness))`, captures events with `std::sync::mpsc`, and returns the emitter so its listener remains alive.

- [ ] **Step 2: Run tests to verify RED**

```bash
cargo test -p fabro-workflow --lib omp_acp_tools_emit_catalog_then_invoked_snapshot
cargo test -p fabro-workflow --lib codex_acp_tools_group_argument_specific_titles
```

Expected: failures because the callback has no harness-aware catalog and exact titles do not collapse.

- [ ] **Step 3: Replace exact-title tracking with `AcpToolInventory`**

Change `session_activity_callback` to accept `harness: Option<&str>` and initialize:

```rust
let inventory = Mutex::new(AcpToolInventory::for_harness(harness));
let initial = inventory.lock_recover().snapshot();
if !initial.is_empty() {
    emit_tools_snapshot(&emitter, &stage_scope, &node_id, &session_id, initial);
}
```

Do not add a lock-extension abstraction. Use an explicit poison-recovery `match` at the two lock sites to avoid hidden failure behavior.

On `ToolStarted`, call:

```rust
let changed = inventory.observe(AcpObservedTool {
    title: &tool_name,
    kind,
    raw_input: &raw_input,
});
if changed {
    emit_tools_snapshot(..., inventory.snapshot());
}
```

Pass `harness.as_deref()` from `run_turn` when constructing the callback. Preserve the original `tool_name` in `AgentEvent::ToolCallStarted`.

- [ ] **Step 4: Run tests and projection tests**

```bash
cargo test -p fabro-workflow --lib handler::llm::acp
cargo test -p fabro-store agent_tools_available_replaces_stage_agent_tools
```

Expected: ACP tests and projection replacement test pass.

- [ ] **Step 5: Commit**

```bash
git add lib/components/fabro-workflow/src/handler/llm/acp.rs
git commit -m "feat: publish canonical ACP tool catalogs"
```

---

### Task 4: Add reported external billing facts

**Files:**
- Modify: `lib/foundation/fabro-model/src/billing.rs:305-364,630-660`
- Modify: `lib/components/fabro-workflow/src/outcome.rs:1-55`
- Test: `lib/foundation/fabro-model/src/billing.rs` in-module tests
- Test: `lib/components/fabro-workflow/src/outcome.rs` in-module tests

**Interfaces:**
- Produces `ModelBillingFacts::Reported`.
- Produces:

```rust
pub fn reported_model_usage(
    model: ModelRef,
    tokens: TokenCounts,
    reported_cost: Option<UsdMicros>,
) -> BilledModelUsage;
```

- Later workflow ACP translation consumes this constructor.

- [ ] **Step 1: Write failing reported-facts billing tests**

In `fabro-model/src/billing.rs`:

```rust
#[test]
fn reported_facts_are_never_catalog_priced() {
    let input = ModelBillingInput {
        usage: ModelUsage {
            model: ModelRef { provider: ProviderId::new("omp"), model_id: ModelId::new("model"), speed: None },
            tokens: TokenCounts { input_tokens: 100, output_tokens: 20, ..TokenCounts::default() },
        },
        facts: ModelBillingFacts::Reported,
    };
    let pricing = sample_openai_pricing();
    assert_eq!(pricing.bill(&input), None);
}
```

In `workflow/src/outcome.rs`:

```rust
#[test]
fn reported_model_usage_keeps_exact_tokens_and_optional_cost() {
    let usage = reported_model_usage(
        model_ref("omp", "deepseek"),
        TokenCounts { input_tokens: 10, output_tokens: 3, cache_read_tokens: 5, ..Default::default() },
        Some(UsdMicros(42_000)),
    );
    assert_eq!(usage.tokens().total_tokens(), 18);
    assert_eq!(usage.total_usd_micros, Some(42_000));
    assert!(matches!(usage.input.facts, ModelBillingFacts::Reported));
}
```

- [ ] **Step 2: Run tests to verify RED**

```bash
cargo test -p fabro-model reported_facts_are_never_catalog_priced
cargo test -p fabro-workflow reported_model_usage_keeps_exact_tokens_and_optional_cost
```

Expected: compile failures because the variant/constructor do not exist.

- [ ] **Step 3: Implement `Reported` and constructor**

Add to `ModelBillingFacts`:

```rust
Reported,
```

The existing wildcard in `ModelPricing::bill` must continue returning `None` when pricing policy and facts do not match. Do not add `Reported` to `for_policy`.

In `workflow/src/outcome.rs`:

```rust
#[must_use]
pub fn reported_model_usage(
    model: ModelRef,
    tokens: TokenCounts,
    reported_cost: Option<UsdMicros>,
) -> BilledModelUsage {
    BilledModelUsage {
        input: ModelBillingInput {
            usage: ModelUsage { model, tokens },
            facts: ModelBillingFacts::Reported,
        },
        total_usd_micros: reported_cost.map(|cost| cost.0),
    }
}
```

- [ ] **Step 4: Run model/workflow tests**

```bash
cargo test -p fabro-model reported
cargo test -p fabro-workflow reported_model_usage
```

Expected: all focused tests pass.

- [ ] **Step 5: Commit**

```bash
git add lib/foundation/fabro-model/src/billing.rs lib/components/fabro-workflow/src/outcome.rs
git commit -m "feat: support provider-reported external billing"
```

---

### Task 5: Preserve exact ACP prompt usage and reported cost

**Files:**
- Modify: `lib/components/fabro-acp/src/session.rs:293-590`
- Modify: `lib/components/fabro-acp/src/lib.rs:18-20`
- Modify: `lib/components/fabro-acp/src/test_support.rs:23-274`
- Test: `lib/components/fabro-acp/src/session.rs` in-module/integration tests

**Interfaces:**
- Produces:

```rust
#[derive(Debug, Clone, Default, PartialEq)]
pub struct AcpRunUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub reasoning_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_write_tokens: u64,
    pub total_tokens: u64,
    pub reported_cost: Option<AcpReportedCost>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AcpReportedCost {
    pub amount: f64,
    pub currency: String,
}
```

- Extends `AcpRunResult` with `pub usage: Option<AcpRunUsage>`.
- Extends `AcpSessionActivity` with:

```rust
UsageUpdated {
    used: u64,
    size: u64,
    cost: Option<AcpReportedCost>,
}
```

- [ ] **Step 1: Extend the fake agent to return exact usage**

Before writing production code, modify the fake script so the final prompt response can be controlled by env:

```python
response = {"stopReason": os.environ.get("ACP_STOP_REASON", "end_turn")}
if os.environ.get("ACP_PROMPT_USAGE"):
    response["usage"] = json.loads(os.environ["ACP_PROMPT_USAGE"])
if os.environ.get("ACP_USAGE_UPDATE"):
    send({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {
            "sessionId": session_id,
            "update": {
                "sessionUpdate": "usage_update",
                **json.loads(os.environ["ACP_USAGE_UPDATE"]),
            },
        },
    })
respond(message, response)
```

Preserve all existing fake modes and send the usage update before the response.

- [ ] **Step 2: Write failing runtime usage test**

```rust
#[tokio::test]
async fn run_acp_turn_preserves_prompt_usage_and_reported_cost() {
    let request = fake_request_with_env(HashMap::from([
        ("ACP_PROMPT_USAGE".into(), r#"{"totalTokens":190,"inputTokens":100,"outputTokens":30,"thoughtTokens":10,"cachedReadTokens":40,"cachedWriteTokens":10}"#.into()),
        ("ACP_USAGE_UPDATE".into(), r#"{"used":190,"size":1000,"cost":{"amount":0.0123,"currency":"USD"}}"#.into()),
    ]));

    let result = run_acp_turn(request).await.unwrap();
    assert_eq!(result.usage, Some(AcpRunUsage {
        input_tokens: 100,
        output_tokens: 30,
        reasoning_tokens: 10,
        cache_read_tokens: 40,
        cache_write_tokens: 10,
        total_tokens: 190,
        reported_cost: Some(AcpReportedCost { amount: 0.0123, currency: "USD".into() }),
    }));
}
```

Add an accumulator unit test with two prompt usages; expected buckets and total are summed exactly once.

- [ ] **Step 3: Run tests to verify RED**

```bash
cargo test -p fabro-acp run_acp_turn_preserves_prompt_usage_and_reported_cost
```

Expected: compile failure because `AcpRunResult.usage` and usage types do not exist.

- [ ] **Step 4: Implement private full-response prompt delivery**

Do not call `ActiveSession::send_prompt`. Add a helper using the active connection:

```rust
fn send_prompt_with_response(
    session: &ActiveSession<'_, Agent>,
    prompt: String,
) -> Result<oneshot::Receiver<Result<PromptResponse, ProtocolError>>, ProtocolError> {
    let (tx, rx) = oneshot::channel();
    session
        .connection()
        .send_request_to(
            Agent,
            PromptRequest::new(session.session_id().clone(), vec![prompt.into()]),
        )
        .on_receiving_result(async move |result| {
            let _ = tx.send(result);
            Ok(())
        })?;
    Ok(rx)
}
```

Use the actual SDK error type required by `send_request_to`; do not convert errors to strings. The completion channel carries the original result so connection/protocol failures preserve their source.

Change `read_live_session` to select over session notifications and the active prompt response receiver. When the response arrives:

- aggregate `response.usage`;
- use `response.stop_reason` for the existing completion/steering logic;
- clear the active receiver;
- start follow-up steering prompts through the same helper.

Remove dependence on `SessionMessage::StopReason`; it will no longer occur because Fabro does not call `ActiveSession::send_prompt`.

- [ ] **Step 5: Parse usage updates and aggregate safely**

Extend `convert_session_update`:

```rust
SessionUpdate::UsageUpdate(update) => vec![AcpSessionActivity::UsageUpdated {
    used: update.used,
    size: update.size,
    cost: update.cost.as_ref().map(|cost| AcpReportedCost {
        amount: cost.amount,
        currency: cost.currency.clone(),
    }),
}],
```

Add a private accumulator that:

- saturating-adds each `PromptResponse.usage` bucket;
- calculates normalized `total_tokens` from the disjoint buckets;
- warns and uses the calculated bucket sum when a harness-reported total differs;
- retains the latest reported cost;
- returns `None` when no prompt usage and no valid cost were observed.

Call the public session-activity callback for `UsageUpdated` after updating the accumulator.

- [ ] **Step 6: Run focused, cancellation, steering, and full ACP tests**

```bash
cargo test -p fabro-acp run_acp_turn_preserves_prompt_usage_and_reported_cost
cargo test -p fabro-acp tool_call_start_and_completion_emit_once
cargo test -p fabro-workflow --lib acp_backend_accepts_steer_and_incorporates_followup_result
cargo test -p fabro-workflow --lib acp_backend_cancelled_stop_reason_maps_to_cancelled_error
cargo test -p fabro-acp
```

Expected: usage, steering, cancellation, tools, and all crate tests pass.

- [ ] **Step 7: Commit**

```bash
git add lib/components/fabro-acp/src/session.rs lib/components/fabro-acp/src/lib.rs lib/components/fabro-acp/src/test_support.rs
git commit -m "feat(acp): preserve exact prompt usage"
```

---

### Task 6: Translate ACP usage into stage and run billing

**Files:**
- Modify: `lib/components/fabro-workflow/src/handler/llm/acp.rs:250-555`
- Test: `lib/components/fabro-workflow/src/handler/llm/acp.rs` in-module integration tests
- Test: `lib/components/fabro-store/src/run_state.rs` billing projection tests

**Interfaces:**
- Consumes `AcpRunUsage`, `AcpReportedCost` from Task 5.
- Consumes `reported_model_usage` from Task 4.
- Produces `CodergenResult::Text.usage = Some(BilledModelUsage)` for valid ACP usage.

- [ ] **Step 1: Write failing workflow usage translation tests**

Add a direct conversion test:

```rust
#[test]
fn acp_usage_maps_to_harness_model_and_reported_cost() {
    let usage = AcpRunUsage { /* 100 in, 30 out, 10 reasoning, 40 cache read, 10 cache write, 190 total, USD 0.0123 */ };
    let billed = billed_acp_usage(Some("omp"), Some("deepseek"), usage).unwrap();
    assert_eq!(billed.model().provider.as_str(), "omp");
    assert_eq!(billed.model_id(), "deepseek");
    assert_eq!(billed.tokens().total_tokens(), 190);
    assert_eq!(billed.total_usd_micros, Some(12_300));
}
```

Add invalid cost cases:

```rust
for cost in [
    AcpReportedCost { amount: -1.0, currency: "USD".into() },
    AcpReportedCost { amount: f64::NAN, currency: "USD".into() },
    AcpReportedCost { amount: 1.0, currency: "EUR".into() },
] {
    assert_eq!(billed_acp_usage(Some("omp"), Some("m"), usage_with(cost)).unwrap().total_usd_micros, None);
}
```

Add fake-agent backend integration asserting `CodergenResult::Text.usage` is present.

- [ ] **Step 2: Run tests to verify RED**

```bash
cargo test -p fabro-workflow --lib acp_usage_maps_to_harness_model_and_reported_cost
```

Expected: compile failure because `billed_acp_usage` does not exist and ACP results still set `usage: None`.

- [ ] **Step 3: Implement conversion**

Add:

```rust
fn billed_acp_usage(
    harness: Option<&str>,
    model: Option<&str>,
    usage: AcpRunUsage,
) -> Option<BilledModelUsage>
```

Rules:

- return `None` only if every token bucket is zero and cost is absent;
- convert `u64` to `i64` with `i64::try_from(value).unwrap_or(i64::MAX)`;
- provider is `ProviderId::new(harness.unwrap_or("acp"))`;
- model id is node model, else harness, else `"external"`;
- convert valid USD using checked `amount * 1_000_000.0`, rounded to nearest integer and bounded by `i64::MAX`;
- warn and ignore unsupported cost.

Return `reported_model_usage(model_ref, tokens, reported_cost)`.

In `run_turn`, retain `harness` and node model through completion, then set:

```rust
usage: result.usage.and_then(|usage| billed_acp_usage(harness.as_deref(), node.model(), usage)),
```

Handle `AcpSessionActivity::UsageUpdated` in the workflow callback as a no-op for now; cost is consumed from `AcpRunResult`.

- [ ] **Step 4: Prove stage and run projections receive usage**

Add/extend a run-state test that applies `StageCompletedProps.billing` from an ACP-like model ref and asserts:

```rust
assert_eq!(stage.usage.input_tokens, 100);
assert_eq!(stage.usage.output_tokens, 30);
assert_eq!(stage.usage.total_usd_micros, Some(12_300));
assert_eq!(stage.model.as_ref().unwrap().provider.as_str(), "omp");
```

- [ ] **Step 5: Run full relevant suites**

```bash
cargo test -p fabro-workflow --lib handler::llm::acp
cargo test -p fabro-store stage
cargo test -p fabro-model
cargo test -p fabro-acp
```

Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add lib/components/fabro-workflow/src/handler/llm/acp.rs lib/components/fabro-store/src/run_state.rs
git commit -m "feat: project ACP usage into stage billing"
```

---

### Task 7: Verify existing UI semantics for catalog counts and ACP billing

**Files:**
- Modify test: `apps/fabro-web/app/components/stage-insights-sidebar.test.tsx`
- Modify test: `apps/fabro-web/app/routes/run-billing.test.tsx`
- No production UI file change expected.

**Interfaces:**
- Consumes existing `StageProjection.agent_tools` and `RunBilling` data.
- Proves no ACP-specific frontend branch is required.

- [ ] **Step 1: Add Tools count regression test**

Render a stage with 23 OMP summaries, four invoked. Assert the sidebar text contains `TOOLS 4/23`, invoked tools have the active indicator, and inactive catalog names remain listed.

Use existing `makeStage` and renderer helpers; construct summaries with real fields:

```ts
const ompTools = OMP_NAMES.map((name, index) => ({
  name,
  description: `${name} tool`,
  source: { kind: "native" },
  category: name === "bash" ? "shell" : "other",
  invoked: index < 4,
}));
```

- [ ] **Step 2: Add Billing exact/unknown-cost regression test**

Render run billing with two stages:

- codex: 100 input, 30 output, 40 cache read, 10 reasoning, no cost;
- omp: 200 input, 50 output, 80 cache read, cost 12_300 micros.

Assert:

- codex and OMP model rows are separate;
- token columns show exact input/output values;
- OMP cost renders `$0.01` according to existing formatting;
- codex cost is unavailable/blank, not `$0.00`.

- [ ] **Step 3: Run tests to verify they pass against existing generic UI**

```bash
bun test apps/fabro-web/app/components/stage-insights-sidebar.test.tsx
bun test apps/fabro-web/app/routes/run-billing.test.tsx
```

If either fails because the UI assumes all tools are invoked or missing cost equals zero, make the smallest generic correction in the corresponding production file and rerun. Do not add harness-name conditionals.

- [ ] **Step 4: Commit**

```bash
git add apps/fabro-web/app/components/stage-insights-sidebar.test.tsx apps/fabro-web/app/routes/run-billing.test.tsx apps/fabro-web/app/components/stage-insights-sidebar.tsx apps/fabro-web/app/routes/run-billing.tsx
git commit -m "test(web): cover ACP tools and billing presentation"
```

---

### Task 8: End-to-end build, deploy, and browser proof

**Files:**
- Modify only if verification finds a real defect.
- Use workflow: `~/Projects/fabro/.fabro/workflows/ui-harness-compare/`

**Interfaces:**
- Verifies the full spec across codex, OMP, and native Fabro stages.

- [ ] **Step 1: Run full engine verification**

```bash
cargo test -p fabro-acp -p fabro-model -p fabro-workflow -p fabro-store
cargo check --workspace
```

Expected: zero failures/errors. Record any known pre-existing unrelated workspace failures separately; do not suppress them.

- [ ] **Step 2: Build the web SPA and release binary**

```bash
bun run --cwd apps/fabro-web build
cargo run -q -p fabro-dev --features dev -- spa refresh
# rust-embed does not track asset-folder changes itself; force this crate to rebuild.
touch lib/apps/fabro-spa/src/lib.rs
cargo build --release
```

Expected: SPA assets refreshed within configured budgets; release build exits zero.

- [ ] **Step 3: Deploy without interrupting an active run**

Confirm the current automation run is terminal before restart. Then:

```bash
systemctl --user stop fabro-server
cp target/release/fabro ~/.fabro/bin/fabro
systemctl --user start fabro-server
systemctl --user is-active fabro-server
curl -fsS http://127.0.0.1:32276/ >/dev/null
```

Expected: service `active`, web request succeeds.

- [ ] **Step 4: Run the three-agent comparison workflow**

```bash
cd ~/Projects/fabro
~/.fabro/bin/fabro run ui-harness-compare
```

Expected: run succeeds and prints a run ID.

- [ ] **Step 5: Verify durable event/projection evidence**

For the new run:

```bash
fabro events <run-id> | grep 'agent.tools.available'
fabro events <run-id> | grep 'stage.completed'
```

Assert:

- OMP first tools snapshot has 23 entries and zero invoked;
- OMP final snapshot marks invoked official tools and appends any extension;
- codex has one `read_file` entry regardless of files read;
- codex and OMP stage completion billing contains nonzero exact token counts;
- absent codex reported cost is omitted, not zero.

- [ ] **Step 6: Browser-drive the actual UI**

Using the browser tool:

1. Open `http://127.0.0.1:32276/` and sign in with the configured dev token.
2. Navigate through the SPA to the new run.
3. Open codex stage: confirm grouped tool names and exact token billing.
4. Open OMP stage: confirm `TOOLS invoked/23` and the official catalog.
5. Open Billing: confirm separate codex and OMP rows, exact token values, and unavailable cost where not reported.
6. Capture screenshots of codex Tools, OMP Tools, and Billing.

- [ ] **Step 7: Final commit if verification required fixes**

```bash
git status --short
# If clean: no commit.
# If verified fixes were necessary:
git add <verified-files>
git commit -m "fix: complete ACP tools and billing integration"
```

- [ ] **Step 8: Report evidence**

Report:

- commit IDs per task;
- focused/full test counts;
- run ID;
- OMP invoked/total count;
- codex canonical tool names;
- codex/OMP token and cost values;
- screenshot paths;
- any remaining protocol limitation (live exact inventory remains unavailable over ACP).
