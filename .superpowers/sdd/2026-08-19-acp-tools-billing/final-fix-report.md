# Final ACP tools and billing fixes

## Findings and TDD evidence

### 1. Case-insensitive USD workflow validation

- **RED:** `cargo test -p fabro-workflow --lib acp_usage_accepts_case_insensitive_usd_currency` failed for `usd`: expected `Some(12300)`, got `None`.
- **Fix:** workflow cost validation now uses case-insensitive USD matching while retaining finite and nonnegative amount checks.
- **GREEN:** the focused test passed for both `usd` and `Usd`.
- **File:** `lib/components/fabro-workflow/src/handler/llm/acp.rs`

### 2. Latest valid cumulative ACP cost retention

- **RED:** `cargo test -p fabro-acp --lib usage_accumulator_retains_latest_valid_cost_after_invalid_updates` failed because the final invalid EUR update replaced the earlier valid `Usd` cost.
- **Fix:** the session accumulator replaces cost only for finite, nonnegative, case-insensitive USD updates. Invalid updates warn and missing updates leave the latest valid cost unchanged. Workflow validation remains in place as defense in depth.
- **GREEN:** the regression passed with a valid `Usd` cost followed by negative USD, NaN usd, EUR, and missing updates.
- **File:** `lib/components/fabro-acp/src/session.rs`

### 3. OMP Think todo/plan canonicalization

- **RED:** the title and raw-input focused tests each failed because the official `todo` entry remained uninvoked.
- **Fix:** OMP `Think` observations inspect title and structured raw-input keys/string values for `todo` or `plan` tokens before using the observed-extension fallback.
- **GREEN:** title (`Plan next steps`) and raw-input (`update_todo`) tests passed; the unrelated Think fallback test also passed and retained `analyze_architecture` as an extension.
- **File:** `lib/components/fabro-workflow/src/handler/llm/acp_tools.rs`

### 4. Effective OMP billing model identity

- **RED:** `cargo test -p fabro-workflow --lib omp_profile_pi_model_is_used_for_billing_without_node_model` failed with model `omp` instead of `profile-model`.
- **Fix:** before moving the resolved process spec into the ACP request, billing identity is captured in precedence order: node model, resolved OMP `PI_MODEL`, profile name, harness fallback.
- **GREEN:** the fake-profile/no-node-model integration test passed and asserted the exact identity `omp:profile-model`.
- **File:** `lib/components/fabro-workflow/src/handler/llm/acp.rs`

## Covering verification

- `cargo test -p fabro-acp` — 35 passed, 0 failed.
- `cargo test -p fabro-workflow --lib handler::llm::acp` — 43 passed, 0 failed.
- `cargo test -p fabro-model -p fabro-store` — 422 passed, 0 failed.

Formatter, lint, build, and project-wide tests were intentionally skipped per the final-fix assignment.

## Commit

This report and all four fixes are included in the single commit with subject `fix: address final ACP tools billing findings`.

## Self-review

- Changes are limited to the three named source files and this required report.
- Session and workflow layers independently enforce the same reported-cost validity contract.
- OMP todo recognition is limited to `Think` and exact delimiter-separated `todo`/`plan` tokens, avoiding false official-tool attribution for unrelated observations.
- Model identity reads the fully resolved process environment after node overrides and before ownership transfer.

## Concerns

None. The todo/plan recognizer intentionally does not treat unrelated words containing those substrings (for example, `planet`) as planning operations.
