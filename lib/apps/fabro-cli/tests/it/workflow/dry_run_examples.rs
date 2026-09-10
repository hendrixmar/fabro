use fabro_test::{fabro_snapshot, test_context};

use crate::support::run_output_filters;

#[test]
fn dry_run_branching() {
    let context = test_context!();
    let workflow = context.install_fixture("branching.fabro");
    let mut cmd = context.run_cmd();
    cmd.args(["--dry-run", "--auto-approve"]);
    cmd.arg(&workflow);
    fabro_snapshot!(run_output_filters(&context), cmd, @"
    success: true
    exit_code: 0
    ----- stdout -----
    ----- stderr -----
        Run: [ULID]
        Web UI: http://localhost:3000/runs/[ULID]
        Sandbox: local (ready in [TIME])
        ✓ Start  [TIME]
        ✓ Plan  [TIME]
        ✓ Implement  [TIME]
        ✓ Validate  [TIME]
        ✓ Tests passing?  [TIME]
        ✓ Exit  [TIME]

    === Run Result ===
    Run:       [ULID]
    Status:    SUCCEEDED
    Duration:  [DURATION]

    === Output ===
    [Simulated] Response for stage: validate
    ");
}

#[test]
fn dry_run_conditions() {
    let context = test_context!();
    let workflow = context.install_fixture("conditions.fabro");
    let mut cmd = context.run_cmd();
    cmd.args(["--dry-run", "--auto-approve"]);
    cmd.arg(&workflow);
    fabro_snapshot!(run_output_filters(&context), cmd, @"
    success: true
    exit_code: 0
    ----- stdout -----
    ----- stderr -----
        Run: [ULID]
        Web UI: http://localhost:3000/runs/[ULID]
        Sandbox: local (ready in [TIME])
        ✓ start  [TIME]
        ✓ Decide  [TIME]
        ✓ Path B  [TIME]
        ✓ exit  [TIME]

    === Run Result ===
    Run:       [ULID]
    Status:    SUCCEEDED
    Duration:  [DURATION]

    === Output ===
    [Simulated] Response for stage: path_b
    ");
}

#[test]
fn dry_run_parallel() {
    let context = test_context!();
    let workflow = context.install_fixture("parallel.fabro");
    let mut cmd = context.run_cmd();
    cmd.args(["--dry-run", "--auto-approve"]);
    cmd.arg(&workflow);
    let mut filters = run_output_filters(&context);
    filters.push((r"\bbranch[12]\b".to_string(), "[BRANCH]".to_string()));
    fabro_snapshot!(filters, cmd, @"
    success: true
    exit_code: 0
    ----- stdout -----
    ----- stderr -----
        Run: [ULID]
        Web UI: http://localhost:3000/runs/[ULID]
        Sandbox: local (ready in [TIME])
        ✓ start  [TIME]
            ✓ [BRANCH]  [TIME]
            ✓ [BRANCH]  [TIME]
        ✓ Fork Work  [TIME]
        ✓ Merge Results  [TIME]
        ✓ Review  [TIME]
        ✓ exit  [TIME]

    === Run Result ===
    Run:       [ULID]
    Status:    SUCCEEDED
    Duration:  [DURATION]

    === Output ===
    [Simulated] Response for stage: review
    ");
}

#[test]
fn dry_run_styled() {
    let context = test_context!();
    let workflow = context.install_fixture("styled.fabro");
    let mut cmd = context.run_cmd();
    cmd.args(["--dry-run", "--auto-approve"]);
    cmd.arg(&workflow);
    fabro_snapshot!(run_output_filters(&context), cmd, @"
    success: true
    exit_code: 0
    ----- stdout -----
    ----- stderr -----
        Run: [ULID]
        Web UI: http://localhost:3000/runs/[ULID]
        Sandbox: local (ready in [TIME])
        ✓ start  [TIME]
        ✓ Plan  [TIME]
        ✓ Implement  [TIME]
        ✓ Critical Review  [TIME]
        ✓ exit  [TIME]

    === Run Result ===
    Run:       [ULID]
    Status:    SUCCEEDED
    Duration:  [DURATION]

    === Output ===
    [Simulated] Response for stage: critical_review
    ");
}

#[test]
fn dry_run_inferred_command() {
    let context = test_context!();
    let workflow = context.install_fixture("inferred_command.fabro");
    let mut cmd = context.run_cmd();
    cmd.args(["--dry-run", "--auto-approve"]);
    cmd.arg(&workflow);
    fabro_snapshot!(run_output_filters(&context), cmd, @"
    success: true
    exit_code: 0
    ----- stdout -----
    ----- stderr -----
        Run: [ULID]
        Web UI: http://localhost:3000/runs/[ULID]
        Sandbox: local (ready in [TIME])
        ✓ Start  [TIME]
        ✓ Echo  [TIME]
        ✓ Exit  [TIME]

    === Run Result ===
    Run:       [ULID]
    Status:    SUCCEEDED
    Duration:  [DURATION]
    ");
}
