use fabro_test::TestContext;

use super::{completed_nodes, find_run_dir, fixture, read_conclusion, sandbox_tests, timeout_for};

sandbox_tests!(command_routing);

fn scenario_command_routing(context: &TestContext, sandbox: &str) {
    let workflow = fixture("command_routing.fabro");

    context.validate().arg(&workflow).assert().success();

    context
        .run_cmd()
        .args(["--auto-approve", "--environment", sandbox])
        .arg(workflow)
        .timeout(timeout_for(sandbox))
        .assert()
        .success();

    let run_dir = find_run_dir(context);
    let conclusion = read_conclusion(&run_dir);
    assert_eq!(conclusion["status"].as_str(), Some("succeeded"));

    let nodes = completed_nodes(&run_dir);
    assert!(
        nodes.contains(&"kept".to_string()),
        "kept node should be in completed_nodes: {nodes:?}"
    );
    assert!(
        !nodes.contains(&"none".to_string()),
        "none node should NOT be in completed_nodes: {nodes:?}"
    );
}
