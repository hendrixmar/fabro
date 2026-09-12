use fabro_tool::ValidatedWorkflowVersionCreate;

pub(super) fn source(entrypoint: &str, files: &[(&str, &str)]) -> ValidatedWorkflowVersionCreate {
    ValidatedWorkflowVersionCreate {
        entrypoint: entrypoint.parse().unwrap(),
        files:      files
            .iter()
            .map(|(path, content)| (path.parse().unwrap(), (*content).to_string()))
            .collect(),
    }
}

pub(super) fn fixture() -> ValidatedWorkflowVersionCreate {
    source("workflow.toml", &[
        (
            "workflow.toml",
            "_version = 1\n[workflow]\ngraph = \"workflow.fabro\"\n",
        ),
        (
            "workflow.fabro",
            r#"digraph W { p [prompt="@prompt.md"] child [stack.child_workflow="child.fabro"] }"#,
        ),
        (
            "prompt.md",
            "Keep {{ secrets.TEST }} and {{ env.TEST }} for runtime.",
        ),
        ("child.fabro", "digraph Child {}"),
    ])
}
