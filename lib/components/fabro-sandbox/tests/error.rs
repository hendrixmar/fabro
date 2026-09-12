#[test]
fn context_error_preserves_source_cause() {
    let source = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "permission denied");

    let error = fabro_sandbox::Error::context("Failed to read file", source);

    assert_eq!(error.to_string(), "Failed to read file");
    assert_eq!(error.causes(), vec!["permission denied"]);
    assert_eq!(
        error.display_with_causes(),
        "Failed to read file\n  caused by: permission denied"
    );
}
