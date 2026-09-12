use fabro_llm::lithos_catalog::Catalog;

use crate::{Diagnostic, Severity};

pub(super) fn check_model_known(
    rule_name: &str,
    catalog: &Catalog,
    model: &str,
    context: &str,
    node_id: Option<String>,
) -> Option<Diagnostic> {
    if catalog.is_model_selector(model) {
        return None;
    }
    Some(Diagnostic {
        rule: rule_name.to_string(),
        severity: Severity::Warning,
        message: format!(
            "Unknown model '{model}' {context}. Run `fabro model list` to see available models"
        ),
        node_id,
        edge: None,
        fix: Some("Use a model ID from `fabro model list`".to_string()),

        ..Diagnostic::default()
    })
}

pub(super) fn check_provider_known(
    rule_name: &str,
    catalog: &Catalog,
    provider: &str,
    context: &str,
    node_id: Option<String>,
) -> Option<Diagnostic> {
    if catalog.enabled_provider(provider).is_some() {
        return None;
    }
    let valid: Vec<String> = catalog
        .listed_providers()
        .iter()
        .map(|provider| provider.id().to_string())
        .collect();
    let valid_str = valid.join(", ");
    Some(Diagnostic {
        rule: rule_name.to_string(),
        severity: Severity::Warning,
        message: format!("Unknown provider '{provider}' {context}. Valid providers: {valid_str}"),
        node_id,
        edge: None,
        fix: Some(format!("Use one of: {valid_str}")),

        ..Diagnostic::default()
    })
}
