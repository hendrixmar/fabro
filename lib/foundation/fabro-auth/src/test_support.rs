//! Test-only credential sources and catalogs.
//!
//! Feature-gated so they never link into production builds. Production code
//! resolves credentials through [`VaultCredentialSource`] over a real vault;
//! these helpers exist so tests can supply a source without one.

use std::collections::HashMap;
use std::sync::Arc;

use fabro_vault::Vault;
use lithos_llm::catalog::Catalog;
use lithos_llm::credentials::CredentialProvider;
use tokio::sync::RwLock as AsyncRwLock;

use crate::vault_source::VaultCredentialSource;

/// The lithos built-in catalog.
#[must_use]
pub fn test_catalog() -> Catalog {
    Catalog::builder()
        .with_builtin()
        .build()
        .expect("built-in catalog should build")
}

/// The built-in catalog with an operator overlay applied.
#[must_use]
pub fn test_catalog_with_overlay(overlay: &str) -> Catalog {
    Catalog::builder()
        .with_builtin()
        .overlay_toml(&format!("schema_version = 1\n{overlay}"))
        .expect("overlay should parse")
        .build()
        .expect("built-in catalog with overlay should build")
}

/// A detached in-memory vault holding no secrets.
#[must_use]
pub fn empty_vault() -> Arc<AsyncRwLock<Vault>> {
    Arc::new(AsyncRwLock::new(Vault::from_entries(HashMap::new())))
}

/// A vault-backed source whose credentials come only from `env_lookup`.
///
/// Tests that inject fake provider keys use this instead of reading the real
/// process environment, which would make them order-dependent.
#[must_use]
pub fn env_credential_source<F>(env_lookup: F) -> Arc<dyn CredentialProvider>
where
    F: Fn(&str) -> Option<String> + Send + Sync + 'static,
{
    Arc::new(VaultCredentialSource::with_env_lookup(
        empty_vault(),
        env_lookup,
    ))
}

/// A vault-backed source over an empty vault with no process-env fallback.
#[must_use]
pub fn vault_only_credential_source() -> Arc<dyn CredentialProvider> {
    Arc::new(VaultCredentialSource::vault_only(empty_vault()))
}
