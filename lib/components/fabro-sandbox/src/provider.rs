//! Fabro's inventory of the sandboxes it manages, across the providers a
//! server has configured.
//!
//! Every entry is a sandbox-driver provider narrowed by fabro's ownership
//! labels, so a listing shows only the sandboxes fabro created and an
//! attach to anything else is refused. A provider connects on first use:
//! the inventory is assembled synchronously at startup, and a provider that
//! is down surfaces as a lookup error rather than a startup failure. The
//! `local` kind has an entry too, so a caller can ask whether the kind is
//! ready, but its sandboxes are directories the run record names and there
//! is nothing to list.

use std::sync::Arc;

use fabro_types::settings::server::ServerSandboxProviderSettings;
use fabro_types::{
    SandboxInfo, SandboxListMeta, SandboxListResponse, SandboxProviderKind,
    SandboxProviderLookupError,
};
use fabro_util::error::collect_chain;
use futures::future::join_all;
use sandbox_driver::{
    Error as DriverError, OwnedProvider, SandboxFilter, SandboxId,
    SandboxProvider as DriverProvider, SandboxState,
};
use tokio::sync::OnceCell;

use crate::driver::{ConnectedProvider, ProviderConnectOptions, connect_provider};
use crate::managed_labels;

/// The sandboxes fabro manages, by provider.
#[derive(Clone, Default)]
pub struct SandboxInventory {
    entries: Vec<Arc<InventoryEntry>>,
}

struct InventoryEntry {
    kind:       SandboxProviderKind,
    connection: Connection,
}

enum Connection {
    /// Sandboxes on this host are directories the run record names;
    /// there is nothing to list.
    HostDirectories,
    Connected(Arc<dyn DriverProvider>),
    /// Connected through [`connect_provider`] on first use.
    Lazy(Box<LazyConnection>),
}

struct LazyConnection {
    settings: ServerSandboxProviderSettings,
    options:  ProviderConnectOptions,
    provider: OnceCell<Arc<dyn DriverProvider>>,
}

impl SandboxInventory {
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// A kind whose sandboxes are directories on this host: ready to run,
    /// nothing to list.
    #[must_use]
    pub fn with_host_directories(self, kind: SandboxProviderKind) -> Self {
        self.with_entry(kind, Connection::HostDirectories)
    }

    /// A provider already connected, tagged with the kind fabro persists
    /// for it.
    #[must_use]
    pub fn with_connected(self, connected: ConnectedProvider) -> Self {
        self.with_entry(
            connected.kind,
            Connection::Connected(owned(connected.provider)),
        )
    }

    /// A provider connected through [`connect_provider`] on first use.
    #[must_use]
    pub fn with_lazy(
        self,
        kind: SandboxProviderKind,
        settings: ServerSandboxProviderSettings,
        options: ProviderConnectOptions,
    ) -> Self {
        self.with_entry(
            kind,
            Connection::Lazy(Box::new(LazyConnection {
                settings,
                options,
                provider: OnceCell::new(),
            })),
        )
    }

    fn with_entry(mut self, kind: SandboxProviderKind, connection: Connection) -> Self {
        self.entries
            .push(Arc::new(InventoryEntry { kind, connection }));
        self
    }

    /// The provider kinds this inventory covers.
    pub fn kinds(&self) -> impl Iterator<Item = &SandboxProviderKind> {
        self.entries.iter().map(|entry| &entry.kind)
    }

    pub async fn list_managed(&self) -> SandboxListResponse {
        let results = join_all(
            self.entries
                .iter()
                .map(|entry| async move { (&entry.kind, entry.list().await) }),
        )
        .await;

        let mut data = Vec::new();
        let mut provider_errors = Vec::new();
        for (kind, result) in results {
            match result {
                Ok(mut sandboxes) => data.append(&mut sandboxes),
                Err(err) => provider_errors.push(provider_error(kind.clone(), &err)),
            }
        }

        SandboxListResponse {
            data,
            meta: SandboxListMeta { provider_errors },
        }
    }

    pub async fn get_managed_by_native_id(
        &self,
        id: &str,
    ) -> Result<SandboxInfo, SandboxLookupError> {
        let results = join_all(
            self.entries
                .iter()
                .map(|entry| async move { (&entry.kind, entry.get(id).await) }),
        )
        .await;

        let mut matches = Vec::new();
        let mut provider_errors = Vec::new();
        for (kind, result) in results {
            match result {
                Ok(Some(sandbox)) => matches.push(sandbox),
                Ok(None) => {}
                Err(err) => provider_errors.push(provider_error(kind.clone(), &err)),
            }
        }

        match matches.len() {
            1 => Ok(matches.remove(0)),
            0 if provider_errors.is_empty() => {
                Err(SandboxLookupError::NotFound { id: id.to_string() })
            }
            0 => Err(SandboxLookupError::ProviderUnavailable {
                id: id.to_string(),
                provider_errors,
            }),
            _ => Err(SandboxLookupError::Conflict {
                id:        id.to_string(),
                providers: matches
                    .into_iter()
                    .map(|sandbox| sandbox.provider)
                    .collect(),
            }),
        }
    }
}

impl InventoryEntry {
    /// The provider narrowed to fabro's sandboxes, connected on first use;
    /// `None` when the kind has nothing to list.
    async fn provider(&self) -> crate::Result<Option<&Arc<dyn DriverProvider>>> {
        match &self.connection {
            Connection::HostDirectories => Ok(None),
            Connection::Connected(provider) => Ok(Some(provider)),
            Connection::Lazy(lazy) => lazy
                .provider
                .get_or_try_init(|| async {
                    connect_provider(&self.kind, &lazy.settings, &lazy.options)
                        .await
                        .map(|connected| owned(connected.provider))
                        .map_err(|error| {
                            crate::Error::context(
                                format!("Failed to connect to the {} provider", self.kind),
                                error,
                            )
                        })
                })
                .await
                .map(Some),
        }
    }

    async fn list(&self) -> crate::Result<Vec<SandboxInfo>> {
        let Some(provider) = self.provider().await? else {
            return Ok(Vec::new());
        };
        let statuses = provider
            .list(&SandboxFilter::default())
            .await
            .map_err(|error| {
                crate::Error::context(format!("Failed to list {} sandboxes", self.kind), error)
            })?;
        Ok(statuses
            .into_iter()
            .map(|status| SandboxInfo {
                provider: self.kind.clone(),
                status,
            })
            .collect())
    }

    async fn get(&self, id: &str) -> crate::Result<Option<SandboxInfo>> {
        let Some(provider) = self.provider().await? else {
            return Ok(None);
        };
        // An id the driver cannot even name is not one of ours.
        let Ok(sandbox_id) = SandboxId::try_new(id) else {
            return Ok(None);
        };
        let handle = match provider.attach(&sandbox_id, None).await {
            Ok(handle) => handle,
            // Unknown to the provider, or not fabro's: neither is in the
            // inventory.
            Err(DriverError::NotFound { .. } | DriverError::NotOwned { .. }) => return Ok(None),
            Err(error) => {
                return Err(crate::Error::context(
                    format!("Failed to look up {} sandbox '{id}'", self.kind),
                    error,
                ));
            }
        };
        let status = handle.describe().await.map_err(|error| {
            crate::Error::context(
                format!("Failed to describe {} sandbox '{id}'", self.kind),
                error,
            )
        })?;
        if status.state == SandboxState::Deleted {
            return Ok(None);
        }
        Ok(Some(SandboxInfo {
            provider: self.kind.clone(),
            status,
        }))
    }
}

/// The provider narrowed to fabro's sandboxes.
fn owned(provider: Arc<dyn DriverProvider>) -> Arc<dyn DriverProvider> {
    Arc::new(OwnedProvider::new(
        provider,
        managed_labels::ownership(None),
    ))
}

#[derive(Debug, thiserror::Error)]
pub enum SandboxLookupError {
    #[error("sandbox '{id}' was not found by any configured provider")]
    NotFound { id: String },
    #[error("sandbox '{id}' matched more than one configured provider")]
    Conflict {
        id:        String,
        providers: Vec<SandboxProviderKind>,
    },
    #[error("sandbox '{id}' could not be found definitively because one or more providers failed")]
    ProviderUnavailable {
        id:              String,
        provider_errors: Vec<SandboxProviderLookupError>,
    },
}

fn provider_error(
    provider: SandboxProviderKind,
    err: &(dyn std::error::Error + 'static),
) -> SandboxProviderLookupError {
    SandboxProviderLookupError {
        provider,
        message: collect_chain(err).join(": "),
    }
}

#[cfg(test)]
mod tests {
    use fabro_types::settings::server::SandboxPluginSettings;
    use sandbox_driver::SandboxState;

    use super::*;
    use crate::test_support::{
        ScriptedSandbox, managed_scripted_sandbox, scripted_inventory_provider,
    };

    fn kind(name: &str) -> SandboxProviderKind {
        SandboxProviderKind::try_new(name).expect("valid kind")
    }

    fn provider(kind: SandboxProviderKind, ids: &[&str]) -> ConnectedProvider {
        scripted_inventory_provider(
            kind,
            ids.iter().map(|id| managed_scripted_sandbox(id)).collect(),
        )
    }

    /// A plugin kind whose executable does not exist, so every lookup fails
    /// to connect.
    fn unreachable_plugin(inventory: SandboxInventory, name: &str) -> SandboxInventory {
        let settings = ServerSandboxProviderSettings {
            enabled: true,
            plugin:  Some(SandboxPluginSettings {
                path: Some(format!("/nonexistent/fabro-sandbox-{name}")),
                dev: true,
                ..SandboxPluginSettings::default()
            }),
        };
        inventory.with_lazy(kind(name), settings, ProviderConnectOptions::default())
    }

    #[tokio::test]
    async fn list_aggregates_fabro_owned_sandboxes_across_providers() {
        let foreign = Arc::new(
            ScriptedSandbox::with_id_and_working_dir("someone-elses", "/work")
                .state(SandboxState::Running),
        );
        let docker = scripted_inventory_provider(SandboxProviderKind::DOCKER, vec![
            managed_scripted_sandbox("docker-1"),
            foreign,
        ]);
        let inventory = SandboxInventory::empty()
            .with_host_directories(SandboxProviderKind::LOCAL)
            .with_connected(docker)
            .with_connected(provider(SandboxProviderKind::DAYTONA, &["daytona-1"]));

        let response = inventory.list_managed().await;

        let mut ids: Vec<_> = response.data.iter().map(|s| s.status.id.as_str()).collect();
        ids.sort_unstable();
        assert_eq!(ids, ["daytona-1", "docker-1"]);
        assert!(response.meta.provider_errors.is_empty());
        let kinds: Vec<_> = inventory.kinds().cloned().collect();
        assert_eq!(kinds, [
            SandboxProviderKind::LOCAL,
            SandboxProviderKind::DOCKER,
            SandboxProviderKind::DAYTONA
        ]);
    }

    #[tokio::test]
    async fn list_reports_a_provider_that_cannot_connect_beside_the_others() {
        let inventory = unreachable_plugin(
            SandboxInventory::empty()
                .with_connected(provider(SandboxProviderKind::DOCKER, &["docker-1"])),
            "e2b",
        );

        let response = inventory.list_managed().await;

        assert_eq!(response.data.len(), 1);
        assert_eq!(response.meta.provider_errors.len(), 1);
        assert_eq!(response.meta.provider_errors[0].provider, kind("e2b"));
        assert!(
            response.meta.provider_errors[0]
                .message
                .contains("Failed to connect to the e2b provider"),
            "{}",
            response.meta.provider_errors[0].message
        );
    }

    #[tokio::test]
    async fn get_finds_one_sandbox_by_native_id() {
        let inventory = SandboxInventory::empty()
            .with_connected(provider(SandboxProviderKind::DOCKER, &[]))
            .with_connected(provider(SandboxProviderKind::DAYTONA, &["native-id"]));

        let sandbox = inventory
            .get_managed_by_native_id("native-id")
            .await
            .expect("one provider matches");

        assert_eq!(sandbox.status.id.as_str(), "native-id");
        assert_eq!(sandbox.provider, SandboxProviderKind::DAYTONA);
    }

    #[tokio::test]
    async fn get_reports_not_found_when_every_provider_misses() {
        let inventory = SandboxInventory::empty()
            .with_host_directories(SandboxProviderKind::LOCAL)
            .with_connected(provider(SandboxProviderKind::DOCKER, &[]));

        let error = inventory
            .get_managed_by_native_id("missing")
            .await
            .expect_err("nothing matches");

        assert!(matches!(error, SandboxLookupError::NotFound { id } if id == "missing"));
    }

    #[tokio::test]
    async fn get_reports_a_conflict_when_two_providers_match() {
        let inventory = SandboxInventory::empty()
            .with_connected(provider(SandboxProviderKind::DOCKER, &["same-id"]))
            .with_connected(provider(SandboxProviderKind::DAYTONA, &["same-id"]));

        let error = inventory
            .get_managed_by_native_id("same-id")
            .await
            .expect_err("two providers match");

        let SandboxLookupError::Conflict { providers, .. } = error else {
            panic!("expected a conflict, got {error:?}");
        };
        assert_eq!(providers, [
            SandboxProviderKind::DOCKER,
            SandboxProviderKind::DAYTONA
        ]);
    }

    #[tokio::test]
    async fn get_is_unavailable_when_no_match_and_a_provider_failed() {
        let inventory = unreachable_plugin(
            SandboxInventory::empty().with_connected(provider(SandboxProviderKind::DOCKER, &[])),
            "e2b",
        );

        let error = inventory
            .get_managed_by_native_id("maybe-missing")
            .await
            .expect_err("the failed provider may have held it");

        let SandboxLookupError::ProviderUnavailable {
            provider_errors, ..
        } = error
        else {
            panic!("expected provider unavailable, got {error:?}");
        };
        assert_eq!(provider_errors.len(), 1);
        assert_eq!(provider_errors[0].provider, kind("e2b"));
    }

    #[tokio::test]
    async fn get_ignores_a_sandbox_without_the_managed_label() {
        let foreign = Arc::new(
            ScriptedSandbox::with_id_and_working_dir("foreign", "/work")
                .state(SandboxState::Running),
        );
        let inventory = SandboxInventory::empty().with_connected(scripted_inventory_provider(
            SandboxProviderKind::DOCKER,
            vec![foreign],
        ));

        let error = inventory
            .get_managed_by_native_id("foreign")
            .await
            .expect_err("a foreign sandbox is not in the inventory");

        assert!(matches!(error, SandboxLookupError::NotFound { .. }));
    }
}
