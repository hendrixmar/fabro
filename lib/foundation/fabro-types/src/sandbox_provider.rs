use std::borrow::Cow;
use std::fmt;
use std::str::FromStr;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize};
use strum::VariantArray as _;

use crate::settings::run::RunMode;

/// Identity of a sandbox provider.
///
/// Open by design: the bundled providers (`local`, `docker`, `daytona`) run
/// in-process, and any other kind names a sandbox-driver plugin executable
/// configured under `server.sandbox.providers.<kind>`. Run records,
/// inventory, and the API carry this type so a plugin sandbox persists and
/// reconnects through the same code path as a bundled one.
///
/// Kind names follow the sandbox-driver rules: lowercase ASCII letters,
/// digits, and interior hyphens, at most 64 bytes. Parsing accepts any ASCII
/// case and lowercases it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct SandboxProviderKind(Cow<'static, str>);

/// The providers linked into fabro itself.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, strum::Display, strum::EnumString, strum::VariantArray,
)]
#[strum(serialize_all = "lowercase")]
pub enum BundledProvider {
    /// Run tools on the fabro host in a caller-designated directory.
    Local,
    /// Run tools inside a Docker container on the operator's daemon.
    Docker,
    /// Run tools inside a Daytona cloud sandbox.
    Daytona,
}

impl BundledProvider {
    #[must_use]
    pub const fn kind(self) -> SandboxProviderKind {
        match self {
            Self::Local => SandboxProviderKind::LOCAL,
            Self::Docker => SandboxProviderKind::DOCKER,
            Self::Daytona => SandboxProviderKind::DAYTONA,
        }
    }
}

/// How a provider obtains the run's workspace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkspacePolicy {
    /// The caller designates an existing host directory; nothing is cloned.
    DesignatedDirectory,
    /// The provider owns an isolated workspace and fabro clones into it.
    Clone,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid sandbox provider kind {value:?}: {reason}")]
pub struct InvalidSandboxProviderKind {
    pub value:  String,
    pub reason: &'static str,
}

const MAX_KIND_LEN: usize = 64;

impl SandboxProviderKind {
    pub const DAYTONA: Self = Self(Cow::Borrowed("daytona"));
    pub const DOCKER: Self = Self(Cow::Borrowed("docker"));
    pub const LOCAL: Self = Self(Cow::Borrowed("local"));

    /// Validates and wraps a provider kind name. Input is lowercased.
    pub fn try_new(value: impl AsRef<str>) -> Result<Self, InvalidSandboxProviderKind> {
        let raw = value.as_ref();
        let value = raw.trim().to_ascii_lowercase();
        let invalid = |reason| InvalidSandboxProviderKind {
            value: raw.to_string(),
            reason,
        };
        if value.is_empty() {
            return Err(invalid("must not be empty"));
        }
        if value.len() > MAX_KIND_LEN {
            return Err(invalid("exceeds 64 bytes"));
        }
        let charset_ok = value
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
        if !charset_ok || value.starts_with('-') || value.ends_with('-') {
            return Err(invalid(
                "must be lowercase ASCII letters, digits, and interior hyphens",
            ));
        }
        Ok(BundledProvider::VARIANTS
            .iter()
            .map(|bundled| bundled.kind())
            .find(|bundled| bundled.0 == value)
            .unwrap_or(Self(Cow::Owned(value))))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The bundled provider this kind names, or `None` for a plugin kind.
    #[must_use]
    pub fn bundled(&self) -> Option<BundledProvider> {
        BundledProvider::VARIANTS
            .iter()
            .copied()
            .find(|bundled| bundled.kind() == *self)
    }

    /// All bundled provider kinds, in display order.
    pub fn bundled_kinds() -> impl Iterator<Item = Self> {
        BundledProvider::VARIANTS
            .iter()
            .map(|bundled| bundled.kind())
    }

    /// True only for `local`. Used by dry-run to force local execution.
    #[must_use]
    pub fn is_local(&self) -> bool {
        *self == Self::LOCAL
    }

    /// How a run's workspace is obtained on this provider: `local` runs in a
    /// caller-designated directory, every other provider clones.
    #[must_use]
    pub fn workspace_policy(&self) -> WorkspacePolicy {
        if self.is_local() {
            WorkspacePolicy::DesignatedDirectory
        } else {
            WorkspacePolicy::Clone
        }
    }

    /// True for providers that clone repository sources into their workspace.
    #[must_use]
    pub fn clones_workspace(&self) -> bool {
        self.workspace_policy() == WorkspacePolicy::Clone
    }

    /// Coerce non-local providers to `local` under dry-run; otherwise
    /// unchanged.
    #[must_use]
    pub fn effective_for(&self, mode: RunMode) -> Self {
        if mode == RunMode::DryRun && !self.is_local() {
            Self::LOCAL
        } else {
            self.clone()
        }
    }
}

impl Default for SandboxProviderKind {
    fn default() -> Self {
        Self::LOCAL
    }
}

impl fmt::Display for SandboxProviderKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for SandboxProviderKind {
    type Err = InvalidSandboxProviderKind;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::try_new(value)
    }
}

impl AsRef<str> for SandboxProviderKind {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<BundledProvider> for SandboxProviderKind {
    fn from(value: BundledProvider) -> Self {
        value.kind()
    }
}

impl PartialEq<BundledProvider> for SandboxProviderKind {
    fn eq(&self, other: &BundledProvider) -> bool {
        *self == other.kind()
    }
}

impl<'de> Deserialize<'de> for SandboxProviderKind {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = Cow::<'de, str>::deserialize(deserializer)?;
        Self::try_new(raw.as_ref()).map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::{BundledProvider, SandboxProviderKind, WorkspacePolicy};
    use crate::settings::run::RunMode;

    #[test]
    fn sandbox_provider_default_is_local() {
        assert_eq!(SandboxProviderKind::default(), SandboxProviderKind::LOCAL);
    }

    #[test]
    fn sandbox_provider_from_str() {
        assert_eq!(
            "local".parse::<SandboxProviderKind>().unwrap(),
            SandboxProviderKind::LOCAL
        );
        assert_eq!(
            "docker".parse::<SandboxProviderKind>().unwrap(),
            SandboxProviderKind::DOCKER
        );
        assert_eq!(
            "daytona".parse::<SandboxProviderKind>().unwrap(),
            SandboxProviderKind::DAYTONA
        );
        assert_eq!(
            "LOCAL".parse::<SandboxProviderKind>().unwrap(),
            SandboxProviderKind::LOCAL
        );
        let plugin = "e2b-cloud".parse::<SandboxProviderKind>().unwrap();
        assert_eq!(plugin.as_str(), "e2b-cloud");
        assert_eq!(plugin.bundled(), None);
        for invalid in ["", "-e2b", "e2b-", "e 2b", "E2B_cloud", &"x".repeat(65)] {
            assert!(
                invalid.parse::<SandboxProviderKind>().is_err(),
                "{invalid:?} should be rejected"
            );
        }
    }

    #[test]
    fn sandbox_provider_display() {
        assert_eq!(SandboxProviderKind::LOCAL.to_string(), "local");
        assert_eq!(SandboxProviderKind::DOCKER.to_string(), "docker");
        assert_eq!(SandboxProviderKind::DAYTONA.to_string(), "daytona");
    }

    #[test]
    fn bundled_kinds_round_trip_through_bundled() {
        for bundled in [
            BundledProvider::Local,
            BundledProvider::Docker,
            BundledProvider::Daytona,
        ] {
            let kind = SandboxProviderKind::from(bundled);
            assert_eq!(kind.bundled(), Some(bundled));
            assert_eq!(kind.to_string(), bundled.to_string());
            assert_eq!(kind, bundled);
        }
    }

    #[test]
    fn workspace_policy_designates_local_and_clones_everything_else() {
        assert_eq!(
            SandboxProviderKind::LOCAL.workspace_policy(),
            WorkspacePolicy::DesignatedDirectory
        );
        assert_eq!(
            SandboxProviderKind::DOCKER.workspace_policy(),
            WorkspacePolicy::Clone
        );
        assert_eq!(
            SandboxProviderKind::try_new("host")
                .unwrap()
                .workspace_policy(),
            WorkspacePolicy::Clone
        );
        assert!(!SandboxProviderKind::LOCAL.clones_workspace());
        assert!(SandboxProviderKind::DAYTONA.clones_workspace());
    }

    #[test]
    fn dry_run_coerces_every_non_local_kind_to_local() {
        assert_eq!(
            SandboxProviderKind::try_new("host")
                .unwrap()
                .effective_for(RunMode::DryRun),
            SandboxProviderKind::LOCAL
        );
        assert_eq!(
            SandboxProviderKind::DOCKER.effective_for(RunMode::Normal),
            SandboxProviderKind::DOCKER
        );
    }

    #[test]
    fn serde_is_a_validated_plain_string() {
        let json = serde_json::to_string(&SandboxProviderKind::DAYTONA).unwrap();
        assert_eq!(json, "\"daytona\"");
        let parsed: SandboxProviderKind = serde_json::from_str("\"host\"").unwrap();
        assert_eq!(parsed.as_str(), "host");
        assert!(serde_json::from_str::<SandboxProviderKind>("\"Bad Kind\"").is_err());
        let keyed: std::collections::BTreeMap<SandboxProviderKind, bool> =
            serde_json::from_str(r#"{"docker": true, "host": false}"#).unwrap();
        assert_eq!(keyed.get(&SandboxProviderKind::DOCKER), Some(&true));
    }
}
