//! The labels that mark a sandbox as fabro's.
//!
//! Providers share a daemon or an organization with every other
//! application, so a persisted id is trusted only when the sandbox behind
//! it still carries fabro's labels. The driver's ownership scope stamps them
//! on every sandbox fabro creates, narrows every listing to them, and
//! refuses to attach to or delete a sandbox without them; this module only
//! says which labels those are.

use fabro_types::RunId;
use sandbox_driver::Ownership;

pub(crate) const MANAGED_LABEL: &str = "sh.fabro.managed";
pub(crate) const MANAGED_LABEL_VALUE: &str = "true";
pub(crate) const RUN_ID_LABEL: &str = "sh.fabro.run_id";

/// Fabro's ownership of a sandbox: everything fabro manages, narrowed to
/// one run when `run_id` is known.
pub(crate) fn ownership(run_id: Option<&RunId>) -> Ownership {
    let ownership = Ownership::label(MANAGED_LABEL, MANAGED_LABEL_VALUE);
    match run_id {
        Some(run_id) => ownership.and_label(RUN_ID_LABEL, run_id.to_string()),
        None => ownership,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use fabro_types::RunId;

    use super::*;

    fn conservative_daytona_key(key: &str) -> bool {
        key.chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '.' | '_'))
    }

    #[test]
    fn managed_label_keys_match_docker_and_use_conservative_ascii() {
        assert_eq!(MANAGED_LABEL, "sh.fabro.managed");
        assert_eq!(RUN_ID_LABEL, "sh.fabro.run_id");
        assert!(conservative_daytona_key(MANAGED_LABEL));
        assert!(conservative_daytona_key(RUN_ID_LABEL));
    }

    #[test]
    fn ownership_requires_fabro_and_the_run_when_known() {
        let run_id: RunId = "01HY0000000000000000000000".parse().unwrap();
        let mut labels = BTreeMap::new();
        assert!(!ownership(None).owns(&labels));
        labels.insert(MANAGED_LABEL.to_string(), "true".to_string());
        assert!(ownership(None).owns(&labels));
        assert!(!ownership(Some(&run_id)).owns(&labels));
        labels.insert(RUN_ID_LABEL.to_string(), run_id.to_string());
        assert!(ownership(Some(&run_id)).owns(&labels));

        // Stamping overrides whatever a caller put under the reserved keys.
        let mut given = BTreeMap::from([
            ("team".to_string(), "platform".to_string()),
            (MANAGED_LABEL.to_string(), "false".to_string()),
            (RUN_ID_LABEL.to_string(), "wrong".to_string()),
        ]);
        ownership(Some(&run_id)).stamp(&mut given);
        assert_eq!(given.get("team").map(String::as_str), Some("platform"));
        assert_eq!(given.get(MANAGED_LABEL).map(String::as_str), Some("true"));
        assert_eq!(
            given.get(RUN_ID_LABEL).map(String::as_str),
            Some("01HY0000000000000000000000")
        );
    }
}
