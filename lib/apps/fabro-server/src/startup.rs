use std::collections::HashMap;
use std::path::Path;

use fabro_static::EnvVars;
use fabro_types::settings::ServerNamespace;
use fabro_vault::Vault;

use crate::jwt_auth::{AuthMode, resolve_auth_mode_with_lookup, validate_auth_configuration};
use crate::server_secrets::ServerSecrets;

pub(crate) fn resolve_startup(
    env_path: &Path,
    env_entries: HashMap<String, String>,
    settings: &ServerNamespace,
    vault: &Vault,
) -> anyhow::Result<(AuthMode, ServerSecrets)> {
    let server_secrets = ServerSecrets::load(env_path, env_entries)?;
    let auth_secret_lookup = |name: &str| match name {
        EnvVars::GITHUB_APP_CLIENT_SECRET => vault.get(name).map(str::to_string),
        _ => server_secrets.get(name),
    };
    let auth_mode = resolve_auth_mode_with_lookup(settings, auth_secret_lookup)?;
    Ok((auth_mode, server_secrets))
}

pub fn validate_startup(
    env_path: &Path,
    env_entries: HashMap<String, String>,
    settings: &ServerNamespace,
    vault: &Vault,
) -> anyhow::Result<()> {
    resolve_startup(env_path, env_entries, settings, vault).map(|_| ())
}

pub fn validate_startup_configuration(settings: &ServerNamespace) -> anyhow::Result<()> {
    validate_auth_configuration(settings)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use fabro_config::ServerSettingsBuilder;
    use fabro_static::EnvVars;
    use fabro_types::settings::ServerNamespace;
    use fabro_vault::{SecretType, Vault};

    use super::validate_startup;

    fn resolved_settings(auth_methods: &[&str]) -> ServerNamespace {
        ServerSettingsBuilder::from_toml(&format!(
            r#"
_version = 1

[server.auth]
methods = [{}]

[server.auth.github]
allowed_usernames = ["octocat"]

[server.integrations.github]
client_id = "Iv1.test"
"#,
            auth_methods
                .iter()
                .map(|method| format!("\"{method}\""))
                .collect::<Vec<_>>()
                .join(", ")
        ))
        .unwrap()
        .server
    }

    fn empty_vault(dir: &tempfile::TempDir) -> Vault {
        Vault::load(dir.path().join("secrets.json")).unwrap()
    }

    #[test]
    fn validate_startup_accepts_configured_secrets() {
        let dir = tempfile::tempdir().unwrap();
        let vault = empty_vault(&dir);
        let env = HashMap::from([
            (
                EnvVars::SESSION_SECRET.to_string(),
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
            ),
            (
                EnvVars::FABRO_DEV_TOKEN.to_string(),
                "fabro_dev_abababababababababababababababababababababababababababababababab"
                    .to_string(),
            ),
        ]);
        let settings = resolved_settings(&["dev-token"]);

        assert!(
            validate_startup(
                dir.path().join("server.env").as_path(),
                env,
                &settings,
                &vault,
            )
            .is_ok()
        );
    }

    #[test]
    fn validate_startup_rejects_missing_secrets() {
        let dir = tempfile::tempdir().unwrap();
        let settings = resolved_settings(&["dev-token"]);
        let vault = empty_vault(&dir);

        assert!(
            validate_startup(
                dir.path().join("server.env").as_path(),
                HashMap::new(),
                &settings,
                &vault,
            )
            .is_err()
        );
    }

    #[test]
    fn validate_startup_requires_github_client_secret_from_vault() {
        let dir = tempfile::tempdir().unwrap();
        let env = HashMap::from([
            (
                EnvVars::SESSION_SECRET.to_string(),
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
            ),
            (
                EnvVars::GITHUB_APP_CLIENT_SECRET.to_string(),
                "server-env-client-secret".to_string(),
            ),
        ]);
        let settings = resolved_settings(&["github"]);
        let vault = empty_vault(&dir);

        let err = validate_startup(
            dir.path().join("server.env").as_path(),
            env,
            &settings,
            &vault,
        )
        .expect_err("github client secret in server.env should not satisfy startup");

        assert!(err.to_string().contains("GITHUB_APP_CLIENT_SECRET"));
    }

    #[test]
    fn validate_startup_accepts_github_client_secret_from_vault() {
        let dir = tempfile::tempdir().unwrap();
        let env = HashMap::from([(
            EnvVars::SESSION_SECRET.to_string(),
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
        )]);
        let settings = resolved_settings(&["github"]);
        let mut vault = empty_vault(&dir);
        vault
            .set(
                EnvVars::GITHUB_APP_CLIENT_SECRET,
                "vault-client-secret",
                SecretType::Token,
                None,
            )
            .unwrap();

        validate_startup(
            dir.path().join("server.env").as_path(),
            env,
            &settings,
            &vault,
        )
        .expect("github client secret in vault should satisfy startup");
    }
}
