use serde::{Deserialize, Serialize};

/// The Git author and committer identity a run resolved once and uses for
/// every commit it creates: engine checkpoints, metadata commits, and any
/// `git commit` a workflow command or agent tool runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitIdentity {
    pub name:   String,
    pub email:  String,
    pub source: GitIdentitySource,
}

/// Where a run's Git identity came from.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum GitIdentitySource {
    /// `run.git.author` supplied both the name and the email.
    Explicit,
    /// The run's GitHub App bot account.
    GithubApp,
    /// The authenticated user of the run's GitHub personal access token.
    GithubPat,
    /// The generic Fabro identity: no usable credential identity.
    Default,
}

impl GitIdentity {
    pub const DEFAULT_EMAIL: &'static str = "noreply@fabro.sh";
    pub const DEFAULT_NAME: &'static str = "Fabro";

    /// The generic identity used when no credential identity is available.
    #[must_use]
    pub fn fabro_default() -> Self {
        Self {
            name:   Self::DEFAULT_NAME.to_string(),
            email:  Self::DEFAULT_EMAIL.to_string(),
            source: GitIdentitySource::Default,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_serializes_as_snake_case() {
        assert_eq!(
            serde_json::to_value(GitIdentitySource::GithubApp).unwrap(),
            serde_json::json!("github_app")
        );
        assert_eq!(GitIdentitySource::GithubPat.to_string(), "github_pat");
        assert_eq!(
            "explicit".parse::<GitIdentitySource>().unwrap(),
            GitIdentitySource::Explicit
        );
    }

    #[test]
    fn identity_round_trips_through_json() {
        let identity = GitIdentity {
            name:   "fabro-bot[bot]".to_string(),
            email:  "1+fabro-bot[bot]@users.noreply.github.com".to_string(),
            source: GitIdentitySource::GithubApp,
        };
        let value = serde_json::to_value(&identity).unwrap();
        assert_eq!(
            value,
            serde_json::json!({
                "name": "fabro-bot[bot]",
                "email": "1+fabro-bot[bot]@users.noreply.github.com",
                "source": "github_app",
            })
        );
        assert_eq!(
            serde_json::from_value::<GitIdentity>(value).unwrap(),
            identity
        );
    }
}
