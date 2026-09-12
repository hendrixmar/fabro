use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
#[cfg(test)]
use std::time::Duration;

use fabro_github::token_source::InstallationTokenSource;
use fabro_hooks::{HookContext, HookDecision, HookExecutionContext, HookRunner};
use fabro_interview::Interviewer;
use fabro_llm::credentials::CredentialProvider;
use fabro_llm::lithos_catalog::Catalog;
use fabro_sandbox::RunSandbox;
use fabro_types::{GitIdentity, ManifestPath, RunId};
use lithos_llm::catalog::ProviderId;
use pebble_coding_agent::tools::{ToolEnvProvider, ToolError};
use tokio_util::sync::CancellationToken;

use crate::event::Emitter;
use crate::git_identity;
use crate::handler::HandlerRegistry;
use crate::interview_runtime::RunInterviewBlocker;
use crate::runtime_store::RunStoreHandle;
use crate::sandbox_git_runtime::SandboxGitRuntime;
use crate::stage_execution::StageExecutionTracker;
use crate::workflow_bundle::WorkflowBundle;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunLocations {
    pub host_source_dir:  Option<PathBuf>,
    pub sandbox_work_dir: Option<PathBuf>,
    pub run_scratch_dir:  PathBuf,
}

impl RunLocations {
    #[must_use]
    pub fn new(
        host_source_dir: Option<PathBuf>,
        sandbox_work_dir: Option<PathBuf>,
        run_scratch_dir: PathBuf,
    ) -> Self {
        Self {
            host_source_dir,
            sandbox_work_dir,
            run_scratch_dir,
        }
    }

    #[must_use]
    pub fn for_sandbox(
        host_source_dir: Option<PathBuf>,
        sandbox: &RunSandbox,
        run_scratch_dir: PathBuf,
    ) -> Self {
        Self::new(
            host_source_dir,
            Some(PathBuf::from(sandbox.working_directory())),
            run_scratch_dir,
        )
    }

    #[must_use]
    pub fn hook_execution_context(&self) -> HookExecutionContext {
        HookExecutionContext {
            host_source_dir:  self.host_source_dir.clone(),
            sandbox_work_dir: self.sandbox_work_dir.clone(),
        }
    }

    #[must_use]
    pub fn with_sandbox_work_dir(&self, sandbox_work_dir: Option<PathBuf>) -> Self {
        Self {
            sandbox_work_dir,
            ..self.clone()
        }
    }
}

#[derive(Clone)]
pub struct FabroRunToolServices {
    pub backend:        Arc<dyn fabro_tool::FabroToolBackend>,
    pub current_run_id: RunId,
}

/// Services shared across workflow phases.
///
/// Production construction is expected to happen from pipeline initialization
/// with the run's root cancellation token. Use
/// [`RunServices::with_cancel_token`] only with the same root token or a
/// `child_token()` derived from it. The token semantically means "cancel this
/// run or child run," not a generic shutdown signal — dropping a `RunServices`
/// does NOT count as cancellation.
#[derive(Clone)]
pub struct RunServices {
    pub run_store:                RunStoreHandle,
    pub emitter:                  Arc<Emitter>,
    pub sandbox:                  Arc<RunSandbox>,
    pub hook_runner:              Option<Arc<HookRunner>>,
    pub locations:                RunLocations,
    pub(crate) cancel_token:      CancellationToken,
    pub provider_id:              ProviderId,
    pub model:                    String,
    pub llm_source:               Arc<dyn CredentialProvider>,
    pub catalog:                  Arc<Catalog>,
    pub(crate) sandbox_git:       Arc<SandboxGitRuntime>,
    pub(crate) interview_blocker: Arc<RunInterviewBlocker>,
    /// Run-scoped stage execution allocator, shared between the core
    /// lifecycle and direct-dispatch handlers such as parallel branches.
    pub(crate) stage_executions:  StageExecutionTracker,
}

impl RunServices {
    #[must_use]
    pub(crate) fn new(
        run_store: RunStoreHandle,
        emitter: Arc<Emitter>,
        sandbox: Arc<RunSandbox>,
        hook_runner: Option<Arc<HookRunner>>,
        locations: RunLocations,
        cancel_token: CancellationToken,
        provider_id: ProviderId,
        model: String,
        llm_source: Arc<dyn CredentialProvider>,
        catalog: Arc<Catalog>,
        sandbox_git: Arc<SandboxGitRuntime>,
        stage_executions: StageExecutionTracker,
    ) -> Arc<Self> {
        Arc::new(Self {
            run_store,
            emitter,
            sandbox,
            hook_runner,
            locations,
            cancel_token,
            provider_id,
            model,
            llm_source,
            catalog,
            sandbox_git,
            interview_blocker: Arc::new(RunInterviewBlocker::new()),
            stage_executions,
        })
    }

    /// The run-level cancellation token. Cancel this to terminate the run.
    /// Derive child tokens via `cancel_token().child_token()` for sandbox
    /// command invocations.
    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel_token.clone()
    }

    /// Run lifecycle hooks and return the merged decision.
    /// Returns `Proceed` if no hook runner is configured.
    pub async fn run_hooks(&self, hook_context: &HookContext) -> HookDecision {
        let Some(ref runner) = self.hook_runner else {
            return HookDecision::Proceed;
        };
        runner
            .run(
                hook_context,
                Arc::clone(&self.sandbox),
                self.locations.hook_execution_context(),
            )
            .await
    }

    #[must_use]
    pub fn with_run_store(self: &Arc<Self>, run_store: RunStoreHandle) -> Arc<Self> {
        Arc::new(Self {
            run_store,
            ..self.as_ref().clone()
        })
    }

    #[must_use]
    pub fn with_emitter(self: &Arc<Self>, emitter: Arc<Emitter>) -> Arc<Self> {
        Arc::new(Self {
            emitter,
            ..self.as_ref().clone()
        })
    }

    #[must_use]
    pub fn with_sandbox(self: &Arc<Self>, sandbox: Arc<RunSandbox>) -> Arc<Self> {
        let locations = self
            .locations
            .with_sandbox_work_dir(Some(PathBuf::from(sandbox.working_directory())));
        Arc::new(Self {
            sandbox,
            locations,
            ..self.as_ref().clone()
        })
    }

    /// Replace the cancellation token. Use only with the same root token or
    /// a child derived from it via `child_token()`.
    #[must_use]
    pub(crate) fn with_cancel_token(
        self: &Arc<Self>,
        cancel_token: CancellationToken,
    ) -> Arc<Self> {
        Arc::new(Self {
            cancel_token,
            ..self.as_ref().clone()
        })
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn with_catalog_context(
        self: &Arc<Self>,
        catalog: Arc<Catalog>,
        provider_id: ProviderId,
        model: String,
    ) -> Arc<Self> {
        Arc::new(Self {
            provider_id,
            model,
            catalog,
            ..self.as_ref().clone()
        })
    }
}

/// Services available only while executing workflow nodes.
#[derive(Clone)]
pub struct EngineServices {
    pub run:             Arc<RunServices>,
    pub registry:        Arc<HandlerRegistry>,
    pub interviewer:     Arc<dyn Interviewer>,
    /// Environment variables from `[sandbox.env]` config.
    pub base_env:        HashMap<String, String>,
    /// GitHub token source used to inject `GITHUB_TOKEN` at the point of use.
    pub github_token:    Option<Arc<InstallationTokenSource>>,
    /// The run's resolved Git identity, injected as the `GIT_AUTHOR_*` /
    /// `GIT_COMMITTER_*` variables into every stage environment.
    pub git_identity:    Option<GitIdentity>,
    /// Typed values from `[run.inputs]`, available to prompt templates.
    pub inputs:          HashMap<String, toml::Value>,
    /// When true, handlers should skip real execution and return simulated
    /// results.
    pub dry_run:         bool,
    /// Manifest path of the current workflow when running from a bundle.
    pub workflow_path:   Option<ManifestPath>,
    /// Bundled workflows available for child-workflow resolution.
    pub workflow_bundle: Option<Arc<WorkflowBundle>>,
}

impl EngineServices {
    pub async fn env_for_stage(&self) -> anyhow::Result<HashMap<String, String>> {
        resolve_workflow_env(
            &self.base_env,
            self.github_token.as_ref(),
            self.git_identity.as_ref(),
        )
        .await
    }

    /// Test-only default: empty registry and cross-phase services.
    #[cfg(test)]
    #[expect(
        clippy::disallowed_methods,
        reason = "Test scaffolding must build a slate-backed run store from sync code."
    )]
    pub fn test_default() -> Self {
        use object_store::memory::InMemory;

        use crate::handler::start;

        #[derive(Debug, Default)]
        struct StubCredentialSource;

        #[async_trait::async_trait]
        impl CredentialProvider for StubCredentialSource {
            async fn credentials(
                &self,
                provider: &fabro_llm::lithos_catalog::CatalogProvider,
            ) -> Result<fabro_llm::credentials::Credentials, fabro_llm::credentials::CredentialError>
            {
                Err(fabro_llm::credentials::CredentialError::NotConfigured {
                    provider: provider.id().clone(),
                })
            }

            async fn is_configured(
                &self,
                _provider: &fabro_llm::lithos_catalog::CatalogProvider,
            ) -> bool {
                false
            }
        }

        let store = Arc::new(fabro_store::test_support::test_database(
            Arc::new(InMemory::new()),
            "",
            Duration::from_millis(1),
            None,
        ));
        let (run_store, sandbox) = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime should initialize")
                .block_on(async {
                    let run_store = store
                        .create_run(&fabro_types::RunId::new())
                        .await
                        .expect("slate-backed test run store should initialize");
                    let sandbox: Arc<RunSandbox> = Arc::new(
                        fabro_sandbox::local_sandbox(
                            std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
                        )
                        .await
                        .expect("local sandbox should be created"),
                    );
                    (run_store, sandbox)
                })
        })
        .join()
        .expect("test run store thread should join");
        let locations = RunLocations::for_sandbox(None, sandbox.as_ref(), PathBuf::from("."));

        Self {
            run:             RunServices::new(
                run_store.into(),
                Arc::new(Emitter::default()),
                sandbox,
                None,
                locations,
                CancellationToken::new(),
                lithos_llm::catalog::builtin::anthropic(),
                "claude-sonnet-4.6".to_string(),
                Arc::new(StubCredentialSource),
                Arc::new(fabro_llm::default_catalog()),
                Arc::new(SandboxGitRuntime::new()),
                StageExecutionTracker::default(),
            ),
            registry:        Arc::new(HandlerRegistry::new(Box::new(start::StartHandler))),
            interviewer:     Arc::new(fabro_interview::AutoApproveInterviewer::engine()),
            base_env:        HashMap::new(),
            github_token:    None,
            git_identity:    None,
            inputs:          HashMap::new(),
            dry_run:         false,
            workflow_path:   None,
            workflow_bundle: None,
        }
    }
}

pub struct WorkflowToolEnvProvider {
    pub base_env:     HashMap<String, String>,
    pub github_token: Option<Arc<InstallationTokenSource>>,
    /// The run's resolved Git identity; see [`EngineServices::git_identity`].
    pub git_identity: Option<GitIdentity>,
}

impl WorkflowToolEnvProvider {
    /// The environment tool processes run with right now: the configured
    /// sandbox env, a fresh `GITHUB_TOKEN` when the run has one, and the
    /// run's Git identity.
    pub async fn resolve(&self) -> anyhow::Result<HashMap<String, String>> {
        resolve_workflow_env(
            &self.base_env,
            self.github_token.as_ref(),
            self.git_identity.as_ref(),
        )
        .await
    }
}

#[async_trait::async_trait]
impl ToolEnvProvider for WorkflowToolEnvProvider {
    async fn resolve(&self) -> Result<HashMap<String, String>, ToolError> {
        Self::resolve(self).await.map_err(|error| {
            ToolError::execution(format!("Failed to resolve tool environment: {error:#}"))
        })
    }
}

async fn resolve_workflow_env(
    base_env: &HashMap<String, String>,
    github_token: Option<&Arc<InstallationTokenSource>>,
    identity: Option<&GitIdentity>,
) -> anyhow::Result<HashMap<String, String>> {
    let mut env = base_env.clone();
    if let Some(source) = github_token {
        let resolved = source.resolve().await?;
        env.insert(
            "GITHUB_TOKEN".to_string(),
            resolved.token.expose().to_owned(),
        );
    }
    // Applied last: the run's identity wins over any `[run.environment]`
    // entry of the same name, so `run.git.author` stays the one control.
    if let Some(identity) = identity {
        git_identity::apply_git_identity_env(&mut env, identity);
    }
    Ok(env)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use anyhow::anyhow;
    use fabro_github::InstallationToken;
    use fabro_github::test_support::{InstallationTokenMinter, installation_token_source};
    use fabro_github::token_source::InstallationTokenSource;

    use super::{EngineServices, WorkflowToolEnvProvider};

    #[tokio::test]
    async fn test_default_uses_stub_credential_source() {
        let services = EngineServices::test_default();

        assert!(
            fabro_llm::configured_providers(
                &services.run.catalog,
                services.run.llm_source.as_ref()
            )
            .await
            .is_empty()
        );
    }

    #[tokio::test]
    async fn workflow_tool_env_provider_returns_base_env_without_github_token() {
        let provider = WorkflowToolEnvProvider {
            base_env:     HashMap::from([("FOO".to_string(), "bar".to_string())]),
            github_token: None,
            git_identity: None,
        };

        let env = provider.resolve().await.unwrap();

        assert_eq!(env.get("FOO").map(String::as_str), Some("bar"));
        assert!(!env.contains_key("GITHUB_TOKEN"));
        assert!(!env.contains_key("GIT_AUTHOR_NAME"));
    }

    #[tokio::test]
    async fn workflow_tool_env_provider_git_identity_wins_over_base_env() {
        let provider = WorkflowToolEnvProvider {
            base_env:     HashMap::from([
                ("GIT_AUTHOR_NAME".to_string(), "from-run-env".to_string()),
                (
                    "GIT_COMMITTER_EMAIL".to_string(),
                    "run@example.com".to_string(),
                ),
            ]),
            github_token: None,
            git_identity: Some(fabro_types::GitIdentity {
                name:   "octocat".to_string(),
                email:  "1+octocat@users.noreply.github.com".to_string(),
                source: fabro_types::GitIdentitySource::GithubPat,
            }),
        };

        let env = provider.resolve().await.unwrap();

        assert_eq!(env["GIT_AUTHOR_NAME"], "octocat");
        assert_eq!(
            env["GIT_AUTHOR_EMAIL"],
            "1+octocat@users.noreply.github.com"
        );
        assert_eq!(env["GIT_COMMITTER_NAME"], "octocat");
        assert_eq!(
            env["GIT_COMMITTER_EMAIL"],
            "1+octocat@users.noreply.github.com"
        );
    }

    #[tokio::test]
    async fn workflow_tool_env_provider_merges_current_github_token() {
        let provider = WorkflowToolEnvProvider {
            base_env:     HashMap::from([("FOO".to_string(), "bar".to_string())]),
            github_token: Some(InstallationTokenSource::pat("ghp_pat".to_string())),
            git_identity: None,
        };

        let env = provider.resolve().await.unwrap();

        assert_eq!(env.get("FOO").map(String::as_str), Some("bar"));
        assert_eq!(env.get("GITHUB_TOKEN").map(String::as_str), Some("ghp_pat"));
    }

    struct FailingMinter;

    #[async_trait::async_trait]
    impl InstallationTokenMinter for FailingMinter {
        async fn mint(&self) -> anyhow::Result<InstallationToken> {
            Err(anyhow!("GITHUB_TOKEN refresh failed"))
        }
    }

    #[tokio::test]
    async fn workflow_tool_env_provider_propagates_token_refresh_errors() {
        let provider = WorkflowToolEnvProvider {
            base_env:     HashMap::new(),
            github_token: Some(installation_token_source(
                "owner/repo",
                Arc::new(FailingMinter),
            )),
            git_identity: None,
        };

        let err = format!("{:#}", provider.resolve().await.unwrap_err());
        assert!(err.contains("GITHUB_TOKEN refresh failed"), "got: {err}");
    }
}
