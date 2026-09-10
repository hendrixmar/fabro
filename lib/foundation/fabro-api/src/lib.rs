#[allow(
    clippy::absolute_paths,
    clippy::all,
    clippy::derivable_impls,
    clippy::disallowed_methods,
    clippy::disallowed_types,
    clippy::needless_lifetimes,
    clippy::unwrap_used,
    unreachable_pub,
    unused_imports,
    reason = "Generated OpenAPI client code intentionally preserves codegen output."
)]
mod generated {
    include!(concat!(env!("OUT_DIR"), "/codegen.rs"));
}
pub mod types {
    pub use fabro_automation::{
        Automation, AutomationDraft as CreateAutomationRequest,
        AutomationReplace as ReplaceAutomationRequest, AutomationTrigger,
    };
    pub use fabro_environment::Environment;
    pub use fabro_model::{
        CostSource, Model, ModelControls, ModelCosts, ModelFeatures, ModelLimits,
        ModelRef as BillingModelRef, ModelTestMode, Provider, ReasoningEffort,
        ReasoningEffortFeature, Speed as BillingSpeed, TokenCounts as CompletionUsage,
    };
    pub use fabro_types::run_event::AgentSessionActivatedProps;
    pub use fabro_types::settings::run::{
        McpHttpProtocol, RunIntegrationsGithubSettings, RunIntegrationsSettings, RunModelControls,
        RunModelSettings,
    };
    pub use fabro_types::settings::server::{
        BugsinkIntegrationSettings, BugsinkProjectSettings, GithubIntegrationSettings,
        GithubIntegrationStrategy, IntegrationWebhooksSettings, LogDestination,
        ObjectStoreSettings, PlaneIntegrationSettings, ServerApiSettings, ServerArtifactsSettings,
        ServerAuthGithubSettings, ServerAuthMethod, ServerAuthSettings, ServerIntegrationsSettings,
        ServerListenSettings, ServerLoggingSettings, ServerSandboxProviderSettings,
        ServerSandboxProvidersSettings, ServerSandboxSettings, ServerSchedulerSettings,
        ServerSlateDbSettings, ServerStorageSettings, ServerWebSettings, SlackIntegrationSettings,
        WebhookStrategy,
    };
    pub use fabro_types::settings::{McpTransport, ServerNamespace};
    pub use fabro_types::status::{
        BlockedReason, FailureReason, PendingReason, RunControlAction, RunStatus, SuccessReason,
    };
    pub use fabro_types::{
        ActivatedSkill, AgentControlState, AgentMcpToolSummary, AgentSkillActivationSource,
        AgentSkillSummary, AgentToolCategory, AgentToolSource, AgentToolSummary,
        AgentToolsAvailableProps, AskFabro, AuthMethod, AutomationRef, BilledTokenCounts, BlobHash,
        CommandTermination, Conclusion, ContentPart, CreateVariableRequest, DiffStats, DiffSummary,
        DirtyStatus, EventEnvelope, ExecOutputTail, FailureCategory, FailureDetail,
        FailureSignature, GitContext, GitRunTarget, GitRunTarget as AutomationGitWorkflowSource,
        IdpIdentity, IntegrationConnectionKind, IntegrationConnectionState,
        IntegrationConnectionStatus, IntegrationProvider, IntegrationStatus, InterviewOption,
        InterviewQuestionRecord, LlmOutputKind, McpServerDraft as CreateMcpServerRequest,
        McpServerProjection, McpServerReplace as ReplaceMcpServerRequest, McpServerStatus,
        McpServerView as McpServer, McpTransportView, Message, PairId, PairMessageId,
        PairMessageRecord, PairMessageRequest, PairRecord, PairStartRequest, PairStatus,
        PairTarget, PairTranscriptEntry, PairTranscriptResponse, ParallelBranchId,
        ParallelBranchResult, PendingInterviewRecord, PermissionLevel, Principal, PullRequest,
        PullRequestCreation, PullRequestCreationId, PullRequestCreationStatus, PullRequestDetails,
        PullRequestDetailsStatus, PullRequestDetailsUnavailableReason, PullRequestLink,
        PullRequestMeta, PullRequestResponse, QuestionType, ReasoningOutput, RepositoryRef,
        ReviewTarget, ReviewTargetKind, Role, Run, RunApproval, RunApprovalState,
        RunClientProvenance, RunEvent, RunEventDetailContentKind, RunEventDetailResponse,
        RunFailure, RunIntent, RunIntentArgs, RunPairStatusResponse, RunProjection, RunProvenance,
        RunRunnableSource, RunSandbox, RunSandboxFailure, RunSandboxInstance, RunSandboxKind,
        RunSandboxPlan, RunSandboxRuntime, RunServerProvenance, RunSize, RunTarget, SandboxDetails,
        SandboxInfo, SandboxListMeta, SandboxListResponse, SandboxNetwork, SandboxNetworkPolicy,
        SandboxNetworkPolicyMode, SandboxProviderKind, SandboxProviderLookupError,
        SandboxResources, SandboxService, SandboxServiceListResponse, SandboxState,
        SandboxTimestamps, SecretMetadata, SecretType, ServerSettings, SessionDetail, SessionId,
        SessionMessage, SessionRecord, SessionStatus, SessionSummary, SessionTurn,
        SkillsProjection, StageCompletion, StageContextWindow, StageContextWindowBreakdownItem,
        StageContextWindowCategory, StageContextWindowCountMethod, StageContextWindowProjection,
        StageContextWindowStaleness, StageContextWindowUnavailableReason,
        StageContextWindowWarning, StageHandler, StageId, StageInferenceProjection,
        StageModelUsage, StageOutcome, StageProjection, StageState, StageToolBatchProjection,
        SubAgentProjection, SubAgentStatus, SystemActorKind, SystemIntegrationStatus,
        SystemIntegrationsResponse, TodoListProjection, TurnId, UpdateVariableRequest,
        UserPrincipal, Variable, VariableListResponse, WorkflowPath, WorkflowSettings,
        WorkflowVersion, WorkflowVersionId,
    };

    pub use crate::generated::types::*;
}
pub use generated::Client as ApiClient;
