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
    pub use fabro_types::run_event::AgentSessionActivatedProps;
    pub use fabro_types::settings::run::{
        McpHttpProtocol, RunIntegrationsGithubSettings, RunIntegrationsSettings, RunModelControls,
        RunModelSettings,
    };
    pub use fabro_types::settings::server::{
        GithubIntegrationSettings, GithubIntegrationStrategy, IntegrationWebhooksSettings,
        LogDestination, ObjectStoreSettings, ServerApiSettings, ServerArtifactsSettings,
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
        ActivatedSkill, AgentControlState, AgentEventProps, AgentMcpToolSummary,
        AgentToolsAvailableProps, AskFabro, AuthMethod, AutomationRef, BilledTokenCounts, BlobHash,
        CommandTermination, Conclusion, ContextWindowBreakdownItem, ContextWindowCategory,
        ContextWindowCountMethod, ContextWindowSnapshot, ContextWindowStaleness,
        ContextWindowWarning, CreateVariableRequest, DiffStats, DiffSummary, DirtyStatus,
        EventEnvelope, ExecOutputTail, FailureCategory, FailureDetail, FailureSignature,
        GitContext, GitRunTarget, GitRunTarget as AutomationGitWorkflowSource, IdpIdentity,
        IntegrationConnectionKind, IntegrationConnectionState, IntegrationConnectionStatus,
        IntegrationProvider, IntegrationStatus, InterviewOption, InterviewQuestionRecord,
        LlmOutputKind, McpServerDraft as CreateMcpServerRequest, McpServerProjection,
        McpServerReplace as ReplaceMcpServerRequest, McpServerStatus, McpServerView as McpServer,
        McpTransportView, Model, ModelControls, ModelCosts, ModelFeatures, ModelLimits,
        ModelRef as BillingModelRef, ModelTestMode, PairId, PairMessageId, PairMessageRecord,
        PairMessageRequest, PairRecord, PairStartRequest, PairStatus, PairTarget,
        PairTranscriptEntry, PairTranscriptResponse, ParallelBranchId, ParallelBranchResult,
        PendingInterviewRecord, PermissionLevel, Principal, Provider, PullRequest,
        PullRequestCreation, PullRequestCreationId, PullRequestCreationStatus, PullRequestDetails,
        PullRequestDetailsStatus, PullRequestDetailsUnavailableReason, PullRequestLink,
        PullRequestMeta, PullRequestResponse, QuestionType, RepositoryRef, ReviewTarget,
        ReviewTargetKind, Run, RunApproval, RunApprovalState, RunClientProvenance, RunEvent,
        RunEventDetailContentKind, RunEventDetailResponse, RunFailure, RunIntent, RunIntentArgs,
        RunPairStatusResponse, RunProjection, RunProvenance, RunRunnableSource, RunSandbox,
        RunSandboxFailure, RunSandboxInstance, RunSandboxKind, RunSandboxPlan, RunSandboxRuntime,
        RunServerProvenance, RunSessionMetadata, RunSize, RunTarget, SandboxDetails, SandboxInfo,
        SandboxListMeta, SandboxListResponse, SandboxProviderKind, SandboxProviderLookupError,
        SandboxService, SandboxServiceListResponse, SecretMetadata, SecretType, ServerSettings,
        SessionDetail, SessionId, SessionStatus, SessionSummary, SessionTurn,
        SkillActivationSource, SkillSummary, SkillsProjection, StageCompletion, StageContextWindow,
        StageContextWindowUnavailableReason, StageHandler, StageId, StageInferenceProjection,
        StageModelUsage, StageOutcome, StageProjection, StageState, StageToolBatchProjection,
        SubAgentProjection, SubAgentStatus, SystemActorKind, SystemIntegrationStatus,
        SystemIntegrationsResponse, TodoListProjection, ToolCategory, ToolSource, ToolSummary,
        TurnId, UpdateVariableRequest, UserPrincipal, Variable, VariableListResponse, WorkflowPath,
        WorkflowSettings, WorkflowVersion, WorkflowVersionId,
    };
    pub use lithos_llm::catalog::{ModelHandle, ProviderId};
    pub use lithos_llm::types::{
        ContentPart, Cost as CompletionCost, CostSource, Message, ReasoningEffort, ReasoningOutput,
        ResponseFormat as CompletionResponseFormat, Role, Speed as BillingSpeed,
        TokenCounts as CompletionUsage, ToolChoice as CompletionToolChoice,
        ToolDefinition as CompletionToolDefinition,
        ToolDefinitionKind as CompletionToolDefinitionKind,
    };
    /// A sandbox's status on the API is the sandbox driver's own type.
    pub use sandbox_driver::{
        NetworkPolicy as SandboxNetworkPolicy, Resources as SandboxResources, SandboxId,
        SandboxKind, SandboxState, SandboxStatus, WorkspaceOwnership as SandboxWorkspaceOwnership,
    };

    pub use crate::generated::types::*;
}
pub use generated::Client as ApiClient;
