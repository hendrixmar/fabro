//! Profile for Claude Fable 5, Opus 5, and Sonnet 5.

use std::sync::Arc;

use fabro_model::{AgentProfileKind, Catalog, ProviderId};

use super::EnvContext;
use crate::agent_profile::AgentProfile;
use crate::config::NativeToolOptions;
use crate::native_tool::{NativeTool, ToolVocabulary};
use crate::profiles::{
    self, BaseProfile, EmbeddedPrompt, ProfileDeps, claude5_tools, impl_base_profile_accessors,
};
use crate::sandbox::Sandbox;
use crate::skills::Skill;
use crate::subagent::{SessionFactory, SubAgentSupervisor};
use crate::todo_tools::{
    make_task_create_tool, make_task_get_tool, make_task_list_tool, make_task_update_tool,
};
use crate::tool_registry::ToolRegistry;
use crate::web_search::SearchBackend;

const CORE_PROMPT: &str = include_str!("prompts/claude5.md.j2");

pub struct Claude5Profile {
    base: BaseProfile,
}

impl Claude5Profile {
    #[must_use]
    pub fn new(model: impl Into<String>) -> Self {
        let deps =
            ProfileDeps::standalone(NativeToolOptions::for_profile(AgentProfileKind::Claude5));
        Self::with_native_tools(model, &deps)
    }

    pub(crate) fn with_native_tools(model: impl Into<String>, deps: &ProfileDeps) -> Self {
        let options = &deps.options;
        let summarizer = deps.summarizer.clone();
        let todo_runtime = Arc::clone(&deps.todo_runtime);
        let mut registry = ToolRegistry::with_vocabulary(ToolVocabulary::Claude5);
        registry.register(claude5_tools::make_read_tool());
        registry.register(claude5_tools::make_write_tool());
        registry.register(claude5_tools::make_edit_tool());
        registry.register(claude5_tools::make_bash_tool(options));
        registry.register(claude5_tools::make_web_fetch_tool(summarizer));
        if let Some(backend) = SearchBackend::from_secrets(&options.secrets) {
            registry.register(claude5_tools::make_web_search_tool(backend));
        }

        registry.register(claude5_tools::strict_object_tool(make_task_create_tool(
            todo_runtime.clone(),
        )));
        registry.register(claude5_tools::strict_object_tool(make_task_update_tool(
            todo_runtime.clone(),
        )));
        registry.register(claude5_tools::strict_object_tool(make_task_get_tool(
            todo_runtime.clone(),
        )));
        registry.register(claude5_tools::strict_object_tool(make_task_list_tool(
            todo_runtime,
        )));

        Self {
            base: BaseProfile {
                profile_kind: AgentProfileKind::Claude5,
                provider_id: ProviderId::anthropic(),
                model: model.into(),
                catalog: None,
                registry,
            },
        }
    }

    /// Override the transport provider while retaining Claude 5 harness
    /// behavior.
    #[must_use]
    pub fn with_provider_id(mut self, provider_id: ProviderId) -> Self {
        self.base.provider_id = provider_id;
        self
    }

    #[must_use]
    pub fn with_catalog(mut self, catalog: Arc<Catalog>) -> Self {
        self.base.catalog = Some(catalog);
        self
    }
}

impl AgentProfile for Claude5Profile {
    impl_base_profile_accessors!();

    fn build_system_prompt(
        &self,
        env: &dyn Sandbox,
        env_context: &EnvContext,
        memory: &[String],
        user_instructions: Option<&str>,
        skills: &[Skill],
    ) -> String {
        let template = EmbeddedPrompt::new("claude5.md.j2", CORE_PROMPT)
            .with_vocabulary(self.base.registry.vocabulary())
            .with_bool(
                "has_agent",
                self.base
                    .registry
                    .get_native(NativeTool::BackgroundAgent)
                    .is_some(),
            )
            .with_bool(
                "has_ask_user_question",
                self.base
                    .registry
                    .get_native(NativeTool::AskUserQuestion)
                    .is_some(),
            )
            .with_bool(
                "has_web_search",
                self.base
                    .registry
                    .get_native(NativeTool::WebSearch)
                    .is_some(),
            );

        profiles::assemble_system_prompt(
            template,
            env,
            env_context,
            memory,
            user_instructions,
            skills,
        )
    }

    fn register_subagent_tools(
        &mut self,
        supervisor: SubAgentSupervisor,
        session_factory: SessionFactory,
        current_depth: usize,
    ) {
        self.base.registry.register(claude5_tools::make_agent_tool(
            supervisor.clone(),
            session_factory,
            current_depth,
        ));
        self.base
            .registry
            .register(claude5_tools::make_task_output_tool(supervisor.clone()));
        self.base
            .registry
            .register(claude5_tools::make_task_stop_tool(supervisor.clone()));
        self.base
            .registry
            .register(claude5_tools::make_send_message_tool(supervisor));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subagent::SessionFactory;
    use crate::test_support::MockSandbox;

    #[test]
    fn profile_identity() {
        let profile = Claude5Profile::new("claude-fable-5");
        assert_eq!(profile.profile_kind(), AgentProfileKind::Claude5);
        assert_eq!(profile.provider_id(), ProviderId::anthropic());
        assert_eq!(profile.model(), "claude-fable-5");
    }

    #[test]
    fn core_tools_match_the_accepted_claude5_surface() {
        let profile = Claude5Profile::new("claude-sonnet-5");
        let mut names = profile.tool_registry().names();
        names.sort();
        assert_eq!(names, vec![
            "Bash",
            "Edit",
            "Read",
            "TaskCreate",
            "TaskGet",
            "TaskList",
            "TaskUpdate",
            "WebFetch",
            "Write",
        ]);
        assert!(!names.iter().any(|name| name == "Grep" || name == "Glob"));
    }

    #[test]
    fn root_agent_tools_use_claude_names() {
        let mut profile = Claude5Profile::new("claude-opus-5");
        let factory: SessionFactory = Arc::new(|| panic!("unused"));
        profile.register_subagent_tools(SubAgentSupervisor::new(3), factory, 0);

        for expected in ["Agent", "TaskOutput", "TaskStop", "SendMessage"] {
            assert!(
                profile.tool_registry().get(expected).is_some(),
                "missing {expected}"
            );
        }
        for absent in ["spawn_agent", "wait", "close_agent", "send_input"] {
            assert!(
                profile.tool_registry().get(absent).is_none(),
                "found {absent}"
            );
        }
    }

    #[test]
    fn prompt_conditionals_follow_registered_tools() {
        let env = MockSandbox::linux();
        let profile = Claude5Profile::new("claude-fable-5");
        let prompt = profile.build_system_prompt(&env, &EnvContext::default(), &[], None, &[]);
        assert!(!prompt.contains("# Background agents"));
        assert!(!prompt.contains("# Asking the user"));
        assert!(!prompt.contains("Use `WebSearch`"));

        let mut profile = profile;
        let factory: SessionFactory = Arc::new(|| panic!("unused"));
        profile.register_subagent_tools(SubAgentSupervisor::new(3), factory, 0);
        let prompt = profile.build_system_prompt(&env, &EnvContext::default(), &[], None, &[]);
        assert!(prompt.contains("# Background agents"));
    }
}
