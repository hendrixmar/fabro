//! Agent event bodies.
//!
//! Pebble owns the coding-agent event vocabulary. Every event a coding agent
//! publishes reaches the run event log as one [`AgentEventProps`]: pebble's
//! full [`CodingAgentEvent`] envelope plus the stage and visit fabro adds at
//! the workflow boundary. The remaining structs here are fabro's own lifecycle
//! events around a session: activation, steering delivery, pairing, and MCP
//! server startup, none of which pebble emits.

use lithos_llm::types::{ReasoningEffort, Speed};
use pebble_coding_agent::events::{CodingAgentEvent, CodingEvent, ToolSummary};
use serde::{Deserialize, Serialize};

use crate::{PairId, PairMessageId, PairSystemMessageKind, PermissionLevel};

/// One coding-agent event placed on a workflow stage.
///
/// `event` is pebble's envelope verbatim, flattened into the properties so a
/// reader sees `seq`, `stream_id`, `session_id`, `timestamp`, and the
/// externally tagged `event` payload exactly as pebble serializes them.
/// `(stream_id, seq)` is the idempotency key for deduplication.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentEventProps {
    /// The node whose stage produced the event.
    pub stage: String,
    /// The graph visit of that stage.
    pub visit: u32,
    #[serde(flatten)]
    pub event: CodingAgentEvent,
}

impl AgentEventProps {
    #[must_use]
    pub fn new(stage: impl Into<String>, visit: u32, event: CodingAgentEvent) -> Self {
        Self {
            stage: stage.into(),
            visit,
            event,
        }
    }

    /// The `agent.*` (or `todo.*`) run event name for this event.
    #[must_use]
    pub fn event_name(&self) -> &'static str {
        coding_event_name(&self.event.event)
    }

    /// What happened, without the envelope.
    #[must_use]
    pub fn coding_event(&self) -> &CodingEvent {
        &self.event.event
    }
}

/// The run event name fabro derives from a pebble event variant.
///
/// Consumers switch on these names; the mapping is append-only.
#[must_use]
pub fn coding_event_name(event: &CodingEvent) -> &'static str {
    match event {
        CodingEvent::SessionStarted { .. } => "agent.session.started",
        CodingEvent::SessionEnded => "agent.session.ended",
        CodingEvent::ProcessingEnd => "agent.processing.end",
        CodingEvent::UserInput { .. } => "agent.input",
        CodingEvent::LlmRequestStarted { .. } => "agent.llm.started",
        CodingEvent::LlmFirstOutput { .. } => "agent.llm.first_output",
        CodingEvent::AssistantOutputReplace { .. } => "agent.output.replace",
        CodingEvent::AssistantMessage { .. } => "agent.message",
        CodingEvent::TextDelta { .. } => "agent.text.delta",
        CodingEvent::ReasoningDelta { .. } => "agent.reasoning.delta",
        CodingEvent::ToolCallStarted { .. } => "agent.tool.started",
        CodingEvent::ToolCallOutputDelta { .. } => "agent.tool.output.delta",
        CodingEvent::ToolCallCompleted { .. } => "agent.tool.completed",
        CodingEvent::ToolProcessCompleted { .. } => "agent.tool.process.completed",
        CodingEvent::Error { .. } => "agent.error",
        CodingEvent::Warning { .. } => "agent.warning",
        CodingEvent::LoopDetected => "agent.loop.detected",
        CodingEvent::ToolRoundsExhausted { .. } => "agent.tool.rounds.exhausted",
        CodingEvent::RouteFailover { .. } => "agent.route.failover",
        CodingEvent::McpServerReady { .. } => "agent.mcp.server.ready",
        CodingEvent::McpServerFailed { .. } => "agent.mcp.server.failed",
        CodingEvent::McpServerDisconnected { .. } => "agent.mcp.server.disconnected",
        CodingEvent::SteeringInjected { .. } => "agent.steering.injected",
        CodingEvent::RoundInterrupted { .. } => "agent.round.interrupted",
        CodingEvent::CompactionStarted { .. } => "agent.compaction.started",
        CodingEvent::CompactionCompleted { .. } => "agent.compaction.completed",
        CodingEvent::CompactionFailed { .. } => "agent.compaction.failed",
        CodingEvent::CompactionCancelled { .. } => "agent.compaction.cancelled",
        CodingEvent::LlmRetry { .. } => "agent.llm.retry",
        CodingEvent::SubAgentSpawned { .. } => "agent.sub.spawned",
        CodingEvent::SubAgentTurnStarted { .. } => "agent.sub.turn.started",
        CodingEvent::SubAgentCompleted { .. } => "agent.sub.completed",
        CodingEvent::SubAgentFailed { .. } => "agent.sub.failed",
        CodingEvent::SubAgentClosed { .. } => "agent.sub.closed",
        CodingEvent::MemoryLoaded { .. } => "agent.memory.loaded",
        CodingEvent::SkillsDiscovered { .. } => "agent.skills.discovered",
        CodingEvent::SkillActivated { .. } => "agent.skill.activated",
        CodingEvent::TodoCreated(_) => "todo.created",
        CodingEvent::TodoUpdated(_) => "todo.updated",
        CodingEvent::TodoDeleted(_) => "todo.deleted",
        // `CodingEvent` is non-exhaustive: a variant this build does not know
        // still gets a stable, recognizable name instead of failing to store.
        _ => "agent.event",
    }
}

/// Every name [`coding_event_name`] can return.
pub const CODING_EVENT_NAMES: &[&str] = &[
    "agent.session.started",
    "agent.session.ended",
    "agent.processing.end",
    "agent.input",
    "agent.llm.started",
    "agent.llm.first_output",
    "agent.output.replace",
    "agent.message",
    "agent.text.delta",
    "agent.reasoning.delta",
    "agent.tool.started",
    "agent.tool.output.delta",
    "agent.tool.completed",
    "agent.tool.process.completed",
    "agent.error",
    "agent.warning",
    "agent.loop.detected",
    "agent.tool.rounds.exhausted",
    "agent.route.failover",
    "agent.mcp.server.ready",
    "agent.mcp.server.failed",
    "agent.mcp.server.disconnected",
    "agent.steering.injected",
    "agent.round.interrupted",
    "agent.compaction.started",
    "agent.compaction.completed",
    "agent.compaction.failed",
    "agent.compaction.cancelled",
    "agent.llm.retry",
    "agent.sub.spawned",
    "agent.sub.turn.started",
    "agent.sub.completed",
    "agent.sub.failed",
    "agent.sub.closed",
    "agent.memory.loaded",
    "agent.skills.discovered",
    "agent.skill.activated",
    "todo.created",
    "todo.updated",
    "todo.deleted",
    "agent.event",
];

/// Whether `name` is a run event name derived from a pebble event.
#[must_use]
pub fn is_coding_event_name(name: &str) -> bool {
    CODING_EVENT_NAMES.contains(&name)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionCapability {
    Steer,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentSessionActivatedProps {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id:        Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider:         Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model:            Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<ReasoningEffort>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub speed:            Option<Speed>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permission_level: Option<PermissionLevel>,
    pub capabilities:     Vec<SessionCapability>,
    pub visit:            u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentSessionDeactivatedProps {
    pub visit: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentToolsAvailableProps {
    #[serde(default)]
    pub tools: Vec<ToolSummary>,
    pub visit: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentPairUserMessageProps {
    pub pair_id:           PairId,
    pub message_id:        PairMessageId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_message_id: Option<String>,
    pub text:              String,
    pub visit:             u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentPairSystemMessageProps {
    pub pair_id: PairId,
    pub kind:    PairSystemMessageKind,
    pub text:    String,
    pub visit:   u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentInterruptInjectedProps {
    pub visit: u32,
}

#[allow(
    clippy::empty_structs_with_brackets,
    reason = "This type must serialize as {} rather than null."
)]
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AgentSteerBufferedProps {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentSteerDroppedReason {
    QueueFull,
    RunEnded,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentSteerDroppedProps {
    pub reason: AgentSteerDroppedReason,
    pub count:  u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentMcpReadyProps {
    pub server_name: String,
    pub tool_count:  usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools:       Vec<AgentMcpToolSummary>,
    /// Whole milliseconds from the server's launch to its tools being
    /// listed. Events written before the field existed read as `0`.
    #[serde(default)]
    pub startup_ms:  u64,
    pub visit:       u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentMcpToolSummary {
    pub name:          String,
    pub original_name: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentMcpFailedProps {
    pub server_name: String,
    pub error:       String,
    /// Whole milliseconds from the server's launch to the failure. Events
    /// written before the field existed read as `0`.
    #[serde(default)]
    pub startup_ms:  u64,
    pub visit:       u32,
}

/// An MCP server that was ready lost its connection during the stage; every
/// later call to its tools fails until the session ends. Pebble reports the
/// disconnect once per server, from whichever session's tool call first
/// observed the closed connection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentMcpDisconnectedProps {
    pub server_name: String,
    /// What closed the connection, as the client observed it.
    pub error:       String,
    pub visit:       u32,
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use pebble_coding_agent::events::TokenUsage;
    use serde_json::json;

    use super::*;

    fn envelope(event: CodingEvent) -> CodingAgentEvent {
        CodingAgentEvent::new("ses_root", event, UNIX_EPOCH + Duration::from_millis(1_500))
            .with_seq(7)
            .with_stream_id("ses_root")
    }

    #[test]
    fn agent_event_props_flatten_pebbles_envelope() {
        let props = AgentEventProps::new(
            "code",
            2,
            envelope(CodingEvent::ToolCallStarted {
                tool_name:    "shell".to_string(),
                tool_call_id: "call_1".to_string(),
                arguments:    json!({"command": "ls"}),
            })
            .with_tool_call_id("call_1"),
        );

        let value = serde_json::to_value(&props).unwrap();
        assert_eq!(
            value,
            json!({
                "stage": "code",
                "visit": 2,
                "seq": 7,
                "stream_id": "ses_root",
                "session_id": "ses_root",
                "tool_call_id": "call_1",
                "timestamp": "1970-01-01T00:00:01.500Z",
                "event": {
                    "ToolCallStarted": {
                        "tool_name": "shell",
                        "tool_call_id": "call_1",
                        "arguments": {"command": "ls"}
                    }
                }
            })
        );
        assert_eq!(props.event_name(), "agent.tool.started");
        let parsed: AgentEventProps = serde_json::from_value(value).unwrap();
        assert_eq!(parsed, props);
    }

    #[test]
    fn every_derived_name_is_listed() {
        let events = vec![
            CodingEvent::SessionEnded,
            CodingEvent::ProcessingEnd,
            CodingEvent::LoopDetected,
            CodingEvent::McpServerDisconnected {
                server: "github".to_string(),
                error:  "transport closed".to_string(),
            },
            CodingEvent::AssistantMessage {
                text:            String::new(),
                model:           "gpt-5.4".to_string(),
                usage:           TokenUsage::default(),
                cost_usd_micros: None,
                cost_source:     None,
                tool_call_count: 0,
                context_window:  None,
                reasoning:       None,
            },
        ];
        for event in events {
            assert!(is_coding_event_name(coding_event_name(&event)));
        }
        assert!(is_coding_event_name("todo.updated"));
        assert!(!is_coding_event_name("agent.session.activated"));
    }
}
