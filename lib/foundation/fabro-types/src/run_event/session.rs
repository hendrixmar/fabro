use fabro_model::ProviderId;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::TurnId;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunSessionCreatedProps {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title:    Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model:    Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<ProviderId>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunSessionTurnStartedProps {
    pub turn_id: TurnId,
    pub input:   String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunSessionUserMessageProps {
    pub turn_id: TurnId,
    pub text:    String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunSessionAssistantDeltaProps {
    pub turn_id: TurnId,
    pub delta:   String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunSessionAssistantMessageProps {
    pub turn_id: TurnId,
    pub text:    String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model:   Option<String>,
    #[serde(default)]
    pub usage:   Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunSessionToolCallStartedProps {
    pub turn_id:      TurnId,
    pub tool_name:    String,
    pub tool_call_id: String,
    pub arguments:    Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunSessionToolCallCompletedProps {
    pub turn_id:               TurnId,
    pub tool_name:             String,
    pub tool_call_id:          String,
    pub output:                Value,
    pub is_error:              bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_bytes_observed: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_bytes_retained: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_bytes_omitted:  Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunSessionTurnSucceededProps {
    pub turn_id: TurnId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output:  Option<String>,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Default,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum RunSessionTurnFailedCode {
    NoSandbox,
    SandboxUnavailable,
    LlmUnconfigured,
    ModelUnavailable,
    ToolDenied,
    #[default]
    AgentError,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunSessionTurnFailedProps {
    pub turn_id:   TurnId,
    pub error:     String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output:    Option<String>,
    #[serde(default)]
    pub code:      RunSessionTurnFailedCode,
    #[serde(default)]
    pub retryable: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunSessionTurnInterruptedProps {
    pub turn_id: TurnId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error:   Option<String>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{RunSessionCreatedProps, RunSessionToolCallCompletedProps};
    use crate::TurnId;

    #[test]
    fn session_created_deserializes_legacy_payload_without_provider() {
        let props: RunSessionCreatedProps = serde_json::from_value(json!({
            "title": "Legacy session",
            "model": "gpt-5.4"
        }))
        .unwrap();

        assert_eq!(props.model.as_deref(), Some("gpt-5.4"));
        assert_eq!(props.provider, None);
    }

    #[test]
    fn tool_completion_deserializes_without_output_byte_counts() {
        let props: RunSessionToolCallCompletedProps = serde_json::from_value(json!({
            "turn_id": TurnId::new(),
            "tool_name": "shell",
            "tool_call_id": "call_1",
            "output": "ok",
            "is_error": false
        }))
        .unwrap();

        assert!(props.output_bytes_observed.is_none());
        assert!(props.output_bytes_retained.is_none());
        assert!(props.output_bytes_omitted.is_none());
    }
}
