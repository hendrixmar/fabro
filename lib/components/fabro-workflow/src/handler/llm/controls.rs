//! Per-request model controls: the reasoning effort and speed a stage asks
//! for, resolved from the node's attributes over the run-level defaults.

use fabro_graphviz::graph::{AttrValue, Node};
use fabro_types::settings::run::RunModelControls;
use lithos_llm::types::{ReasoningEffort, Speed};

use crate::error::Error;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EffectiveRequestControls {
    pub(crate) reasoning_effort: Option<ReasoningEffort>,
    pub(crate) speed:            Option<Speed>,
}

pub(crate) fn effective_request_controls(
    run_model_controls: &RunModelControls,
    node: &Node,
) -> Result<EffectiveRequestControls, Error> {
    let reasoning_effort = match control_attr(node, "reasoning_effort")
        .or(run_model_controls.reasoning_effort.as_deref())
    {
        Some(value) => Some(parse_reasoning_effort(node, value)?),
        None => None,
    };
    let speed = control_attr(node, "speed")
        .or(run_model_controls.speed.as_deref())
        .map(|value| parse_speed(node, value))
        .transpose()?;

    Ok(EffectiveRequestControls {
        reasoning_effort,
        speed,
    })
}

fn control_attr<'a>(node: &'a Node, key: &str) -> Option<&'a str> {
    node.attrs.get(key).and_then(AttrValue::as_str)
}

fn parse_reasoning_effort(node: &Node, value: &str) -> Result<ReasoningEffort, Error> {
    value.parse().map_err(|_| {
        Error::handler(format!(
            "Invalid reasoning_effort \"{value}\" for node \"{}\"; expected one of: {}",
            node.id,
            expected_values(
                ReasoningEffort::ALL
                    .into_iter()
                    .map(ReasoningEffort::as_str)
            ),
        ))
    })
}

fn parse_speed(node: &Node, value: &str) -> Result<Speed, Error> {
    value.parse().map_err(|_| {
        Error::handler(format!(
            "Invalid speed \"{value}\" for node \"{}\"; expected one of: {}",
            node.id,
            expected_values(Speed::ALL.into_iter().map(Speed::as_str)),
        ))
    })
}

fn expected_values<'a>(values: impl Iterator<Item = &'a str>) -> String {
    values.collect::<Vec<_>>().join(", ")
}

/// Node-level `max_tokens`, as the client's `u32` output budget.
pub(crate) fn node_max_output_tokens(node: &Node) -> Option<u32> {
    node.max_tokens()
        .and_then(|tokens| u32::try_from(tokens).ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_model_controls_apply_when_node_omits_controls() {
        let run_controls = RunModelControls {
            reasoning_effort: Some("low".to_string()),
            speed:            Some("fast".to_string()),
        };
        let node = Node::new("work");

        let controls = effective_request_controls(&run_controls, &node).unwrap();

        assert_eq!(controls.reasoning_effort, Some(ReasoningEffort::Low));
        assert_eq!(controls.speed, Some(Speed::Fast));
    }

    #[test]
    fn node_controls_override_run_model_controls() {
        let run_controls = RunModelControls {
            reasoning_effort: Some("low".to_string()),
            speed:            Some("fast".to_string()),
        };
        let mut node = Node::new("work");
        node.attrs.insert(
            "reasoning_effort".to_string(),
            AttrValue::String("high".to_string()),
        );
        node.attrs.insert(
            "speed".to_string(),
            AttrValue::String("balanced".to_string()),
        );

        let controls = effective_request_controls(&run_controls, &node).unwrap();

        assert_eq!(controls.reasoning_effort, Some(ReasoningEffort::High));
        assert_eq!(controls.speed, Some(Speed::Balanced));
    }

    #[test]
    fn omitted_reasoning_effort_stays_unset() {
        let node = Node::new("work");

        let controls = effective_request_controls(&RunModelControls::default(), &node).unwrap();

        assert_eq!(controls.reasoning_effort, None);
        assert_eq!(controls.speed, None);
    }

    #[test]
    fn invalid_reasoning_effort_names_the_node() {
        let mut node = Node::new("work");
        node.attrs.insert(
            "reasoning_effort".to_string(),
            AttrValue::String("maximal".to_string()),
        );

        let error = effective_request_controls(&RunModelControls::default(), &node).unwrap_err();

        assert!(error.to_string().contains("node \"work\""), "{error}");
    }
}
