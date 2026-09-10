//! Response decoding: Chat Completions body → canonical `Response`.

use super::translate::{self, map_finish_reason};
use super::wire::{ApiResponse, ApiUsage, ReasoningDetails};
use crate::codec::CodecCtx;
use crate::error::{Error, ProviderErrorDetail, ProviderErrorKind};
use crate::types::{
    ContentPart, Message, RateLimitInfo, Response, Role, ThinkingData, TokenCounts, ToolCall,
};

pub(super) fn decode_response(
    body: &str,
    ctx: &CodecCtx<'_>,
    rate_limit: Option<RateLimitInfo>,
) -> Result<Response, Error> {
    let mut api_resp: ApiResponse = serde_json::from_str(body)
        .map_err(|e| Error::network(format!("failed to parse response: {e}"), e))?;

    let choice = api_resp
        .choices
        .first_mut()
        .ok_or_else(|| Error::Provider {
            kind:   ProviderErrorKind::Server,
            detail: Box::new(ProviderErrorDetail::new(
                "no choices in response",
                ctx.provider_name,
            )),
        })?;

    let mut content_parts = Vec::new();
    if let Some(payload) = choice.message.reasoning_details.take() {
        content_parts.extend(ReasoningDetails::from_complete_payload(payload).into_content_part());
    }
    if let Some(reasoning) = choice.message.reasoning() {
        if !reasoning.is_empty() {
            content_parts.push(ContentPart::Thinking(ThinkingData {
                text:      reasoning.to_string(),
                signature: None,
                redacted:  false,
            }));
        }
    }
    if let Some(text) = &choice.message.content {
        if !text.is_empty() {
            content_parts.push(ContentPart::text(text));
        }
    }
    if let Some(tool_calls) = &choice.message.tool_calls {
        let custom_tool_names = translate::custom_tool_names(ctx.request);
        for tc in tool_calls {
            let arguments = translate::parse_tool_arguments(
                &tc.function.name,
                &tc.function.arguments,
                &custom_tool_names,
            );
            let mut tool_call = ToolCall::new(&tc.id, &tc.function.name, arguments);
            tool_call.raw_arguments = Some(tc.function.arguments.clone());
            content_parts.push(ContentPart::ToolCall(tool_call));
        }
    }

    let finish_reason = map_finish_reason(choice.finish_reason.as_deref());

    let wire_usage = api_resp.usage.as_ref();
    let usage = wire_usage.map_or_else(TokenCounts::default, ApiUsage::token_counts);
    let cost_usd = wire_usage
        .and_then(|usage| usage.cost)
        .or_else(|| api_resp.cost.as_ref().and_then(|cost| cost.usd));
    let cost_source = translate::authoritative_cost_source(cost_usd);

    Ok(Response {
        id: api_resp.id,
        model: api_resp.model,
        provider: ctx.provider_name.to_string(),
        message: Message {
            role:         Role::Assistant,
            content:      content_parts,
            name:         None,
            tool_call_id: None,
        },
        finish_reason,
        usage,
        raw: serde_json::from_str(body).ok(),
        warnings: vec![],
        rate_limit,
        cost_usd,
        cost_source,
    })
}
