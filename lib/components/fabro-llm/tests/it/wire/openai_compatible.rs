//! Wire snapshots for the OpenAI Chat Completions dialect served by
//! `OpenAiCompatibleAdapter` (kimi, zai, minimax, venice, inception, ollama,
//! litellm — all config-only routes over this adapter).

use std::sync::Arc;

use fabro_llm::generate::StreamAccumulator;
use fabro_llm::provider::ProviderAdapter;
use fabro_llm::providers::OpenAiCompatibleAdapter;
use fabro_llm::types::{
    Message, ReasoningEffort, Request, ResponseFormat, ResponseFormatType, ToolChoice,
    ToolDefinition,
};
use fabro_model::catalog::LlmCatalogSettings;
use fabro_model::{Catalog, ProviderId};
use httpmock::prelude::*;

use crate::support::{
    self, WireCapture, base_request, corpus_audio_attachment, corpus_bad_file_path_attachments,
    corpus_inline_attachments, corpus_multi_turn, corpus_provider_options, corpus_response_format,
    corpus_sampling_params, corpus_thinking_round_trip, corpus_tool_round_trip, corpus_tools,
    corpus_url_attachments, json_schema_format, mount_capture, mount_capture_sse, take_capture,
};

const MODEL: &str = "test-model";

/// Fixed `created` timestamp for canned bodies (named to satisfy clippy's
/// unreadable-literal lint without touching the JSON wire value).
const CREATED_TS: i64 = 1_700_000_000;

/// Minimal valid Chat Completions body for encode-side tests.
fn minimal_body() -> serde_json::Value {
    body_with_message(&serde_json::json!({"role": "assistant", "content": "ok"}))
}

/// Wraps an assistant message in a complete Chat Completions body.
fn body_with_message(message: &serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "id": "chatcmpl_test",
        "object": "chat.completion",
        "created": CREATED_TS,
        "model": MODEL,
        "choices": [{
            "index": 0,
            "message": message,
            "finish_reason": "stop"
        }],
        "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}
    })
}

fn adapter(server: &MockServer) -> OpenAiCompatibleAdapter {
    OpenAiCompatibleAdapter::new("test-key", server.base_url())
}

/// Runs `complete()` against a capture mock and returns the captured wire
/// request.
async fn encode_capture_with(
    request: &Request,
    configure: impl FnOnce(OpenAiCompatibleAdapter) -> OpenAiCompatibleAdapter,
) -> WireCapture {
    let server = MockServer::start();
    let (mock, slot) = mount_capture(&server, "/chat/completions", minimal_body());
    let adapter = configure(adapter(&server));
    adapter
        .complete(request)
        .await
        .expect("complete should succeed");
    mock.assert();
    take_capture(&slot)
}

async fn encode_capture(request: &Request) -> WireCapture {
    encode_capture_with(request, |adapter| adapter).await
}

/// Runs `stream()` against an SSE transcript and returns the captured wire
/// request plus every emitted stream item as JSON.
async fn stream_capture(
    request: &Request,
    sse_body: &str,
) -> (WireCapture, Vec<serde_json::Value>) {
    let server = MockServer::start();
    let (mock, slot) = mount_capture_sse(&server, "/chat/completions", sse_body);
    let adapter = adapter(&server);
    let events = support::collect_stream_events(&adapter, request).await;
    mock.assert();
    (take_capture(&slot), events)
}

// ---------------------------------------------------------------------------
// Round trip (encode + decode)
// ---------------------------------------------------------------------------

/// Shared setup for the system+tools round trip; the encode and decode halves
/// are pinned by separate tests.
async fn system_and_tools_roundtrip() -> (WireCapture, fabro_llm::types::Response) {
    let server = MockServer::start();
    let (mock, slot) = mount_capture(
        &server,
        "/chat/completions",
        serde_json::json!({
            "id": "chatcmpl_test",
            "object": "chat.completion",
            "created": CREATED_TS,
            "model": MODEL,
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": "Hello back"},
                "finish_reason": "stop"
            }],
            "usage": {"prompt_tokens": 42, "completion_tokens": 7, "total_tokens": 49}
        }),
    );

    let adapter = adapter(&server);
    let request = Request {
        messages: vec![Message::system("Be concise"), Message::user("Hello")],
        tools: Some(vec![ToolDefinition::function(
            "search",
            "Search files",
            serde_json::json!({"type": "object", "properties": {"query": {"type": "string"}}}),
        )]),
        temperature: Some(0.5),
        ..base_request(MODEL)
    };

    let response = adapter
        .complete(&request)
        .await
        .expect("complete should succeed");
    mock.assert();
    (take_capture(&slot), response)
}

#[tokio::test]
async fn system_and_tools_encode() {
    let (capture, _) = system_and_tools_roundtrip().await;
    fabro_test::fabro_json_snapshot!(capture);
}

#[tokio::test]
async fn system_and_tools_decode() {
    let (_, response) = system_and_tools_roundtrip().await;
    fabro_test::fabro_json_snapshot!(response);
}

// ---------------------------------------------------------------------------
// Encode
// ---------------------------------------------------------------------------

#[tokio::test]
async fn encode_multi_turn() {
    let capture = encode_capture(&corpus_multi_turn(MODEL)).await;
    fabro_test::fabro_json_snapshot!(capture.body);
}

#[tokio::test]
async fn encode_tool_choice_auto() {
    let capture = encode_capture(&corpus_tools(MODEL, Some(ToolChoice::Auto))).await;
    fabro_test::fabro_json_snapshot!(capture.body);
}

#[tokio::test]
async fn encode_tool_choice_required() {
    let capture = encode_capture(&corpus_tools(MODEL, Some(ToolChoice::Required))).await;
    fabro_test::fabro_json_snapshot!(capture.body);
}

#[tokio::test]
async fn encode_tool_choice_named() {
    let capture = encode_capture(&corpus_tools(MODEL, Some(ToolChoice::named("search")))).await;
    fabro_test::fabro_json_snapshot!(capture.body);
}

#[tokio::test]
async fn encode_tool_choice_none() {
    let capture = encode_capture(&corpus_tools(MODEL, Some(ToolChoice::None))).await;
    fabro_test::fabro_json_snapshot!(capture.body);
}

#[tokio::test]
async fn encode_tool_round_trip() {
    let capture = encode_capture(&corpus_tool_round_trip(MODEL)).await;
    fabro_test::fabro_json_snapshot!(capture.body);
}

/// Assistant thinking parts echo back as `reasoning_content` (required by
/// Kimi and DeepSeek during tool-call continuations).
#[tokio::test]
async fn encode_thinking_round_trip_as_reasoning_content() {
    let capture = encode_capture(&corpus_thinking_round_trip(MODEL)).await;
    fabro_test::fabro_json_snapshot!(capture.body);
}

/// The compat encoder performs no attachment I/O: images are dropped
/// outright, documents become fallback text.
#[tokio::test]
async fn encode_inline_attachments() {
    let capture = encode_capture(&corpus_inline_attachments(MODEL)).await;
    fabro_test::fabro_json_snapshot!(capture.body);
}

#[tokio::test]
async fn encode_url_attachments() {
    let capture = encode_capture(&corpus_url_attachments(MODEL)).await;
    fabro_test::fabro_json_snapshot!(capture.body);
}

#[tokio::test]
async fn encode_bad_file_path_attachments() {
    let capture = encode_capture(&corpus_bad_file_path_attachments(MODEL)).await;
    fabro_test::fabro_json_snapshot!(capture.body);
}

#[tokio::test]
async fn encode_audio_attachment() {
    let capture = encode_capture(&corpus_audio_attachment(MODEL)).await;
    fabro_test::fabro_json_snapshot!(capture.body);
}

#[tokio::test]
async fn encode_response_format_json_object() {
    let format = ResponseFormat {
        kind:        ResponseFormatType::JsonObject,
        json_schema: None,
        strict:      false,
    };
    let capture = encode_capture(&corpus_response_format(MODEL, format)).await;
    fabro_test::fabro_json_snapshot!(capture.body);
}

#[tokio::test]
async fn encode_response_format_json_schema() {
    let capture = encode_capture(&corpus_response_format(MODEL, json_schema_format())).await;
    fabro_test::fabro_json_snapshot!(capture.body);
}

#[tokio::test]
async fn encode_sampling_params() {
    let capture = encode_capture(&corpus_sampling_params(MODEL)).await;
    fabro_test::fabro_json_snapshot!(capture.body);
}

#[tokio::test]
async fn encode_kimi_k3_uses_catalog_reasoning_and_sampling_controls() {
    let catalog = Arc::new(Catalog::from_builtin().expect("built-in catalog should build"));
    let request = Request {
        model: "kimi-k3".to_string(),
        reasoning_effort: Some(ReasoningEffort::High),
        temperature: Some(0.7),
        top_p: Some(0.9),
        ..base_request(MODEL)
    };
    let capture = encode_capture_with(&request, move |adapter| {
        adapter.with_name("moonshot").with_catalog(catalog)
    })
    .await;

    assert_eq!(capture.body["model"], "kimi-k3");
    assert_eq!(capture.body["reasoning_effort"], "high");
    assert!(capture.body.get("temperature").is_none());
    assert!(capture.body.get("top_p").is_none());
}

/// Counts JSON objects anywhere in `value` carrying a `cache_control` key.
fn count_cache_control_breakpoints(value: &serde_json::Value) -> usize {
    match value {
        serde_json::Value::Object(map) => {
            usize::from(map.contains_key("cache_control"))
                + map
                    .values()
                    .map(count_cache_control_breakpoints)
                    .sum::<usize>()
        }
        serde_json::Value::Array(items) => items.iter().map(count_cache_control_breakpoints).sum(),
        _ => 0,
    }
}

/// Builtin catalog with the opt-in OpenRouter provider enabled.
fn openrouter_catalog() -> Arc<Catalog> {
    let overrides: LlmCatalogSettings = toml::from_str("[providers.openrouter]\nenabled = true\n")
        .expect("override TOML should parse");
    Arc::new(
        Catalog::from_builtin_with_overrides(&overrides)
            .expect("catalog with OpenRouter enabled should build"),
    )
}

/// System + tools + two user turns against an OpenRouter model.
fn openrouter_multi_turn(model: &str) -> Request {
    Request {
        messages: vec![
            Message::system("You are a careful reviewer."),
            Message::user("Review this."),
            Message::assistant("Looking now."),
            Message::user("Focus on the tests."),
        ],
        ..corpus_tools(model, None)
    }
}

/// OpenRouter serves Claude through this adapter, and Anthropic prompt
/// caching is opt-in per request: OpenRouter only forwards a cache write when
/// the body carries explicit ephemeral `cache_control` breakpoints (OpenAI
/// models cache implicitly; Anthropic models never do). The catalog row
/// declares `cache_control_breakpoints`, so the encoded request must mark the
/// cacheable prefix — otherwise every turn bills at the full uncached input
/// rate.
#[tokio::test]
async fn encode_openrouter_claude_marks_prompt_cache_breakpoints() {
    let catalog = openrouter_catalog();
    let model = catalog
        .get_on_provider(&ProviderId::new("openrouter"), "claude-fable-5")
        .expect("OpenRouter Claude row should exist in the built-in catalog");
    assert!(model.features.prompt_cache);
    assert!(model.features.cache_control_breakpoints);

    let request = openrouter_multi_turn("claude-fable-5");
    let capture = encode_capture_with(&request, move |adapter| {
        adapter.with_name("openrouter").with_catalog(catalog)
    })
    .await;

    assert_eq!(capture.body["model"], "anthropic/claude-fable-5");
    let messages = &capture.body["messages"];
    // The system prompt converts to parts form carrying a breakpoint; it
    // covers the tool definitions too (tools precede system upstream).
    assert_eq!(messages[0]["content"][0]["type"], "text");
    assert_eq!(
        messages[0]["content"][0]["text"],
        "You are a careful reviewer."
    );
    assert_eq!(
        messages[0]["content"][0]["cache_control"]["type"],
        "ephemeral"
    );
    // The second-to-last user turn carries the conversation breakpoint...
    assert_eq!(messages[1]["content"][0]["text"], "Review this.");
    assert_eq!(
        messages[1]["content"][0]["cache_control"]["type"],
        "ephemeral"
    );
    // ...and the newest turn stays in plain-string form.
    assert_eq!(messages[3]["content"], "Focus on the tests.");
    assert_eq!(count_cache_control_breakpoints(&capture.body), 2);
}

/// Models with implicit (server-side) caching must NOT get breakpoints even
/// though they support prompt caching — the annotation is an Anthropic-ism
/// the catalog row has to opt into.
#[tokio::test]
async fn encode_openrouter_implicit_cache_model_stays_plain() {
    let catalog = openrouter_catalog();
    let model = catalog
        .get_on_provider(&ProviderId::new("openrouter"), "gpt-5.6-luna")
        .expect("OpenRouter GPT row should exist in the built-in catalog");
    assert!(model.features.prompt_cache);
    assert!(!model.features.cache_control_breakpoints);

    let request = openrouter_multi_turn("gpt-5.6-luna");
    let capture = encode_capture_with(&request, move |adapter| {
        adapter.with_name("openrouter").with_catalog(catalog)
    })
    .await;

    assert_eq!(count_cache_control_breakpoints(&capture.body), 0);
    assert_eq!(
        capture.body["messages"][0]["content"],
        "You are a careful reviewer."
    );
}

/// `provider_options.openrouter.auto_cache = false` disables the breakpoints,
/// and the control key is consumed rather than merged into the body.
#[tokio::test]
async fn encode_openrouter_claude_auto_cache_opt_out() {
    let request = Request {
        provider_options: Some(serde_json::json!({"openrouter": {"auto_cache": false}})),
        ..openrouter_multi_turn("claude-fable-5")
    };
    let capture = encode_capture_with(&request, move |adapter| {
        adapter
            .with_name("openrouter")
            .with_catalog(openrouter_catalog())
    })
    .await;

    assert_eq!(count_cache_control_breakpoints(&capture.body), 0);
    assert!(capture.body.get("auto_cache").is_none());
}

/// The provider_options namespace key is the runtime adapter NAME, not a
/// static "openai_compatible" key (pinned in-module by
/// `provider_options_uses_adapter_name`; this pins it from outside).
#[tokio::test]
async fn encode_provider_options_keyed_by_adapter_name() {
    let request = corpus_provider_options(
        MODEL,
        serde_json::json!({"moonshot": {"repetition_penalty": 1.2}}),
    );
    let capture = encode_capture_with(&request, |adapter| adapter.with_name("moonshot")).await;
    fabro_test::fabro_json_snapshot!(capture.body);
}

/// Options under a key that does not match the adapter name must not merge.
#[tokio::test]
async fn encode_provider_options_other_namespace_ignored() {
    let request = corpus_provider_options(
        MODEL,
        serde_json::json!({"openai": {"repetition_penalty": 1.2}}),
    );
    let capture = encode_capture_with(&request, |adapter| adapter.with_name("moonshot")).await;
    fabro_test::fabro_json_snapshot!(capture.body);
}

/// The compat adapter has no count-tokens wire route.
#[tokio::test]
async fn count_input_tokens_unavailable() {
    let server = MockServer::start();
    let adapter = adapter(&server);
    let count = adapter
        .count_input_tokens(&base_request(MODEL))
        .await
        .unwrap();
    assert!(count.is_none());
}

// ---------------------------------------------------------------------------
// Decode
// ---------------------------------------------------------------------------

async fn decode_response(body: serde_json::Value) -> fabro_llm::types::Response {
    let server = MockServer::start();
    let (mock, _slot) = mount_capture(&server, "/chat/completions", body);
    let adapter = adapter(&server);
    let response = adapter
        .complete(&base_request(MODEL))
        .await
        .expect("complete should succeed");
    mock.assert();
    response
}

/// Streams an SSE transcript and returns the final accumulated response.
async fn stream_final_response(sse_body: &str) -> fabro_llm::types::Response {
    use futures::StreamExt;

    let server = MockServer::start();
    let (mock, _slot) = mount_capture_sse(&server, "/chat/completions", sse_body);
    let adapter = adapter(&server);
    let mut stream = adapter
        .stream(&base_request(MODEL))
        .await
        .expect("stream should start");
    let mut accumulator = StreamAccumulator::new();
    while let Some(item) = stream.next().await {
        accumulator.process(&item.expect("stream event should decode"));
    }
    mock.assert();
    accumulator
        .response()
        .cloned()
        .expect("stream should emit a finish event")
}

#[tokio::test]
async fn decode_tool_calls_with_string_arguments() {
    let response = decode_response(serde_json::json!({
        "id": "chatcmpl_test",
        "object": "chat.completion",
        "created": CREATED_TS,
        "model": MODEL,
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": null,
                "tool_calls": [{
                    "id": "call_abc",
                    "type": "function",
                    "function": {"name": "search", "arguments": "{\"query\":\"foo\"}"}
                }]
            },
            "finish_reason": "tool_calls"
        }],
        "usage": {"prompt_tokens": 30, "completion_tokens": 12, "total_tokens": 42}
    }))
    .await;
    fabro_test::fabro_json_snapshot!(response);
}

#[tokio::test]
async fn decode_reasoning_content_as_thinking() {
    let response = decode_response(serde_json::json!({
        "id": "chatcmpl_test",
        "object": "chat.completion",
        "created": CREATED_TS,
        "model": MODEL,
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": "4.",
                "reasoning_content": "The user wants 2+2."
            },
            "finish_reason": "stop"
        }],
        "usage": {"prompt_tokens": 25, "completion_tokens": 40, "total_tokens": 65}
    }))
    .await;
    fabro_test::fabro_json_snapshot!(response);
}

// ---------------------------------------------------------------------------
// Structured reasoning details
// ---------------------------------------------------------------------------

/// The structured channel classifies summary and trace independently.
#[tokio::test]
async fn decode_reasoning_details_normalize_summary_and_trace() {
    let response = decode_response(body_with_message(&serde_json::json!({
        "role": "assistant",
        "content": "4.",
        "reasoning_details": [
            {"type": "reasoning.summary", "summary": "the user wants 2+2", "index": 0},
            {"type": "reasoning.text", "text": "2 plus 2 is 4", "index": 1},
        ]
    })))
    .await;

    let reasoning = response.reasoning_output().expect("reasoning present");
    assert_eq!(reasoning.summary(), Some("the user wants 2+2"));
    assert_eq!(reasoning.trace(), Some("2 plus 2 is 4"));
}

/// Encrypted entries stay in the opaque provider part for future replay but
/// never reach the normalized output.
#[tokio::test]
async fn decode_reasoning_details_preserve_encrypted_entries_opaquely() {
    let response = decode_response(body_with_message(&serde_json::json!({
        "role": "assistant",
        "content": "4.",
        "reasoning_details": [
            {"type": "reasoning.encrypted", "data": "gAAAAAopaque", "index": 0},
            {"type": "reasoning.summary", "summary": "visible", "index": 1},
        ]
    })))
    .await;

    let opaque = response
        .message
        .content
        .iter()
        .find_map(|part| match part {
            fabro_llm::types::ContentPart::Other { kind, data }
                if kind == fabro_llm::types::ContentPart::OPENAI_COMPAT_REASONING_DETAILS =>
            {
                Some(data)
            }
            _ => None,
        })
        .expect("opaque reasoning details preserved");
    assert_eq!(opaque[0]["data"], "gAAAAAopaque");

    let reasoning = response.reasoning_output().expect("reasoning present");
    assert_eq!(reasoning.summary(), Some("visible"));
    assert!(reasoning.trace().is_none());
}

/// Complete-response details are already assembled and must retain their
/// received block boundaries.
#[tokio::test]
async fn decode_reasoning_details_preserves_complete_entries_verbatim() {
    let details = serde_json::json!([
        {"type": "reasoning.summary", "summary": "first"},
        {"type": "reasoning.summary", "summary": "second"},
    ]);
    let response = decode_response(body_with_message(&serde_json::json!({
        "role": "assistant",
        "content": "4.",
        "reasoning_details": details,
    })))
    .await;

    let opaque = response
        .message
        .content
        .iter()
        .find_map(|part| match part {
            fabro_llm::types::ContentPart::Other { kind, data }
                if kind == fabro_llm::types::ContentPart::OPENAI_COMPAT_REASONING_DETAILS =>
            {
                Some(data)
            }
            _ => None,
        })
        .expect("opaque reasoning details preserved");
    assert_eq!(opaque, &details);

    let reasoning = response.reasoning_output().expect("reasoning present");
    assert_eq!(reasoning.summary(), Some("first\n\nsecond"));
}

/// Unknown and malformed detail entries must not fail an otherwise valid
/// completion.
#[tokio::test]
async fn decode_tolerates_unknown_and_malformed_reasoning_details() {
    let response = decode_response(body_with_message(&serde_json::json!({
        "role": "assistant",
        "content": "4.",
        "reasoning_details": [
            {"type": "reasoning.future", "text": "new channel", "extra": {"nested": true}},
            {"type": "reasoning.summary", "summary": 7},
            "not-an-object",
            42,
        ]
    })))
    .await;

    assert_eq!(response.text(), "4.");
    assert!(response.reasoning_output().is_none());
}

/// A scalar `reasoning_details` carries nothing replayable and is dropped
/// without disturbing the rest of the response.
#[tokio::test]
async fn decode_ignores_scalar_reasoning_details() {
    let response = decode_response(body_with_message(&serde_json::json!({
        "role": "assistant",
        "content": "4.",
        "reasoning_details": "unexpected"
    })))
    .await;

    assert_eq!(response.text(), "4.");
    assert!(response.reasoning_output().is_none());
}

/// OpenRouter returns both the structured channel and a flattened copy of
/// the same material; the summary must not appear twice.
#[tokio::test]
async fn decode_structured_details_suppress_the_duplicate_flattened_value() {
    let response = decode_response(body_with_message(&serde_json::json!({
        "role": "assistant",
        "content": "4.",
        "reasoning": "the user wants 2+2",
        "reasoning_details": [
            {"type": "reasoning.summary", "summary": "the user wants 2+2", "index": 0},
        ]
    })))
    .await;

    let reasoning = response.reasoning_output().expect("reasoning present");
    assert_eq!(reasoning.summary(), Some("the user wants 2+2"));
    assert!(reasoning.trace().is_none());
}

/// A structured trace takes precedence over the flattened trace channel.
#[tokio::test]
async fn decode_structured_trace_takes_precedence_over_flattened_trace() {
    let response = decode_response(body_with_message(&serde_json::json!({
        "role": "assistant",
        "content": "4.",
        "reasoning": "flattened trace",
        "reasoning_details": [{"type": "reasoning.text", "text": "verbatim trace", "index": 0}]
    })))
    .await;

    let reasoning = response.reasoning_output().expect("reasoning present");
    assert!(reasoning.summary().is_none());
    assert_eq!(reasoning.trace(), Some("verbatim trace"));
}

/// A structured summary and distinct flattened trace are both retained.
#[tokio::test]
async fn decode_structured_summary_keeps_distinct_flattened_trace() {
    let response = decode_response(body_with_message(&serde_json::json!({
        "role": "assistant",
        "content": "4.",
        "reasoning": "full verbatim trace",
        "reasoning_details": [
            {"type": "reasoning.summary", "summary": "short summary", "index": 0},
        ]
    })))
    .await;

    let reasoning = response.reasoning_output().expect("reasoning present");
    assert_eq!(reasoning.summary(), Some("short summary"));
    assert_eq!(reasoning.trace(), Some("full verbatim trace"));
}

/// Streamed detail fragments coalesce back into the same normalized output
/// the non-streaming body produces.
#[tokio::test]
async fn stream_reasoning_details_normalize_like_the_non_streaming_body() {
    let sse = support::sse_data_transcript(&[
        r#"{"id":"chatcmpl_stream","object":"chat.completion.chunk","created":1700000000,"model":"test-model","choices":[{"index":0,"delta":{"role":"assistant","reasoning_details":[{"type":"reasoning.summary","summary":"the user ","index":0},{"type":"reasoning.text","text":"2 plus ","index":1}]},"finish_reason":null}]}"#,
        r#"{"id":"chatcmpl_stream","object":"chat.completion.chunk","created":1700000000,"model":"test-model","choices":[{"index":0,"delta":{"reasoning_details":[{"type":"reasoning.summary","summary":"wants 2+2","index":0},{"type":"reasoning.text","text":"2 is 4","index":1}]},"finish_reason":null}]}"#,
        r#"{"id":"chatcmpl_stream","object":"chat.completion.chunk","created":1700000000,"model":"test-model","choices":[{"index":0,"delta":{"content":"4."},"finish_reason":null}]}"#,
        r#"{"id":"chatcmpl_stream","object":"chat.completion.chunk","created":1700000000,"model":"test-model","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}"#,
        "[DONE]",
    ]);
    let streamed = stream_final_response(&sse).await;

    let non_streamed = decode_response(body_with_message(&serde_json::json!({
        "role": "assistant",
        "content": "4.",
        "reasoning_details": [
            {"type": "reasoning.summary", "summary": "the user wants 2+2", "index": 0},
            {"type": "reasoning.text", "text": "2 plus 2 is 4", "index": 1},
        ]
    })))
    .await;

    assert_eq!(streamed.reasoning_output(), non_streamed.reasoning_output());
    let reasoning = streamed.reasoning_output().expect("reasoning present");
    assert_eq!(reasoning.summary(), Some("the user wants 2+2"));
    assert_eq!(reasoning.trace(), Some("2 plus 2 is 4"));
}

/// Providers may omit the optional index after the first fragment; the type
/// still identifies the logical detail being continued.
#[tokio::test]
async fn stream_reasoning_details_coalesce_when_a_later_fragment_omits_index() {
    let sse = support::sse_data_transcript(&[
        r#"{"id":"chatcmpl_stream","object":"chat.completion.chunk","created":1700000000,"model":"test-model","choices":[{"index":0,"delta":{"role":"assistant","reasoning_details":[{"type":"reasoning.text","text":"first ","index":0}]},"finish_reason":null}]}"#,
        r#"{"id":"chatcmpl_stream","object":"chat.completion.chunk","created":1700000000,"model":"test-model","choices":[{"index":0,"delta":{"reasoning_details":[{"type":"reasoning.text","text":"second"}]},"finish_reason":null}]}"#,
        r#"{"id":"chatcmpl_stream","object":"chat.completion.chunk","created":1700000000,"model":"test-model","choices":[{"index":0,"delta":{"content":"done"},"finish_reason":null}]}"#,
        r#"{"id":"chatcmpl_stream","object":"chat.completion.chunk","created":1700000000,"model":"test-model","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}"#,
        "[DONE]",
    ]);

    let response = stream_final_response(&sse).await;
    let reasoning = response.reasoning_output().expect("reasoning present");
    assert_eq!(reasoning.trace(), Some("first second"));
}

/// Cached and reasoning detail tokens are split into their own disjoint
/// buckets and subtracted out of input/output.
#[tokio::test]
async fn decode_usage_parses_token_details() {
    let response = decode_response(serde_json::json!({
        "id": "chatcmpl_test",
        "object": "chat.completion",
        "created": CREATED_TS,
        "model": MODEL,
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": "ok"},
            "finish_reason": "length"
        }],
        "usage": {
            "prompt_tokens": 100,
            "completion_tokens": 50,
            "total_tokens": 150,
            "prompt_tokens_details": {"cached_tokens": 80},
            "completion_tokens_details": {"reasoning_tokens": 20}
        }
    }))
    .await;
    fabro_test::fabro_json_snapshot!(response);
}

/// OpenRouter usage superset: in-band `cost` becomes an authoritative
/// `cost_usd`, and `cache_write_tokens` lands in its own disjoint bucket.
/// Unmodeled fields (`cost_details`, `audio_tokens`, top-level `provider`,
/// `native_finish_reason`) are tolerated and ignored.
#[tokio::test]
async fn decode_usage_openrouter_cost_and_cache_write() {
    let response = decode_response(serde_json::json!({
        "id": "gen_or_test",
        "object": "chat.completion",
        "created": CREATED_TS,
        "model": MODEL,
        "provider": "Anthropic",
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": "ok"},
            "finish_reason": "stop",
            "native_finish_reason": "end_turn"
        }],
        "usage": {
            "prompt_tokens": 200,
            "completion_tokens": 10,
            "total_tokens": 210,
            "cost": 0.0042,
            "cost_details": {"upstream_inference_cost": null},
            "prompt_tokens_details": {"cached_tokens": 50, "cache_write_tokens": 100, "audio_tokens": 0},
            "completion_tokens_details": {"reasoning_tokens": 0}
        }
    }))
    .await;
    fabro_test::fabro_json_snapshot!(response);
}

/// Venice reports authoritative USD cost in a top-level object rather than
/// the OpenRouter `usage.cost` field.
#[tokio::test]
async fn decode_usage_venice_top_level_cost() {
    let response = decode_response(serde_json::json!({
        "id": "chatcmpl_venice_test",
        "object": "chat.completion",
        "created": CREATED_TS,
        "model": MODEL,
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": "ok"},
            "finish_reason": "stop"
        }],
        "cost": {"usd": 0.00042, "diem": 0.0},
        "usage": {
            "prompt_tokens": 12,
            "completion_tokens": 2,
            "total_tokens": 14
        }
    }))
    .await;
    fabro_test::fabro_json_snapshot!(response);
}

// ---------------------------------------------------------------------------
// Stream
// ---------------------------------------------------------------------------

/// Shared setup for the happy-path text stream; the request and event halves
/// are pinned by separate tests.
async fn stream_text_happy_path_capture() -> (WireCapture, Vec<serde_json::Value>) {
    let sse = support::sse_data_transcript(&[
        r#"{"id":"chatcmpl_stream","object":"chat.completion.chunk","created":1700000000,"model":"test-model","choices":[{"index":0,"delta":{"role":"assistant","content":"Hel"},"finish_reason":null}]}"#,
        r#"{"id":"chatcmpl_stream","object":"chat.completion.chunk","created":1700000000,"model":"test-model","choices":[{"index":0,"delta":{"content":"lo"},"finish_reason":null}]}"#,
        r#"{"id":"chatcmpl_stream","object":"chat.completion.chunk","created":1700000000,"model":"test-model","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}"#,
        r#"{"id":"chatcmpl_stream","object":"chat.completion.chunk","created":1700000000,"model":"test-model","choices":[],"usage":{"prompt_tokens":11,"completion_tokens":5,"total_tokens":16}}"#,
        "[DONE]",
    ]);
    stream_capture(&base_request(MODEL), &sse).await
}

/// The captured request pins the streaming request shape, including the usage
/// opt-in required for the trailing usage chunk.
#[tokio::test]
async fn stream_text_happy_path_request() {
    let (capture, _) = stream_text_happy_path_capture().await;
    fabro_test::fabro_json_snapshot!(capture.body);
}

#[tokio::test]
async fn stream_text_happy_path_events() {
    let (_, events) = stream_text_happy_path_capture().await;
    support::assert_stream_starts(&events);
    fabro_test::fabro_json_snapshot!(events);
}

/// OpenRouter streams report `cost` in the usage chunk; the Finish response
/// carries it as authoritative, with cached tokens in their own bucket.
#[tokio::test]
async fn stream_usage_openrouter_cost() {
    let sse = support::sse_data_transcript(&[
        r#"{"id":"gen_or_stream","object":"chat.completion.chunk","created":1700000000,"model":"test-model","choices":[{"index":0,"delta":{"role":"assistant","content":"Hi"},"finish_reason":null}]}"#,
        r#"{"id":"gen_or_stream","object":"chat.completion.chunk","created":1700000000,"model":"test-model","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}"#,
        r#"{"id":"gen_or_stream","object":"chat.completion.chunk","created":1700000000,"model":"test-model","choices":[],"usage":{"prompt_tokens":12,"completion_tokens":2,"total_tokens":14,"cost":0.00031,"prompt_tokens_details":{"cached_tokens":4,"cache_write_tokens":0}}}"#,
        "[DONE]",
    ]);
    let (_capture, events) = stream_capture(&base_request(MODEL), &sse).await;
    fabro_test::fabro_json_snapshot!(events);
}

/// Venice streams authoritative USD cost in a top-level object on the usage
/// chunk.
#[tokio::test]
async fn stream_usage_venice_top_level_cost() {
    let sse = support::sse_data_transcript(&[
        r#"{"id":"chatcmpl_venice_stream","object":"chat.completion.chunk","created":1700000000,"model":"test-model","choices":[{"index":0,"delta":{"role":"assistant","content":"Hi"},"finish_reason":null}]}"#,
        r#"{"id":"chatcmpl_venice_stream","object":"chat.completion.chunk","created":1700000000,"model":"test-model","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}"#,
        r#"{"id":"chatcmpl_venice_stream","object":"chat.completion.chunk","created":1700000000,"model":"test-model","choices":[],"cost":{"usd":0.00031,"diem":0.0},"usage":{"prompt_tokens":12,"completion_tokens":2,"total_tokens":14}}"#,
        "[DONE]",
    ]);
    let (_capture, events) = stream_capture(&base_request(MODEL), &sse).await;
    fabro_test::fabro_json_snapshot!(events);
}

#[tokio::test]
async fn stream_tool_call_deltas() {
    let sse = support::sse_data_transcript(&[
        r#"{"id":"chatcmpl_stream","object":"chat.completion.chunk","created":1700000000,"model":"test-model","choices":[{"index":0,"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_abc","type":"function","function":{"name":"search","arguments":""}}]},"finish_reason":null}]}"#,
        r#"{"id":"chatcmpl_stream","object":"chat.completion.chunk","created":1700000000,"model":"test-model","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"qu"}}]},"finish_reason":null}]}"#,
        r#"{"id":"chatcmpl_stream","object":"chat.completion.chunk","created":1700000000,"model":"test-model","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"ery\":\"foo\"}"}}]},"finish_reason":null}]}"#,
        r#"{"id":"chatcmpl_stream","object":"chat.completion.chunk","created":1700000000,"model":"test-model","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}"#,
        r#"{"id":"chatcmpl_stream","object":"chat.completion.chunk","created":1700000000,"model":"test-model","choices":[],"usage":{"prompt_tokens":20,"completion_tokens":9,"total_tokens":29}}"#,
        "[DONE]",
    ]);
    let (_capture, events) =
        stream_capture(&corpus_tools(MODEL, Some(ToolChoice::Auto)), &sse).await;
    fabro_test::fabro_json_snapshot!(events);
}

#[tokio::test]
async fn stream_reasoning_deltas() {
    let sse = support::sse_data_transcript(&[
        r#"{"id":"chatcmpl_stream","object":"chat.completion.chunk","created":1700000000,"model":"test-model","choices":[{"index":0,"delta":{"role":"assistant","reasoning":"Let me "},"finish_reason":null}]}"#,
        r#"{"id":"chatcmpl_stream","object":"chat.completion.chunk","created":1700000000,"model":"test-model","choices":[{"index":0,"delta":{"reasoning_content":"think"},"finish_reason":null}]}"#,
        r#"{"id":"chatcmpl_stream","object":"chat.completion.chunk","created":1700000000,"model":"test-model","choices":[{"index":0,"delta":{"content":"4."},"finish_reason":null}]}"#,
        r#"{"id":"chatcmpl_stream","object":"chat.completion.chunk","created":1700000000,"model":"test-model","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}"#,
        "[DONE]",
    ]);
    let (_capture, events) = stream_capture(&base_request(MODEL), &sse).await;
    fabro_test::fabro_json_snapshot!(events);
}

/// Minimax tolerance: a stream that ends without `[DONE]` still synthesizes
/// the finish — but only because content was started.
#[tokio::test]
async fn stream_without_done_synthesizes_finish_when_content_started() {
    let sse = support::sse_data_transcript(&[
        r#"{"id":"chatcmpl_stream","object":"chat.completion.chunk","created":1700000000,"model":"test-model","choices":[{"index":0,"delta":{"role":"assistant","content":"Hello"},"finish_reason":null}]}"#,
        r#"{"id":"chatcmpl_stream","object":"chat.completion.chunk","created":1700000000,"model":"test-model","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}"#,
    ]);
    let (_capture, events) = stream_capture(&base_request(MODEL), &sse).await;
    fabro_test::fabro_json_snapshot!(events);
}

/// The other half of the minimax contract: no content started and no
/// `[DONE]` — nothing is synthesized. `StreamStart` is not synthesis: the
/// provider did send a chunk, so the liveness edge is a fact about this
/// stream even though nothing usable followed.
#[tokio::test]
async fn stream_without_done_or_content_synthesizes_nothing() {
    let sse = support::sse_data_transcript(&[
        r#"{"id":"chatcmpl_stream","object":"chat.completion.chunk","created":1700000000,"model":"test-model","choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null}]}"#,
    ]);
    let (_capture, events) = stream_capture(&base_request(MODEL), &sse).await;
    fabro_test::fabro_json_snapshot!(events);
}

// ---------------------------------------------------------------------------
// Custom-named route identity
// ---------------------------------------------------------------------------

/// The compat adapter already stamped the configured name; pinned here to
/// complete the per-dialect identity matrix.
#[tokio::test]
async fn custom_named_complete_identity() {
    let server = MockServer::start();
    let (mock, _slot) = mount_capture(&server, "/chat/completions", minimal_body());
    let adapter = adapter(&server).with_name("moonshot");
    let response = adapter
        .complete(&base_request(MODEL))
        .await
        .expect("complete should succeed");
    mock.assert();
    assert_eq!(response.provider, "moonshot");
}
