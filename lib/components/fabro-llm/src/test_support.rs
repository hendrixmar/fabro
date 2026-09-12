//! Test doubles for crates that drive the LLM client.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use fabro_auth::test_support::env_credential_source;
use fabro_config::LlmLayer;
use futures::stream;
use lithos_llm::adapter::{ProviderAdapter, ResolvedCall};
use lithos_llm::catalog::{AdapterId, Catalog, ModelId, ProviderId};
use lithos_llm::client::Client;
use lithos_llm::middleware::RetryPolicy;
use lithos_llm::types::{
    ContentBlockId, ContentBlockKind, ContentPart, Error, FinishReason, Response, ResponseStream,
    StreamEvent, TokenCounts, ToolCallKind, ToolInput,
};

use crate::client::{ClientOptions, build_client, build_offline_client};

/// The lithos built-in catalog, as Fabro ships it.
#[must_use]
pub fn test_catalog() -> Catalog {
    crate::build_catalog(&LlmLayer::default(), &|_| None).expect("test catalog should build")
}

/// The test catalog with an operator overlay applied.
#[must_use]
pub fn test_catalog_with_overlay(overlay: &str) -> Catalog {
    let overlay = LlmLayer(toml::from_str(overlay).expect("overlay should parse"));
    crate::build_catalog(&overlay, &|_| None).expect("test catalog with overlay should build")
}

/// The test catalog with one provider's base URL pointed elsewhere.
#[must_use]
pub fn test_catalog_with_provider_base_url(provider: &str, base_url: &str) -> Catalog {
    test_catalog_with_overlay(&format!(
        "[providers.{provider}]\nbase_url = {}\n",
        toml::Value::String(base_url.to_string())
    ))
}

/// Builds a text response attributed to `provider/model`.
#[must_use]
pub fn text_response(provider: &str, model: &str, text: &str) -> Response {
    let mut response = Response::new(ProviderId::new(provider), ModelId::new(model), vec![
        ContentPart::Text {
            text: text.to_string(),
        },
    ]);
    response.usage = TokenCounts {
        input: 10,
        output: 5,
        ..TokenCounts::default()
    };
    response
}

/// Replays a response as the event stream a codec would produce.
#[must_use]
pub fn response_to_stream(response: Response) -> ResponseStream {
    let mut events: Vec<Result<StreamEvent, Error>> = vec![Ok(StreamEvent::Started {
        id: response.id.clone(),
    })];
    for (index, part) in response.content.iter().enumerate() {
        let id = ContentBlockId::new(format!("block_{index}"));
        match part {
            ContentPart::Text { text } => {
                events.push(Ok(StreamEvent::ContentBlockStart {
                    id:   id.clone(),
                    kind: ContentBlockKind::Text,
                }));
                events.push(Ok(StreamEvent::TextDelta {
                    id:   id.clone(),
                    text: text.clone(),
                }));
            }
            ContentPart::Reasoning(reasoning) => {
                events.push(Ok(StreamEvent::ContentBlockStart {
                    id:   id.clone(),
                    kind: ContentBlockKind::Reasoning,
                }));
                events.push(Ok(StreamEvent::ReasoningDelta {
                    id:   id.clone(),
                    text: reasoning.text.clone(),
                }));
            }
            ContentPart::ToolCall(call) => {
                events.push(Ok(StreamEvent::ContentBlockStart {
                    id:   id.clone(),
                    kind: ContentBlockKind::ToolCall {
                        id:   call.id.clone(),
                        name: Some(call.name.clone()),
                        kind: match call.input {
                            ToolInput::Custom(_) => ToolCallKind::Custom,
                            _ => ToolCallKind::Function,
                        },
                    },
                }));
                events.push(Ok(StreamEvent::ToolCallDelta {
                    id:        id.clone(),
                    arguments: call.input.raw().to_string(),
                }));
            }
            _ => {}
        }
        events.push(Ok(StreamEvent::ContentBlockEnd {
            id,
            part: part.clone(),
        }));
    }
    events.push(Ok(StreamEvent::Usage {
        usage: response.usage,
    }));
    events.push(Ok(StreamEvent::Ended {
        response: Box::new(response),
    }));
    ResponseStream::new(stream::iter(events))
}

/// An adapter that answers from a script of responses, repeating the last.
pub struct ScriptedAdapter {
    id:         AdapterId,
    responses:  Vec<Response>,
    call_index: AtomicUsize,
}

impl ScriptedAdapter {
    #[must_use]
    pub fn new(responses: Vec<Response>) -> Self {
        Self {
            id: AdapterId::new("scripted"),
            responses,
            call_index: AtomicUsize::new(0),
        }
    }

    fn next_response(&self) -> Response {
        let index = self.call_index.fetch_add(1, Ordering::SeqCst);
        self.responses[index.min(self.responses.len() - 1)].clone()
    }

    #[must_use]
    pub fn calls(&self) -> usize {
        self.call_index.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ProviderAdapter for ScriptedAdapter {
    fn id(&self) -> &AdapterId {
        &self.id
    }

    async fn complete(&self, _call: &ResolvedCall) -> Result<Response, Error> {
        Ok(self.next_response())
    }

    async fn stream(&self, _call: &ResolvedCall) -> Result<ResponseStream, Error> {
        Ok(response_to_stream(self.next_response()))
    }
}

/// A retry policy for tests: three attempts with no delay between them, so a
/// test counts provider calls without waiting.
pub fn test_retry_policy() -> RetryPolicy {
    RetryPolicy::exponential()
        .max_attempts(3)
        .initial_delay(Duration::ZERO)
        .max_delay(Duration::ZERO)
        .jitter(false)
}

/// An adapter that fails every call with a fresh error from `factory`.
pub struct FailingAdapter {
    id:      AdapterId,
    factory: Box<dyn Fn() -> Error + Send + Sync>,
    calls:   AtomicUsize,
}

impl FailingAdapter {
    pub fn new(factory: impl Fn() -> Error + Send + Sync + 'static) -> Self {
        Self {
            id:      AdapterId::new("failing"),
            factory: Box::new(factory),
            calls:   AtomicUsize::new(0),
        }
    }

    #[must_use]
    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ProviderAdapter for FailingAdapter {
    fn id(&self) -> &AdapterId {
        &self.id
    }

    async fn complete(&self, _call: &ResolvedCall) -> Result<Response, Error> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err((self.factory)())
    }

    async fn stream(&self, _call: &ResolvedCall) -> Result<ResponseStream, Error> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err((self.factory)())
    }
}

/// A client over the test catalog that routes `provider` to `adapter`, with
/// no retries.
#[must_use]
pub fn client_with_adapter(provider: &str, adapter: Arc<dyn ProviderAdapter>) -> Client {
    client_with_adapters(vec![(provider, adapter)], ClientOptions::default())
}

/// A client over the test catalog whose providers are all served by the given
/// adapters, built with `options`.
#[must_use]
pub fn client_with_adapters(
    adapters: Vec<(&str, Arc<dyn ProviderAdapter>)>,
    mut options: ClientOptions,
) -> Client {
    for (provider, adapter) in adapters {
        options.adapters.push((ProviderId::new(provider), adapter));
    }
    build_offline_client(test_catalog(), options)
        .expect("test client should build")
        .client
}

/// A client whose ready providers come only from `env_lookup`.
pub async fn client_from_env<F>(catalog: Catalog, env_lookup: F, options: ClientOptions) -> Client
where
    F: Fn(&str) -> Option<String> + Send + Sync + 'static,
{
    build_client(catalog, env_credential_source(env_lookup), options)
        .await
        .expect("test client should build")
        .client
}

/// A finished response marker for tests that need a finish reason.
#[must_use]
pub fn with_finish_reason(mut response: Response, finish_reason: FinishReason) -> Response {
    response.finish_reason = finish_reason;
    response
}
