use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use fabro_auth::{ApiCredential, CredentialSource};
use fabro_model::{AdapterKind, Catalog, ModelSelectionError, ProviderId};
use tracing::debug;

use crate::adapter_registry::{
    AdapterConfig, AdapterKindOptions, OpenAiAdapterOptions, factory_for,
};
use crate::cost;
use crate::error::{Error, ProviderErrorKind};
use crate::middleware::{Middleware, NextFn, NextStreamFn};
use crate::provider::{ProviderAdapter, StreamEventStream};
use crate::token_count::{
    InputTokenCount, InputTokenCountMethod, InputTokenCountPreference, estimate_input_tokens,
};
use crate::types::{Request, Response, Speed, StreamEvent, Warning};

/// The core client that routes requests to provider adapters (Section 2.2, 3).
#[derive(Clone)]
pub struct Client {
    providers:        HashMap<String, Arc<dyn ProviderAdapter>>,
    default_provider: Option<String>,
    middleware:       Vec<Arc<dyn Middleware>>,
    catalog:          Option<Arc<Catalog>>,
}

#[derive(Debug, Clone)]
pub struct ProviderRegistrationIssue {
    pub provider: ProviderId,
    pub error:    Error,
}

#[derive(Clone)]
pub struct ClientRegistrationReport {
    pub client:              Client,
    pub registration_issues: Vec<ProviderRegistrationIssue>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RegistrationMode {
    FailFast,
    CollectIssues,
}

struct ResolvedRequest {
    provider: Arc<dyn ProviderAdapter>,
    request:  Request,
}

impl Client {
    /// Create a new Client with explicit configuration.
    #[must_use]
    pub fn new(
        providers: HashMap<String, Arc<dyn ProviderAdapter>>,
        default_provider: Option<String>,
        middleware: Vec<Arc<dyn Middleware>>,
    ) -> Self {
        Self {
            providers,
            default_provider,
            middleware,
            catalog: None,
        }
    }

    /// Create a Client from a credential source.
    ///
    /// # Errors
    ///
    /// Returns `Error` if the source cannot resolve credentials or any provider
    /// adapter fails to initialize.
    pub async fn from_source(
        source: &dyn CredentialSource,
        catalog: Arc<Catalog>,
    ) -> Result<Self, Error> {
        let resolved = source
            .resolve(&catalog)
            .await
            .map_err(|err| Error::Configuration {
                message: format!("Failed to resolve LLM credentials: {err}"),
                source:  None,
            })?;
        Self::from_credentials(resolved.credentials, catalog).await
    }

    /// Create a Client report from a credential source.
    ///
    /// # Errors
    ///
    /// Returns `Error` only when the credential source itself fails. Provider
    /// adapter construction/registration failures are recorded on the report.
    pub async fn from_source_report(
        source: &dyn CredentialSource,
        catalog: Arc<Catalog>,
    ) -> Result<ClientRegistrationReport, Error> {
        let resolved = source
            .resolve(&catalog)
            .await
            .map_err(|err| Error::Configuration {
                message: format!("Failed to resolve LLM credentials: {err}"),
                source:  None,
            })?;
        Ok(Self::from_credentials_report(resolved.credentials, catalog).await)
    }

    /// Create a Client from typed provider credentials.
    ///
    /// # Errors
    ///
    /// Returns `Error` if any provider adapter fails to initialize.
    pub async fn from_credentials(
        credentials: Vec<ApiCredential>,
        catalog: Arc<Catalog>,
    ) -> Result<Self, Error> {
        let (report, error) =
            Self::from_credentials_internal(credentials, catalog, RegistrationMode::FailFast).await;
        if let Some(error) = error {
            return Err(error);
        }
        Ok(report.client)
    }

    /// Create a Client while collecting provider adapter registration failures.
    ///
    /// Providers whose credentials resolve but whose adapter cannot be
    /// constructed or initialized are omitted from the returned client and
    /// reported in `registration_issues`.
    pub async fn from_credentials_report(
        credentials: Vec<ApiCredential>,
        catalog: Arc<Catalog>,
    ) -> ClientRegistrationReport {
        let (report, _) =
            Self::from_credentials_internal(credentials, catalog, RegistrationMode::CollectIssues)
                .await;
        report
    }

    async fn from_credentials_internal(
        credentials: Vec<ApiCredential>,
        catalog: Arc<Catalog>,
        mode: RegistrationMode,
    ) -> (ClientRegistrationReport, Option<Error>) {
        let mut client = Self {
            providers:        HashMap::new(),
            default_provider: None,
            middleware:       Vec::new(),
            catalog:          Some(Arc::clone(&catalog)),
        };
        let mut registration_issues = Vec::new();

        for credential in credentials {
            let provider_id = credential.provider.clone();
            let adapter = if let Some(provider) = catalog.provider(&provider_id) {
                let factory = factory_for(provider.adapter);
                let kind_options = match provider.adapter {
                    AdapterKind::OpenAi => AdapterKindOptions::OpenAi(OpenAiAdapterOptions {
                        codex_mode: credential.codex_mode,
                        org_id:     credential.org_id,
                        project_id: credential.project_id,
                    }),
                    _ => AdapterKindOptions::None,
                };
                factory(AdapterConfig {
                    provider_id: provider.id.to_string(),
                    auth_header: credential.auth_header,
                    base_url: credential.base_url.or_else(|| provider.base_url.clone()),
                    extra_headers: credential.extra_headers,
                    kind_options,
                    catalog: Some(Arc::clone(&catalog)),
                })
            } else {
                Err(Error::Configuration {
                    message: format!(
                        "Provider \"{provider_id}\" is not supported by credential-only registration"
                    ),
                    source:  None,
                })
            };
            match adapter {
                Ok(adapter) => {
                    if let Err(error) = client.register_provider(adapter).await {
                        if mode == RegistrationMode::FailFast {
                            return (
                                ClientRegistrationReport {
                                    client,
                                    registration_issues,
                                },
                                Some(error),
                            );
                        }
                        registration_issues.push(ProviderRegistrationIssue {
                            provider: provider_id,
                            error,
                        });
                    }
                }
                Err(error) => {
                    if mode == RegistrationMode::FailFast {
                        return (
                            ClientRegistrationReport {
                                client,
                                registration_issues,
                            },
                            Some(error),
                        );
                    }
                    registration_issues.push(ProviderRegistrationIssue {
                        provider: provider_id,
                        error,
                    });
                }
            }
        }

        debug!(
            providers = ?client.provider_names(),
            default = ?client.default_provider(),
            "LLM client initialized from typed credentials"
        );

        (
            ClientRegistrationReport {
                client,
                registration_issues,
            },
            None,
        )
    }

    /// Register a provider adapter. Calls `initialize()` on the adapter
    /// (Section 2.4).
    ///
    /// # Errors
    ///
    /// Returns `Error` if the adapter's `initialize()` method fails.
    pub async fn register_provider(
        &mut self,
        adapter: Arc<dyn ProviderAdapter>,
    ) -> Result<(), Error> {
        adapter.initialize().await?;
        let name = adapter.name().to_string();
        if self.default_provider.is_none() {
            self.default_provider = Some(name.clone());
        }
        self.providers.insert(name.clone(), adapter);
        debug!(provider = %name, "Provider registered");
        Ok(())
    }

    /// Add middleware.
    pub fn add_middleware(&mut self, mw: Arc<dyn Middleware>) {
        self.middleware.push(mw);
    }

    fn canonical_provider_name(&self, provider_name: &str) -> String {
        self.catalog
            .as_ref()
            .and_then(|catalog| catalog.provider(&ProviderId::new(provider_name)))
            .map_or_else(
                || provider_name.to_string(),
                |provider| provider.id.to_string(),
            )
    }

    fn provider_adapter(&self, provider_name: &str) -> Option<Arc<dyn ProviderAdapter>> {
        let canonical = self.canonical_provider_name(provider_name);
        self.providers.get(&canonical).cloned().or_else(|| {
            self.providers.iter().find_map(|(name, adapter)| {
                (self.canonical_provider_name(name) == canonical).then(|| Arc::clone(adapter))
            })
        })
    }

    fn eligible_provider_ids(&self) -> HashSet<ProviderId> {
        self.providers
            .keys()
            .map(|provider| ProviderId::new(self.canonical_provider_name(provider)))
            .collect()
    }

    /// Resolve one concrete provider/model offering and canonicalize a cloned
    /// request. Explicit-provider unknown models remain passthrough values.
    fn resolve_request_with_adapter(&self, request: &Request) -> Result<ResolvedRequest, Error> {
        let mut resolved = request.clone();
        let Some(catalog) = &self.catalog else {
            let provider_name = request
                .provider
                .as_deref()
                .or(self.default_provider.as_deref())
                .ok_or_else(|| Error::Configuration {
                    message: "No provider specified and no default provider set".into(),
                    source:  None,
                })?;
            let provider =
                self.provider_adapter(provider_name)
                    .ok_or_else(|| Error::Configuration {
                        message: format!("Provider '{provider_name}' not registered"),
                        source:  None,
                    })?;
            resolved.provider = Some(provider.name().to_string());
            return Ok(ResolvedRequest {
                provider,
                request: resolved,
            });
        };

        let eligible = self.eligible_provider_ids();
        if let Some(explicit) = request.provider.as_deref() {
            let explicit = ProviderId::new(explicit);
            if catalog.provider(&explicit).is_some() {
                let selected = catalog
                    .resolve_selection(Some(&request.model), Some(&explicit), &eligible)
                    .map_err(selection_error)?;
                let provider = self
                    .provider_adapter(selected.provider.as_str())
                    .ok_or_else(|| {
                        selection_error(ModelSelectionError::ProviderUnavailable {
                            provider: selected.provider.clone(),
                        })
                    })?;
                resolved.model = selected.model;
                resolved.provider = Some(selected.provider.into_inner());
                return Ok(ResolvedRequest {
                    provider,
                    request: resolved,
                });
            }

            let provider =
                self.provider_adapter(explicit.as_str())
                    .ok_or_else(|| Error::Configuration {
                        message: format!("Provider '{explicit}' not registered"),
                        source:  None,
                    })?;
            resolved.provider = Some(provider.name().to_string());
            return Ok(ResolvedRequest {
                provider,
                request: resolved,
            });
        }

        match catalog.select(&request.model, None, &eligible) {
            Ok(model) => {
                let provider = self
                    .provider_adapter(model.provider.as_str())
                    .ok_or_else(|| {
                        selection_error(ModelSelectionError::ProviderUnavailable {
                            provider: model.provider.clone(),
                        })
                    })?;
                resolved.model = model.id.to_string();
                resolved.provider = Some(model.provider.to_string());
                Ok(ResolvedRequest {
                    provider,
                    request: resolved,
                })
            }
            Err(ModelSelectionError::UnknownSelector { .. }) => {
                let provider_name =
                    self.default_provider
                        .as_deref()
                        .ok_or_else(|| Error::Configuration {
                            message: "No provider specified and no default provider set".into(),
                            source:  None,
                        })?;
                let provider =
                    self.provider_adapter(provider_name)
                        .ok_or_else(|| Error::Configuration {
                            message: format!("Provider '{provider_name}' not registered"),
                            source:  None,
                        })?;
                resolved.provider = Some(self.canonical_provider_name(provider.name()));
                Ok(ResolvedRequest {
                    provider,
                    request: resolved,
                })
            }
            Err(error) => Err(selection_error(error)),
        }
    }

    /// Resolve the concrete provider/model route and return a canonicalized
    /// clone of the request without dispatching it.
    ///
    /// This is useful at persistence and API boundaries that must expose the
    /// selected provider alongside the canonical model ID. The caller-owned
    /// request is never modified.
    pub fn resolve_request(&self, request: &Request) -> Result<Request, Error> {
        self.resolve_request_with_adapter(request)
            .map(|resolved| resolved.request)
    }

    fn validate_request_controls(&self, request: &Request) -> Result<(), Error> {
        let Some(catalog) = &self.catalog else {
            return Ok(());
        };
        let Some(provider) = request.provider.as_deref() else {
            return Ok(());
        };
        let Some(model) = catalog.get_on_provider(&ProviderId::new(provider), &request.model)
        else {
            return Ok(());
        };
        let Some(settings) = catalog.settings_for(model) else {
            return Ok(());
        };
        let model_id = model.id.as_str();

        if let Some(effort) = request.reasoning_effort {
            if !settings.controls.reasoning_effort.contains(&effort) {
                return Err(Error::InvalidRequest {
                    message: format!(
                        "model '{model_id}' does not support reasoning_effort '{effort}'; allowed values: {}",
                        format_control_values(&settings.controls.reasoning_effort),
                    ),
                });
            }
        }

        if let Some(speed) = request.speed {
            if speed != Speed::Standard && !settings.controls.speed.contains(&speed) {
                return Err(Error::InvalidRequest {
                    message: format!(
                        "model '{model_id}' does not support speed '{speed}'; allowed values: standard{}",
                        format_additional_speeds(&settings.controls.speed),
                    ),
                });
            }
        }

        Ok(())
    }

    /// Send a blocking request (Section 4.1).
    ///
    /// # Errors
    ///
    /// Returns `Error::InvalidRequest` when a catalog-declared request control
    /// is unsupported, `Error::Configuration` if no provider is specified or
    /// registered, or any provider/middleware error encountered during the
    /// request.
    pub async fn complete(&self, request: &Request) -> Result<Response, Error> {
        let ResolvedRequest { provider, request } = self.resolve_request_with_adapter(request)?;
        self.validate_request_controls(&request)?;

        if self.middleware.is_empty() {
            return complete_stamped(&provider, self.catalog.as_deref(), &request).await;
        }

        // Build middleware chain. Cost is stamped at the base so middleware
        // observes the final response.
        let catalog = self.catalog.clone();
        let base: NextFn = Arc::new(move |req: Request| {
            let provider = provider.clone();
            let catalog = catalog.clone();
            Box::pin(async move { complete_stamped(&provider, catalog.as_deref(), &req).await })
        });

        let chain = self.middleware.iter().rev().fold(base, |next, mw| {
            let mw = mw.clone();
            Arc::new(move |req: Request| {
                let mw = mw.clone();
                let next = next.clone();
                Box::pin(async move { mw.handle_complete(req, next).await })
            })
        });

        chain(request).await
    }

    /// Send a streaming request (Section 4.2).
    ///
    /// # Errors
    ///
    /// Returns `Error::InvalidRequest` when a catalog-declared request control
    /// is unsupported, `Error::Configuration` if no provider is specified or
    /// registered, or any provider/middleware error encountered during the
    /// request.
    pub async fn stream(&self, request: &Request) -> Result<StreamEventStream, Error> {
        let ResolvedRequest { provider, request } = self.resolve_request_with_adapter(request)?;
        self.validate_request_controls(&request)?;

        if self.middleware.is_empty() {
            return stream_stamped(&provider, self.catalog.clone(), &request).await;
        }

        // Build streaming middleware chain. Cost is stamped at the base so
        // middleware observes the final Finish events.
        let catalog = self.catalog.clone();
        let base: NextStreamFn = Arc::new(move |req: Request| {
            let provider = provider.clone();
            let catalog = catalog.clone();
            Box::pin(async move { stream_stamped(&provider, catalog, &req).await })
        });

        let chain = self.middleware.iter().rev().fold(base, |next, mw| {
            let mw = mw.clone();
            Arc::new(move |req: Request| {
                let mw = mw.clone();
                let next = next.clone();
                Box::pin(async move { mw.handle_stream(req, next).await })
            })
        });

        chain(request).await
    }

    /// Count the model-visible input/context tokens for a request without
    /// creating a completion.
    ///
    /// # Errors
    ///
    /// Returns request validation/provider resolution errors, and returns
    /// provider count errors when the selected preference requires provider
    /// semantics or when the error is not fallback-eligible.
    pub async fn count_input_tokens(
        &self,
        request: &Request,
        preference: InputTokenCountPreference,
    ) -> Result<InputTokenCount, Error> {
        let ResolvedRequest { provider, request } = self.resolve_request_with_adapter(request)?;
        self.validate_request_controls(&request)?;
        provider.validate_request(&request)?;

        if preference == InputTokenCountPreference::EstimateOnly {
            return Ok(estimate_input_tokens(&request, provider.name()));
        }

        match provider.count_input_tokens(&request).await {
            Ok(Some(count)) => Ok(count),
            Ok(None) if preference == InputTokenCountPreference::PreferProvider => {
                Ok(fallback_estimate(
                    &request,
                    provider.name(),
                    "provider_token_count_unsupported",
                    "provider does not support input token counting; returned local estimate",
                ))
            }
            Ok(None) => Err(Error::Configuration {
                message: format!(
                    "provider '{}' does not support input token counting",
                    provider.name()
                ),
                source:  None,
            }),
            Err(error)
                if preference == InputTokenCountPreference::PreferProvider
                    && token_count_fallback_eligible(&error) =>
            {
                Ok(fallback_estimate(
                    &request,
                    provider.name(),
                    "provider_token_count_failed",
                    "provider input token counting failed; returned local estimate",
                ))
            }
            Err(error) => Err(error),
        }
    }

    /// Close all provider adapters.
    ///
    /// # Errors
    ///
    /// Returns any error from a provider adapter's `close()` method.
    pub async fn close(&self) -> Result<(), Error> {
        for provider in self.providers.values() {
            provider.close().await?;
        }
        Ok(())
    }

    /// Get the list of registered provider names.
    #[must_use]
    pub fn provider_names(&self) -> Vec<&str> {
        self.providers
            .keys()
            .map(std::string::String::as_str)
            .collect()
    }

    /// Canonical IDs for provider adapters that registered successfully.
    #[must_use]
    pub fn provider_ids(&self) -> HashSet<ProviderId> {
        self.eligible_provider_ids()
    }

    /// Check whether a provider adapter is registered.
    #[must_use]
    pub fn has_provider(&self, name: &str) -> bool {
        self.providers.contains_key(name)
            || self
                .catalog
                .as_ref()
                .and_then(|catalog| catalog.provider(&ProviderId::new(name)))
                .is_some_and(|provider| self.providers.contains_key(provider.id.as_str()))
    }

    /// Get the default provider name.
    #[must_use]
    pub fn default_provider(&self) -> Option<&str> {
        self.default_provider.as_deref()
    }
}

fn selection_error(error: ModelSelectionError) -> Error {
    Error::configuration_error(error.to_string(), error)
}

/// Validate, run, and cost-stamp a blocking request. Shared by
/// [`Client::complete`]'s direct path and its middleware-chain base so cost
/// stamping stays single-sited.
async fn complete_stamped(
    provider: &Arc<dyn ProviderAdapter>,
    catalog: Option<&Catalog>,
    request: &Request,
) -> Result<Response, Error> {
    provider.validate_request(request)?;
    let mut response = provider.complete(request).await?;
    let selected_provider = request
        .provider
        .as_deref()
        .unwrap_or_else(|| provider.name());
    response.model.clone_from(&request.model);
    response.provider = selected_provider.to_string();
    cost::apply_estimated_cost(
        catalog,
        selected_provider,
        &request.model,
        request.speed,
        &mut response,
    );
    Ok(response)
}

/// Validate and run a streaming request, cost-stamping terminal
/// [`StreamEvent::Finish`] responses. Shared by [`Client::stream`]'s direct
/// path and its middleware-chain base so cost stamping stays single-sited.
async fn stream_stamped(
    provider: &Arc<dyn ProviderAdapter>,
    catalog: Option<Arc<Catalog>>,
    request: &Request,
) -> Result<StreamEventStream, Error> {
    provider.validate_request(request)?;
    let stream = provider.stream(request).await?;
    Ok(stamp_stream_costs(
        catalog,
        request
            .provider
            .clone()
            .unwrap_or_else(|| provider.name().to_string()),
        request.model.clone(),
        request.speed,
        stream,
    ))
}

/// Wrap a provider event stream so terminal [`StreamEvent::Finish`]
/// responses carry a catalog-estimated cost, mirroring what
/// [`Client::complete`] stamps on blocking responses.
fn stamp_stream_costs(
    catalog: Option<Arc<Catalog>>,
    provider: String,
    model: String,
    speed: Option<Speed>,
    stream: StreamEventStream,
) -> StreamEventStream {
    use futures::StreamExt;

    Box::pin(stream.map(move |event| {
        event.map(|mut event| {
            if let StreamEvent::Finish { response, .. } | StreamEvent::StepFinish { response, .. } =
                &mut event
            {
                response.model.clone_from(&model);
                response.provider.clone_from(&provider);
                cost::apply_estimated_cost(catalog.as_deref(), &provider, &model, speed, response);
            }
            event
        })
    }))
}

fn token_count_fallback_eligible(error: &Error) -> bool {
    matches!(
        error,
        Error::Network { .. }
            | Error::RequestTimeout { .. }
            | Error::Provider {
                kind: ProviderErrorKind::RateLimit | ProviderErrorKind::Server,
                ..
            }
    )
}

fn fallback_estimate(
    request: &Request,
    provider: &str,
    code: &'static str,
    message: &'static str,
) -> InputTokenCount {
    let mut count = estimate_input_tokens(request, provider);
    if count.method == InputTokenCountMethod::LocalEstimate
        && !count
            .warnings
            .iter()
            .any(|warning| warning.code.as_deref() == Some(code))
    {
        count.warnings.push(Warning {
            message: message.to_string(),
            code:    Some(code.to_string()),
        });
    }
    count
}

fn format_control_values<T: ToString>(values: &[T]) -> String {
    if values.is_empty() {
        "none".to_string()
    } else {
        values
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    }
}

fn format_additional_speeds(values: &[Speed]) -> String {
    if values.is_empty() {
        String::new()
    } else {
        format!(", {}", format_control_values(values))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use fabro_auth::{ApiKeyHeader, CredentialSource, ResolvedCredentials};
    use fabro_model::ProviderId;
    use fabro_model::catalog::LlmCatalogSettings;
    use futures::stream;

    use super::*;
    use crate::adapter_registry;
    use crate::error::ProviderErrorDetail;
    use crate::providers::openai_compatible;
    use crate::types::*;

    /// A mock provider for testing.
    struct MockProvider {
        provider_name: String,
        response_text: String,
    }

    impl MockProvider {
        fn new(name: &str, response: &str) -> Self {
            Self {
                provider_name: name.to_string(),
                response_text: response.to_string(),
            }
        }
    }

    #[async_trait::async_trait]
    impl ProviderAdapter for MockProvider {
        fn name(&self) -> &str {
            &self.provider_name
        }

        async fn complete(&self, _request: &Request) -> Result<Response, Error> {
            Ok(Response {
                id:            "resp_mock".into(),
                model:         "mock-model".into(),
                provider:      self.provider_name.clone(),
                message:       Message::assistant(&self.response_text),
                finish_reason: FinishReason::Stop,
                usage:         TokenCounts {
                    input_tokens: 10,
                    output_tokens: 20,
                    ..Default::default()
                },
                raw:           None,
                warnings:      vec![],
                rate_limit:    None,
                cost_usd:      None,
                cost_source:   None,
            })
        }

        async fn stream(&self, _request: &Request) -> Result<StreamEventStream, Error> {
            let text = self.response_text.clone();
            let provider = self.provider_name.clone();
            let events = vec![
                Ok(StreamEvent::text_delta(&text, Some("t1".into()))),
                Ok(StreamEvent::finish(
                    FinishReason::Stop,
                    TokenCounts::default(),
                    Response {
                        id: "resp_mock".into(),
                        model: "mock-model".into(),
                        provider,
                        message: Message::assistant(&text),
                        finish_reason: FinishReason::Stop,
                        usage: TokenCounts::default(),
                        raw: None,
                        warnings: vec![],
                        rate_limit: None,
                        cost_usd: None,
                        cost_source: None,
                    },
                )),
            ];
            Ok(Box::pin(stream::iter(events)))
        }
    }

    fn test_request() -> Request {
        Request {
            model:            "mock-model".into(),
            messages:         vec![Message::user("Hello")],
            provider:         None,
            tools:            None,
            tool_choice:      None,
            response_format:  None,
            temperature:      None,
            top_p:            None,
            max_tokens:       None,
            stop_sequences:   None,
            reasoning_effort: None,
            speed:            None,
            metadata:         None,
            provider_options: None,
        }
    }

    struct CountingProvider {
        provider_name: String,
        count_result:  std::sync::Mutex<Result<Option<InputTokenCount>, Error>>,
        count_calls:   Arc<AtomicUsize>,
        reject_named:  bool,
    }

    impl CountingProvider {
        fn new(result: Result<Option<InputTokenCount>, Error>) -> Self {
            Self {
                provider_name: "counter".to_string(),
                count_result:  std::sync::Mutex::new(result),
                count_calls:   Arc::new(AtomicUsize::new(0)),
                reject_named:  false,
            }
        }

        fn with_name(mut self, name: &str) -> Self {
            self.provider_name = name.to_string();
            self
        }

        fn count_calls(&self) -> Arc<AtomicUsize> {
            Arc::clone(&self.count_calls)
        }

        fn rejecting_named(mut self) -> Self {
            self.reject_named = true;
            self
        }
    }

    #[async_trait::async_trait]
    impl ProviderAdapter for CountingProvider {
        fn name(&self) -> &str {
            &self.provider_name
        }

        async fn complete(&self, _request: &Request) -> Result<Response, Error> {
            unimplemented!()
        }

        async fn stream(&self, _request: &Request) -> Result<StreamEventStream, Error> {
            unimplemented!()
        }

        fn supports_tool_choice(&self, mode: &str) -> bool {
            !(self.reject_named && mode == "named")
        }

        async fn count_input_tokens(
            &self,
            _request: &Request,
        ) -> Result<Option<InputTokenCount>, Error> {
            self.count_calls.fetch_add(1, Ordering::SeqCst);
            self.count_result.lock().unwrap().clone()
        }
    }

    fn provider_count(tokens: i64) -> InputTokenCount {
        InputTokenCount {
            input_tokens: tokens,
            method:       InputTokenCountMethod::ProviderApi,
            provider:     "counter".to_string(),
            model:        "mock-model".to_string(),
            warnings:     vec![],
        }
    }

    fn warning_codes(count: &InputTokenCount) -> Vec<&str> {
        count
            .warnings
            .iter()
            .filter_map(|warning| warning.code.as_deref())
            .collect()
    }

    fn provider_error(kind: ProviderErrorKind) -> Error {
        Error::Provider {
            kind,
            detail: Box::new(ProviderErrorDetail::new("provider failed", "counter")),
        }
    }

    async fn client_with_counting_provider(
        provider: CountingProvider,
    ) -> (Client, Arc<AtomicUsize>) {
        let calls = provider.count_calls();
        let mut client = Client::new(HashMap::new(), None, vec![]);
        client.register_provider(Arc::new(provider)).await.unwrap();
        (client, calls)
    }

    struct StubSource {
        credentials: Vec<ApiCredential>,
    }

    fn catalog_with(overrides: &str) -> Arc<Catalog> {
        let settings: LlmCatalogSettings = toml::from_str(overrides).unwrap();
        Arc::new(Catalog::from_builtin_with_overrides(&settings).unwrap())
    }

    #[async_trait]
    impl CredentialSource for StubSource {
        async fn resolve(&self, catalog: &Catalog) -> anyhow::Result<ResolvedCredentials> {
            let _ = catalog;
            Ok(ResolvedCredentials {
                credentials: self.credentials.clone(),
                auth_issues: Vec::new(),
            })
        }

        async fn configured_providers(&self, catalog: &Catalog) -> Vec<fabro_model::ProviderId> {
            let _ = catalog;
            self.credentials
                .iter()
                .map(|credential| credential.provider.clone())
                .collect()
        }
    }

    #[tokio::test]
    async fn complete_routes_to_default_provider() {
        let mut client = Client::new(HashMap::new(), None, vec![]);
        client
            .register_provider(Arc::new(MockProvider::new("test", "Hello!")))
            .await
            .unwrap();

        let response = client.complete(&test_request()).await.unwrap();
        assert_eq!(response.text(), "Hello!");
        assert_eq!(response.provider, "test");
    }

    /// Hermetic catalog pricing `mock-model` under the `test` provider so
    /// cost stamping has something to estimate from.
    fn priced_mock_catalog() -> Arc<Catalog> {
        let settings: LlmCatalogSettings = toml::from_str(
            r#"
[providers.test]
display_name = "Test"
adapter = "openai_compatible"
base_url = "https://test.invalid/v1"

[models."mock-model"]
provider = "test"
display_name = "Mock"
family = "mock"
default = true

[models."mock-model".limits]
context_window = 100000

[models."mock-model".features]
tools = false
vision = false
reasoning = false

[models."mock-model".costs]
input_cost_per_mtok = 1.0
output_cost_per_mtok = 2.0
"#,
        )
        .unwrap();
        Arc::new(Catalog::from_settings(&settings).unwrap())
    }

    fn portable_model_catalog(openrouter_base_url: &str) -> Arc<Catalog> {
        let settings: LlmCatalogSettings = toml::from_str(&format!(
            r#"
[providers.openai]
display_name = "OpenAI"
adapter = "openai_compatible"
agent_profile = "openai"
base_url = "https://openai.invalid/v1"
priority = 90

[providers.openai.models."gpt-5.6-sol"]
display_name = "GPT-5.6 Sol"
family = "gpt-5"
aliases = ["gpt-56-sol"]
default = true

[providers.openai.models."gpt-5.6-sol".limits]
context_window = 1000

[providers.openai.models."gpt-5.6-sol".features]
tools = true
vision = false
reasoning = false

[providers.openai.models."gpt-5.6-sol".costs]
input_cost_per_mtok = 1.0
output_cost_per_mtok = 2.0

[providers.openrouter]
display_name = "OpenRouter"
adapter = "openai_compatible"
agent_profile = "openai"
base_url = "{openrouter_base_url}"
priority = 25

[providers.openrouter.models."gpt-5.6-sol"]
api_id = "openai/gpt-5.6-sol"
display_name = "GPT-5.6 Sol (via OpenRouter)"
family = "gpt-5"
aliases = ["gpt-56-sol"]
default = true

[providers.openrouter.models."gpt-5.6-sol".limits]
context_window = 1000

[providers.openrouter.models."gpt-5.6-sol".features]
tools = true
vision = false
reasoning = false

[providers.openrouter.models."gpt-5.6-sol".costs]
input_cost_per_mtok = 10.0
output_cost_per_mtok = 20.0
"#,
        ))
        .unwrap();
        Arc::new(Catalog::from_settings(&settings).unwrap())
    }

    async fn portable_mock_client(catalog: &Arc<Catalog>, providers: &[&str]) -> Client {
        let mut client = Client::new(HashMap::new(), None, vec![]);
        for provider in providers {
            client
                .register_provider(Arc::new(MockProvider::new(provider, provider)))
                .await
                .unwrap();
        }
        client.catalog = Some(Arc::clone(catalog));
        client
    }

    #[tokio::test]
    async fn shared_alias_selects_by_ready_providers_and_priority_without_mutating_request() {
        let catalog = portable_model_catalog("https://openrouter.invalid/v1");
        let mut original = test_request();
        original.model = "gpt-56-sol".to_string();

        let direct = portable_mock_client(&catalog, &["openai"]).await;
        let direct_request = direct.resolve_request(&original).unwrap();
        assert_eq!(direct_request.model, "gpt-5.6-sol");
        assert_eq!(direct_request.provider.as_deref(), Some("openai"));

        let aggregator = portable_mock_client(&catalog, &["openrouter"]).await;
        let aggregator_request = aggregator.resolve_request(&original).unwrap();
        assert_eq!(aggregator_request.model, "gpt-5.6-sol");
        assert_eq!(aggregator_request.provider.as_deref(), Some("openrouter"));

        let both = portable_mock_client(&catalog, &["openrouter", "openai"]).await;
        let both_request = both.resolve_request(&original).unwrap();
        assert_eq!(both_request.provider.as_deref(), Some("openai"));

        assert_eq!(original.model, "gpt-56-sol");
        assert_eq!(original.provider, None);

        let response = aggregator.complete(&original).await.unwrap();
        assert_eq!(response.model, "gpt-5.6-sol");
        assert_eq!(response.provider, "openrouter");
        assert_eq!(response.cost_source, Some(CostSource::Estimated));
        assert_eq!(response.cost_usd, Some(0.0005));
    }

    #[tokio::test]
    async fn explicit_provider_pins_shared_alias_and_preserves_unknown_passthrough() {
        let catalog = portable_model_catalog("https://openrouter.invalid/v1");
        let client = portable_mock_client(&catalog, &["openai", "openrouter"]).await;

        let mut aliased = test_request();
        aliased.model = "gpt-56-sol".to_string();
        aliased.provider = Some("openrouter".to_string());
        let resolved = client.resolve_request(&aliased).unwrap();
        assert_eq!(resolved.model, "gpt-5.6-sol");
        assert_eq!(resolved.provider.as_deref(), Some("openrouter"));

        let mut unknown = test_request();
        unknown.model = "provider-private-preview".to_string();
        unknown.provider = Some("openrouter".to_string());
        let resolved = client.resolve_request(&unknown).unwrap();
        assert_eq!(resolved.model, "provider-private-preview");
        assert_eq!(resolved.provider.as_deref(), Some("openrouter"));
    }

    #[tokio::test]
    async fn selected_offering_api_id_reaches_openai_compatible_wire_request() {
        let upstream = httpmock::MockServer::start_async().await;
        let completion = upstream
            .mock_async(|when, then| {
                when.method(httpmock::Method::POST)
                    .path("/chat/completions")
                    .json_body_includes(r#"{"model":"openai/gpt-5.6-sol"}"#);
                then.status(200)
                    .header("content-type", "application/json")
                    .json_body(serde_json::json!({
                        "id": "chatcmpl-portable",
                        "model": "openai/gpt-5.6-sol",
                        "choices": [{
                            "message": {"role": "assistant", "content": "OK"},
                            "finish_reason": "stop"
                        }],
                        "usage": {
                            "prompt_tokens": 1,
                            "completion_tokens": 1,
                            "total_tokens": 2
                        }
                    }));
            })
            .await;
        let catalog = portable_model_catalog(&upstream.base_url());
        let adapter = openai_compatible::Adapter::new("test-key", upstream.base_url())
            .with_name("openrouter")
            .with_catalog(Arc::clone(&catalog));
        let mut client = Client::new(HashMap::new(), None, vec![]);
        client.register_provider(Arc::new(adapter)).await.unwrap();
        client.catalog = Some(catalog);
        let mut request = test_request();
        request.model = "gpt-56-sol".to_string();

        let response = client.complete(&request).await.unwrap();

        assert_eq!(response.model, "gpt-5.6-sol");
        assert_eq!(response.provider, "openrouter");
        completion.assert_async().await;
    }

    #[tokio::test]
    async fn modal_routes_kimi_k3_with_proxy_headers_and_no_bearer_auth() {
        let upstream = httpmock::MockServer::start_async().await;
        let completion = upstream
            .mock_async(|when, then| {
                when.method(httpmock::Method::POST)
                    .path("/v1/chat/completions")
                    .header("Modal-Key", "wk-test")
                    .header("Modal-Secret", "ws-test")
                    .header_missing("Authorization")
                    .json_body_includes(r#"{"model":"moonshotai/Kimi-K3"}"#);
                then.status(200)
                    .header("content-type", "application/json")
                    .json_body(serde_json::json!({
                        "id": "chatcmpl-modal",
                        "model": "moonshotai/Kimi-K3",
                        "choices": [{
                            "message": {"role": "assistant", "content": "OK"},
                            "finish_reason": "stop"
                        }],
                        "usage": {
                            "prompt_tokens": 1,
                            "completion_tokens": 1,
                            "total_tokens": 2
                        }
                    }));
            })
            .await;
        let catalog = catalog_with(&format!(
            r#"
[providers.modal]
enabled = true
base_url = "{}/v1"
"#,
            upstream.base_url()
        ));
        let modal = ProviderId::new("modal");
        let client = Client::from_credentials(
            vec![ApiCredential::with_extra_headers(
                modal.clone(),
                HashMap::from([
                    ("Modal-Key".to_string(), "wk-test".to_string()),
                    ("Modal-Secret".to_string(), "ws-test".to_string()),
                ]),
            )],
            catalog,
        )
        .await
        .unwrap();
        let mut request = test_request();
        request.model = "kimi-k3".to_string();
        request.provider = Some(modal.to_string());

        let response = client.complete(&request).await.unwrap();

        assert_eq!(response.text(), "OK");
        assert_eq!(response.model, "kimi-k3");
        assert_eq!(response.provider, "modal");
        completion.assert_async().await;
    }

    #[tokio::test]
    async fn fireworks_routes_kimi_k3_fast_to_router_model_id() {
        let upstream = httpmock::MockServer::start_async().await;
        let completion = upstream
            .mock_async(|when, then| {
                when.method(httpmock::Method::POST)
                    .path("/inference/v1/chat/completions")
                    .header("Authorization", "Bearer test-key")
                    .json_body_includes(r#"{"model":"accounts/fireworks/routers/kimi-k3-fast"}"#);
                then.status(200)
                    .header("content-type", "application/json")
                    .json_body(serde_json::json!({
                        "id": "chatcmpl-fireworks",
                        "model": "accounts/fireworks/routers/kimi-k3-fast",
                        "choices": [{
                            "message": {"role": "assistant", "content": "OK"},
                            "finish_reason": "stop"
                        }],
                        "usage": {
                            "prompt_tokens": 1,
                            "completion_tokens": 1,
                            "total_tokens": 2
                        }
                    }));
            })
            .await;
        let catalog = catalog_with(&format!(
            r#"
[providers.fireworks]
enabled = true
base_url = "{}/inference/v1"
"#,
            upstream.base_url()
        ));
        let fireworks = ProviderId::new("fireworks");
        let client = Client::from_credentials(
            vec![
                ApiCredential::from_api_key(fireworks.clone(), "test-key".to_string(), &catalog)
                    .unwrap(),
            ],
            catalog,
        )
        .await
        .unwrap();
        let mut request = test_request();
        request.model = "kimi-k3-fast".to_string();
        request.provider = Some(fireworks.to_string());

        let response = client.complete(&request).await.unwrap();

        assert_eq!(response.text(), "OK");
        assert_eq!(response.model, "kimi-k3-fast");
        assert_eq!(response.provider, "fireworks");
        completion.assert_async().await;
    }

    #[tokio::test]
    async fn complete_stamps_estimated_cost_from_catalog() {
        let mut client = Client::new(HashMap::new(), None, vec![]);
        client
            .register_provider(Arc::new(MockProvider::new("test", "Hello!")))
            .await
            .unwrap();
        client.catalog = Some(priced_mock_catalog());

        let response = client.complete(&test_request()).await.unwrap();

        // 10 input tokens at $1/MTok + 20 output tokens at $2/MTok.
        assert_eq!(response.cost_source, Some(CostSource::Estimated));
        let cost = response.cost_usd.expect("cost should be stamped");
        assert!((cost - 0.000_05).abs() < 1e-12, "got {cost}");
    }

    #[tokio::test]
    async fn complete_leaves_cost_unset_without_catalog() {
        let mut client = Client::new(HashMap::new(), None, vec![]);
        client
            .register_provider(Arc::new(MockProvider::new("test", "Hello!")))
            .await
            .unwrap();

        let response = client.complete(&test_request()).await.unwrap();

        assert_eq!(response.cost_usd, None);
        assert_eq!(response.cost_source, None);
    }

    #[tokio::test]
    async fn complete_stamps_cost_beneath_middleware() {
        struct Passthrough;

        #[async_trait]
        impl Middleware for Passthrough {
            async fn handle_complete(
                &self,
                request: Request,
                next: NextFn,
            ) -> Result<Response, Error> {
                next(request).await
            }

            async fn handle_stream(
                &self,
                request: Request,
                next: NextStreamFn,
            ) -> Result<StreamEventStream, Error> {
                next(request).await
            }
        }

        let mut client = Client::new(HashMap::new(), None, vec![Arc::new(Passthrough)]);
        client
            .register_provider(Arc::new(MockProvider::new("test", "Hello!")))
            .await
            .unwrap();
        client.catalog = Some(priced_mock_catalog());

        let response = client.complete(&test_request()).await.unwrap();

        assert_eq!(response.cost_source, Some(CostSource::Estimated));
    }

    #[tokio::test]
    async fn stream_stamps_estimated_cost_on_finish() {
        use futures::StreamExt;

        let mut client = Client::new(HashMap::new(), None, vec![]);
        client
            .register_provider(Arc::new(MockProvider::new("test", "Hello!")))
            .await
            .unwrap();
        client.catalog = Some(priced_mock_catalog());

        let mut stream = client.stream(&test_request()).await.unwrap();
        let mut finish_response = None;
        while let Some(event) = stream.next().await {
            if let StreamEvent::Finish { response, .. } = event.unwrap() {
                finish_response = Some(response);
            }
        }

        let response = finish_response.expect("stream should yield a Finish event");
        // MockProvider's Finish usage is zero tokens — priced, just $0.
        assert_eq!(response.cost_source, Some(CostSource::Estimated));
        assert_eq!(response.cost_usd, Some(0.0));
    }

    #[tokio::test]
    async fn count_input_tokens_returns_provider_result() {
        let (client, calls) =
            client_with_counting_provider(CountingProvider::new(Ok(Some(provider_count(42)))))
                .await;

        let count = client
            .count_input_tokens(&test_request(), InputTokenCountPreference::PreferProvider)
            .await
            .unwrap();

        assert_eq!(count.input_tokens, 42);
        assert_eq!(count.method, InputTokenCountMethod::ProviderApi);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn count_input_tokens_prefer_provider_falls_back_for_unsupported_adapter() {
        let mut client = Client::new(HashMap::new(), None, vec![]);
        client
            .register_provider(Arc::new(MockProvider::new("test", "")))
            .await
            .unwrap();

        let count = client
            .count_input_tokens(&test_request(), InputTokenCountPreference::PreferProvider)
            .await
            .unwrap();

        assert_eq!(count.method, InputTokenCountMethod::LocalEstimate);
        assert!(warning_codes(&count).contains(&"provider_token_count_unsupported"));
    }

    #[tokio::test]
    async fn count_input_tokens_require_provider_errors_for_unsupported_adapter() {
        let mut client = Client::new(HashMap::new(), None, vec![]);
        client
            .register_provider(Arc::new(MockProvider::new("test", "")))
            .await
            .unwrap();

        let error = client
            .count_input_tokens(&test_request(), InputTokenCountPreference::RequireProvider)
            .await
            .unwrap_err();

        assert!(matches!(error, Error::Configuration { .. }));
    }

    #[tokio::test]
    async fn count_input_tokens_prefer_provider_falls_back_for_eligible_errors() {
        let errors = vec![
            Error::Network {
                message: "network down".to_string(),
                source:  None,
            },
            Error::RequestTimeout {
                message: "timed out".to_string(),
                source:  None,
            },
            provider_error(ProviderErrorKind::RateLimit),
            provider_error(ProviderErrorKind::Server),
        ];

        for error in errors {
            let (client, _) =
                client_with_counting_provider(CountingProvider::new(Err(error))).await;
            let count = client
                .count_input_tokens(&test_request(), InputTokenCountPreference::PreferProvider)
                .await
                .unwrap();

            assert_eq!(count.method, InputTokenCountMethod::LocalEstimate);
            assert!(warning_codes(&count).contains(&"provider_token_count_failed"));
        }
    }

    #[tokio::test]
    async fn count_input_tokens_prefer_provider_returns_non_fallback_errors() {
        let errors = vec![
            provider_error(ProviderErrorKind::InvalidRequest),
            provider_error(ProviderErrorKind::Authentication),
            provider_error(ProviderErrorKind::AccessDenied),
            provider_error(ProviderErrorKind::NotFound),
            provider_error(ProviderErrorKind::ContextLength),
            provider_error(ProviderErrorKind::ContentFilter),
            provider_error(ProviderErrorKind::QuotaExceeded),
            Error::Configuration {
                message: "bad config".to_string(),
                source:  None,
            },
            Error::UnsupportedToolChoice {
                message: "bad tool choice".to_string(),
            },
        ];

        for error in errors {
            let (client, _) =
                client_with_counting_provider(CountingProvider::new(Err(error))).await;
            let err = client
                .count_input_tokens(&test_request(), InputTokenCountPreference::PreferProvider)
                .await
                .unwrap_err();

            assert!(!token_count_fallback_eligible(&err));
        }
    }

    #[tokio::test]
    async fn count_input_tokens_require_provider_returns_fallback_eligible_errors() {
        let (client, _) = client_with_counting_provider(CountingProvider::new(Err(
            provider_error(ProviderErrorKind::RateLimit),
        )))
        .await;

        let err = client
            .count_input_tokens(&test_request(), InputTokenCountPreference::RequireProvider)
            .await
            .unwrap_err();

        assert!(matches!(err, Error::Provider {
            kind: ProviderErrorKind::RateLimit,
            ..
        }));
    }

    #[tokio::test]
    async fn count_input_tokens_estimate_only_does_not_call_adapter() {
        let provider = CountingProvider::new(Ok(Some(provider_count(99))));
        let calls = provider.count_calls();
        let (client, _) = client_with_counting_provider(provider).await;

        let count = client
            .count_input_tokens(&test_request(), InputTokenCountPreference::EstimateOnly)
            .await
            .unwrap();

        assert_eq!(count.method, InputTokenCountMethod::LocalEstimate);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn count_input_tokens_validation_errors_still_return_err() {
        let (client, calls) = client_with_counting_provider(
            CountingProvider::new(Ok(Some(provider_count(1))))
                .with_name("restricted")
                .rejecting_named(),
        )
        .await;
        let mut request = test_request();
        request.tool_choice = Some(ToolChoice::named("search"));

        let err = client
            .count_input_tokens(&request, InputTokenCountPreference::PreferProvider)
            .await
            .unwrap_err();

        assert!(matches!(err, Error::UnsupportedToolChoice { .. }));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn complete_routes_to_named_provider() {
        let mut client = Client::new(HashMap::new(), None, vec![]);
        client
            .register_provider(Arc::new(MockProvider::new("provider_a", "from A")))
            .await
            .unwrap();
        client
            .register_provider(Arc::new(MockProvider::new("provider_b", "from B")))
            .await
            .unwrap();

        let mut req = test_request();
        req.provider = Some("provider_b".into());
        let response = client.complete(&req).await.unwrap();
        assert_eq!(response.text(), "from B");
    }

    #[tokio::test]
    async fn complete_errors_on_missing_provider() {
        let client = Client::new(HashMap::new(), None, vec![]);
        let result = client.complete(&test_request()).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Configuration { .. }));
    }

    #[tokio::test]
    async fn complete_errors_on_unknown_provider() {
        let mut client = Client::new(HashMap::new(), None, vec![]);
        client
            .register_provider(Arc::new(MockProvider::new("test", "Hello")))
            .await
            .unwrap();

        let mut req = test_request();
        req.provider = Some("nonexistent".into());
        let result = client.complete(&req).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Configuration { .. }));
    }

    #[tokio::test]
    async fn complete_rejects_unsupported_reasoning_effort_before_dispatch() {
        let catalog = Arc::new(Catalog::from_builtin().unwrap());
        let mut client = Client::new(HashMap::new(), None, vec![]);
        client.catalog = Some(Arc::clone(&catalog));
        client
            .register_provider(Arc::new(MockProvider::new(
                "moonshot",
                "should not dispatch",
            )))
            .await
            .unwrap();

        let mut request = test_request();
        request.model = "kimi-k2.5".to_string();
        request.provider = Some("moonshot".to_string());
        request.reasoning_effort = Some(ReasoningEffort::High);

        let err = client.complete(&request).await.unwrap_err();

        assert!(matches!(
            err,
            Error::InvalidRequest {
                ref message,
            } if message.contains("model 'kimi-k2.5' does not support reasoning_effort 'high'")
        ));
    }

    #[tokio::test]
    async fn complete_accepts_supported_kimi_k3_reasoning_effort() {
        let catalog = Arc::new(Catalog::from_builtin().unwrap());
        let mut client = Client::new(HashMap::new(), None, vec![]);
        client.catalog = Some(Arc::clone(&catalog));
        client
            .register_provider(Arc::new(MockProvider::new("moonshot", "accepted")))
            .await
            .unwrap();

        let mut request = test_request();
        request.model = "kimi-k3".to_string();
        request.provider = Some("moonshot".to_string());
        request.reasoning_effort = Some(ReasoningEffort::High);

        let response = client.complete(&request).await.unwrap();

        assert_eq!(response.text(), "accepted");
    }

    #[tokio::test]
    async fn complete_rejects_unsupported_speed_before_dispatch() {
        let catalog = Arc::new(Catalog::from_builtin().unwrap());
        let mut client = Client::new(HashMap::new(), None, vec![]);
        client.catalog = Some(Arc::clone(&catalog));
        client
            .register_provider(Arc::new(MockProvider::new("openai", "should not dispatch")))
            .await
            .unwrap();

        let mut request = test_request();
        request.model = "gpt-5.4".to_string();
        request.provider = Some("openai".to_string());
        request.speed = Some(Speed::Fast);

        let err = client.complete(&request).await.unwrap_err();

        assert!(matches!(
            err,
            Error::InvalidRequest {
                ref message,
            } if message.contains("model 'gpt-5.4' does not support speed 'fast'")
        ));
    }

    #[tokio::test]
    async fn complete_accepts_standard_speed_without_catalog_declaration() {
        let catalog = Arc::new(Catalog::from_builtin().unwrap());
        let mut client = Client::new(HashMap::new(), None, vec![]);
        client.catalog = Some(Arc::clone(&catalog));
        client
            .register_provider(Arc::new(MockProvider::new("openai", "standard")))
            .await
            .unwrap();

        let mut request = test_request();
        request.model = "gpt-5.4".to_string();
        request.provider = Some("openai".to_string());
        request.speed = Some(Speed::Standard);

        let response = client.complete(&request).await.unwrap();

        assert_eq!(response.text(), "standard");
    }

    #[tokio::test]
    async fn complete_accepts_reasoning_effort_for_anthropic_budget_fallback_model() {
        let catalog = Arc::new(Catalog::from_builtin().unwrap());
        let mut client = Client::new(HashMap::new(), None, vec![]);
        client.catalog = Some(Arc::clone(&catalog));
        client
            .register_provider(Arc::new(MockProvider::new("anthropic", "accepted")))
            .await
            .unwrap();

        let mut request = test_request();
        request.model = "claude-sonnet-4-5".to_string();
        request.provider = Some("anthropic".to_string());
        request.reasoning_effort = Some(ReasoningEffort::Low);

        let response = client.complete(&request).await.unwrap();

        assert_eq!(response.text(), "accepted");
    }

    #[tokio::test]
    async fn complete_skips_control_validation_for_unknown_model_passthrough() {
        let catalog = Arc::new(Catalog::from_builtin().unwrap());
        let mut client = Client::new(HashMap::new(), None, vec![]);
        client.catalog = Some(Arc::clone(&catalog));
        client
            .register_provider(Arc::new(MockProvider::new("openai", "passthrough")))
            .await
            .unwrap();

        let mut request = test_request();
        request.model = "custom-model".to_string();
        request.provider = Some("openai".to_string());
        request.reasoning_effort = Some(ReasoningEffort::High);
        request.speed = Some(Speed::Fast);

        let response = client.complete(&request).await.unwrap();

        assert_eq!(response.text(), "passthrough");
    }

    #[tokio::test]
    async fn stream_rejects_unsupported_speed_before_dispatch() {
        let catalog = Arc::new(Catalog::from_builtin().unwrap());
        let mut client = Client::new(HashMap::new(), None, vec![]);
        client.catalog = Some(Arc::clone(&catalog));
        client
            .register_provider(Arc::new(MockProvider::new("openai", "should not dispatch")))
            .await
            .unwrap();

        let mut request = test_request();
        request.model = "gpt-5.4".to_string();
        request.provider = Some("openai".to_string());
        request.speed = Some(Speed::Fast);

        let Err(err) = client.stream(&request).await else {
            panic!("unsupported speed should fail before stream dispatch");
        };

        assert!(matches!(
            err,
            Error::InvalidRequest {
                ref message,
            } if message.contains("model 'gpt-5.4' does not support speed 'fast'")
        ));
    }

    #[tokio::test]
    async fn from_credentials_registers_multiple_providers() {
        let catalog = catalog_with("");
        let client = Client::from_credentials(
            vec![
                ApiCredential {
                    provider:      ProviderId::anthropic(),
                    auth_header:   Some(ApiKeyHeader::Custom {
                        name:  "x-api-key".to_string(),
                        value: "anthropic-key".to_string(),
                    }),
                    extra_headers: HashMap::new(),
                    base_url:      None,
                    codex_mode:    false,
                    org_id:        None,
                    project_id:    None,
                },
                ApiCredential {
                    provider:      ProviderId::openai(),
                    auth_header:   Some(ApiKeyHeader::Bearer("openai-key".to_string())),
                    extra_headers: HashMap::new(),
                    base_url:      None,
                    codex_mode:    false,
                    org_id:        None,
                    project_id:    None,
                },
            ],
            catalog,
        )
        .await
        .unwrap();

        let mut providers = client.provider_names();
        providers.sort_unstable();
        assert_eq!(providers, vec!["anthropic", "openai"]);
        assert_eq!(client.default_provider(), Some("anthropic"));
    }

    #[tokio::test]
    async fn from_credentials_supports_builtin_openai_compatible_providers() {
        let catalog = catalog_with("");
        let client = Client::from_credentials(
            vec![ApiCredential {
                provider:      ProviderId::new("moonshot"),
                auth_header:   Some(ApiKeyHeader::Bearer("kimi-key".to_string())),
                extra_headers: HashMap::new(),
                base_url:      None,
                codex_mode:    false,
                org_id:        None,
                project_id:    None,
            }],
            catalog,
        )
        .await
        .unwrap();

        assert_eq!(client.provider_names(), vec!["moonshot"]);
        assert_eq!(client.default_provider(), Some("moonshot"));
    }

    #[tokio::test]
    async fn from_credentials_rejects_custom_provider_id_without_adapter() {
        let catalog = catalog_with("");
        let result = Client::from_credentials(
            vec![ApiCredential {
                provider:      fabro_model::ProviderId::new("custom"),
                auth_header:   Some(ApiKeyHeader::Bearer("custom-key".to_string())),
                extra_headers: HashMap::new(),
                base_url:      None,
                codex_mode:    false,
                org_id:        None,
                project_id:    None,
            }],
            catalog,
        )
        .await;
        let Err(err) = result else {
            panic!("custom provider credentials should fail without a registered adapter");
        };

        assert!(matches!(
            err,
            Error::Configuration {
                ref message,
                ..
            } if message == "Provider \"custom\" is not supported by credential-only registration"
        ));
    }

    #[tokio::test]
    async fn from_credentials_report_skips_provider_that_cannot_register() {
        let catalog = catalog_with(
            r#"
[providers.acme]
display_name = "Acme"
adapter = "openai_compatible"
agent_profile = "openai"

[providers.acme.auth]
credentials = ["env:ACME_API_KEY"]

[models."acme-large"]
provider = "acme"
display_name = "Acme Large"
family = "acme"
default = true

[models."acme-large".limits]
context_window = 128000

[models."acme-large".features]
tools = true
vision = false
reasoning = false
"#,
        );
        let report = Client::from_credentials_report(
            vec![
                ApiCredential {
                    provider:      ProviderId::new("acme"),
                    auth_header:   Some(ApiKeyHeader::Bearer("acme-key".to_string())),
                    extra_headers: HashMap::new(),
                    base_url:      None,
                    codex_mode:    false,
                    org_id:        None,
                    project_id:    None,
                },
                ApiCredential {
                    provider:      ProviderId::openai(),
                    auth_header:   Some(ApiKeyHeader::Bearer("openai-key".to_string())),
                    extra_headers: HashMap::new(),
                    base_url:      None,
                    codex_mode:    false,
                    org_id:        None,
                    project_id:    None,
                },
            ],
            Arc::clone(&catalog),
        )
        .await;

        assert_eq!(report.client.provider_names(), vec!["openai"]);
        assert_eq!(report.registration_issues.len(), 1);
        assert_eq!(
            report.registration_issues[0].provider,
            ProviderId::new("acme")
        );
        assert!(
            report.registration_issues[0]
                .error
                .to_string()
                .contains("uses openai_compatible adapter but does not configure base_url")
        );
    }

    #[tokio::test]
    async fn from_source_registers_provider_from_resolved_credentials() {
        let source = StubSource {
            credentials: vec![ApiCredential {
                provider:      ProviderId::anthropic(),
                auth_header:   Some(ApiKeyHeader::Custom {
                    name:  "x-api-key".to_string(),
                    value: "anthropic-key".to_string(),
                }),
                extra_headers: HashMap::new(),
                base_url:      None,
                codex_mode:    false,
                org_id:        None,
                project_id:    None,
            }],
        };
        let catalog = catalog_with("");

        let client = Client::from_source(&source, catalog).await.unwrap();

        assert_eq!(client.provider_names(), vec!["anthropic"]);
    }

    #[tokio::test]
    async fn from_credentials_registers_custom_openai_compatible_provider() {
        let catalog = catalog_with(
            r#"
[providers.acme]
display_name = "Acme"
adapter = "openai_compatible"
agent_profile = "openai"
base_url = "https://api.acme.test/v1"
aliases = ["acme-ai"]

[providers.acme.auth]
credentials = ["env:ACME_API_KEY"]

[models."acme-large"]
provider = "acme"
display_name = "Acme Large"
family = "acme"
default = true

[models."acme-large".limits]
context_window = 128000

[models."acme-large".features]
tools = true
vision = false
reasoning = false
"#,
        );

        let client = Client::from_credentials(
            vec![ApiCredential {
                provider:      fabro_model::ProviderId::new("acme"),
                auth_header:   Some(ApiKeyHeader::Bearer("acme-key".to_string())),
                extra_headers: HashMap::new(),
                base_url:      None,
                codex_mode:    false,
                org_id:        None,
                project_id:    None,
            }],
            Arc::clone(&catalog),
        )
        .await
        .unwrap();

        assert_eq!(client.provider_names(), vec!["acme"]);
        assert!(client.has_provider("acme"));
        assert!(client.has_provider("acme-ai"));
    }

    #[tokio::test]
    async fn resolve_provider_accepts_catalog_provider_alias() {
        let catalog = catalog_with(
            r#"
[providers.acme]
display_name = "Acme"
adapter = "openai_compatible"
agent_profile = "openai"
base_url = "https://api.acme.test/v1"
aliases = ["acme-ai"]

[providers.acme.auth]
credentials = ["env:ACME_API_KEY"]

[models."acme-large"]
provider = "acme"
display_name = "Acme Large"
family = "acme"
default = true

[models."acme-large".limits]
context_window = 128000

[models."acme-large".features]
tools = true
vision = false
reasoning = false
"#,
        );

        let client = Client::from_credentials(
            vec![ApiCredential {
                provider:      fabro_model::ProviderId::new("acme"),
                auth_header:   Some(ApiKeyHeader::Bearer("acme-key".to_string())),
                extra_headers: HashMap::new(),
                base_url:      None,
                codex_mode:    false,
                org_id:        None,
                project_id:    None,
            }],
            Arc::clone(&catalog),
        )
        .await
        .unwrap();
        let mut request = test_request();
        request.provider = Some("acme-ai".to_string());

        let provider = client
            .resolve_request_with_adapter(&request)
            .unwrap()
            .provider;

        assert_eq!(provider.name(), "acme");
    }

    /// Build a Client with one registered mock per catalog provider, so
    /// dispatch tests can observe which provider a request resolves to.
    async fn client_with_all_catalog_providers(catalog: &Arc<Catalog>) -> Client {
        let mut client = Client::new(HashMap::new(), None, vec![]);
        for provider in catalog.providers() {
            client
                .register_provider(Arc::new(MockProvider::new(provider.id.as_str(), "ok")))
                .await
                .unwrap();
        }
        client.catalog = Some(Arc::clone(catalog));
        client
    }

    /// For every built-in model selector, live dispatch and catalog selection
    /// choose the same provider from the same ready-provider set.
    #[tokio::test]
    async fn dispatch_agrees_with_resolve_route_for_every_builtin_model() {
        let catalog = catalog_with("");
        let client = client_with_all_catalog_providers(&catalog).await;
        let ready_providers = catalog.all_provider_ids();

        for model in catalog.list(None) {
            let selected = catalog
                .select(model.id.as_str(), None, &ready_providers)
                .expect("built-in model should be selectable");
            let route = adapter_registry::resolve_route(&catalog, selected)
                .expect("selected built-in model should resolve to a route");
            let mut request = test_request();
            request.model = model.id.to_string();

            let provider = client
                .resolve_request_with_adapter(&request)
                .unwrap()
                .provider;

            assert_eq!(provider.name(), route.provider.as_str(), "{}", model.id);
        }
    }

    #[tokio::test]
    async fn explicit_provider_wins_over_the_model_route() {
        let catalog = catalog_with("");
        let client = client_with_all_catalog_providers(&catalog).await;

        let mut request = test_request();
        request.model = "gpt-5.4-mini".to_string();
        request.provider = Some("anthropic".to_string());

        let provider = client
            .resolve_request_with_adapter(&request)
            .unwrap()
            .provider;

        assert_eq!(provider.name(), "anthropic");
    }

    #[tokio::test]
    async fn unknown_model_falls_back_to_default_provider() {
        let catalog = catalog_with("");
        let client = client_with_all_catalog_providers(&catalog).await;
        let default = client.default_provider().unwrap().to_string();

        let mut request = test_request();
        request.model = "model-not-in-any-catalog".to_string();

        let provider = client
            .resolve_request_with_adapter(&request)
            .unwrap()
            .provider;

        assert_eq!(provider.name(), default);
    }

    #[tokio::test]
    async fn from_credentials_registers_no_auth_provider_with_extra_headers() {
        let catalog = catalog_with(
            r#"
[providers.portkey]
display_name = "Portkey Bedrock"
adapter = "anthropic"
agent_profile = "anthropic"
base_url = "https://api.portkey.ai/v1"

[providers.portkey.extra_headers]
x-portkey-api-key = "pk-live"

[models."portkey-claude"]
provider = "portkey"
display_name = "Portkey Claude"
family = "claude"
default = true

[models."portkey-claude".limits]
context_window = 200000

[models."portkey-claude".features]
tools = true
vision = true
reasoning = true
reasoning_effort = "levels"
"#,
        );

        let client = Client::from_credentials(
            vec![ApiCredential {
                provider:      fabro_model::ProviderId::new("portkey"),
                auth_header:   None,
                extra_headers: HashMap::from([(
                    "x-portkey-api-key".to_string(),
                    "pk-live".to_string(),
                )]),
                base_url:      None,
                codex_mode:    false,
                org_id:        None,
                project_id:    None,
            }],
            Arc::clone(&catalog),
        )
        .await
        .unwrap();

        assert_eq!(client.provider_names(), vec!["portkey"]);
    }

    #[tokio::test]
    async fn from_source_supports_empty_credentials() {
        let source = StubSource {
            credentials: Vec::new(),
        };
        let catalog = catalog_with("");

        let client = Client::from_source(&source, catalog).await.unwrap();

        assert!(client.provider_names().is_empty());
    }

    #[tokio::test]
    async fn register_sets_first_as_default() {
        let mut client = Client::new(HashMap::new(), None, vec![]);
        assert_eq!(client.default_provider(), None);

        client
            .register_provider(Arc::new(MockProvider::new("first", "1")))
            .await
            .unwrap();
        assert_eq!(client.default_provider(), Some("first"));

        client
            .register_provider(Arc::new(MockProvider::new("second", "2")))
            .await
            .unwrap();
        assert_eq!(client.default_provider(), Some("first"));
    }

    #[tokio::test]
    async fn stream_routes_to_provider() {
        use futures::StreamExt;

        let mut client = Client::new(HashMap::new(), None, vec![]);
        client
            .register_provider(Arc::new(MockProvider::new("test", "streamed")))
            .await
            .unwrap();

        let mut stream = client.stream(&test_request()).await.unwrap();
        let first = stream.next().await.unwrap().unwrap();
        match &first {
            StreamEvent::TextDelta { delta, .. } => assert_eq!(delta, "streamed"),
            other => panic!("Expected TextDelta, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn provider_names_returns_registered() {
        let mut client = Client::new(HashMap::new(), None, vec![]);
        client
            .register_provider(Arc::new(MockProvider::new("alpha", "")))
            .await
            .unwrap();
        client
            .register_provider(Arc::new(MockProvider::new("beta", "")))
            .await
            .unwrap();
        let mut names = client.provider_names();
        names.sort_unstable();
        assert_eq!(names, vec!["alpha", "beta"]);
    }

    /// Test middleware gets called
    struct UppercaseMiddleware;

    #[async_trait::async_trait]
    impl Middleware for UppercaseMiddleware {
        async fn handle_complete(&self, request: Request, next: NextFn) -> Result<Response, Error> {
            let mut response = next(request).await?;
            let text = response.text().to_uppercase();
            response.message = Message::assistant(text);
            Ok(response)
        }

        async fn handle_stream(
            &self,
            request: Request,
            next: NextStreamFn,
        ) -> Result<StreamEventStream, Error> {
            next(request).await
        }
    }

    #[tokio::test]
    async fn middleware_wraps_complete() {
        let mut client = Client::new(HashMap::new(), None, vec![]);
        client
            .register_provider(Arc::new(MockProvider::new("test", "hello")))
            .await
            .unwrap();
        client.add_middleware(Arc::new(UppercaseMiddleware));

        let response = client.complete(&test_request()).await.unwrap();
        assert_eq!(response.text(), "HELLO");
    }
}
