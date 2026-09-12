//! Fabro's integration layer over [`lithos_llm`].
//!
//! lithos owns the LLM vocabulary, the provider catalog, the wire codecs, and
//! the client. This crate adds what is specific to Fabro:
//!
//! - building the catalog from the lithos built-ins and the operator `[llm]`
//!   overlay, and reading the agent harness a model expects ([`catalog`]);
//! - Fabro's passthrough policy for selections made before a request exists
//!   ([`selection`]); at request time the lithos resolver enforces `enabled`
//!   and `stands_in_for` itself;
//! - constructing a client from a Fabro credential store ([`client`]);
//! - model and provider probes ([`probe`]), and the API views of the catalog
//!   ([`api`]);
//! - the `fabro exec` gateway adapter that speaks to a Fabro server
//!   ([`gateway`]);
//! - the failure signature loop detection reads ([`error`]).
//!
//! Local-file inlining, structured output, readable-reasoning normalization,
//! and the retry, auth, and failover predicates are lithos-llm's own.

pub mod api;
pub mod catalog;
pub mod client;
pub mod error;
pub mod gateway;
pub mod probe;
pub mod selection;
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

pub use catalog::{build_catalog, default_catalog};
pub use client::{
    ClientOptions, FabroClient, LlmSetupError, RetryListener, RetryNotice, build_client,
    build_offline_client, configured_providers,
};
pub use error::failure_signature_hint;
pub use lithos_llm::client::{Client, ClientBuild};
pub use lithos_llm::middleware::{CallContext, CancellationToken, RetryPolicy, RetryStage};
pub use lithos_llm::resolver::ModelSelectionError as RouteSelectionError;
pub use lithos_llm::types::{
    Error, ErrorData, ErrorKind, FinishReason, Request, Response, ResponseStream,
    RetryClassification, StreamEvent,
};
pub use lithos_llm::{
    adapter, catalog as lithos_catalog, credentials, estimate, middleware, types,
};
pub use selection::{FallbackTarget, ModelSelectionError, SelectedModel};
