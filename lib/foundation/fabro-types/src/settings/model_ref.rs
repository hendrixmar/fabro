//! Model references for `run.model.fallbacks`.
//!
//! Each fallback-chain entry is one of:
//!
//! - a bare token such as `openai` or `gpt-5.4` — the parser cannot tell alone
//!   whether the token is a provider name or a model alias
//! - a qualified reference such as `gemini:gemini-flash`, which names both a
//!   provider and a model selector
//! - a legacy qualified reference such as `gemini/gemini-flash`, which is
//!   accepted on input and serialized using the canonical `provider:selector`
//!   form
//!
//! The parser produces [`ModelRef`]; ambiguity resolution against a known
//! registry of providers and models happens at consumption time via
//! [`ModelRef::resolve`].
//!
//! That split matters for the `:` form. Model IDs legitimately contain colons —
//! ollama `name:tag` values, Bedrock inference-profile ARNs — so no separator
//! is safe to split on by shape alone. Whichever separator appears first
//! decides the form: a `/` before any `:` is the legacy pin, and its selector
//! may contain colons. Otherwise the token stays bare and
//! [`ModelRef::qualify`] promotes it only when the prefix names a provider.

use std::fmt;
use std::str::FromStr;

use lithos_llm::catalog::Catalog;
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A parsed model reference. Bare tokens remain ambiguous until resolved
/// against a registry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelRef {
    /// A bare token. May be a provider name, a model alias, or a model id.
    Bare(String),
    /// A provider-qualified model selector.
    Qualified { provider: String, selector: String },
}

/// An error returned when parsing a model reference fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseModelRefError {
    /// The input was empty or whitespace only.
    Empty,
    /// A legacy slash-qualified input contained more than one `/`.
    TooManySlashes { input: String },
    /// The provider or selector side of a qualified reference was empty.
    EmptySide { input: String },
}

impl fmt::Display for ParseModelRefError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("model reference is empty"),
            Self::TooManySlashes { input } => {
                write!(
                    f,
                    "model reference {input:?}: qualify it as \"provider:selector\" when the selector contains \"/\""
                )
            }
            Self::EmptySide { input } => {
                write!(
                    f,
                    "model reference {input:?}: provider and selector sides must both be non-empty"
                )
            }
        }
    }
}

impl std::error::Error for ParseModelRefError {}

impl FromStr for ModelRef {
    type Err = ParseModelRefError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return Err(ParseModelRefError::Empty);
        }

        // Whichever separator comes first decides the form.
        let (provider, selector) = match (trimmed.find('/'), trimmed.find(':')) {
            // A `/` before any `:` is the legacy `provider/model` form. Its
            // selector may itself contain colons, as Bedrock API IDs do.
            (Some(slash), colon) if colon.is_none_or(|colon| slash < colon) => {
                (&trimmed[..slash], &trimmed[slash + 1..])
            }
            // Otherwise a `:` may separate provider from selector — but model
            // IDs legitimately contain colons (ollama `name:tag` values,
            // Bedrock ARNs) and only a registry can tell those apart, so leave
            // the token bare for [`ModelRef::qualify`] to promote.
            _ => return Ok(Self::Bare(trimmed.to_owned())),
        };

        if selector.contains('/') {
            return Err(ParseModelRefError::TooManySlashes {
                input: input.to_owned(),
            });
        }
        if provider.is_empty() || selector.is_empty() {
            return Err(ParseModelRefError::EmptySide {
                input: input.to_owned(),
            });
        }
        Ok(Self::Qualified {
            provider: provider.to_owned(),
            selector: selector.to_owned(),
        })
    }
}

impl fmt::Display for ModelRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bare(token) => f.write_str(token),
            Self::Qualified { provider, selector } => write!(f, "{provider}:{selector}"),
        }
    }
}

/// An error returned when resolving an ambiguous bare model reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AmbiguousModelRef {
    pub input:     String,
    pub providers: Vec<String>,
    pub models:    Vec<String>,
}

impl fmt::Display for AmbiguousModelRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "model reference {:?} is ambiguous: matches provider names {:?} and model names {:?}; qualify it as \"provider:model\"",
            self.input, self.providers, self.models
        )
    }
}

impl std::error::Error for AmbiguousModelRef {}

/// The resolved form of a model reference after registry lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolvedModelRef {
    /// The reference named a provider; the runtime should pick the best model
    /// from that provider.
    Provider(String),
    /// The reference named a model (qualified or unambiguously bare).
    Model {
        provider: Option<String>,
        selector: String,
    },
}

/// A minimal registry view used by [`ModelRef::resolve`].
///
/// Each method reports whether a bare token is a known provider, model, or
/// both. The registry is abstract so unit tests and runtime resolution can
/// share the same logic.
pub trait ModelRegistry {
    fn is_provider(&self, token: &str) -> bool;
    fn is_model(&self, token: &str) -> bool;
}

impl ModelRef {
    /// Promote a bare `provider:selector` token to [`ModelRef::Qualified`] when
    /// the prefix names a known provider.
    ///
    /// Parsing alone cannot do this. A model ID may itself contain a colon —
    /// ollama `name:tag` values, Bedrock inference-profile ARNs — and those
    /// must stay whole. Anything else is returned unchanged.
    #[must_use]
    pub fn qualify(self, registry: &dyn ModelRegistry) -> Self {
        let Self::Bare(token) = self else {
            return self;
        };
        match token.split_once(':') {
            Some((provider, selector))
                if !provider.is_empty()
                    && !selector.is_empty()
                    && registry.is_provider(provider) =>
            {
                Self::Qualified {
                    provider: provider.to_owned(),
                    selector: selector.to_owned(),
                }
            }
            _ => Self::Bare(token),
        }
    }

    /// Resolve this reference against a registry.
    ///
    /// - [`ModelRef::Qualified`] always resolves to a model.
    /// - A bare `provider:selector` token is qualified first — see
    ///   [`ModelRef::qualify`].
    /// - [`ModelRef::Bare`] resolves to a provider if the token is only a
    ///   provider, to a model if the token is only a model, and returns
    ///   [`AmbiguousModelRef`] if the token matches both a provider and a model
    ///   name.
    pub fn resolve(
        &self,
        registry: &dyn ModelRegistry,
    ) -> Result<ResolvedModelRef, AmbiguousModelRef> {
        match self.clone().qualify(registry) {
            Self::Qualified { provider, selector } => Ok(ResolvedModelRef::Model {
                provider: Some(provider),
                selector,
            }),
            Self::Bare(token) => {
                let is_provider = registry.is_provider(&token);
                let is_model = registry.is_model(&token);
                match (is_provider, is_model) {
                    (true, false) => Ok(ResolvedModelRef::Provider(token)),
                    (true, true) => Err(AmbiguousModelRef {
                        input:     token.clone(),
                        providers: vec![token.clone()],
                        models:    vec![token],
                    }),
                    // Known and unknown bare models leave provider selection to the runtime.
                    (false, _) => Ok(ResolvedModelRef::Model {
                        provider: None,
                        selector: token,
                    }),
                }
            }
        }
    }
}

impl ModelRegistry for Catalog {
    fn is_provider(&self, token: &str) -> bool {
        self.provider(token).is_ok()
    }

    fn is_model(&self, token: &str) -> bool {
        self.providers()
            .any(|provider| provider.model(token).is_some())
    }
}

impl Serialize for ModelRef {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for ModelRef {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ModelRefVisitor;

        impl Visitor<'_> for ModelRefVisitor {
            type Value = ModelRef;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(
                    r#"a model reference such as "openai", "gpt-5.4", or "gemini:gemini-flash""#,
                )
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<ModelRef, E> {
                value.parse().map_err(de::Error::custom)
            }

            fn visit_string<E: de::Error>(self, value: String) -> Result<ModelRef, E> {
                self.visit_str(&value)
            }
        }

        deserializer.deserialize_str(ModelRefVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestRegistry {
        providers: &'static [&'static str],
        models:    &'static [&'static str],
    }

    impl ModelRegistry for TestRegistry {
        fn is_provider(&self, token: &str) -> bool {
            self.providers.contains(&token)
        }
        fn is_model(&self, token: &str) -> bool {
            self.models.contains(&token)
        }
    }

    #[test]
    fn parses_bare_token() {
        assert_eq!(
            "openai".parse::<ModelRef>().unwrap(),
            ModelRef::Bare("openai".into())
        );
    }

    /// Parsing cannot tell a provider prefix from a model ID that contains a
    /// colon, so it defers to [`ModelRef::qualify`].
    #[test]
    fn parses_colon_tokens_as_bare() {
        for input in [
            "gemini:gemini-flash",
            "openrouter:moonshotai/kimi-k3",
            "bedrock:us.anthropic.claude-haiku-4-5-20251001-v1:0",
        ] {
            assert_eq!(
                input.parse::<ModelRef>().unwrap(),
                ModelRef::Bare(input.into()),
                "{input}"
            );
        }
    }

    #[test]
    fn parses_legacy_slash_qualified() {
        assert_eq!(
            "gemini/gemini-flash".parse::<ModelRef>().unwrap(),
            ModelRef::Qualified {
                provider: "gemini".into(),
                selector: "gemini-flash".into(),
            }
        );
    }

    /// A `/` before any `:` keeps the legacy pin, so Bedrock-style API IDs
    /// stay on the selector side instead of splitting at the wrong colon.
    #[test]
    fn legacy_slash_selector_may_contain_colons() {
        for (input, provider, selector) in [
            (
                "bedrock/us.anthropic.claude-haiku-4-5-20251001-v1:0",
                "bedrock",
                "us.anthropic.claude-haiku-4-5-20251001-v1:0",
            ),
            ("openai/gpt-5.6-sol:0", "openai", "gpt-5.6-sol:0"),
        ] {
            assert_eq!(
                input.parse::<ModelRef>().unwrap(),
                ModelRef::Qualified {
                    provider: provider.into(),
                    selector: selector.into(),
                },
                "{input}"
            );
        }
    }

    /// A `:` before any `/` wins, so a provider-qualified selector keeps its
    /// slashes.
    #[test]
    fn first_separator_decides_the_form() {
        assert_eq!(
            "openrouter:moonshotai/kimi-k3".parse::<ModelRef>().unwrap(),
            ModelRef::Bare("openrouter:moonshotai/kimi-k3".into())
        );
        assert_eq!(
            "bedrock/us.anthropic:0".parse::<ModelRef>().unwrap(),
            ModelRef::Qualified {
                provider: "bedrock".into(),
                selector: "us.anthropic:0".into(),
            }
        );
    }

    #[test]
    fn qualify_promotes_a_known_provider_prefix() {
        let reg = TestRegistry {
            providers: &["openrouter", "bedrock"],
            models:    &[],
        };
        for (input, selector) in [
            ("openrouter:kimi-k3", "kimi-k3"),
            ("openrouter:moonshotai/kimi-k3", "moonshotai/kimi-k3"),
            (
                "bedrock:us.anthropic.claude-haiku-4-5-20251001-v1:0",
                "us.anthropic.claude-haiku-4-5-20251001-v1:0",
            ),
        ] {
            let qualified = input.parse::<ModelRef>().unwrap().qualify(&reg);
            let provider = input.split_once(':').unwrap().0;
            assert_eq!(
                qualified,
                ModelRef::Qualified {
                    provider: provider.into(),
                    selector: selector.into(),
                },
                "{input}"
            );
        }
    }

    /// The regression this guards: a model ID that merely contains a colon —
    /// an ollama `name:tag`, a Bedrock ARN — must not be read as qualified.
    #[test]
    fn qualify_leaves_colon_bearing_model_ids_bare() {
        let reg = TestRegistry {
            providers: &["ollama", "bedrock"],
            models:    &[],
        };
        for input in [
            "llama3:8b",
            "qwen3.5:latest",
            "us.anthropic.claude-haiku-4-5-20251001-v1:0",
            "arn:aws:bedrock:us-east-1:1234:inference-profile/us.anthropic.claude-fable-5",
            ":foo",
            "foo:",
        ] {
            let parsed = input.parse::<ModelRef>().unwrap();
            assert_eq!(
                parsed.clone().qualify(&reg),
                parsed,
                "{input} should stay bare"
            );
        }
    }

    #[test]
    fn rejects_too_many_slashes() {
        let err = "a/b/c".parse::<ModelRef>().unwrap_err();
        assert!(matches!(err, ParseModelRefError::TooManySlashes { .. }));
        assert_eq!(
            err.to_string(),
            r#"model reference "a/b/c": qualify it as "provider:selector" when the selector contains "/""#
        );
    }

    #[test]
    fn rejects_empty_side() {
        assert!(matches!(
            "/foo".parse::<ModelRef>().unwrap_err(),
            ParseModelRefError::EmptySide { .. }
        ));
        assert!(matches!(
            "foo/".parse::<ModelRef>().unwrap_err(),
            ParseModelRefError::EmptySide { .. }
        ));
    }

    #[test]
    fn rejects_empty_input() {
        assert!(matches!(
            "".parse::<ModelRef>().unwrap_err(),
            ParseModelRefError::Empty
        ));
    }

    #[test]
    fn resolves_unique_provider_token() {
        let reg = TestRegistry {
            providers: &["openai"],
            models:    &[],
        };
        let resolved = ModelRef::Bare("openai".into()).resolve(&reg).unwrap();
        assert_eq!(resolved, ResolvedModelRef::Provider("openai".into()));
    }

    #[test]
    fn resolves_unique_model_token() {
        let reg = TestRegistry {
            providers: &[],
            models:    &["gpt-5.4"],
        };
        let resolved = ModelRef::Bare("gpt-5.4".into()).resolve(&reg).unwrap();
        assert_eq!(resolved, ResolvedModelRef::Model {
            provider: None,
            selector: "gpt-5.4".into(),
        });
    }

    #[test]
    fn ambiguous_bare_token_errors() {
        let reg = TestRegistry {
            providers: &["ambiguous"],
            models:    &["ambiguous"],
        };
        let err = ModelRef::Bare("ambiguous".into())
            .resolve(&reg)
            .unwrap_err();
        assert_eq!(err.input, "ambiguous");
    }

    #[test]
    fn qualified_never_ambiguous() {
        let reg = TestRegistry {
            providers: &["ambiguous"],
            models:    &["ambiguous"],
        };
        let resolved = ModelRef::Qualified {
            provider: "a".into(),
            selector: "b".into(),
        }
        .resolve(&reg)
        .unwrap();
        assert_eq!(resolved, ResolvedModelRef::Model {
            provider: Some("a".into()),
            selector: "b".into(),
        });
    }

    #[test]
    fn display_round_trip() {
        for input in [
            "openai",
            "gpt-5.4",
            "gemini:gemini-flash",
            "openrouter:moonshotai/kimi-k3",
        ] {
            let parsed: ModelRef = input.parse().unwrap();
            assert_eq!(parsed.to_string(), input);
        }
    }

    #[test]
    fn display_canonicalizes_legacy_slash_separator() {
        let parsed: ModelRef = "gemini/gemini-flash".parse().unwrap();
        assert_eq!(parsed.to_string(), "gemini:gemini-flash");
    }

    #[test]
    fn serde_round_trip_via_json() {
        #[derive(Debug, serde::Deserialize, serde::Serialize, PartialEq)]
        struct Wrap {
            m: ModelRef,
        }

        // Colon tokens stay bare until a registry qualifies them, and survive
        // the round trip verbatim either way.
        let input = r#"{"m":"openrouter:moonshotai/kimi-k3"}"#;
        let parsed: Wrap = serde_json::from_str(input).unwrap();
        assert_eq!(
            parsed.m,
            ModelRef::Bare("openrouter:moonshotai/kimi-k3".into())
        );
        let rendered = serde_json::to_string(&parsed).unwrap();
        assert_eq!(rendered, input);

        let legacy: Wrap = serde_json::from_str(r#"{"m":"gemini/gemini-flash"}"#).unwrap();
        assert_eq!(
            serde_json::to_string(&legacy).unwrap(),
            r#"{"m":"gemini:gemini-flash"}"#
        );
    }
}
