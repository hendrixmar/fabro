//! `[llm]` settings layer.
//!
//! Operator model catalog overrides. The table uses the lithos-llm catalog
//! schema verbatim, minus `schema_version`, and is applied as an overlay layer
//! on top of the lithos built-in catalog:
//!
//! ```toml
//! [llm.providers.openrouter]
//! priority = 60
//! enabled = true
//!
//! [llm.providers.openrouter.models."kimi-k2.5"]
//! small_default = true
//! ```
//!
//! Layers merge the same way lithos merges overlays: tables merge key by key
//! and every other value replaces. Fabro never interprets the table; lithos
//! validates it when the catalog is built.

use serde::{Deserialize, Serialize};
use toml::Table;

use super::combine::Combine;

/// Top-level `[llm]` settings layer: a raw lithos catalog overlay.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LlmLayer(pub Table);

impl LlmLayer {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Render this layer as a lithos catalog overlay document.
    #[must_use]
    pub fn to_overlay_toml(&self) -> String {
        toml::to_string(&self.0).expect("a TOML table always serializes")
    }
}

impl Combine for LlmLayer {
    fn combine(self, other: Self) -> Self {
        let mut base = toml::Value::Table(other.0);
        merge(&mut base, toml::Value::Table(self.0));
        match base {
            toml::Value::Table(table) => Self(table),
            _ => unreachable!("merging two tables yields a table"),
        }
    }
}

fn merge(base: &mut toml::Value, overlay: toml::Value) {
    match (base, overlay) {
        (toml::Value::Table(base), toml::Value::Table(overlay)) => {
            for (key, value) in overlay {
                if let Some(existing) = base.get_mut(&key) {
                    merge(existing, value);
                } else {
                    base.insert(key, value);
                }
            }
        }
        (base, overlay) => *base = overlay,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn layer(source: &str) -> LlmLayer {
        LlmLayer(toml::from_str(source).unwrap())
    }

    #[test]
    fn higher_layer_wins_scalars_and_merges_tables() {
        let higher = layer(
            r"
[providers.acme]
priority = 10
enabled = false
",
        );
        let lower = layer(
            r#"
[providers.acme]
priority = 5
base_url = "https://acme.test"
enabled = true
[providers.acme.metadata.agent]
profile = "openai"
"#,
        );
        let merged = higher.combine(lower).0;
        let acme = &merged["providers"]["acme"];
        assert_eq!(acme["priority"].as_integer(), Some(10));
        assert_eq!(acme["base_url"].as_str(), Some("https://acme.test"));
        assert_eq!(acme["enabled"].as_bool(), Some(false));
        assert_eq!(
            acme["metadata"]["agent"]["profile"].as_str(),
            Some("openai")
        );
    }

    #[test]
    fn arrays_replace_whole() {
        let higher = layer("[providers.acme]\naliases = [\"a\"]\n");
        let lower = layer("[providers.acme]\naliases = [\"b\", \"c\"]\n");
        let merged = higher.combine(lower).0;
        assert_eq!(
            merged["providers"]["acme"]["aliases"]
                .as_array()
                .map(Vec::len),
            Some(1)
        );
    }

    #[test]
    fn overlay_toml_round_trips() {
        let source = layer("[providers.acme]\npriority = 3\n");
        let rendered = source.to_overlay_toml();
        assert_eq!(layer(&rendered), source);
    }
}
