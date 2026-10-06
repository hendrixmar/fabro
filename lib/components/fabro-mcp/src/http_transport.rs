#![expect(
    clippy::disallowed_types,
    reason = "MCP HTTP helpers parse operator-provided endpoint URLs; callers control what is logged"
)]

use anyhow::{Context as _, Result};
use fabro_http::Url;

use crate::config::McpHttpProtocol;

pub fn sandbox_mcp_http_url(protocol: McpHttpProtocol, preview_url: &str) -> Result<String> {
    let mut url = Url::parse(preview_url).context("invalid sandbox MCP preview URL")?;
    let endpoint = match protocol {
        McpHttpProtocol::StreamableHttp => "mcp",
        McpHttpProtocol::Sse => "sse",
    };
    let path = url.path().trim_end_matches('/');
    if path.rsplit('/').next() != Some(endpoint) {
        url.set_path(&format!("{path}/{endpoint}"));
    }
    Ok(url.to_string())
}
