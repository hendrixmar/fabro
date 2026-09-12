//! MCP server settings for fabro's agents, and how they reach pebble.
//!
//! Fabro configures MCP servers in its settings ([`config`]); pebble starts
//! them, registers their tools, and closes them with the agent. [`pebble`]
//! maps one to the other. The stdio client fabro once ran itself is gone;
//! [`test_support`] keeps a small one for the tests of fabro's own MCP server.

pub mod config;
pub mod pebble;

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
