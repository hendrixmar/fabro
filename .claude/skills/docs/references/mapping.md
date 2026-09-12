# Code-to-Doc Mapping

Which source files affect which doc pages. Use this as guidance — also apply judgment for unmapped files that clearly affect user-facing behavior.

| Source | Docs |
|--------|------|
| `lib/apps/fabro-cli/src/main.rs`, `lib/components/fabro-workflow/src/cli/mod.rs`, `lib/components/fabro-workflow/src/cli/run.rs` | `docs/public/reference/cli.mdx` |
| `lib/apps/fabro-cli/src/cli_config.rs` | `docs/public/reference/cli-configuration.mdx` |
| `lib/components/fabro-llm/src/cli.rs` | `docs/public/reference/cli.mdx` |
| `lib/foundation/fabro-api/src/serve.rs` | `docs/public/reference/cli.mdx` |
| `lib/components/fabro-workflow/src/parser/*.rs` | `docs/public/reference/dot-language.mdx` |
| `lib/components/fabro-workflow/src/condition.rs` | `docs/public/reference/dot-language.mdx` |
| `lib/components/fabro-workflow/src/cli/validate.rs` | `docs/public/reference/dot-language.mdx` |
| `lib/components/fabro-workflow/src/stylesheet.rs` | `docs/public/workflows/stylesheets.mdx` |
| `lib/components/fabro-workflow/src/transform.rs` | `docs/public/workflows/variables.mdx` |
| `lib/components/fabro-workflow/src/handler/*.rs` | `docs/public/workflows/stages-and-nodes.mdx`, `docs/public/reference/dot-language.mdx` |
| `lib/components/fabro-workflow/src/handler/human.rs` | `docs/public/workflows/human-in-the-loop.mdx` |
| `lib/components/fabro-workflow/src/cli/run_config.rs` | `docs/public/execution/run-configuration.mdx` |
| `lib/components/fabro-workflow/src/engine.rs` | `docs/public/core-concepts/how-arc-works.mdx` |
| `lib/components/fabro-workflow/src/context/*.rs` | `docs/public/execution/context.mdx` |
| `lib/components/fabro-workflow/src/checkpoint.rs` | `docs/public/execution/checkpoints.mdx` |
| `lib/components/fabro-workflow/src/retro.rs`, `lib/components/fabro-workflow/src/retro_agent.rs` | `docs/public/execution/retros.mdx` |
| `lib/components/fabro-workflow/src/interviewer/*.rs` | `docs/public/execution/interviews.mdx` |
| `lib/components/fabro-workflow/src/hook/*.rs` | `docs/public/agents/hooks.mdx` |
| `lib/components/fabro-workflow/src/daytona_sandbox.rs` | `docs/public/integrations/daytona.mdx`, `docs/public/execution/environments.mdx` |
| `lib/components/fabro-sandbox/src/environment.rs`, pebble's `pebble-coding-agent` tools | `docs/public/agents/tools.mdx` |
| `lib/apps/fabro-cli/src/commands/exec.rs` | `docs/public/agents/permissions.mdx` |
| pebble's `pebble-coding-agent` subagents | `docs/public/agents/subagents.mdx` |
| `lib/components/fabro-workflow/src/handler/llm/sandbox_mcp.rs`, `lib/components/fabro-mcp/src/connection_manager.rs` | `docs/public/agents/mcp.mdx` |
| `lib/components/fabro-llm/src/catalog.rs`, `lib/components/fabro-llm/src/providers/*.rs` | `docs/public/core-concepts/models.mdx` |
| `lib/components/fabro-slack/src/*.rs` | `docs/public/integrations/slack.mdx` |
| `lib/components/fabro-mcp/src/*.rs` | `docs/public/agents/mcp.mdx` |
| `lib/foundation/fabro-api/src/*.rs` | `docs/public/api-reference/overview.mdx`, `docs/public/api-reference/demo-mode.mdx` |
| `lib/foundation/fabro-api/src/server_config.rs` | `docs/public/administration/server-configuration.mdx` |
