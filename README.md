# agent-mesh

An MCP control plane that lets coding agents talk to each other's sessions.

Six agent CLIs run on a typical machine (`claude`, `codex`, `grok`, `gemini`, `opencode`,
`cursor-agent`), each with its own session store. Work done in one is invisible to the others, so
a human is the only bridge between them. agent-mesh is an MCP server that every agent can attach:
once attached, any agent can enumerate its peers, reach into a peer's live session, prompt it, and
read the reply.

## How it talks to agents

Four of the six already speak a standard, ACP (Agent Client Protocol), so one client covers them:

| Agent | Transport |
|---|---|
| `opencode` | ACP (`opencode acp`) |
| `gemini` | ACP (`gemini --acp`) |
| `grok` | ACP (`grok agent stdio`) |
| `cursor-agent` | ACP (`cursor-agent acp`, hidden subcommand) |
| `claude` | bespoke: `-p --input-format stream-json` |
| `codex` | bespoke: `codex app-server` |

The premise is that ACP's `session/load` can reach a session from a *different process* and
replays its transcript, which is verified against the real binary in
`crates/mesh-acp/tests/live_opencode.rs`.

## Layout

- `mesh-core` -- session identity, the transport trait, the registry, the ask-chain loop guard
- `mesh-acp` -- ACP client (JSON-RPC over stdio) plus the responder that answers permission prompts
- `mesh-claude`, `mesh-codex` -- the two bespoke adapters
- `mesh-mcp` -- the MCP server binary

## Tests

`cargo test` runs everything. The `mesh-acp` integration tests drive a real `opencode` process on
a free model, and skip themselves when `opencode` is not installed.
