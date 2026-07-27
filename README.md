# agent-mesh

**Let your coding agents talk to each other.**

You probably have several agent CLIs installed: Claude Code, Codex, opencode, Gemini, Grok. Each
keeps its own sessions, and none of them can see the others. If Codex figured something out and you
want Claude to know it, you are the one copying it across.

agent-mesh is an MCP server that removes you from that loop. Attach it to any agent and that agent
can list its peers, open or join a peer's session, send it a prompt, and read what came back.

```
  Claude Code ──┐                        ┌── opencode session
                │                        │
     Codex ─────┼──▶ agent-mesh (MCP) ───┼── codex session
                │                        │
    opencode ──┘                         └── claude session
```

Real example, straight from the test suite; two different agents running two different models:

```
codex   ← "Remember this: the deploy key is ORCHID-77"     → "stored"
opencode← "What is the deploy key?"                        → "UNKNOWN"
codex   ← "What is the deploy key?"                        → "ORCHID-77"
opencode← "A peer agent says the key is ORCHID-77. Echo it"→ "ORCHID-77"
```

## Install

You need [Rust](https://rustup.rs) (1.85+ for edition 2024) and at least one supported agent CLI.

```bash
git clone https://github.com/mubashir1osmani/agent-mesh.git
cd agent-mesh
cargo build --release
```

The binary lands at `target/release/agent-mesh`. Copy it onto your `PATH` if you like:

```bash
cp target/release/agent-mesh ~/.local/bin/
```

## Quick start

Point an agent at it. The server speaks MCP over stdio, so any MCP client can attach.

**Claude Code**

```bash
claude mcp add agent-mesh -- /full/path/to/agent-mesh
```

**opencode** — add to `~/.config/opencode/opencode.json`:

```json
{
  "mcp": {
    "agent-mesh": {
      "type": "local",
      "command": ["/full/path/to/agent-mesh"],
      "enabled": true
    }
  }
}
```

**Codex** — add to `~/.codex/config.toml`:

```toml
[mcp_servers.agent-mesh]
command = "/full/path/to/agent-mesh"
```

Then just ask, in plain language:

> Open a session with codex, ask it to summarize the auth module, and tell me what it said

## The six tools

| Tool | What it does |
|---|---|
| `list_agents` | Which agents exist, whether they're installed, whether they can resume |
| `open_session` | Start a fresh conversation with an agent |
| `attach_session` | Join a conversation that already exists, and get its transcript |
| `ask_agent` | Send a prompt into a session; returns the reply, tokens, and cost |
| `read_session` | Read a conversation without prompting it |
| `list_sessions` | List known sessions; `discover_in` also finds ones started outside the mesh |
| `get_usage` | Tokens and cost spent per agent so far |

## Configuration

It works with no config at all. To customize, drop an `agents.toml` in your working directory, or
point `AGENT_MESH_CONFIG` at one, or use `~/.config/agent-mesh/agents.toml`.

```toml
# How many sessions one relay may pass through before it is refused.
max_ask_depth = 4
# How long to wait for a single agent turn.
turn_timeout_seconds = 300

[agents.opencode]
transport = "acp"
command = "opencode"
args = ["acp"]
model = "opencode/deepseek-v4-flash-free"   # optional

[agents.claude]
transport = "claude"
model = "claude-haiku-4-5-20251001"          # optional

[agents.codex]
transport = "codex"

[agents.gemini]
transport = "acp"
command = "gemini"
args = ["--acp"]

[agents.grok]
transport = "acp"
command = "grok"
args = ["agent", "stdio"]

[agents.cursor]
transport = "acp"
command = "cursor-agent"
args = ["acp"]
enabled = false   # hidden, unsupported subcommand; opt in at your own risk
```

Set `AGENT_MESH_LOG=debug` for verbose logs. Logs go to stderr, because stdout is the MCP transport.

### Telemetry

```toml
[telemetry]
# Export traces to an OTLP collector. Omit to disable.
otlp_endpoint = "http://localhost:4317"
# Serve Prometheus metrics. Omit to disable.
prometheus_listen = "127.0.0.1:9464"
```

Traces and the `get_usage` tool work anywhere. The Prometheus endpoint needs one long-lived
instance: under stdio each MCP client spawns its own agent-mesh process, and only one can hold a
port. If the port is taken, startup fails loudly rather than serving nothing quietly.

```
agent_mesh_asks_total{agent="opencode",outcome="success"} 1
agent_mesh_tokens_total{agent="opencode",direction="input"} 8719
agent_mesh_ask_duration_seconds_sum{agent="opencode"} 3.83
```

`outcome` separates `success`, `timeout`, `refused` (relay guard) and `agent_error`, so a wedged
agent doesn't hide inside a generic failure count.

A note on cost: `cost_usd` is null when the agent never reported spend, which is not the same as
free. `cost_is_complete` tells you whether a total covers every turn.

## Supported agents

Four of the five wired agents speak [ACP](https://agentclientprotocol.com), a standard protocol for
driving coding agents, so a single client covers them. Two needed bespoke adapters.

| Agent | Transport | Resume | Reports cost |
|---|---|---|---|
| opencode | ACP (`opencode acp`) | yes | no |
| gemini | ACP (`gemini --acp`) | yes | no |
| grok | ACP (`grok agent stdio`) | yes | yes* |
| cursor-agent | ACP (`cursor-agent acp`) | yes | no |
| claude | `-p --input-format stream-json` | yes | yes |
| codex | `codex app-server` | yes | no |

\* grok reports cost on its CLI surface; the ACP path does not expose it.

Only Claude and Grok report what a turn cost. When `cost_usd` is absent it means the agent did not
report spend, not that the turn was free.

## How it works, and the parts that are easy to get wrong

**Resume is the whole trick.** ACP's `session/load` reaches a session from a *different process*
and replays its transcript, so the mesh can bridge into a conversation it did not start. There's a
test that creates a session in one process, drops it, reattaches from a second, and asserts the
prior exchange came back.

**Session ids work three different ways,** which is the single sharpest edge here:

- `claude` lets you pin an id whenever you like
- `grok` and `gemini` accept a pinned id *only* for a session that does not exist yet, and hard-error
  otherwise
- `codex` will not let you pin one at all; it mints the id and hands it back

So the registry tracks whether each session is `NotStarted`, `Live`, or `Detached` and derives
create-vs-resume from that. A single "upsert" code path passes turn 1 and breaks turn 2.

**Relays are bounded by depth, not by forbidding revisits.** Going back to a session you already
spoke to is the main workflow ("ask codex, then tell opencode what it said"), so only an immediate
self-ask is refused outright. `max_ask_depth` is what guarantees termination.

**Agents get auto-approved.** There is no human in an orchestrated session to answer a permission
prompt, so the mesh answers for them (`bypassPermissions` for claude, `approvalPolicy: never` for
codex, first-offered-option for ACP). Point it at code you're willing to let agents touch.

## Tests

```bash
cargo test              # everything
cargo test -p mesh-core # fast, no agent processes
```

The integration tests drive a real `opencode` process against a free model, so they cost nothing and
skip themselves when `opencode` isn't installed.

## Limitations

- `cursor-agent acp` is a hidden, undocumented subcommand and could disappear; off by default
- `codex app-server` is marked experimental upstream
- One agent process per working directory, so many concurrent sessions in one repo share a process
- Sessions live in memory: restart the server and you'll need `attach_session` to rejoin
- Codex reports usage per turn; other agents vary in what they report at all

## License

MIT
