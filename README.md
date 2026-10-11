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

macOS, one command (universal binary, Apple Silicon and Intel):

```bash
brew install mubashir1osmani/agent-mesh/agent-mesh
```

From source, which is the route on Linux; needs [Rust](https://rustup.rs) 1.85+ for edition 2024:

```bash
git clone https://github.com/mubashir1osmani/agent-mesh.git
cd agent-mesh
cargo build --release   # binary at target/release/agent-mesh
```

You also need at least one supported agent CLI installed. `agent-mesh` on its own has nothing to
talk to; run `list_agents` to see which ones it can find.

## Quick start

Point an agent at it. The server speaks MCP over stdio, so any MCP client can attach.

**Claude Code**

```bash
claude mcp add --scope user agent-mesh -- agent-mesh
```

`--scope user` makes it available in every project rather than just the current one. Restart the
CLI afterwards; MCP servers are loaded at startup.

**opencode** — add to `~/.config/opencode/opencode.json`:

```json
{
  "mcp": {
    "agent-mesh": {
      "type": "local",
      "command": ["agent-mesh"],
      "enabled": true
    }
  }
}
```

**Codex** — add to `~/.codex/config.toml`:

```toml
[mcp_servers.agent-mesh]
command = "agent-mesh"
```

These use the bare command name, resolved from `PATH`. Note that neither file expands shell
substitutions, so `"$(brew --prefix)/bin/agent-mesh"` would be treated as a literal path and fail
to start. Use a bare name, or a fully written-out path.

Then just ask, in plain language:

> Open a session with codex, ask it to summarize the auth module, and tell me what it said

## Session tools

| Tool | What it does |
|---|---|
| `list_agents` | Which agents exist, whether they're installed, whether they can resume |
| `open_session` | Start a fresh conversation with an agent |
| `attach_session` | Join a conversation that already exists, and get its transcript |
| `ask_agent` | Send a prompt into a session; returns the reply, tokens, and cost |
| `read_session` | Read a conversation without prompting it |
| `list_sessions` | List known sessions; `discover_in` also finds ones started outside the mesh |
| `get_usage` | Tokens and cost spent per agent so far |

## Live nodes

Every agent that loads agent-mesh joins your **hub** as a *node*, so live sessions can message each
other while they run. There is one hub per user (`~/.agent-mesh/hub.sock`, private to you); the
first agent-mesh to start launches it in the background, so you never start it by hand.

The hub owns **every agent process the mesh runs**: tmux nodes from `spawn_node` and the background
processes behind `ask_agent`. Each agent's own agent-mesh is just a thin client, which is what lets
the hub count them, cap them, and close them.

```
                 ┌───────────────────────┐
   claude TUI ───┤                       ├─── spawned codex  (tmux: mesh-codex-…)
   grok TUI   ───┤   agent-mesh hub      ├─── spawned claude (tmux: mesh-claude-…)
                 └───────────────────────┘
```

| Tool | What it does |
|---|---|
| `list_nodes` | Live agent sessions on this machine, and which one is you |
| `send_message` | Message a node (by id or name) and return immediately; the reply comes back as a message |
| `check_inbox` | Collect messages that were queued for you |
| `spawn_node` | Start an agent in its own tmux session, optionally with a first prompt and a `name` |
| `peek_node` | Read what is on a tmux node's screen without disturbing it |
| `kill_node` | Close a node the mesh spawned, by id or name |

### Keeping track of what's running

```
$ agent-mesh ps
3 of 4 agent processes (max_processes)

ID                       KIND      PID           MEM      AGE     IDLE  SPAWNED BY
codex-2c058244           tmux      36632        250M      55s      55s  claude-34527dcb
codex-c7d8266b           tmux      36466        253M      55s      12s  claude-34527dcb
claude/40060             headless  40060        300M        -       0s  claude-34527dcb

total memory: 803M

your own sessions on the mesh (not counted, never killed):
  claude-34527dcb        pid 32186    /Users/you/project

$ agent-mesh kill codex-2c058244     # a tmux node, or a background process like claude/40060
$ agent-mesh kill --all              # everything the mesh spawned
```

Memory is the whole process tree (the agent plus its MCP servers and tool subprocesses), and
killing a node stops that whole tree, not just the tmux session.

- **Cap.** At most `max_processes` (default 4) agent processes at once. Past that, `spawn_node` and
  `ask_agent` are refused with a list of what's running, so the agent (or you) can close something.
- **Idle reaping.** Spawned nodes and background processes unused for `idle_timeout_minutes`
  (default 30) are closed. A node counts as active while it prints to its pane, sends or receives
  a message, or you type into it, so long tasks and nodes you're watching aren't cut off. A
  background session reaped this way resumes on its next `ask_agent`.
- **Your own sessions are never touched.** They show up in `ps` but don't count against the cap,
  and neither `kill` nor `kill_node` will close them.
- **Restarts.** Spawned nodes are recorded in `~/.agent-mesh/nodes.json`, and a restarted hub
  re-adopts any `mesh-*` tmux session it finds, so nothing becomes an orphan you can't see.

**How a message arrives** depends on where the recipient runs:

- **In tmux** (anything `spawn_node` started, or any agent you launched inside tmux): it is typed
  straight into the session and the agent acts on it immediately.
- **Claude Code outside tmux**: install the hooks below. Queued messages ride along with your next
  prompt, and a Claude that is mid-task keeps going to handle a message that arrived while it
  worked.
- **Anything else**: it waits until the agent calls `check_inbox`.

Watch a spawned node with `tmux attach -t mesh-<node-id>` (the id `spawn_node` returned).

Give `spawn_node` a `name` (say `reviewer`) and `send_message`, `kill_node` and `agent-mesh kill`
accept it as well as the generated id, which never changes. `list_nodes` shows it. A name shared by
two live nodes is refused rather than guessed, and sessions you started yourself never get one.

Messages carry **your authority**: every node is one of your sessions, so a message arrives as a
normal prompt with a footer naming the sender and how to reply. That also means any agent on the
mesh can direct any other, including ones running with permissions bypassed; the process cap, a hop
limit (`max_ask_depth`), and a limit of 6 messages a minute between any two nodes are the guardrails
against runaway loops.

**Claude Code hooks** — add to `~/.claude/settings.json`:

```json
{
  "hooks": {
    "UserPromptSubmit": [
      { "hooks": [{ "type": "command", "command": "agent-mesh", "args": ["hook", "user-prompt-submit"] }] }
    ],
    "Stop": [
      { "hooks": [{ "type": "command", "command": "agent-mesh", "args": ["hook", "stop"] }] }
    ]
  }
}
```

`args` matters: it runs the hook directly rather than through a shell, which is how the hook finds
the session it belongs to.

## Configuration

It works with no config at all. To customize, use `~/.config/agent-mesh/agents.toml` or point
`AGENT_MESH_CONFIG` at a file. The hub reads it once, from your home directory, and it applies to
every agent on the mesh; restart the hub (`pkill -f "agent-mesh hub"`) to pick up changes.

```toml
# How many sessions one relay may pass through before it is refused.
max_ask_depth = 4
# How long to wait for a single agent turn.
turn_timeout_seconds = 300
# Most agent processes the hub runs at once (spawned nodes + background ask_agent processes).
max_processes = 4
# Close spawned nodes and background processes unused for this long. 0 disables it.
idle_timeout_minutes = 30

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
- A live TUI outside tmux cannot be interrupted while idle: its messages wait for the next turn
- `ask_agent` on a session that is also open in a TUI resumes a separate headless copy; use
  `send_message` to reach the live one
- Spawned nodes run with each agent's permission-bypass flag, matching the headless transports
- Configuration is read by the hub, from your home directory; a project-local `agents.toml` no
  longer applies (use `~/.config/agent-mesh/agents.toml` or `$AGENT_MESH_CONFIG`)
- After upgrading, restart your agent CLIs and run `pkill -f "agent-mesh hub"`: an old client and a
  new hub (or the reverse) refuse to talk, by design
- Codex reports usage per turn; other agents vary in what they report at all

## License

MIT
