# Handoff

The address is the agent name. It defaults to the agent kind and is stored under the mesh home directory.

Claude Code inserts queued messages from `agent-mesh hook user-prompt-submit` and `agent-mesh hook stop`. A live tmux pane gets the text when it arrives.

Codex gets the text in a live tmux pane. An idle Codex session with a thread id is resumed for one turn, first through `codex app-server` and otherwise with `codex exec resume`. An idle Claude Code session is resumed for one turn with `claude -p`. Hooks still insert messages into a Claude turn that is already running. Each sender is limited to a few wakes an hour.
