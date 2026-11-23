# Handoff

The address is the agent name. It defaults to the agent kind and is stored under the mesh home directory.

Claude Code inserts queued messages from `agent-mesh hook user-prompt-submit` and `agent-mesh hook stop`. A live tmux pane gets the text when it arrives.

Codex gets the text in a live tmux pane. An idle Codex session does not. It sees the message from `check_inbox`.
