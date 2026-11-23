# Handoff

A stable name is the address. It defaults to the agent kind and is kept in the mesh home directory, so a restarted session is the same addressee.

Claude Code surfaces a queued message on the next `user-prompt-submit` or `stop` hook (`agent-mesh hook user-prompt-submit` and `agent-mesh hook stop`). A live tmux pane receives the text as soon as it arrives.

Codex receives the text immediately in a live tmux pane. An idle Codex session is not woken; it sees the message when `check_inbox` runs.
