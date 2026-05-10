---
description: Show the latest continuation root session and how to open it
agent: build
subtask: false
---

Read `.opencode/runtime/new-session-result.json`.

Report these fields when present:
- `status`
- `title`
- `reason`
- `sourceSessionID`
- `rootSessionID`
- `recordedAt`
- `tuiResumeCommand`
- `cliOpenCommand`
- `recommendedAction`
- `role`
- `stage`
- `issueNumber`
- `branch`
- `error`

If `status` is `success`, tell me exactly how to inspect the root session:
1. In OpenCode TUI, run `/sessions` and switch to `rootSessionID`.
2. Or run `cliOpenCommand` from a shell.

If `status` is `error`, explain that no root session is available yet and include the recorded error.
