# autodev

Standalone autonomous development loop extracted into its own workspace at `~/apps/autodev`.

## Included

- Orchestrator bootstrap, supervisor, compact payload, and GitHub issue intake scripts
- Workflow policy, runtime docs, e2e/refactor runbooks, and compact artifact templates
- OpenCode slash-command docs under `.opencode/commands/`
- Script-level pytest coverage

## Run

```bash
PYTHONPATH=. python3 scripts/orchestrator_bootstrap_runner.py --issue-number 32 --dispatch-now --source-session-id auto-dev
```

## Tracker repo

GitHub intake defaults to `paulpai0412/wferp`. Override it when needed:

```bash
AUTODEV_GITHUB_REPO=<owner/repo> PYTHONPATH=. python3 scripts/issue_packet_intake.py
```
