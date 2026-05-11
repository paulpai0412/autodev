# autodev

Standalone autonomous development loop extracted into its own workspace at `~/apps/autodev`.

## Included

- Orchestrator bootstrap, supervisor, compact payload, and GitHub issue intake scripts
- Workflow policy, runtime docs, e2e/refactor runbooks, and compact artifact templates
- Autodev-owned project init, global command install, doctor, and migration tooling
- Script-level pytest coverage

## Consumer project setup

Initialize a project so it can be driven by the shared autodev workflow:

```bash
PYTHONPATH=. python3 scripts/autodev_project.py init --project-root /path/to/project --github-repo owner/repo
```

Install user-global OpenCode commands with explicit autodev names:

```bash
PYTHONPATH=. python3 scripts/autodev_project.py install-commands
```

From an initialized project, use `/autodev-start <issue-number>`, `/autodev-reconcile`, `/autodev-show-session`, and `/autodev-doctor`.

Remove old project-local workflow code only after reviewing the dry-run report:

```bash
PYTHONPATH=. python3 scripts/autodev_project.py migrate --project-root /path/to/project --dry-run
PYTHONPATH=. python3 scripts/autodev_project.py migrate --project-root /path/to/project --remove-legacy
```

## Run

```bash
PYTHONPATH=. python3 scripts/orchestrator_bootstrap_runner.py --issue-number 32 --dispatch-now --source-session-id auto-dev
```

## Tracker repo

GitHub intake defaults to `paulpai0412/wferp`. Override it when needed:

```bash
AUTODEV_GITHUB_REPO=<owner/repo> PYTHONPATH=. python3 scripts/issue_packet_intake.py
```
