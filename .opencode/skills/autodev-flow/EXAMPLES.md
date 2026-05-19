# Examples: autodev-flow

## Example 1: New consumer repo from scratch

User intent: initialize and start autonomous loop.

Suggested runbook:

1. Dry-run plan showing commands and stop conditions.
2. Execute:

```bash
AUTODEV_HOME="${AUTODEV_HOME:-$HOME/apps/autodev}" PYTHONPATH="$AUTODEV_HOME" python3 "$AUTODEV_HOME/scripts/autodev_project.py" init --project-root /path/to/project --github-repo owner/repo
AUTODEV_HOME="${AUTODEV_HOME:-$HOME/apps/autodev}" PYTHONPATH="$AUTODEV_HOME" python3 "$AUTODEV_HOME/scripts/autodev_project.py" install-commands
AUTODEV_HOME="${AUTODEV_HOME:-$HOME/apps/autodev}" PYTHONPATH="$AUTODEV_HOME" python3 "$AUTODEV_HOME/scripts/autodev_project.py" doctor --project-root /path/to/project
```

3. Start loop:

```bash
AUTODEV_HOME="${AUTODEV_HOME:-$HOME/apps/autodev}" PYTHONPATH="$AUTODEV_HOME" python3 "$AUTODEV_HOME/scripts/autodev_project.py" reconcile-watch --project-root /path/to/project --interval-seconds 30
```

## Example 2: Start a specific issue and watch

```bash
AUTODEV_HOME="${AUTODEV_HOME:-$HOME/apps/autodev}" PYTHONPATH="$AUTODEV_HOME" python3 "$AUTODEV_HOME/scripts/autodev_project.py" start --project-root /path/to/project --issue-number 42
AUTODEV_HOME="${AUTODEV_HOME:-$HOME/apps/autodev}" PYTHONPATH="$AUTODEV_HOME" python3 "$AUTODEV_HOME/scripts/autodev_project.py" reconcile-watch --project-root /path/to/project --interval-seconds 30 --iterations 10 --stop-on-error
```

Report each cycle from DB state (`state`, role/stage/status, session id, next action).

## Example 3: Recovery after quarantine

1. If issue is quarantined and matches transient+retryable+cooldown conditions, perform a single auto-resume:

```bash
AUTODEV_HOME="${AUTODEV_HOME:-$HOME/apps/autodev}" PYTHONPATH="$AUTODEV_HOME" python3 "$AUTODEV_HOME/scripts/orchestrator_supervisor.py" resume-quarantined --base-dir /path/to/project --issue-number 42 --reason "single auto-resume after cooldown"
```

2. If it fails again, stop auto-recovery and require operator action:

```bash
AUTODEV_HOME="${AUTODEV_HOME:-$HOME/apps/autodev}" PYTHONPATH="$AUTODEV_HOME" python3 "$AUTODEV_HOME/scripts/orchestrator_supervisor.py" fail-quarantined --base-dir /path/to/project --issue-number 42 --reason "auto-resume exhausted; needs manual intervention"
```

## Example 4: Retry a retryable failed issue

```bash
AUTODEV_HOME="${AUTODEV_HOME:-$HOME/apps/autodev}" PYTHONPATH="$AUTODEV_HOME" python3 "$AUTODEV_HOME/scripts/orchestrator_supervisor.py" retry-failed --base-dir /path/to/project --issue-number 42 --reason "retryable failure under policy limit"
```

Then run one reconcile cycle:

```bash
AUTODEV_HOME="${AUTODEV_HOME:-$HOME/apps/autodev}" PYTHONPATH="$AUTODEV_HOME" python3 "$AUTODEV_HOME/scripts/autodev_project.py" reconcile --project-root /path/to/project
```
