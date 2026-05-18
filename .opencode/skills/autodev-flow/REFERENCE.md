# Autodev Flow Reference

This reference binds the `autodev-flow` skill to the DB-only runtime contract and concrete command surface.

## Required runtime contract

- Source of truth: `.opencode/runtime/control-plane.sqlite3`
- Runtime tables: `issues`, `issue_history`
- Single current session pointer: `issues.current_session_id`
- Valid lifecycle states include: `ready`, `claimed`, `dispatching`, `running`, `verifying`, `verified`, `release_pending`, `completed`, `failed`, `quarantined`

Never gate workflow decisions on local JSON/YAML runtime artifacts.

## Command forms

Assume:

```bash
AUTODEV_HOME="${AUTODEV_HOME:-$HOME/apps/autodev}"
```

### 1) Initialize consumer project (idempotent)

```bash
PYTHONPATH="$AUTODEV_HOME" python3 "$AUTODEV_HOME/scripts/autodev_project.py" init --project-root <project_root> --github-repo <owner/repo>
```

### 2) Install host commands

```bash
PYTHONPATH="$AUTODEV_HOME" python3 "$AUTODEV_HOME/scripts/autodev_project.py" install-commands
```

### 3) Doctor check

```bash
PYTHONPATH="$AUTODEV_HOME" python3 "$AUTODEV_HOME/scripts/autodev_project.py" doctor --project-root <project_root>
```

### 4) Start one issue

```bash
PYTHONPATH="$AUTODEV_HOME" python3 "$AUTODEV_HOME/scripts/autodev_project.py" start --project-root <project_root> --issue-number <n>
```

### 5) Reconcile once (workspace)

```bash
PYTHONPATH="$AUTODEV_HOME" python3 "$AUTODEV_HOME/scripts/autodev_project.py" reconcile --project-root <project_root>
```

### 6) Reconcile watch loop

```bash
PYTHONPATH="$AUTODEV_HOME" python3 "$AUTODEV_HOME/scripts/autodev_project.py" reconcile-watch --project-root <project_root> --interval-seconds <sec> [--iterations <n>] [--stop-on-error]
```

### 7) Inspect active session pointers

```bash
PYTHONPATH="$AUTODEV_HOME" python3 "$AUTODEV_HOME/scripts/autodev_project.py" show-session --project-root <project_root>
```

### 8) Quarantine and recovery actions

```bash
PYTHONPATH="$AUTODEV_HOME" python3 "$AUTODEV_HOME/scripts/orchestrator_supervisor.py" quarantine --base-dir <project_root> --issue-number <n> --reason "<why>"
PYTHONPATH="$AUTODEV_HOME" python3 "$AUTODEV_HOME/scripts/orchestrator_supervisor.py" resume-quarantined --base-dir <project_root> --issue-number <n> --reason "<why>"
PYTHONPATH="$AUTODEV_HOME" python3 "$AUTODEV_HOME/scripts/orchestrator_supervisor.py" fail-quarantined --base-dir <project_root> --issue-number <n> --reason "<why>"
PYTHONPATH="$AUTODEV_HOME" python3 "$AUTODEV_HOME/scripts/orchestrator_supervisor.py" retry-failed --base-dir <project_root> --issue-number <n> --reason "<why>"
PYTHONPATH="$AUTODEV_HOME" python3 "$AUTODEV_HOME/scripts/orchestrator_supervisor.py" retry-github-sync --base-dir <project_root> --issue-number <n> --command-id <id>
```

## Decision policy

1. **Default mode:** dry-run plan -> explicit confirmation -> execute.
2. **Scheduler mode:** fixed-interval polling (`reconcile-watch`) with DB-backed `reconcile-workspace` semantics.
3. **Capacity default:** `AUTODEV_DEVELOPMENT_CAPACITY=1` unless explicitly overridden.
4. **Failure policy:**
   - if retryable and under limit -> retry path
   - else -> quarantine
   - quarantined auto-resume at most once when transient + retryable + cooldown reached
   - second failure after auto-resume -> require operator decision
5. **Completion gate:** require verifier/release owned evidence and valid DB state transitions before declaring done.

## Operator report format (each loop cycle)

- current `issue_number`
- `state`, `current_role`, `current_stage`, `current_status`
- `current_session_id`
- supervisor decision summary
- action taken this cycle
- next action and why
- if blocked: precise unblock command

## Stop conditions

- missing required config (`project_root`, repo binding, runtime DB)
- non-retryable failure
- retries exhausted
- strict gate failed
- ambiguous destructive action

When stopped, print one next-step command instead of broad suggestions.
