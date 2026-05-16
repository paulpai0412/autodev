# Host adapter strategy

## Status

- Status: active branch direction
- Branch: `db-only-control-plane`
- Depends on: `docs/agents/runtime/db-only-control-plane-spec.md`

## Purpose

Define how the DB-only autodev runtime stays portable across OpenCode, Claude Code, and Codex.

The key rule is simple:

> The orchestration engine owns state, decisions, and policy.
> The host adapter owns session launch, command UX, and platform integration.

## Decision summary

- The DB-only control plane is the product core.
- OpenCode is an adapter, not the product core.
- Claude Code is a target adapter.
- Codex is a target adapter.
- Packaging should be plugin-first and skill-assisted.

## Why this split exists

The current autodev repo still contains OpenCode-coupled behavior such as:

- `.opencode/commands/*`
- `.opencode/runtime/*` assumptions
- `opencode run` launch behavior
- OpenCode session tracing and session summary parsing
- command wrappers installed by `scripts/autodev_project.py install-commands`

That coupling makes portability expensive if it remains in the core runtime. The rewrite must move those concerns to adapters.

## Architecture layers

### 1. Core orchestration engine

This layer is host-agnostic.

Responsibilities:

- `issues` and `issue_history` schema
- state machine
- selection and reconcile logic
- retry and quarantine rules
- verifier-owned PR creation policy
- independent release coordination
- GitHub synchronization policy
- operator actions and audit

This layer must not know about:

- `.opencode/commands`
- `opencode run`
- Claude slash commands
- Codex CLI command syntax
- host-specific session table formats

### 2. Host adapter layer

This layer translates core operations into host-native execution.

Minimum responsibilities:

- start a root issue session
- launch a child role execution unit
- observe or poll a session outcome
- resume or link back to a live session
- expose operator entrypoints for start, reconcile, inspect, doctor, and release

The adapter must normalize host outputs into a host-neutral shape before they reach the core.

### 3. Host packaging layer

This layer is the user-facing delivery surface.

Examples:

- OpenCode plugin + command wrappers
- Claude Code plugin + slash commands + subagent setup
- Codex plugin + custom agents + command wrappers
- thin skills that teach operators how to use the plugin

This layer may be host-specific, but it must stay thin.

## Suggested adapter interface

The branch does not need to lock a final code signature yet, but adapters should cover this minimum contract:

- `start_root_session(context) -> runtime_session_id`
- `start_child_role(role, context) -> runtime_session_id`
- `read_session_outcome(runtime_session_id) -> normalized_outcome`
- `resume_link(runtime_session_id) -> operator_resume_hint`
- `operator_entrypoints() -> host_specific_commands`
- `capabilities() -> background/subagent/plugin support flags`

The core should consume only normalized outcomes, for example:

- `status`
- `session_id`
- `started_at`
- `ended_at`
- `error_kind`
- `resume_hint`
- `metadata`

## Current extraction targets

These are the main OpenCode-coupled surfaces that should move behind the adapter boundary:

- `scripts/orchestrator_sessions.py`
- `scripts/opencode_session_trace.py`
- `scripts/subagent_startup_repro.py`
- `.opencode/commands/auto-dev.md`
- `.opencode/commands/supervisor-reconcile.md`
- `scripts/autodev_project.py install-commands`

## Platform fit

### OpenCode

- Best current fit because the repo already targets it.
- Should become the first adapter implementation, not the core runtime model.

### Claude Code

- Strong fit because it has plugins, commands, subagents/background execution, and MCP-friendly integration.
- Expected to be the easiest second adapter once the core is host-agnostic.

### Codex

- Viable fit, but expect more adapter work around command surface and session/result plumbing.
- Still worth supporting if the core is cleanly separated.

## Packaging recommendation

- Put executable integration logic in a plugin or plugin-like package.
- Keep skills thin and instructional.
- Do not put the orchestration engine itself inside a skill.

In other words:

- plugin = autodev runtime integration
- skill = how to operate the plugin safely

## Acceptance criteria

This strategy is satisfied when:

1. core orchestration code can run with a fake adapter in tests
2. OpenCode-specific code is isolated to an adapter/package boundary
3. adding Claude Code support is mostly adapter work, not core schema work
4. adding Codex support is mostly adapter work, not core state-machine work
5. the core DB schema contains no host-specific assumptions
