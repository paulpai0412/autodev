# AGENTS.md

## DB-only rewrite branch
- On branch `db-only-control-plane`, the active runtime contract is a **DB-only control plane** built on SQLite tables `issues` and `issue_history`.
- While on this branch, read `docs/agents/runtime/db-only-control-plane-spec.md` first, `docs/agents/runtime/db-only-control-plane-implementation-plan.md` second, `docs/agents/runtime/host-adapter-strategy.md` third, `docs/agents/runtime/product-positioning.md` fourth, and `docs/agents/runtime/multi-issue-concurrency.md` fifth. These branch-local docs take precedence over any older file-backed runtime story.
- After every context compaction or context reset, re-read `docs/agents/runtime/db-only-control-plane-spec.md` and `docs/agents/runtime/db-only-control-plane-implementation-plan.md` before making decisions or writing code. Re-read the other branch-local runtime docs whenever the work touches adapter boundaries, product semantics, or concurrency/release behavior.
- Branch rule: runtime control must live only in SQLite tables `issues` and `issue_history`. Do not preserve `orchestrator-ledger.json`, `new-session-request.json`, `new-session-result.json`, issue-lock files, checkpoints, issue-packet files, handoff files, or worker/verifier/release result files as runtime dependencies.
- Branch rule: use `issues.current_session_id` as the single current session pointer. Do not add role-specific current session columns such as `current_root_session_id` or `current_verifier_session_id`; child session ids belong in `issue_history.payload_json`.
- Branch rule: treat OpenCode as only one host adapter. Do not let OpenCode-specific commands, session traces, or `.opencode/` runtime assumptions leak into the core DB-only orchestration engine.
- Branch rule: the product is an orchestration/control-plane harness above coding-agent runtimes, not a replacement for Claude Code, Codex, or OpenCode.
- Branch rule: the target execution model is bounded multi-issue concurrency. Multiple issues may run concurrently, but the same issue must never have more than one active root orchestrator at a time.

## Repo shape
- Standalone autonomous-development loop extracted from `wferp`. The executable orchestration code is in `scripts/`; the workflow contract, templates, and runbooks are in `docs/agents/`; live runtime state is in `.opencode/runtime/`.
- Real entrypoints: `scripts/autodev_project.py`, `scripts/orchestrator_bootstrap_runner.py`, `scripts/orchestrator_supervisor.py`, and `scripts/issue_packet_intake.py`. The host-neutral adapter facade lives in `scripts/orchestrator_sessions.py`; the current OpenCode adapter implementation lives in `scripts/opencode_host_adapter.py`; issue claim/lifecycle helpers live in `scripts/orchestrator_lifecycle.py`; prompt/request builders live in `scripts/orchestrator_requests.py`; issue selection/intake helpers live in `scripts/orchestrator_selection.py`; reconcile transition/recovery, role-specific branch handlers, and main-orchestrator branch helpers live in `scripts/orchestrator_reconcile.py`. `scripts/orchestrator_artifacts.py` now survives only as compact parsing/compatibility glue, not as a runtime source of truth.
- Script-level regressions live in `tests/scripts/`. If you change one orchestrator script, update the matching test file.

## Exact operator commands
- Preferred operator interface: `autodev-flow` skill contracts (`C0..C6`). Use script-level commands below only as low-level execution/debug surfaces.
- Minimal verified setup: `python3 -m pip install pytest`
- Initialize a consumer project and bootstrap git/GitHub wiring: `PYTHONPATH=. python3 scripts/autodev_project.py init --project-root <project> --github-repo <owner/repo>`
- Install user-global autodev host commands (OpenCode adapter by default): `PYTHONPATH=. python3 scripts/autodev_project.py install-commands`
- Check a consumer project: `PYTHONPATH=. python3 scripts/autodev_project.py doctor --project-root <project>`
- Start a selected AFK issue: `PYTHONPATH=. python3 scripts/orchestrator_bootstrap_runner.py --base-dir <project> --issue-number <n> --source-session-id auto-dev`
- Reconcile one DB-backed issue: `PYTHONPATH=. python3 scripts/orchestrator_supervisor.py reconcile --base-dir <project> --issue-number <n>`
- Reconcile the whole workspace and fill free development capacity: `PYTHONPATH=. python3 scripts/orchestrator_supervisor.py reconcile-workspace --base-dir <project>`
- Inspect control-plane state: `PYTHONPATH=. python3 scripts/orchestrator_supervisor.py inspect --base-dir <project> --issue-number <n>`
- Quarantine a running issue: `PYTHONPATH=. python3 scripts/orchestrator_supervisor.py quarantine --base-dir <project> --issue-number <n> --reason <why>`
- Resume a quarantined issue: `PYTHONPATH=. python3 scripts/orchestrator_supervisor.py resume-quarantined --base-dir <project> --issue-number <n> --reason <why>`
- Fail a quarantined issue: `PYTHONPATH=. python3 scripts/orchestrator_supervisor.py fail-quarantined --base-dir <project> --issue-number <n> --reason <why>`
- Retry a retryable failed issue: `PYTHONPATH=. python3 scripts/orchestrator_supervisor.py retry-failed --base-dir <project> --issue-number <n> --reason <why>`
- Retry a failed GitHub sync attempt: `PYTHONPATH=. python3 scripts/orchestrator_supervisor.py retry-github-sync --base-dir <project> --issue-number <n> --command-id <id>`
- Sync ready GitHub issues into SQLite-backed issue ingestion inputs: `AUTODEV_GITHUB_REPO=<owner/repo> PYTHONPATH=. python3 scripts/issue_packet_intake.py --project-root <project>`
- Broad local regression sweep for this repo: `python3 -m pytest tests/scripts -q`
- Focused regression: `python3 -m pytest tests/scripts/test_<script_name>.py -q`

## Workflow rules agents are likely to miss
- `main_orchestrator` is orchestration-only. It validates contracts and routes work; it does **not** implement issue scope or perform final issue QA itself.
- `issue_worker` and `pr_verifier` run as subagents inside the current root orchestrator session, and the root should launch them synchronously with `run_in_background: false`. `release_worker` runs as a synchronous foreground subagent inside an independent release root session claimed by the release command. Fresh root-session dispatch is only for `main_orchestrator` bootstrap/recovery handoff and the dedicated release root-session entrypoint.
- On this branch, development scheduling is bounded and issue-scoped: multiple issues may progress concurrently, but the same issue must never have more than one active root orchestrator or development path.
- `scripts/orchestrator_bootstrap_runner.py` and `scripts/orchestrator_supervisor.py` are now coupled through the SQLite control-plane schema and DB-backed dispatch/reconcile flow. Breaking issue/history field expectations will break bootstrap and recovery.

## Runtime and artifact contract
- Consumer projects keep `.autodev.yaml`, domain docs, generated artifacts, and runtime state; they must not keep local copies of workflow scripts, templates, command docs, or plugins.
- Runtime source of truth is `.opencode/runtime/control-plane.sqlite3`; issue selection, lifecycle, audit, dispatch facts, and resume state must all be recoverable from SQLite.
- Local issue packet / handoff / worker-result / evidence / release-result files must not be required for runtime progress on this branch. If compatibility artifacts still exist, they are historical projections only; runtime gates must read DB facts from `issues` and `issue_history`.
- GitHub intake defaults to `paulpai0412/autodev`; override `AUTODEV_GITHUB_REPO` when this workspace should target a different tracker.
- Keep repo artifacts compact and index-only. Any historical artifact docs that remain should stay compact and non-canonical.
- `python3 scripts/agent_context_budget_check.py` is the artifact gate. Do not paste raw test logs, browser traces, screenshots, SQL logs, or full transcripts into repo docs or GitHub comments; store only compact summaries plus refs.

## High-value docs to read first
- `README.md` for the operator-facing DB-only command surface and tracker override.
- `docs/agents/runtime/db-only-control-plane-spec.md` for the clean-slate DB-only runtime contract on branch `db-only-control-plane`.
- `docs/agents/runtime/db-only-control-plane-implementation-plan.md` for the rewrite phases and deletion plan on branch `db-only-control-plane`.
- `docs/agents/runtime/host-adapter-strategy.md` for the host-agnostic adapter boundary and OpenCode/Claude Code/Codex portability plan.
- `docs/agents/runtime/product-positioning.md` for harness-product positioning and value relative to coding-agent runtimes.
- `docs/agents/runtime/multi-issue-concurrency.md` for the bounded multi-issue scheduler model on branch `db-only-control-plane`.
- `docs/agents/autonomous-development-workflow.yaml` for role boundaries, gates, and bounded issue-scoped concurrency policy.
- `docs/agents/issue-tracker.md` for GitHub tracker commands and verifier/release evidence rules.
- `scripts/autodev_project.py` for consumer project init, global command install, and doctor checks.
