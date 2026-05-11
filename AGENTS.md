# AGENTS.md

## Repo shape
- Standalone autonomous-development loop extracted from `wferp`. The executable orchestration code is in `scripts/`; the workflow contract, templates, and runbooks are in `docs/agents/`; live runtime state is in `.opencode/runtime/`.
- Real entrypoints: `scripts/autodev_project.py`, `scripts/orchestrator_bootstrap_runner.py`, `scripts/orchestrator_supervisor.py`, `scripts/orchestrator_compact_payload.py`, and `scripts/issue_packet_intake.py`.
- Script-level regressions live in `tests/scripts/`. If you change one orchestrator script, update the matching test file.

## Exact operator commands
- Minimal verified setup: `python3 -m pip install pytest`
- Initialize a consumer project: `PYTHONPATH=. python3 scripts/autodev_project.py init --project-root <project> --github-repo <owner/repo>`
- Install user-global OpenCode commands: `PYTHONPATH=. python3 scripts/autodev_project.py install-commands`
- Check a consumer project: `PYTHONPATH=. python3 scripts/autodev_project.py doctor --project-root <project>`
- Report/remove legacy local workflow files: `PYTHONPATH=. python3 scripts/autodev_project.py migrate --project-root <project> --dry-run`
- Start a selected AFK issue: `PYTHONPATH=. python3 scripts/orchestrator_bootstrap_runner.py --issue-number <n> --dispatch-now --source-session-id auto-dev`
- Reconcile after a worker/verifier/release artifact lands: `PYTHONPATH=. python3 scripts/orchestrator_supervisor.py reconcile --ledger .opencode/runtime/orchestrator-ledger.json --source-session-id supervisor-reconcile`
- Materialize ready GitHub issues into local packets: `AUTODEV_GITHUB_REPO=<owner/repo> PYTHONPATH=. python3 scripts/issue_packet_intake.py`
- Broad local regression sweep for this repo: `pytest tests/scripts -q`
- Focused regression: `pytest tests/scripts/test_<script_name>.py -q`

## Workflow rules agents are likely to miss
- `main_orchestrator` is orchestration-only. It validates contracts and routes work; it does **not** implement issue scope or perform final issue QA itself.
- `issue_worker`, `pr_verifier`, and `release_worker` run as subagents inside the current root orchestrator session. Fresh root-session dispatch is only for `main_orchestrator` bootstrap/recovery handoff.
- Issue execution is serial: one selected `ready-for-agent` issue, one branch, one PR, one orchestrator path at a time.
- `scripts/orchestrator_bootstrap_runner.py` and `scripts/orchestrator_supervisor.py` are coupled through shared ledger/request/result schema. Changing runtime artifact names or paths breaks reconcile/dispatch flow.

## Runtime and artifact contract
- Runtime JSON lives at `.opencode/runtime/orchestrator-ledger.json`, `.opencode/runtime/new-session-request.json`, and `.opencode/runtime/new-session-result.json`.
- Consumer projects keep `.autodev.yaml`, domain docs, generated artifacts, and runtime state; they must not keep local copies of workflow scripts, templates, command docs, or plugins.
- Local issue packets must live under `docs/agents/issue-packets/issue-<n>.yaml`. If a packet is missing, bootstrap/recovery may attempt one GitHub intake run.
- GitHub intake defaults to `paulpai0412/wferp`; override `AUTODEV_GITHUB_REPO` when this workspace should target a different tracker.
- Keep repo artifacts compact and index-only. Enforced caps are: issue packets/checkpoints/worker results/test-case catalogs/failure registries/refactor audits <= 80 lines; evidence packets/release results <= 60; issue handoffs <= 35.
- `python3 scripts/agent_context_budget_check.py` is the artifact gate. Do not paste raw test logs, browser traces, screenshots, SQL logs, or full transcripts into repo docs or GitHub comments; store only compact summaries plus refs.

## High-value docs to read first
- `README.md` for the bootstrap command and tracker override.
- `docs/agents/autonomous-development-workflow.yaml` for role boundaries, gates, and serial execution policy.
- `docs/agents/issue-tracker.md` for GitHub tracker commands and verifier/release evidence rules.
- `scripts/autodev_project.py` for consumer project init, global command install, doctor checks, and legacy migration.
