# autodev

Standalone autonomous development harness extracted into its own workspace at `~/apps/autodev`.

## Included

- DB-only control-plane runtime built on SQLite `issues` and `issue_history`
- Orchestrator bootstrap, supervisor, and GitHub issue intake scripts
- Host-adapter-aware project init, global command install, and doctor tooling
- Script-level pytest coverage

## Current architecture map (P0/P1/P2 complete)

- **Control-plane truth (DB-only)**
  - `scripts/control_plane_db.py`: public runtime DB API and schema ownership
  - `scripts/control_plane_repository.py`: low-level repository seam (snapshot/history/occupancy)
- **Host adapter seam**
  - `scripts/host_adapter.py`: host-neutral session contracts (`SessionStartContext`, `SessionStartResult`, `SessionOutcome`)
  - `scripts/orchestrator_sessions.py`: adapter registry/factory + default resolver (`AUTODEV_HOST_ADAPTER`)
  - `scripts/opencode_host_adapter.py`: shipped OpenCode adapter implementation
- **Selection/dependency seam**
  - `scripts/issue_dependency.py`: canonical dependency parsing primitives
  - `scripts/issue_selection_projection.py`: readiness/base-branch projection rules
  - `scripts/orchestrator_selection.py`: DB-backed selection + intake orchestration helpers
- **Policy/prompt seams**
  - `scripts/orchestrator_policy.py`: reconcile/dispatch/release admission classifiers
  - `scripts/orchestrator_requests.py`: role/stage prompt and request spec builders
- **Supervisor composition layer**
  - `scripts/orchestrator_supervisor.py`: orchestrator entrypoint that composes lifecycle/selection/policy/request/host seams

## Documentation

- Chinese user manual: [`docs/autodev-user-manual.zh-TW.md`](docs/autodev-user-manual.zh-TW.md)
- Runtime architecture one-pager: [`docs/agents/runtime/current-architecture.md`](docs/agents/runtime/current-architecture.md)

## Consumer project setup

Initialize a project so it can be driven by the shared autodev workflow:

```bash
PYTHONPATH=. python3 scripts/autodev_project.py init --project-root /path/to/project --github-repo owner/repo
```

`init` bootstraps the consumer project contract **and** repository wiring:

- initializes a local git repository on `main` when needed
- adds `origin` for `https://github.com/<owner/repo>.git`
- creates the GitHub repository when it does not exist yet
- provisions the canonical autodev issue labels on that repository

Install user-global autodev host commands (the shipped default is the OpenCode adapter command set):

```bash
PYTHONPATH=. python3 scripts/autodev_project.py install-commands
```

From an initialized project, prefer the `autodev-flow` skill contracts (`C0..C6`) as the primary operator entrypoint.
Keep `/autodev-start <issue-number>`, `/autodev-reconcile`, `/autodev-release [issue-number]`, `/autodev-show-session`, and `/autodev-doctor` only as host wrappers behind that skill.
These wrappers sit over the same DB-only control plane in `.opencode/runtime/control-plane.sqlite3`.

For continuous development backfill, run the high-level watch wrapper:

```bash
PYTHONPATH=. python3 scripts/autodev_project.py reconcile-watch --project-root /path/to/project --interval-seconds 30
```

`reconcile-watch` keeps `reconcile-workspace` as the one-shot scheduler and repeats it on an interval. Use `--iterations <n>` for bounded test runs and `--stop-on-error` to stop after the first failing cycle.

## Run

```bash
PYTHONPATH=. python3 scripts/orchestrator_bootstrap_runner.py --base-dir /path/to/project --issue-number 32 --source-session-id auto-dev
```

`--source-session-id` is an explicit caller tag (the bootstrap runner default is `orchestrator-bootstrap`).

The runtime source of truth is `.opencode/runtime/control-plane.sqlite3`.
Supervisor decisions, dispatch results, resumable session state, verifier-owned PR facts, and issue packet context are persisted in SQLite rows. Local JSON/YAML artifacts are outside the active runtime contract.

Release/merge is intentionally separate from the per-issue development loop: verifier acceptance leaves an issue in `verified`, and `/autodev-release [issue-number]` claims it into `release_pending` before launching an independent release root session for PR merge/release work. Inside that release root session, the actual `release_worker` must run as a foreground subagent. Workspace reconcile can also auto-backfill verified issues into release when `AUTODEV_RELEASE_BACKFILL_MODE=auto`; pair that with `AUTODEV_AUTO_RELEASE_APPROVAL_MODE=bypass_approval` only if you want auto-started release workers to skip the human PR approval requirement while still enforcing all other release gates. After a successful merged release on GitHub-backed issues, autodev now closes the linked GitHub issue through the release completion path instead of relying only on PR closing keywords.

The active branch contract is:

- runtime control lives only in SQLite tables `issues` and `issue_history`
- `issues.current_session_id` is the single current-session pointer
- OpenCode is the default shipped host adapter, not a control-plane dependency
- bounded issue-scoped concurrency is allowed, but duplicate start for the same issue is not

## Tracker repo

GitHub intake defaults to `paulpai0412/autodev`. Point it at the consumer project so `ready-for-agent` issues are synced into that project's SQLite-backed intake flow:

```bash
AUTODEV_GITHUB_REPO=<owner/repo> PYTHONPATH=. python3 scripts/issue_packet_intake.py --project-root /path/to/project
```
