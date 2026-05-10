# AGENTS.md

## Repo Purpose
- Repo root is `/home/timmypai/apps/autodev`.
- This repository contains the standalone autonomous development loop workspace extracted from `wferp`.
- The repo owns orchestrator bootstrap, supervisor recovery, checkpoint compaction, GitHub ready-issue intake, and compact workflow artifacts.

## High-Value Structure
- `scripts/orchestrator_bootstrap_runner.py` creates checkpoints, ledgers, and root-session requests.
- `scripts/orchestrator_supervisor.py` reconciles worker/verifier/release artifacts and routes the next role.
- `scripts/orchestrator_compact_payload.py` keeps checkpoint payloads compact for fresh-session bootstrap.
- `scripts/issue_packet_intake.py` materializes `ready-for-agent` GitHub issues into local issue packets.
- `docs/agents/` stores the workflow contract, runtime docs, compact artifact templates, and issue-loop runbooks.
- `.opencode/commands/` exposes the main operator commands for bootstrap, reconcile, and session lookup.
- `tests/scripts/` is the regression suite for the standalone loop.

## Setup
- No dependency manifest is checked in. Install the minimum verified dependency manually:

```bash
python3 -m pip install pytest
```

## Notes
- `scripts/issue_packet_intake.py` defaults to GitHub repo `paulpai0412/wferp`; override with `AUTODEV_GITHUB_REPO` if this workspace should target a different tracker.
- Runtime artifacts live under `.opencode/runtime/`.
- Keep artifact docs compact; the context budget gate enforces line caps.
