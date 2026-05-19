# Issue tracker: GitHub

Issues and PRDs for this repo live as GitHub issues:

- Repository: `paulpai0412/autodev`
- URL: <https://github.com/paulpai0412/autodev>
- Source skill setup: <https://github.com/mattpocock/skills/tree/main/skills/engineering/setup-matt-pocock-skills>

Use the `gh` CLI for issue operations when a skill says to publish to or fetch from the issue tracker.

## Runtime prerequisites

- `gh auth status --repo paulpai0412/autodev` should succeed on any machine expected to materialize issue packets from GitHub.
- Network access to GitHub is required for live issue intake.
- When GitHub is temporarily unavailable, the autonomous loop can keep working only from issue data already ingested into SQLite.
- Intake default repo is `paulpai0412/autodev`; override it per consumer project with `AUTODEV_GITHUB_REPO=<owner/repo>`.

## Conventions

- **Create an issue**: `gh issue create --repo paulpai0412/autodev --title "..." --body "..."`. Use a heredoc for multi-line bodies.
- **Read an issue**: `gh issue view <number> --repo paulpai0412/autodev --comments`, filtering comments by `jq` and also fetching labels when needed.
- **List issues**: `gh issue list --repo paulpai0412/autodev --state open --json number,title,body,labels,comments --jq '[.[] | {number, title, body, labels: [.labels[].name], comments: [.comments[].body]}]'` with appropriate `--label` and `--state` filters.
- **Comment on an issue**: `gh issue comment <number> --repo paulpai0412/autodev --body "..."`.
- **Apply / remove labels**: `gh issue edit <number> --repo paulpai0412/autodev --add-label "..."` / `--remove-label "..."`.
- **Close**: `gh issue close <number> --repo paulpai0412/autodev --comment "..."`.

When running inside this clone, `gh` can also infer the repository from `git remote -v`.

## Autonomous workflow tracker rules

- PR bodies and issue comments may summarize verification only by referencing a verifier-owned evidence fact recorded in SQLite, for example a compact DB ref such as `db:issue-history/evidence_packet:<issue>:<pr>`.
- Do not paste raw test logs, browser traces, SQL execution logs, or verbose manual QA transcripts into issue comments or PR bodies.
- A final QA statement must identify the verifier-owned evidence fact and must not be written as a direct main-agent QA claim.
- If no verifier-owned evidence fact exists, the issue or PR must be marked blocked for missing verification evidence instead of reporting tests or QA as passed.
- Worker self-check summaries may be mentioned only as implementation feedback; they are not acceptance evidence unless independently confirmed by verifier-owned evidence.
- `verifier_read_worker_result_only: false` is allowed when the verifier also reads compact DB-backed refs such as the stored issue packet body, PR diff, or PR page; it must not import full worker transcripts or raw logs.
- Artifact bodies that remain as historical projections should stay compact: issue packet bodies <=80 lines, handoffs <=35 lines, evidence payload projections <=60 lines, checkpoints <=80 lines, and worker results <=80 lines.
- Raw evidence is index-only in repo docs and main-agent context; keep full logs/traces/screenshots in external artifact bundles referenced by manifest IDs.
- A `release_worker` may merge and close only after verifier-owned evidence passes, the PR is mergeable, required checks pass, and human merge approval policy is satisfied; otherwise it must report blocked.
- Default merge approval mode is `human_required`.
- Autonomous workflow start may explicitly set `approval_override_mode: bypass_approval` for that workflow run only.
- `bypass_approval` may bypass only `human_merge_approval_policy_satisfied`; it must not bypass verifier pass, required checks, PR mergeability, review gate, diagnostics/build gate, or surface QA gate.
- `bypass_approval` must be declared at workflow start, recorded in SQLite runtime state, remain immutable after start, and apply only to PRs created by that workflow run.
- Release summaries or PR comments for bypassed merges must record `merge_approval_mode`, `human_approval_skipped`, `override_source`, and `override_scope`.
- After a `release_worker` merge succeeds, it must run post-merge workspace hygiene before closing the linked issue.
- Post-merge workspace hygiene may modify only workspace hygiene state: preserve dirty primary workspace state, switch the primary workspace back to `main`, fast-forward `main`, and remove a clean merged issue worktree when safe.
- If the primary workspace is dirty, the `release_worker` should preserve it with a WIP branch named `agent/wip/post-merge-issue-{issue_number}-{yyyyMMdd-HHmm}` and a stash message `post-merge hygiene preserve issue-{issue_number} from {source_branch}` before restoring `main`.
- If post-merge workspace hygiene fails, the linked issue must remain open and the tracker comment must report a compact hygiene summary instead of claiming issue closure.
- Post-merge workspace hygiene summaries must use fixed fields in DB-backed runtime state and GitHub comments: `primary_workspace_branch_before`, `primary_workspace_branch_after`, `dirty_state_detected`, `wip_branch_created`, `stash_created`, `workspace_clean_after`, `issue_worktree_removed`, `cleanup_status`, and `blocked_reason`.
- `blocked_reason` values for hygiene failures should use fixed enums: `dirty_workspace_preserve_failed`, `switch_main_failed`, `fast_forward_main_failed`, `worktree_remove_failed`, `workspace_not_clean_after_cleanup`, and `issue_worktree_dirty_blocked`.
- Do not use `ultrawork` or any continuous autonomous loop to select and implement multiple `ready-for-agent` AFK issues concurrently from one orchestrator path.
- Concurrency is issue-scoped: one active autodev root orchestrator per issue, enforced by SQLite issue state and GitHub coordination labels.
- Different issues may run in parallel from separate OpenCode sessions or worktrees, but the same issue must not be started twice while `agent-in-progress` is present.
- Autodev start should add `agent-in-progress` and remove `ready-for-agent` when an issue is claimed. If bootstrap dispatch fails before the root session starts, it should restore `ready-for-agent` and remove `agent-in-progress`.
- `scripts/issue_packet_intake.py` is the supported bridge from live GitHub `ready-for-agent` issues into SQLite-backed intake inputs.
- Supervisor recovery may invoke that intake script automatically when no eligible next issue is already present in DB-backed intake state.
- Intake fallback is best-effort: if `gh` auth fails, GitHub is unreachable, or no eligible issue is returned, the supervisor must keep the result compact in SQLite-backed runtime state and avoid inventing a next issue.

### Post-merge workspace hygiene comment template

Use this compact GitHub comment shape after `release_worker` merge-time cleanup completes or blocks:

```md
## Post-merge workspace hygiene
- primary_workspace_branch_before: <branch>
- primary_workspace_branch_after: <branch>
- dirty_state_detected: <true|false>
- wip_branch_created: <none-or-branch-name>
- stash_created: <none-or-stash-ref>
- workspace_clean_after: <true|false>
- issue_worktree_removed: <true|false|blocked>
- cleanup_status: <pass|blocked>
- blocked_reason: <none|dirty_workspace_preserve_failed|switch_main_failed|fast_forward_main_failed|worktree_remove_failed|workspace_not_clean_after_cleanup|issue_worktree_dirty_blocked>
```

## When a skill says "publish to the issue tracker"

Create a GitHub issue in `paulpai0412/autodev`.

## When a skill says "fetch the relevant ticket"

Run `gh issue view <number> --repo paulpai0412/autodev --comments`.
