"""Issue lifecycle, lock, and GitHub label helpers for the autodev supervisor."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Callable, cast
from uuid import uuid4

from scripts.control_plane_db import (
    ensure_control_plane_db,
    ensure_issue_row,
    read_artifact_fact,
    read_issue,
    read_runtime_context,
    record_admin_decision,
    record_github_sync_attempt,
    sync_issue_runtime_context,
    transition_issue_state,
    upsert_issue_state,
)
JsonObject = dict[str, object]
NowFunc = Callable[[str | None], str]
SyncProgressLabel = Callable[..., str]
TransitionIssueState = Callable[..., None]
ReleaseIssueExecution = Callable[..., None]
SyncLocalMainAfterReleaseMerge = Callable[..., str]
CloseGitHubIssueAfterReleaseMerge = Callable[..., str]


READY_FOR_AGENT_LABEL = "ready-for-agent"
AGENT_DISPATCHING_LABEL = "agent-dispatching"
AGENT_IN_PROGRESS_LABEL = "agent-in-progress"
QUARANTINED_LABEL = "quarantined"
def _issue_backing_type(base_dir: Path, issue_number: str) -> str:
    issue = read_issue(base_dir, issue_number) or {}
    issue_packet = cast(dict[str, object], json.loads(str(issue.get("issue_packet_json") or "{}"))) if issue else {}
    return str(issue_packet.get("backing_type") or "github")


def scheduler_id(base_dir: Path) -> str:
    return f"scheduler:{base_dir.resolve()}"


def transition_issue_state_if_possible(
    *,
    base_dir: Path,
    issue_number: str,
    to_state: str,
    command_id: str,
    updated_at: str,
    reason: str,
    from_state: str | None = None,
    current_session_id: str | None = None,
) -> None:
    transition_issue_state(
        base_dir,
        issue_number=issue_number,
        to_state=to_state,
        command_id=command_id,
        scheduler_id=scheduler_id(base_dir),
        reason=reason,
        updated_at=updated_at,
        from_state=from_state,
        current_session_id=current_session_id,
    )


def has_issue_execution_lock(base_dir: Path, issue_number: str) -> bool:
    issue = read_issue(base_dir, issue_number) or {}
    return str(issue.get("state") or "") in {"claimed", "dispatching", "running", "verifying", "quarantined"}


def update_issue_execution_claim(
    *,
    base_dir: Path,
    issue_number: str,
    updates: JsonObject,
    now: NowFunc,
) -> None:
    issue = read_issue(base_dir, issue_number) or {}
    payload: JsonObject = cast(dict[str, object], json.loads(str(issue.get("artifact_refs_json") or "{}"))) if issue else {"issueNumber": issue_number}
    for key, value in updates.items():
        payload[str(key)] = value
    _ = sync_issue_runtime_context(
        base_dir,
        issue_number=issue_number,
        updated_at=str(payload.get("recordedAt") or now(None)),
        artifact_refs=payload,
    )


def clear_issue_execution_claim_projection(*, base_dir: Path, issue_number: str, updated_at: str) -> None:
    issue = read_issue(base_dir, issue_number) or {}
    existing_artifacts = cast(dict[str, object], json.loads(str(issue.get("artifact_refs_json") or "{}"))) if issue else {}
    for key in ["issueNumber", "branch", "sourceSessionID", "createdAt", "status", "rootSessionID", "verifierSessionID", "recordedAt"]:
        existing_artifacts.pop(key, None)
    _ = sync_issue_runtime_context(
        base_dir,
        issue_number=issue_number,
        updated_at=updated_at,
        artifact_refs=existing_artifacts,
    )


def clear_issue_session_ids(*, base_dir: Path, issue_number: str, updated_at: str) -> None:
    _ = upsert_issue_state(
        base_dir,
        issue_number=issue_number,
        state=str((read_issue(base_dir, issue_number) or {}).get("state") or "ready"),
        command_id=f"clear-session-ids:{issue_number}:{updated_at}",
        updated_at=updated_at,
        current_session_id="",
    )


def sync_issue_progress_label(
    *,
    base_dir: Path,
    issue_number: str,
    repo: str,
    add_labels: list[str],
    remove_labels: list[str],
    now: NowFunc,
    run: Callable[..., subprocess.CompletedProcess[str]],
    command_id: str | None = None,
    updated_at: str | None = None,
) -> str:
    backing_type = _issue_backing_type(base_dir, issue_number)
    if backing_type == "local_seeded":
        if command_id:
            record_github_sync_attempt(
                base_dir,
                command_id=command_id,
                issue_number=issue_number,
                add_labels=add_labels,
                remove_labels=remove_labels,
                status="skipped",
                updated_at=now(updated_at),
                last_error="skipped GitHub label sync for local-seeded issue",
            )
        return ""
    if not repo:
        if command_id:
            record_github_sync_attempt(
                base_dir,
                command_id=command_id,
                issue_number=issue_number,
                add_labels=add_labels,
                remove_labels=remove_labels,
                status="skipped",
                updated_at=now(updated_at),
            )
        return ""
    command = ["gh", "issue", "edit", issue_number, "--repo", repo]
    for label in add_labels:
        command.extend(["--add-label", label])
    for label in remove_labels:
        command.extend(["--remove-label", label])
    completed = run(command, cwd=base_dir, check=False, capture_output=True, text=True)
    if completed.returncode == 0:
        if command_id:
            record_github_sync_attempt(
                base_dir,
                command_id=command_id,
                issue_number=issue_number,
                add_labels=add_labels,
                remove_labels=remove_labels,
                status="success",
                updated_at=now(updated_at),
            )
        return ""
    error = (completed.stderr or completed.stdout).strip() or f"gh issue edit failed with exit code {completed.returncode}"
    if command_id:
        record_github_sync_attempt(
            base_dir,
            command_id=command_id,
            issue_number=issue_number,
            add_labels=add_labels,
            remove_labels=remove_labels,
            status="failed",
            updated_at=now(updated_at),
            last_error=error,
        )
    return error


def sync_local_main_after_release_merge(
    *,
    base_dir: Path,
    issue_number: str,
    updated_at: str | None = None,
) -> str:
    del updated_at
    issue = read_issue(base_dir, issue_number) or {}
    release_result = read_artifact_fact(base_dir, issue_number, "release_result")
    if not bool(release_result.get("parse_ok")):
        return ""

    status = str(release_result.get("status") or "").strip().lower()
    if status not in {"success", "completed"}:
        return ""

    merge_payload_raw = release_result.get("merge")
    merge_payload = cast(dict[str, object], merge_payload_raw) if isinstance(merge_payload_raw, dict) else {}
    merged = bool(merge_payload.get("merged")) or bool(str(merge_payload.get("merged_sha") or ""))
    if not merged:
        return ""

    branch = str(issue.get("branch") or "").strip()
    issue_worktree_path = str((read_runtime_context(base_dir, issue_number) or {}).get("issue_worktree_path") or "").strip()

    workspace_dir = Path(issue_worktree_path) if issue_worktree_path else base_dir

    def _run_git(command: list[str], *, cwd: Path) -> str:
        completed = subprocess.run(command, cwd=cwd, check=False, capture_output=True, text=True)
        if completed.returncode == 0:
            return ""
        return (completed.stderr or completed.stdout).strip() or (
            f"{' '.join(command)} failed with exit code {completed.returncode}"
        )

    if workspace_dir != base_dir:
        repo_probe = _run_git(["git", "rev-parse", "--is-inside-work-tree"], cwd=workspace_dir)
        if repo_probe:
            return ""
        sync_error = _run_git(["git", "fetch", "origin", "main"], cwd=workspace_dir)
        if sync_error:
            return f"failed issue worktree sync after release merge for issue #{issue_number}: {sync_error}"
        if branch and branch != "main":
            sync_error = _run_git(["git", "merge", "--ff-only", "origin/main"], cwd=workspace_dir)
            if sync_error:
                return f"failed issue worktree branch sync after release merge for issue #{issue_number}: {sync_error}"
        return ""

    repo_probe = _run_git(["git", "rev-parse", "--is-inside-work-tree"], cwd=base_dir)
    if repo_probe:
        return ""

    for command in (
        ["git", "fetch", "origin", "main"],
        ["git", "checkout", "main"],
        ["git", "pull", "--ff-only", "origin", "main"],
    ):
        sync_error = _run_git(command, cwd=base_dir)
        if sync_error:
            return f"failed local main sync after release merge for issue #{issue_number}: {sync_error}"

    if branch and branch != "main":
        branch_exists_error = _run_git(["git", "rev-parse", "--verify", branch], cwd=base_dir)
        if not branch_exists_error:
            for command in (
                ["git", "checkout", branch],
                ["git", "merge", "--ff-only", "main"],
                ["git", "checkout", "main"],
            ):
                sync_error = _run_git(command, cwd=base_dir)
                if sync_error:
                    return f"failed local branch/main sync after release merge for issue #{issue_number}: {sync_error}"

    return ""


def close_github_issue_after_release_merge(
    *,
    base_dir: Path,
    issue_number: str,
    repo: str,
    now: NowFunc,
    run: Callable[..., subprocess.CompletedProcess[str]],
    command_id: str | None = None,
    updated_at: str | None = None,
) -> str:
    backing_type = _issue_backing_type(base_dir, issue_number)
    timestamp = now(updated_at)
    if backing_type == "local_seeded":
        if command_id:
            record_github_sync_attempt(
                base_dir,
                command_id=command_id,
                issue_number=issue_number,
                add_labels=[],
                remove_labels=[],
                status="skipped",
                updated_at=timestamp,
                last_error="skipped GitHub issue close for local-seeded issue",
            )
        return ""
    if not repo:
        if command_id:
            record_github_sync_attempt(
                base_dir,
                command_id=command_id,
                issue_number=issue_number,
                add_labels=[],
                remove_labels=[],
                status="skipped",
                updated_at=timestamp,
                last_error="skipped GitHub issue close because project github_repo is unset",
            )
        return ""

    release_result = read_artifact_fact(base_dir, issue_number, "release_result")
    if not bool(release_result.get("parse_ok")):
        return ""
    status = str(release_result.get("status") or "").strip().lower()
    if status not in {"success", "completed"}:
        return ""
    merge_payload_raw = release_result.get("merge")
    merge_payload = cast(dict[str, object], merge_payload_raw) if isinstance(merge_payload_raw, dict) else {}
    merged = bool(release_result.get("merged")) or bool(merge_payload.get("merged")) or bool(str(merge_payload.get("merged_sha") or ""))
    if not merged:
        return ""

    pr_number = str(release_result.get("pr_number") or "")
    comment = (
        f"Closing after PR #{pr_number} was merged by autodev release workflow."
        if pr_number
        else "Closing after merged release completed in autodev."
    )
    command = ["gh", "issue", "close", issue_number, "--repo", repo, "--comment", comment]
    completed = run(command, cwd=base_dir, check=False, capture_output=True, text=True)
    if completed.returncode == 0:
        if command_id:
            record_github_sync_attempt(
                base_dir,
                command_id=command_id,
                issue_number=issue_number,
                add_labels=[],
                remove_labels=[],
                status="success",
                updated_at=timestamp,
            )
        return ""
    error = (completed.stderr or completed.stdout).strip() or f"gh issue close failed with exit code {completed.returncode}"
    if command_id:
        record_github_sync_attempt(
            base_dir,
            command_id=command_id,
            issue_number=issue_number,
            add_labels=[],
            remove_labels=[],
            status="failed",
            updated_at=timestamp,
            last_error=error,
        )
    return error


def claim_issue_execution(
    *,
    base_dir: Path,
    issue_number: str,
    branch: str,
    source_session_id: str,
    now: NowFunc,
    sync_progress_label: SyncProgressLabel,
    transition_state: TransitionIssueState,
    updated_at: str | None = None,
) -> None:
    timestamp = now(updated_at)
    ensure_control_plane_db(base_dir)
    existing_issue = read_issue(base_dir, issue_number)
    if existing_issue is not None and str(existing_issue.get("state") or "") in {
        "claimed",
        "dispatching",
        "running",
        "verifying",
        "quarantined",
    }:
        holder = str(existing_issue.get("current_session_id") or source_session_id or "unknown-session")
        created_at = str(
            existing_issue.get("claimed_at")
            or existing_issue.get("dispatching_at")
            or existing_issue.get("running_at")
            or existing_issue.get("updated_at")
            or "unknown-time"
        )
        raise RuntimeError(
            f"issue #{issue_number} is already in progress via {holder} since {created_at}; refusing duplicate start."
        )

    command_id = uuid4().hex
    ensure_issue_row(base_dir, issue_number=issue_number, updated_at=timestamp)
    _ = sync_issue_runtime_context(
        base_dir,
        issue_number=issue_number,
        updated_at=timestamp,
        artifact_refs={
            "issueNumber": issue_number,
            "branch": branch,
            "sourceSessionID": source_session_id,
            "createdAt": timestamp,
            "status": "claimed",
        },
    )
    transition_state(
        base_dir=base_dir,
        issue_number=issue_number,
        to_state="claimed",
        command_id=command_id,
        updated_at=timestamp,
        reason=f"Claim issue #{issue_number} for scheduler dispatch.",
        from_state="ready",
    )
    sync_error = sync_progress_label(
        base_dir=base_dir,
        issue_number=issue_number,
        add_labels=[AGENT_DISPATCHING_LABEL],
        remove_labels=[READY_FOR_AGENT_LABEL],
        command_id=command_id,
        updated_at=timestamp,
    )
    if sync_error:
        _ = upsert_issue_state(
            base_dir,
            issue_number=issue_number,
            state="ready",
            command_id=f"{command_id}:rollback",
            updated_at=timestamp,
        )
        clear_issue_execution_claim_projection(base_dir=base_dir, issue_number=issue_number, updated_at=timestamp)
        raise RuntimeError(f"failed to sync GitHub in-progress state for issue #{issue_number}: {sync_error}")


def release_issue_execution(
    *,
    base_dir: Path,
    issue_number: str,
    restore_ready_for_agent: bool,
    now: NowFunc,
    sync_progress_label: SyncProgressLabel,
    sync_local_main_after_release_merge: SyncLocalMainAfterReleaseMerge,
    close_github_issue_after_release_merge: CloseGitHubIssueAfterReleaseMerge,
    transition_state: TransitionIssueState,
    final_state: str | None = None,
    updated_at: str | None = None,
) -> None:
    timestamp = now(updated_at)
    ensure_control_plane_db(base_dir)

    remove_labels = [AGENT_DISPATCHING_LABEL, AGENT_IN_PROGRESS_LABEL, QUARANTINED_LABEL]
    if not restore_ready_for_agent:
        remove_labels = [READY_FOR_AGENT_LABEL, *remove_labels]
    add_labels = [READY_FOR_AGENT_LABEL] if restore_ready_for_agent else []
    command_id = uuid4().hex
    _ = sync_progress_label(
        base_dir=base_dir,
        issue_number=issue_number,
        add_labels=add_labels,
        remove_labels=remove_labels,
        command_id=command_id,
        updated_at=timestamp,
    )
    issue_state = read_issue(base_dir, issue_number)
    target_state = final_state or ("ready" if restore_ready_for_agent else "failed")
    if issue_state is None:
        ensure_issue_row(base_dir, issue_number=issue_number, updated_at=timestamp)
        issue_state = read_issue(base_dir, issue_number)
    if issue_state is None:
        raise ValueError(f"issue #{issue_number} is missing from control-plane state")

    current_state = str(issue_state.get("state") or "")

    if target_state == "completed":
        sync_error = sync_local_main_after_release_merge(
            base_dir=base_dir,
            issue_number=issue_number,
            updated_at=timestamp,
        )
        if sync_error:
            record_admin_decision(
                base_dir,
                command_id=f"{command_id}:admin-local-main-sync-failed",
                issue_number=issue_number,
                decision_type="admin_local_main_sync_failure",
                from_state=current_state,
                to_state=current_state,
                reason=sync_error,
                updated_at=timestamp,
            )
            raise RuntimeError(sync_error)

        close_error = close_github_issue_after_release_merge(
            base_dir=base_dir,
            issue_number=issue_number,
            command_id=f"{command_id}:github-close",
            updated_at=timestamp,
        )
        if close_error:
            record_admin_decision(
                base_dir,
                command_id=f"{command_id}:admin-github-issue-close-failed",
                issue_number=issue_number,
                decision_type="admin_github_issue_close_failure",
                from_state=current_state,
                to_state=current_state,
                reason=close_error,
                updated_at=timestamp,
            )
            raise RuntimeError(close_error)

    if target_state == "ready" and current_state in {"ready", "claimed", "dispatching"}:
        clear_issue_execution_claim_projection(base_dir=base_dir, issue_number=issue_number, updated_at=timestamp)
    elif target_state in {"verified", "failed", "completed"}:
        clear_issue_execution_claim_projection(base_dir=base_dir, issue_number=issue_number, updated_at=timestamp)

    if current_state == target_state:
        if target_state in {"failed", "completed"}:
            clear_issue_session_ids(base_dir=base_dir, issue_number=issue_number, updated_at=timestamp)
        return

    if target_state == "ready" and current_state in {"claimed", "dispatching"}:
        transition_state(
            base_dir=base_dir,
            issue_number=issue_number,
            to_state="ready",
            command_id=command_id,
            updated_at=timestamp,
            reason=f"Release issue #{issue_number} back to ready-for-agent.",
            from_state=current_state,
        )
        return

    if target_state == "verified" and current_state == "release_pending":
        transition_state(
            base_dir=base_dir,
            issue_number=issue_number,
            to_state="verified",
            command_id=command_id,
            updated_at=timestamp,
            reason=f"Return issue #{issue_number} to verified after non-terminal release block.",
            from_state="release_pending",
            current_session_id="",
        )
        return

    if target_state == "verified" and current_state in {"ready", "failed"}:
        _ = upsert_issue_state(
            base_dir,
            issue_number=issue_number,
            state="verified",
            command_id=command_id,
            updated_at=timestamp,
            current_session_id="",
        )
        record_admin_decision(
            base_dir,
            command_id=f"{command_id}:admin-verified",
            issue_number=issue_number,
            decision_type="admin_verified_recovery",
            from_state=current_state,
            to_state="verified",
            reason=f"Recover issue #{issue_number} into verified after a late successful verifier evidence packet arrived.",
            updated_at=timestamp,
        )
        return

    if target_state == "failed" and current_state in {"running", "verifying", "release_pending", "quarantined"}:
        transition_state(
            base_dir=base_dir,
            issue_number=issue_number,
            to_state="quarantined" if current_state == "running" else "failed",
            command_id=command_id,
            updated_at=timestamp,
            reason=(
                f"Quarantine issue #{issue_number} before terminal failure release."
                if current_state == "running"
                else f"Release issue #{issue_number} into failed terminal state."
            ),
            from_state=current_state,
        )
        if current_state == "running":
            transition_state(
                base_dir=base_dir,
                issue_number=issue_number,
                to_state="failed",
            command_id=f"{command_id}:failed",
            updated_at=timestamp,
            reason=f"Release issue #{issue_number} into failed terminal state.",
            from_state="quarantined",
            current_session_id="",
        )
        else:
            clear_issue_session_ids(base_dir=base_dir, issue_number=issue_number, updated_at=timestamp)
        return

    if target_state == "failed" and current_state in {"ready", "claimed", "dispatching"}:
        _ = upsert_issue_state(
            base_dir,
            issue_number=issue_number,
            state="failed",
            command_id=command_id,
            updated_at=timestamp,
            current_session_id="",
        )
        record_admin_decision(
            base_dir,
            command_id=f"{command_id}:admin-failed",
            issue_number=issue_number,
            decision_type="admin_terminal_failure",
            from_state=current_state,
            to_state="failed",
            reason=f"Release issue #{issue_number} into failed terminal state before a root session was confirmed.",
            updated_at=timestamp,
        )
        return

    if target_state == "completed" and current_state in {"verifying", "release_pending"}:
        transition_state(
            base_dir=base_dir,
            issue_number=issue_number,
            to_state="completed",
            command_id=command_id,
            updated_at=timestamp,
            reason=f"Release issue #{issue_number} into completed terminal state.",
            from_state=current_state,
            current_session_id="",
        )
        return

    if target_state == "completed" and current_state in {"failed", "ready"}:
        _ = upsert_issue_state(
            base_dir,
            issue_number=issue_number,
            state="completed",
            command_id=command_id,
            updated_at=timestamp,
            current_session_id="",
        )
        record_admin_decision(
            base_dir,
            command_id=f"{command_id}:admin-completed",
            issue_number=issue_number,
            decision_type="admin_terminal_completion_recovery",
            from_state=current_state,
            to_state="completed",
            reason=f"Recover issue #{issue_number} into completed after a late successful release result arrived.",
            updated_at=timestamp,
        )
        return

    raise ValueError(f"cannot release issue #{issue_number} from {current_state!r} to {target_state!r}")


def quarantine_issue_execution(
    *,
    base_dir: Path,
    issue_number: str,
    reason: str,
    now: NowFunc,
    sync_progress_label: SyncProgressLabel,
    transition_state: TransitionIssueState,
    updated_at: str | None = None,
) -> None:
    timestamp = now(updated_at)
    ensure_control_plane_db(base_dir)
    issue = read_issue(base_dir, issue_number) or {}
    from_state = str(issue.get("state") or "running")
    if from_state not in {"running", "dispatching", "verifying", "release_pending"}:
        raise ValueError(f"cannot quarantine issue #{issue_number} from {from_state!r}")
    transition_state(
        base_dir=base_dir,
        issue_number=issue_number,
        to_state="quarantined",
        command_id=uuid4().hex,
        updated_at=timestamp,
        reason=reason,
        from_state=from_state,
    )
    _ = sync_progress_label(
        base_dir=base_dir,
        issue_number=issue_number,
        add_labels=[QUARANTINED_LABEL],
        remove_labels=[READY_FOR_AGENT_LABEL, AGENT_IN_PROGRESS_LABEL, AGENT_DISPATCHING_LABEL],
        command_id=uuid4().hex,
        updated_at=timestamp,
    )


def resume_quarantined_issue_execution(
    *,
    base_dir: Path,
    issue_number: str,
    reason: str,
    now: NowFunc,
    sync_progress_label: SyncProgressLabel,
    transition_state: TransitionIssueState,
    updated_at: str | None = None,
) -> None:
    timestamp = now(updated_at)
    ensure_control_plane_db(base_dir)
    transition_state(
        base_dir=base_dir,
        issue_number=issue_number,
        to_state="running",
        command_id=uuid4().hex,
        updated_at=timestamp,
        reason=reason,
        from_state="quarantined",
    )
    _ = sync_progress_label(
        base_dir=base_dir,
        issue_number=issue_number,
        add_labels=[AGENT_IN_PROGRESS_LABEL],
        remove_labels=[READY_FOR_AGENT_LABEL, QUARANTINED_LABEL, AGENT_DISPATCHING_LABEL],
        command_id=uuid4().hex,
        updated_at=timestamp,
    )


def redispatch_quarantined_issue_execution(
    *,
    base_dir: Path,
    issue_number: str,
    branch: str,
    source_session_id: str,
    reason: str,
    now: NowFunc,
    sync_progress_label: SyncProgressLabel,
    transition_state: TransitionIssueState,
    updated_at: str | None = None,
) -> None:
    timestamp = now(updated_at)
    ensure_control_plane_db(base_dir)
    command_id = uuid4().hex
    transition_state(
        base_dir=base_dir,
        issue_number=issue_number,
        to_state="claimed",
        command_id=command_id,
        updated_at=timestamp,
        reason=reason,
        from_state="quarantined",
        current_session_id="",
    )
    claim_payload: JsonObject = {
        "issueNumber": issue_number,
        "branch": branch,
        "sourceSessionID": source_session_id,
        "createdAt": timestamp,
        "status": "claimed",
    }

    issue = read_issue(base_dir, issue_number) or {}
    existing_artifacts = cast(dict[str, object], json.loads(str(issue.get("artifact_refs_json") or "{}"))) if issue else {}
    for key in ["issueNumber", "branch", "sourceSessionID", "createdAt", "status", "rootSessionID", "recordedAt"]:
        existing_artifacts.pop(key, None)
    existing_artifacts.update(claim_payload)
    _ = sync_issue_runtime_context(
        base_dir,
        issue_number=issue_number,
        updated_at=timestamp,
        artifact_refs=existing_artifacts,
    )

    sync_error = sync_progress_label(
        base_dir=base_dir,
        issue_number=issue_number,
        add_labels=[AGENT_DISPATCHING_LABEL],
        remove_labels=[READY_FOR_AGENT_LABEL, QUARANTINED_LABEL, AGENT_IN_PROGRESS_LABEL],
        command_id=f"{command_id}:dispatching-labels",
        updated_at=timestamp,
    )
    if sync_error:
        clear_issue_execution_claim_projection(base_dir=base_dir, issue_number=issue_number, updated_at=timestamp)
        _ = upsert_issue_state(
            base_dir,
            issue_number=issue_number,
            state="quarantined",
            command_id=f"{command_id}:rollback",
            updated_at=timestamp,
            current_session_id="",
        )
        raise RuntimeError(f"failed to sync GitHub redispatch labels for issue #{issue_number}: {sync_error}")


def fail_quarantined_issue_execution(
    *,
    base_dir: Path,
    issue_number: str,
    reason: str,
    now: NowFunc,
    transition_state: TransitionIssueState,
    release_issue: ReleaseIssueExecution,
    updated_at: str | None = None,
) -> None:
    timestamp = now(updated_at)
    ensure_control_plane_db(base_dir)
    transition_state(
        base_dir=base_dir,
        issue_number=issue_number,
        to_state="failed",
        command_id=uuid4().hex,
        updated_at=timestamp,
        reason=reason,
        from_state="quarantined",
    )
    release_issue(
        base_dir=base_dir,
        issue_number=issue_number,
        restore_ready_for_agent=False,
        final_state="failed",
        updated_at=updated_at,
    )
