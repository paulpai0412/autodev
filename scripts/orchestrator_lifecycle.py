"""Issue lifecycle, lock, and GitHub label helpers for the autodev supervisor."""

from __future__ import annotations

import json
import hashlib
import os
import subprocess
from pathlib import Path
from typing import Callable, cast
from uuid import uuid4

import yaml

from scripts.control_plane_db import (
    append_issue_history,
    claim_issue_if_ready,
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
CleanupIssueWorktreeAfterReleaseMerge = Callable[..., str]


PROJECTION_BODY_START = "<!-- autodev:projection:start -->"
PROJECTION_BODY_END = "<!-- autodev:projection:end -->"
STATUS_COMMENT_MARKER = "<!-- autodev:status-comment -->"


def _upsert_projection_block(*, body: str, projection_markdown: str) -> tuple[str, bool]:
    block = f"{PROJECTION_BODY_START}\n{projection_markdown.strip()}\n{PROJECTION_BODY_END}"
    start = body.find(PROJECTION_BODY_START)
    end = body.find(PROJECTION_BODY_END)
    if start != -1 and end != -1 and end > start:
        end_index = end + len(PROJECTION_BODY_END)
        updated = f"{body[:start].rstrip()}\n\n{block}\n{body[end_index:].lstrip()}"
        return updated.rstrip() + "\n", updated.strip() != body.strip()
    trimmed = body.rstrip()
    if trimmed:
        updated = f"{trimmed}\n\n{block}\n"
    else:
        updated = f"{block}\n"
    return updated, True


def _issue_backing_type(base_dir: Path, issue_number: str) -> str:
    issue = read_issue(base_dir, issue_number) or {}
    issue_packet = cast(dict[str, object], json.loads(str(issue.get("issue_packet_json") or "{}"))) if issue else {}
    return str(issue_packet.get("backing_type") or "github")


def scheduler_id(base_dir: Path) -> str:
    return f"scheduler:{base_dir.resolve()}"


def _read_autodev_config(base_dir: Path) -> dict[str, object]:
    config_path = base_dir / ".autodev.yaml"
    if not config_path.exists():
        return {}
    try:
        parsed = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError):
        return {}
    return cast(dict[str, object], parsed) if isinstance(parsed, dict) else {}


def _string_map(raw: object) -> dict[str, str]:
    if not isinstance(raw, dict):
        return {}
    result: dict[str, str] = {}
    for key, value in raw.items():
        normalized_key = str(key or "").strip()
        normalized_value = str(value or "").strip()
        if normalized_key and normalized_value:
            result[normalized_key] = normalized_value
    return result


def _nested_string_map(raw: object) -> dict[str, dict[str, str]]:
    if not isinstance(raw, dict):
        return {}
    nested: dict[str, dict[str, str]] = {}
    for key, value in raw.items():
        normalized_key = str(key or "").strip()
        if not normalized_key:
            continue
        normalized_value = _string_map(value)
        if normalized_value:
            nested[normalized_key] = normalized_value
    return nested


def _resolve_project_sync_bindings(
    *,
    base_dir: Path,
    issue_number: str,
) -> tuple[str, dict[str, str], dict[str, dict[str, str]]]:
    runtime_context = read_runtime_context(base_dir, issue_number) or {}
    runtime_field_ids = _string_map(runtime_context.get("github_project_field_ids"))
    runtime_option_ids = _nested_string_map(runtime_context.get("github_project_field_option_ids"))

    config_payload = _read_autodev_config(base_dir)
    config_field_ids = _string_map(config_payload.get("github_project_field_ids"))
    config_option_ids = _nested_string_map(config_payload.get("github_project_field_option_ids"))

    field_ids: dict[str, str] = {}
    for key in ("state", "stage", "pr_workflow"):
        value = str(runtime_field_ids.get(key) or config_field_ids.get(key) or "").strip()
        if value:
            field_ids[key] = value

    option_ids: dict[str, dict[str, str]] = {}
    for key in ("state", "pr_workflow"):
        merged = dict(config_option_ids.get(key, {}))
        merged.update(runtime_option_ids.get(key, {}))
        if merged:
            option_ids[key] = merged

    project_id = str(runtime_context.get("github_project_id") or "").strip()
    if not project_id:
        project_id = str(os.environ.get("AUTODEV_GITHUB_PROJECT_ID", "")).strip()
    if not project_id:
        project_id = str(config_payload.get("github_project_id") or "").strip()

    return project_id, field_ids, option_ids


def _resolve_project_single_select_option_ids(
    *,
    base_dir: Path,
    project_id: str,
    run: Callable[..., subprocess.CompletedProcess[str]],
) -> tuple[dict[str, dict[str, str]], str]:
    query = (
        "query($project:ID!){node(id:$project){... on ProjectV2{fields(first:100){nodes{... on ProjectV2SingleSelectField{id options{id name}}}}}}}"
    )
    completed = run(
        [
            "gh",
            "api",
            "graphql",
            "-f",
            f"query={query}",
            "-F",
            f"project={project_id}",
        ],
        cwd=base_dir,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        error = (completed.stderr or completed.stdout).strip() or (
            f"gh api graphql project field metadata lookup failed with exit code {completed.returncode}"
        )
        return {}, error

    try:
        payload = json.loads(completed.stdout or "{}")
    except json.JSONDecodeError:
        return {}, "failed to parse GitHub project field metadata lookup payload"

    nodes_payload = payload.get("data", {}).get("node", {}).get("fields", {}).get("nodes", []) if isinstance(payload, dict) else []
    nodes = nodes_payload if isinstance(nodes_payload, list) else []
    mapping: dict[str, dict[str, str]] = {}
    for node in nodes:
        if not isinstance(node, dict):
            continue
        field_id = str(node.get("id") or "").strip()
        if not field_id:
            continue
        options_payload = node.get("options")
        options = options_payload if isinstance(options_payload, list) else []
        option_map: dict[str, str] = {}
        for option in options:
            if not isinstance(option, dict):
                continue
            name = str(option.get("name") or "").strip()
            option_id = str(option.get("id") or "").strip()
            if name and option_id:
                option_map[name] = option_id
        if option_map:
            mapping[field_id] = option_map

    return mapping, ""


def _select_single_select_option_id(options: dict[str, str], value: str) -> str:
    normalized_value = str(value or "").strip()
    if not normalized_value:
        return ""
    direct = str(options.get(normalized_value) or "").strip()
    if direct:
        return direct

    normalized_options: dict[str, str] = {}
    for key, option_id in options.items():
        normalized_key = str(key or "").strip().lower()
        normalized_option_id = str(option_id or "").strip()
        if normalized_key and normalized_option_id:
            normalized_options[normalized_key] = normalized_option_id

    lower_value = normalized_value.lower()
    for candidate in {
        lower_value,
        lower_value.replace("_", " "),
        lower_value.replace(" ", "_"),
        lower_value.replace("-", " "),
        lower_value.replace(" ", "-"),
    }:
        candidate_option_id = str(normalized_options.get(candidate) or "").strip()
        if candidate_option_id:
            return candidate_option_id

    return ""


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


def clear_issue_runtime_phase_projection(*, base_dir: Path, issue_number: str, updated_at: str) -> None:
    apply_issue_runtime_phase_projection_policy(
        base_dir=base_dir,
        issue_number=issue_number,
        updated_at=updated_at,
        policy="clear",
    )


def apply_issue_runtime_phase_projection_policy(
    *,
    base_dir: Path,
    issue_number: str,
    updated_at: str,
    policy: str,
) -> None:
    if policy != "clear":
        raise ValueError(f"unknown runtime phase projection policy {policy!r}")

    _ = sync_issue_runtime_context(
        base_dir,
        issue_number=issue_number,
        updated_at=updated_at,
        current_role="",
        current_stage="",
        current_status="",
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
                projection_target="labels",
                projection_payload={"repo": repo, "issue_number": issue_number},
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
                projection_target="labels",
                projection_payload={"repo": repo, "issue_number": issue_number},
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
                projection_target="labels",
                projection_payload={"repo": repo, "issue_number": issue_number},
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
            projection_target="labels",
            projection_payload={"repo": repo, "issue_number": issue_number},
        )
    return error


def sync_issue_body_projection(
    *,
    base_dir: Path,
    issue_number: str,
    repo: str,
    projection_markdown: str,
    now: NowFunc,
    run: Callable[..., subprocess.CompletedProcess[str]],
    command_id: str | None = None,
    updated_at: str | None = None,
) -> str:
    timestamp = now(updated_at)
    backing_type = _issue_backing_type(base_dir, issue_number)
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
                last_error="skipped GitHub issue body projection for local-seeded issue",
                projection_target="issue_body",
                projection_payload={"repo": repo, "issue_number": issue_number},
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
                last_error="skipped GitHub issue body projection because project github_repo is unset",
                projection_target="issue_body",
                projection_payload={"repo": repo, "issue_number": issue_number},
            )
        return ""

    view_completed = run(
        ["gh", "issue", "view", issue_number, "--repo", repo, "--json", "body"],
        cwd=base_dir,
        check=False,
        capture_output=True,
        text=True,
    )
    if view_completed.returncode != 0:
        error = (view_completed.stderr or view_completed.stdout).strip() or (
            f"gh issue view failed with exit code {view_completed.returncode}"
        )
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
                projection_target="issue_body",
                projection_payload={"repo": repo, "issue_number": issue_number},
            )
        return error

    parsed = cast(dict[str, object], json.loads(view_completed.stdout or "{}"))
    current_body = str(parsed.get("body") or "")
    updated_body, changed = _upsert_projection_block(body=current_body, projection_markdown=projection_markdown)
    if not changed:
        if command_id:
            record_github_sync_attempt(
                base_dir,
                command_id=command_id,
                issue_number=issue_number,
                add_labels=[],
                remove_labels=[],
                status="skipped",
                updated_at=timestamp,
                last_error="skipped GitHub issue body projection because rendered content is unchanged",
                projection_target="issue_body",
                projection_payload={
                    "repo": repo,
                    "issue_number": issue_number,
                    "content_hash": hashlib.sha256(updated_body.encode("utf-8")).hexdigest(),
                },
            )
        return ""

    edit_completed = run(
        ["gh", "issue", "edit", issue_number, "--repo", repo, "--body", updated_body],
        cwd=base_dir,
        check=False,
        capture_output=True,
        text=True,
    )
    if edit_completed.returncode == 0:
        if command_id:
            record_github_sync_attempt(
                base_dir,
                command_id=command_id,
                issue_number=issue_number,
                add_labels=[],
                remove_labels=[],
                status="success",
                updated_at=timestamp,
                projection_target="issue_body",
                projection_payload={
                    "repo": repo,
                    "issue_number": issue_number,
                    "content_hash": hashlib.sha256(updated_body.encode("utf-8")).hexdigest(),
                },
            )
        return ""

    error = (edit_completed.stderr or edit_completed.stdout).strip() or (
        f"gh issue edit --body failed with exit code {edit_completed.returncode}"
    )
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
            projection_target="issue_body",
            projection_payload={"repo": repo, "issue_number": issue_number},
        )
    return error


def sync_issue_status_comment(
    *,
    base_dir: Path,
    issue_number: str,
    repo: str,
    comment_markdown: str,
    now: NowFunc,
    run: Callable[..., subprocess.CompletedProcess[str]],
    command_id: str | None = None,
    updated_at: str | None = None,
) -> str:
    timestamp = now(updated_at)
    body = f"{STATUS_COMMENT_MARKER}\n{comment_markdown.strip()}"
    backing_type = _issue_backing_type(base_dir, issue_number)
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
                last_error="skipped GitHub status comment sync for local-seeded issue",
                projection_target="status_comment",
                projection_payload={"repo": repo, "issue_number": issue_number},
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
                last_error="skipped GitHub status comment sync because project github_repo is unset",
                projection_target="status_comment",
                projection_payload={"repo": repo, "issue_number": issue_number},
            )
        return ""

    comments_completed = run(
        ["gh", "api", f"repos/{repo}/issues/{issue_number}/comments"],
        cwd=base_dir,
        check=False,
        capture_output=True,
        text=True,
    )
    if comments_completed.returncode != 0:
        error = (comments_completed.stderr or comments_completed.stdout).strip() or (
            f"gh api issue comments failed with exit code {comments_completed.returncode}"
        )
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
                projection_target="status_comment",
                projection_payload={"repo": repo, "issue_number": issue_number},
            )
        return error

    comments = json.loads(comments_completed.stdout or "[]")
    comment_id = ""
    current_body = ""
    if isinstance(comments, list):
        for item in reversed(comments):
            if not isinstance(item, dict):
                continue
            candidate = str(item.get("body") or "")
            if STATUS_COMMENT_MARKER in candidate:
                comment_id = str(item.get("id") or "")
                current_body = candidate
                break

    if current_body.strip() == body.strip():
        if command_id:
            record_github_sync_attempt(
                base_dir,
                command_id=command_id,
                issue_number=issue_number,
                add_labels=[],
                remove_labels=[],
                status="skipped",
                updated_at=timestamp,
                last_error="skipped GitHub status comment sync because rendered content is unchanged",
                projection_target="status_comment",
                projection_payload={
                    "repo": repo,
                    "issue_number": issue_number,
                    "content_hash": hashlib.sha256(body.encode("utf-8")).hexdigest(),
                },
            )
        return ""

    if comment_id:
        command = [
            "gh",
            "api",
            f"repos/{repo}/issues/comments/{comment_id}",
            "--method",
            "PATCH",
            "-f",
            f"body={body}",
        ]
    else:
        command = [
            "gh",
            "api",
            f"repos/{repo}/issues/{issue_number}/comments",
            "--method",
            "POST",
            "-f",
            f"body={body}",
        ]
    sync_completed = run(
        command,
        cwd=base_dir,
        check=False,
        capture_output=True,
        text=True,
    )
    if sync_completed.returncode == 0:
        if command_id:
            record_github_sync_attempt(
                base_dir,
                command_id=command_id,
                issue_number=issue_number,
                add_labels=[],
                remove_labels=[],
                status="success",
                updated_at=timestamp,
                projection_target="status_comment",
                projection_payload={
                    "repo": repo,
                    "issue_number": issue_number,
                    "comment_id": comment_id,
                    "content_hash": hashlib.sha256(body.encode("utf-8")).hexdigest(),
                },
            )
        return ""

    error = (sync_completed.stderr or sync_completed.stdout).strip() or (
        f"gh api status comment sync failed with exit code {sync_completed.returncode}"
    )
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
            projection_target="status_comment",
            projection_payload={"repo": repo, "issue_number": issue_number, "comment_id": comment_id},
        )
    return error


def sync_project_fields_projection(
    *,
    base_dir: Path,
    issue_number: str,
    repo: str,
    fields: dict[str, str],
    now: NowFunc,
    run: Callable[..., subprocess.CompletedProcess[str]],
    command_id: str | None = None,
    updated_at: str | None = None,
) -> str:
    timestamp = now(updated_at)
    project_id, configured_field_ids, configured_option_ids = _resolve_project_sync_bindings(
        base_dir=base_dir,
        issue_number=issue_number,
    )

    if not repo or not project_id:
        if command_id:
            record_github_sync_attempt(
                base_dir,
                command_id=command_id,
                issue_number=issue_number,
                add_labels=[],
                remove_labels=[],
                status="skipped",
                updated_at=timestamp,
                last_error="skipped GitHub project field sync because repo/project is not configured",
                projection_target="project_fields",
                projection_payload={"repo": repo, "issue_number": issue_number, "project_id": project_id},
            )
        return ""

    owner, _, name = repo.partition("/")
    if not owner or not name:
        error = f"invalid github repo format for project field sync: {repo!r}"
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
                projection_target="project_fields",
                projection_payload={"repo": repo, "issue_number": issue_number, "project_id": project_id},
            )
        return error

    query = (
        "query($owner:String!,$repo:String!,$number:Int!){repository(owner:$owner,name:$repo){issue(number:$number){id projectItems(first:100){nodes{id project{id}}}}}}"
    )
    lookup = run(
        [
            "gh",
            "api",
            "graphql",
            "-f",
            f"query={query}",
            "-F",
            f"owner={owner}",
            "-F",
            f"repo={name}",
            "-F",
            f"number={issue_number}",
        ],
        cwd=base_dir,
        check=False,
        capture_output=True,
        text=True,
    )
    if lookup.returncode != 0:
        error = (lookup.stderr or lookup.stdout).strip() or f"gh api graphql lookup failed with exit code {lookup.returncode}"
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
                projection_target="project_fields",
                projection_payload={"repo": repo, "issue_number": issue_number, "project_id": project_id},
            )
        return error

    item_id = ""
    payload = json.loads(lookup.stdout or "{}")
    issue_payload = payload.get("data", {}).get("repository", {}).get("issue", {}) if isinstance(payload, dict) else {}
    issue_node_id = str(issue_payload.get("id") or "") if isinstance(issue_payload, dict) else ""
    nodes = issue_payload.get("projectItems", {}).get("nodes", []) if isinstance(issue_payload, dict) else []
    if isinstance(nodes, list):
        for node in nodes:
            if not isinstance(node, dict):
                continue
            node_project_id = str((node.get("project") or {}).get("id") or "") if isinstance(node.get("project"), dict) else ""
            if node_project_id == project_id:
                item_id = str(node.get("id") or "")
                break

    if not item_id:
        if not issue_node_id:
            if command_id:
                record_github_sync_attempt(
                    base_dir,
                    command_id=command_id,
                    issue_number=issue_number,
                    add_labels=[],
                    remove_labels=[],
                    status="skipped",
                    updated_at=timestamp,
                    last_error="skipped GitHub project field sync because issue node id is unavailable",
                    projection_target="project_fields",
                    projection_payload={"repo": repo, "issue_number": issue_number, "project_id": project_id},
                )
            return ""

        add_item_mutation = (
            "mutation($project:ID!,$content:ID!){addProjectV2ItemById(input:{projectId:$project,contentId:$content}){item{id}}}"
        )
        add_item = run(
            [
                "gh",
                "api",
                "graphql",
                "-f",
                f"query={add_item_mutation}",
                "-F",
                f"project={project_id}",
                "-F",
                f"content={issue_node_id}",
            ],
            cwd=base_dir,
            check=False,
            capture_output=True,
            text=True,
        )
        if add_item.returncode != 0:
            error = (add_item.stderr or add_item.stdout).strip() or (
                f"gh api graphql add project item failed with exit code {add_item.returncode}"
            )
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
                    projection_target="project_fields",
                    projection_payload={"repo": repo, "issue_number": issue_number, "project_id": project_id},
                )
            return error

        add_payload = json.loads(add_item.stdout or "{}")
        item_id = (
            str(add_payload.get("data", {}).get("addProjectV2ItemById", {}).get("item", {}).get("id") or "")
            if isinstance(add_payload, dict)
            else ""
        )
        if not item_id:
            error = "failed to parse GitHub project item id after adding issue to project"
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
                    projection_target="project_fields",
                    projection_payload={"repo": repo, "issue_number": issue_number, "project_id": project_id},
                )
            return error

    state_field_id = str(configured_field_ids.get("state") or "").strip()
    pr_workflow_field_id = str(configured_field_ids.get("pr_workflow") or "").strip()
    state_options = _string_map(configured_option_ids.get("state"))
    pr_workflow_options = _string_map(configured_option_ids.get("pr_workflow"))
    dynamic_single_select_options: dict[str, dict[str, str]] = {}
    dynamic_option_lookup_error = ""

    for field_id, field_value in fields.items():
        field_id = str(field_id or "").strip()
        field_value = str(field_value or "").strip()
        if not field_id:
            continue

        is_single_select_field = field_id in {state_field_id, pr_workflow_field_id}
        option_id = ""
        if field_value:
            if field_id == state_field_id:
                option_id = _select_single_select_option_id(state_options, field_value)
            elif field_id == pr_workflow_field_id:
                option_id = _select_single_select_option_id(pr_workflow_options, field_value)

            if is_single_select_field and not option_id:
                if not dynamic_single_select_options and not dynamic_option_lookup_error:
                    dynamic_single_select_options, dynamic_option_lookup_error = _resolve_project_single_select_option_ids(
                        base_dir=base_dir,
                        project_id=project_id,
                        run=run,
                    )
                option_id = _select_single_select_option_id(dynamic_single_select_options.get(field_id, {}), field_value)

        if is_single_select_field and not field_value:
            continue

        if is_single_select_field and field_value and not option_id:
            error = dynamic_option_lookup_error or (
                f"missing GitHub project single-select option id for field {field_id} value {field_value!r}"
            )
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
                    projection_target="project_fields",
                    projection_payload={
                        "repo": repo,
                        "issue_number": issue_number,
                        "project_id": project_id,
                        "item_id": item_id,
                        "field_id": field_id,
                        "field_value": field_value,
                    },
                )
            return error

        if option_id:
            mutation = (
                "mutation($project:ID!,$item:ID!,$field:ID!,$option: String!){updateProjectV2ItemFieldValue(input:{projectId:$project,itemId:$item,fieldId:$field,value:{singleSelectOptionId:$option}}){projectV2Item{id}}}"
            )
            update = run(
                [
                    "gh",
                    "api",
                    "graphql",
                    "-f",
                    f"query={mutation}",
                    "-F",
                    f"project={project_id}",
                    "-F",
                    f"item={item_id}",
                    "-F",
                    f"field={field_id}",
                    "-F",
                    f"option={option_id}",
                ],
                cwd=base_dir,
                check=False,
                capture_output=True,
                text=True,
            )
        else:
            mutation = (
                "mutation($project:ID!,$item:ID!,$field:ID!,$value:String!){updateProjectV2ItemFieldValue(input:{projectId:$project,itemId:$item,fieldId:$field,value:{text:$value}}){projectV2Item{id}}}"
            )
            update = run(
                [
                    "gh",
                    "api",
                    "graphql",
                    "-f",
                    f"query={mutation}",
                    "-F",
                    f"project={project_id}",
                    "-F",
                    f"item={item_id}",
                    "-F",
                    f"field={field_id}",
                    "-F",
                    f"value={field_value}",
                ],
                cwd=base_dir,
                check=False,
                capture_output=True,
                text=True,
            )
        if update.returncode != 0:
            error = (update.stderr or update.stdout).strip() or (
                f"gh api graphql project field update failed with exit code {update.returncode}"
            )
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
                    projection_target="project_fields",
                    projection_payload={"repo": repo, "issue_number": issue_number, "project_id": project_id, "item_id": item_id},
                )
            return error

    if command_id:
        record_github_sync_attempt(
            base_dir,
            command_id=command_id,
            issue_number=issue_number,
            add_labels=[],
            remove_labels=[],
            status="success",
            updated_at=timestamp,
            projection_target="project_fields",
            projection_payload={
                "repo": repo,
                "issue_number": issue_number,
                "project_id": project_id,
                "item_id": item_id,
                "fields_hash": hashlib.sha256(json.dumps(fields, sort_keys=True).encode("utf-8")).hexdigest(),
            },
        )
    return ""


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
        workspace_dir = base_dir

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
                projection_target="issue_close",
                projection_payload={"repo": repo, "issue_number": issue_number},
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
                projection_target="issue_close",
                projection_payload={"repo": repo, "issue_number": issue_number},
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
                projection_target="issue_close",
                projection_payload={"repo": repo, "issue_number": issue_number, "pr_number": pr_number},
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
            projection_target="issue_close",
            projection_payload={"repo": repo, "issue_number": issue_number, "pr_number": pr_number},
        )
    return error


def cleanup_issue_worktree_after_release_merge(
    *,
    base_dir: Path,
    issue_number: str,
    updated_at: str | None = None,
) -> str:
    timestamp = updated_at or ""
    issue = read_issue(base_dir, issue_number) or {}
    release_result = read_artifact_fact(base_dir, issue_number, "release_result")
    if not bool(release_result.get("parse_ok")):
        return ""

    status = str(release_result.get("status") or "").strip().lower()
    if status not in {"success", "completed"}:
        return ""

    merge_payload_raw = release_result.get("merge")
    merge_payload = cast(dict[str, object], merge_payload_raw) if isinstance(merge_payload_raw, dict) else {}
    merged = bool(release_result.get("merged")) or bool(merge_payload.get("merged")) or bool(
        str(merge_payload.get("merged_sha") or "")
    )
    if not merged:
        return ""

    issue_worktree_path = str((read_runtime_context(base_dir, issue_number) or {}).get("issue_worktree_path") or "").strip()
    if not issue_worktree_path:
        return ""

    worktree_path = Path(issue_worktree_path)
    if worktree_path == base_dir:
        return ""

    if not worktree_path.exists():
        _ = sync_issue_runtime_context(
            base_dir,
            issue_number=issue_number,
            updated_at=timestamp,
            runtime_context={"issue_worktree_path": ""},
            worktree_path="",
        )
        return ""

    completed = subprocess.run(
        ["git", "worktree", "remove", str(worktree_path), "--force"],
        cwd=base_dir,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        return (completed.stderr or completed.stdout).strip() or (
            f"failed issue worktree cleanup after release merge for issue #{issue_number}"
        )

    _ = sync_issue_runtime_context(
        base_dir,
        issue_number=issue_number,
        updated_at=timestamp,
        runtime_context={"issue_worktree_path": ""},
        worktree_path="",
    )
    return ""


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
    command_id = uuid4().hex
    ensure_issue_row(base_dir, issue_number=issue_number, updated_at=timestamp)
    try:
        _ = claim_issue_if_ready(
            base_dir,
            issue_number=issue_number,
            command_id=command_id,
            scheduler_id=scheduler_id(base_dir),
            reason=f"Claim issue #{issue_number} for scheduler dispatch.",
            updated_at=timestamp,
        )
    except ValueError as error:
        raise RuntimeError(str(error)) from error
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
    sync_error = sync_progress_label(
        base_dir=base_dir,
        issue_number=issue_number,
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
    cleanup_issue_worktree_after_release_merge: CleanupIssueWorktreeAfterReleaseMerge,
    transition_state: TransitionIssueState,
    rollback_reason: str = "",
    final_state: str | None = None,
    updated_at: str | None = None,
) -> None:
    timestamp = now(updated_at)
    ensure_control_plane_db(base_dir)

    command_id = uuid4().hex
    _ = sync_progress_label(
        base_dir=base_dir,
        issue_number=issue_number,
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

        cleanup_error = cleanup_issue_worktree_after_release_merge(
            base_dir=base_dir,
            issue_number=issue_number,
            updated_at=timestamp,
        )
        if cleanup_error:
            record_admin_decision(
                base_dir,
                command_id=f"{command_id}:admin-issue-worktree-cleanup-failed",
                issue_number=issue_number,
                decision_type="admin_issue_worktree_cleanup_failure",
                from_state=current_state,
                to_state=current_state,
                reason=cleanup_error,
                updated_at=timestamp,
            )
            raise RuntimeError(cleanup_error)

    if target_state == "ready" and current_state in {"ready", "claimed", "dispatching"}:
        clear_issue_execution_claim_projection(base_dir=base_dir, issue_number=issue_number, updated_at=timestamp)
    elif target_state in {"verified", "failed", "completed"}:
        clear_issue_execution_claim_projection(base_dir=base_dir, issue_number=issue_number, updated_at=timestamp)

    if current_state == target_state:
        if target_state in {"failed", "completed"}:
            clear_issue_session_ids(base_dir=base_dir, issue_number=issue_number, updated_at=timestamp)
            clear_issue_runtime_phase_projection(base_dir=base_dir, issue_number=issue_number, updated_at=timestamp)
        return

    if target_state == "ready" and current_state in {"claimed", "dispatching"}:
        payload = {
            "rollback_reason": rollback_reason or "unspecified",
            "restored_ready_for_agent": True,
            "from_state": current_state,
        }
        _ = append_issue_history(
            base_dir,
            issue_number=issue_number,
            entry_type="admin_action",
            created_at=timestamp,
            status="ready_rollback",
            command_id=f"{command_id}:rollback-reason",
            summary=f"Rollback issue #{issue_number} to ready-for-agent: {payload['rollback_reason']}",
            payload=payload,
            unique_key=f"ready-rollback:{issue_number}:{command_id}",
        )
        transition_state(
            base_dir=base_dir,
            issue_number=issue_number,
            to_state="ready",
            command_id=command_id,
            updated_at=timestamp,
            reason=f"Release issue #{issue_number} back to ready-for-agent.",
            from_state=current_state,
        )
        clear_issue_runtime_phase_projection(base_dir=base_dir, issue_number=issue_number, updated_at=timestamp)
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
        clear_issue_runtime_phase_projection(base_dir=base_dir, issue_number=issue_number, updated_at=timestamp)
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
        clear_issue_runtime_phase_projection(base_dir=base_dir, issue_number=issue_number, updated_at=timestamp)
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
        clear_issue_runtime_phase_projection(base_dir=base_dir, issue_number=issue_number, updated_at=timestamp)
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
        clear_issue_runtime_phase_projection(base_dir=base_dir, issue_number=issue_number, updated_at=timestamp)
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
        clear_issue_runtime_phase_projection(base_dir=base_dir, issue_number=issue_number, updated_at=timestamp)
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
        clear_issue_runtime_phase_projection(base_dir=base_dir, issue_number=issue_number, updated_at=timestamp)
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
