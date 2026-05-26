#!/usr/bin/env python3
"""Runtime supervisor for nonstop autonomous issue dispatch."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from types import ModuleType
from uuid import uuid4
from typing import Callable, NotRequired, Protocol, TypedDict, cast
from urllib.parse import unquote, urlparse

import yaml

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.control_plane_db import (
    available_development_slots,
    available_release_slots,
    append_issue_event,
    append_issue_history,
    canonical_control_plane_base_dir,
    control_plane_db_path,
    completed_issue_numbers,
    describe_control_plane_schema,
    ensure_control_plane_db,
    ensure_issue_row,
    ingest_issue_packet,
    issue_rows_with_packets,
    list_issues,
    issues_in_states,
    ready_issues_for_selection,
    read_latest_history_entry,
    read_issue_packet,
    record_admin_decision,
    read_latest_decision,
    read_latest_github_sync_attempt,
    read_github_sync_attempt_by_command_id,
    read_artifact_fact,
    read_issue,
    read_latest_ref,
    read_runtime_context,
    read_release_child_session,
    record_artifact_fact,
    record_github_sync_attempt,
    record_latest_ref_snapshot,
    record_pr_opened,
    sync_issue_runtime_context,
    transition_issue_state,
    upsert_issue_ranking,
    upsert_issue_state,
    claim_issue_if_ready,
)
from scripts.host_adapter import HostAdapter, SessionStartContext, session_result_field
from scripts.orchestrator_artifacts import (
    _is_successful_release_status,
    issue_packet_record_from_json,
    issue_packet_record_to_json,
    parse_issue_packet_text,
)
from scripts.state_projection import (
    issue_projection,
    label_delta_for_projection,
    load_state_projection_config,
)


JsonObject = dict[str, object]

ARTIFACT_REF_KEYS: dict[str, str] = {
    "worker_result": "worker_result_ref",
    "evidence_packet": "evidence_packet_ref",
    "release_result": "release_result_ref",
}

WORKTREE_LOCAL_EXCLUDE_PATTERNS: tuple[str, ...] = (
    ".opencode/",
    ".playwright-mcp/",
    "artifacts/",
)


def _artifact_ref_value(artifacts: dict[str, object], artifact_kind: str) -> str:
    key = ARTIFACT_REF_KEYS.get(artifact_kind)
    if key is None:
        return ""
    return str(artifacts.get(key) or "")


def _ensure_control_plane_db_with_diagnostic(
    *,
    base_dir: Path,
    command_label: str,
    allow_create: bool = True,
) -> None:
    db_path = control_plane_db_path(base_dir)
    existed_before = db_path.exists()
    if not existed_before:
        print(
            f"[autodev:{command_label}] control-plane-db-missing-before-command={db_path}",
            file=sys.stderr,
        )
        if not allow_create:
            raise RuntimeError(
                f"control-plane DB missing at {db_path}; refusing to recreate during active {command_label} workflow"
            )
    ensure_control_plane_db(base_dir)
    if not existed_before and db_path.exists():
        print(
            f"[autodev:{command_label}] control-plane-db-created={db_path}",
            file=sys.stderr,
        )


class IssuePacketRecord(Protocol):
    issue_number: str
    title: str
    branch: str
    base_branch: str
    backing_type: str
    prior_handoff: str
    labels: list[str]
    parent_reference: str
    dependencies: list[str]
    raw_text: str


class IssueSelectionCandidate(Protocol):
    issue_number: str
    branch: str


def _normalize_requested_issue_number(issue_number: str) -> str:
    normalized = issue_number.strip().removeprefix("#").removeprefix("issue-")
    if not normalized.isdigit():
        raise RuntimeError(f"issue number must be numeric, got {issue_number!r}")
    return normalized


def _validate_start_packet_issue_number(*, requested_issue_number: str, packet: IssuePacketRecord) -> str:
    packet_issue_number = str(packet.issue_number).strip()
    if packet_issue_number != requested_issue_number or not packet_issue_number.isdigit():
        raise RuntimeError(
            f"issue packet number mismatch for requested issue #{requested_issue_number}: got {packet_issue_number!r}"
        )
    return packet_issue_number


def _run_git_command(base_dir: Path, command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=base_dir, check=False, capture_output=True, text=True)


def _is_git_repo(base_dir: Path) -> bool:
    if not (base_dir / ".git").exists():
        return False
    completed = _run_git_command(base_dir, ["git", "rev-parse", "--is-inside-work-tree"])
    return completed.returncode == 0


def _issue_worktree_path(base_dir: Path, issue_number: str) -> Path:
    base_dir = canonical_control_plane_base_dir(base_dir)
    return base_dir / ".opencode" / "runtime" / "issue-worktrees" / f"issue-{issue_number}"


def _canonical_supervisor_base_dir(base_dir: Path) -> Path:
    return canonical_control_plane_base_dir(base_dir)


def _list_branch_worktrees(base_dir: Path) -> dict[str, Path]:
    completed = _run_git_command(base_dir, ["git", "worktree", "list", "--porcelain"])
    if completed.returncode != 0:
        return {}
    branch_to_path: dict[str, Path] = {}
    current_path = ""
    for line in completed.stdout.splitlines():
        if line.startswith("worktree "):
            current_path = line[len("worktree ") :].strip()
            continue
        if line.startswith("branch refs/heads/") and current_path:
            branch = line[len("branch refs/heads/") :].strip()
            if branch:
                branch_to_path[branch] = Path(current_path)
    return branch_to_path


def _branch_exists_locally(base_dir: Path, branch: str) -> bool:
    completed = _run_git_command(base_dir, ["git", "show-ref", "--verify", "--quiet", f"refs/heads/{branch}"])
    return completed.returncode == 0


def _git_path(base_dir: Path, pathspec: str) -> Path | None:
    completed = _run_git_command(base_dir, ["git", "rev-parse", "--git-path", pathspec])
    if completed.returncode != 0:
        return None
    raw_path = (completed.stdout or "").strip()
    if not raw_path:
        return None
    path = Path(raw_path)
    if not path.is_absolute():
        path = (base_dir / path).resolve()
    return path


def _ensure_git_info_exclude(base_dir: Path, patterns: tuple[str, ...]) -> None:
    exclude_path = _git_path(base_dir, "info/exclude")
    if exclude_path is None:
        raise RuntimeError(f"failed to resolve git info/exclude path for {base_dir}")
    exclude_path.parent.mkdir(parents=True, exist_ok=True)
    existing = exclude_path.read_text(encoding="utf-8") if exclude_path.exists() else ""
    existing_lines = existing.splitlines()
    missing = [pattern for pattern in patterns if pattern not in existing_lines]
    if not missing:
        return
    separator = "" if not existing or existing.endswith("\n") else "\n"
    block = "".join(f"{pattern}\n" for pattern in missing)
    exclude_path.write_text(f"{existing}{separator}{block}", encoding="utf-8")


def _ensure_worktree_local_exclude(worktree_path: Path) -> None:
    _ensure_git_info_exclude(worktree_path, WORKTREE_LOCAL_EXCLUDE_PATTERNS)


def _ensure_issue_worktree(
    *,
    base_dir: Path,
    issue_number: str,
    branch: str,
    base_branch: str,
    updated_at: str,
) -> Path:
    del updated_at
    normalized_branch = branch.strip()
    normalized_base_branch = (base_branch or "").strip() or "main"
    if not normalized_branch:
        raise RuntimeError(f"issue #{issue_number} target branch is empty; refusing to prepare issue worktree")
    if normalized_branch == normalized_base_branch:
        raise RuntimeError(
            f"issue #{issue_number} target branch {normalized_branch!r} must differ from base branch {normalized_base_branch!r} "
            "to avoid worktree checkout conflicts"
        )

    if not _is_git_repo(base_dir):
        return base_dir

    branch_worktrees = _list_branch_worktrees(base_dir)
    existing = branch_worktrees.get(normalized_branch)
    if existing is not None:
        _ensure_worktree_local_exclude(existing)
        return existing

    worktree_path = _issue_worktree_path(base_dir, issue_number)
    worktree_path.parent.mkdir(parents=True, exist_ok=True)
    if worktree_path.exists() and any(worktree_path.iterdir()):
        raise RuntimeError(
            f"issue #{issue_number} worktree path {worktree_path} already exists but is not registered; clean it before retry"
        )

    if _branch_exists_locally(base_dir, normalized_branch):
        add_command = ["git", "worktree", "add", str(worktree_path), normalized_branch]
    else:
        add_command = ["git", "worktree", "add", "-b", normalized_branch, str(worktree_path), normalized_base_branch]
    completed = _run_git_command(base_dir, add_command)
    if completed.returncode != 0:
        error = (completed.stderr or completed.stdout).strip() or "git worktree add failed"
        raise RuntimeError(f"failed to prepare worktree for issue #{issue_number}: {error}")
    _ensure_worktree_local_exclude(worktree_path)
    return worktree_path


def _issue_dispatch_workdir(*, base_dir: Path, issue_number: str, branch: str, base_branch: str, updated_at: str) -> Path:
    del branch
    del base_branch
    del updated_at
    issue = read_issue(base_dir, issue_number) or {}
    worktree_path = str(issue.get("worktree_path") or "").strip()
    if worktree_path:
        path = Path(worktree_path)
        if path.exists():
            return path
    runtime_context = read_runtime_context(base_dir, issue_number)
    runtime_worktree = str(runtime_context.get("issue_worktree_path") or "").strip()
    if runtime_worktree:
        path = Path(runtime_worktree)
        if path.exists():
            return path
    return base_dir


def _artifact_status_snapshot(
    *,
    artifact_kind: str,
    artifact_path: Path,
    observed_at: str,
    parsed: JsonObject,
) -> JsonObject:
    snapshot: JsonObject = {
        "path": str(artifact_path),
        "observed_at": observed_at,
        "parse_ok": True,
    }
    if artifact_kind == "worker_result":
        snapshot.update(
            {
                "status": str(parsed.get("status") or ""),
                "pr_number": str(parsed.get("pr_number") or ""),
                "pr_url": str(parsed.get("pr_url") or ""),
                "completed_at": str(parsed.get("completed_at") or ""),
            }
        )
    elif artifact_kind == "evidence_packet":
        snapshot.update(
            {
                "status": str(parsed.get("status") or ""),
                "pr_number": str(parsed.get("pr_number") or ""),
                "pr_url": str(parsed.get("pr_url") or ""),
                "verifier_session_id": str(parsed.get("verifier_session_id") or ""),
            }
        )
    elif artifact_kind == "release_result":
        merge_gate_raw = parsed.get("merge_gate")
        merge_gate = cast(dict[str, object], merge_gate_raw) if isinstance(merge_gate_raw, dict) else {}
        workspace_hygiene_raw = parsed.get("workspace_hygiene")
        workspace_hygiene = (
            cast(dict[str, object], workspace_hygiene_raw) if isinstance(workspace_hygiene_raw, dict) else {}
        )
        snapshot.update(
            {
                "status": str(parsed.get("status") or ""),
                "blocked_reason": str(parsed.get("blocked_reason") or ""),
                "merge_gate": merge_gate,
                "workspace_hygiene": workspace_hygiene,
            }
        )
    return snapshot


def _read_artifact_status(issue: dict[str, object] | None) -> dict[str, object]:
    if not issue:
        return {}
    raw = str(issue.get("artifact_status_json") or "{}")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _artifact_fact(issue: dict[str, object] | None, artifact_kind: str) -> dict[str, object]:
    artifact_status = _read_artifact_status(issue)
    payload = artifact_status.get(artifact_kind, {})
    return payload if isinstance(payload, dict) else {}


def _read_db_artifact_fact(*, base_dir: Path, issue_number: str, artifact_kind: str) -> dict[str, object]:
    return read_artifact_fact(base_dir, issue_number, artifact_kind)


def _record_artifact_status(
    *,
    base_dir: Path,
    issue_number: str,
    artifact_kind: str,
    artifact_path: Path,
    observed_at: str,
    parsed: JsonObject,
) -> None:
    issue = read_issue(base_dir, issue_number) or {}
    artifact_status = _read_artifact_status(issue)
    artifact_status[artifact_kind] = _artifact_status_snapshot(
        artifact_kind=artifact_kind,
        artifact_path=artifact_path,
        observed_at=observed_at,
        parsed=parsed,
    )
    _ = sync_issue_runtime_context(
        base_dir,
        issue_number=issue_number,
        updated_at=observed_at,
        artifact_status=artifact_status,
    )


def _record_db_artifact_fact(
    *,
    base_dir: Path,
    issue_number: str,
    artifact_kind: str,
    parsed: JsonObject,
    observed_at: str,
    body_text: str = "",
) -> dict[str, object]:
    entry_type = artifact_kind
    return record_artifact_fact(
        base_dir,
        issue_number=issue_number,
        entry_type=entry_type,
        created_at=observed_at,
        payload=cast(dict[str, object], dict(parsed)),
        summary=f"Record {artifact_kind} for issue #{issue_number} in SQLite control plane.",
        session_id=str(parsed.get("verifier_session_id") or parsed.get("worker_session_id") or ""),
        command_id=str(parsed.get("command_id") or ""),
        body_text=body_text,
    )


def _artifact_fact_ref(artifact_kind: str, persisted: dict[str, object]) -> str:
    history_id = str(persisted.get("history_id") or "").strip()
    content_hash = str(persisted.get("content_hash") or "").strip()
    if history_id and content_hash:
        return f"db:{artifact_kind}:history:{history_id}:{content_hash}"
    if history_id:
        return f"db:{artifact_kind}:history:{history_id}"
    if content_hash:
        return f"db:{artifact_kind}:{content_hash}"
    return ""


def _strip_evidence_ref_suffix(value: str) -> str:
    return value.split("#", 1)[0].split("?", 1)[0].strip()


def _decode_supported_evidence_uri(value: str) -> str:
    parsed = urlparse(value)
    if parsed.scheme not in {"file", "browser"}:
        return value

    decoded_path = unquote(parsed.path or "")
    host = (parsed.netloc or "").strip()
    if host and host not in {"localhost", "127.0.0.1"}:
        decoded_path = f"//{host}{decoded_path}"

    if len(decoded_path) >= 3 and decoded_path[0] == "/" and decoded_path[1].isalpha() and decoded_path[2] == ":":
        decoded_path = decoded_path[1:]

    return decoded_path or value


def _to_worktree_relative_or_staged_path(*, base_dir: Path, issue_number: str, evidence_ref: str, evidence_kind: str) -> str:
    cleaned_ref = _strip_evidence_ref_suffix(evidence_ref)
    if not cleaned_ref or cleaned_ref.startswith("db:"):
        return cleaned_ref

    decoded_ref = _decode_supported_evidence_uri(cleaned_ref)
    candidate = Path(decoded_ref)
    absolute_candidate = candidate if candidate.is_absolute() else (base_dir / candidate)

    try:
        relative_candidate = absolute_candidate.resolve(strict=False).relative_to(base_dir.resolve(strict=False))
        return relative_candidate.as_posix()
    except ValueError:
        pass

    if absolute_candidate.exists() and absolute_candidate.is_file():
        target_dir = base_dir / ".opencode" / "runtime" / "evidence" / f"issue-{issue_number}" / (evidence_kind or "artifact")
        target_dir.mkdir(parents=True, exist_ok=True)
        stat = absolute_candidate.stat()
        digest = hashlib.sha256(f"{absolute_candidate}:{stat.st_size}:{stat.st_mtime_ns}".encode("utf-8")).hexdigest()[:12]
        target_path = target_dir / f"{digest}-{absolute_candidate.name}"
        if not target_path.exists():
            shutil.copy2(absolute_candidate, target_path)
        return target_path.relative_to(base_dir).as_posix()

    return cleaned_ref


def _browser_evidence_ref_is_path_like(evidence_ref: str) -> bool:
    cleaned_ref = _strip_evidence_ref_suffix(evidence_ref)
    if not cleaned_ref or cleaned_ref.startswith("db:"):
        return True

    decoded_ref = _decode_supported_evidence_uri(cleaned_ref)
    return "/" in decoded_ref or "\\" in decoded_ref or "." in Path(decoded_ref).name


def _normalize_evidence_packet_refs(*, base_dir: Path, issue_number: str, payload: JsonObject) -> None:
    status = str(payload.get("status") or "").strip().lower()
    if status != "pass":
        return

    gates_raw = payload.get("gates")
    if not isinstance(gates_raw, dict):
        return

    surface_gate_raw = gates_raw.get("surface_qa_gate")
    if not isinstance(surface_gate_raw, dict):
        return

    gates = dict(gates_raw)
    surface_gate = dict(cast(dict[str, object], surface_gate_raw))
    evidence_ref = str(surface_gate.get("evidence_ref") or "").strip()
    if evidence_ref:
        evidence_kind = str(surface_gate.get("evidence_kind") or "browser").strip().lower() or "browser"
        normalized_ref = _to_worktree_relative_or_staged_path(
            base_dir=base_dir,
            issue_number=issue_number,
            evidence_ref=evidence_ref,
            evidence_kind=evidence_kind,
        )
        if normalized_ref and normalized_ref != evidence_ref:
            surface_gate["evidence_ref"] = normalized_ref
            gates["surface_qa_gate"] = surface_gate
            payload["gates"] = gates

    artifact_manifest_raw = payload.get("artifact_manifest")
    if not isinstance(artifact_manifest_raw, list):
        return

    normalized_manifest: list[object] = []
    changed = False
    for entry in artifact_manifest_raw:
        if isinstance(entry, str):
            normalized_entry = _to_worktree_relative_or_staged_path(
                base_dir=base_dir,
                issue_number=issue_number,
                evidence_ref=entry,
                evidence_kind="browser",
            )
            normalized_manifest.append(normalized_entry)
            changed = changed or normalized_entry != entry
        else:
            normalized_manifest.append(entry)

    if changed:
        payload["artifact_manifest"] = normalized_manifest


def _validated_artifact_payload(*, base_dir: Path, issue_number: str, artifact_kind: str, payload: JsonObject) -> JsonObject:
    normalized_payload: JsonObject = dict(payload)
    status_raw = normalized_payload.get("status")
    if not isinstance(status_raw, str) or not status_raw.strip():
        raise ValueError(f"{artifact_kind} payload requires non-empty string field 'status'")

    status = status_raw.strip().lower()
    allowed_statuses: dict[str, set[str]] = {
        "worker_result": {"success", "blocked", "failed"},
        "evidence_packet": {"pass", "blocked", "fail", "failed"},
        "release_result": {"success", "completed", "blocked", "fail", "failed"},
    }
    allowed = allowed_statuses.get(artifact_kind, set())
    if status not in allowed:
        allowed_text = ", ".join(sorted(allowed))
        raise ValueError(
            f"{artifact_kind} payload status={status_raw!r} is invalid; allowed statuses: {allowed_text}"
        )

    normalized_payload["status"] = status

    if artifact_kind == "evidence_packet":
        subject_raw = normalized_payload.get("subject")
        if isinstance(subject_raw, dict):
            subject = cast(dict[str, object], subject_raw)
            subject_pr_number = str(subject.get("pr_number") or subject.get("prNumber") or "").strip()
            if subject_pr_number and not str(normalized_payload.get("pr_number") or "").strip():
                normalized_payload["pr_number"] = subject_pr_number
            subject_base_branch = str(subject.get("base_branch") or subject.get("baseBranch") or "").strip()
            if subject_base_branch and not str(normalized_payload.get("base_branch") or "").strip():
                normalized_payload["base_branch"] = subject_base_branch
        _normalize_evidence_packet_refs(base_dir=base_dir, issue_number=issue_number, payload=normalized_payload)
        if status == "pass":
            gates_raw = normalized_payload.get("gates")
            if not isinstance(gates_raw, dict):
                raise ValueError("evidence_packet payload requires object field 'gates' when status is pass")
            surface_gate_raw = gates_raw.get("surface_qa_gate")
            if not isinstance(surface_gate_raw, dict):
                raise ValueError("evidence_packet payload requires object field 'gates.surface_qa_gate' when status is pass")
            surface_gate = cast(dict[str, object], surface_gate_raw)
            evidence_ref = str(surface_gate.get("evidence_ref") or "").strip()
            if not evidence_ref:
                raise ValueError("evidence_packet payload requires non-empty string field 'gates.surface_qa_gate.evidence_ref' when status is pass")
            evidence_kind = str(surface_gate.get("evidence_kind") or "").strip().lower()
            if evidence_kind == "browser" and not _browser_evidence_ref_is_path_like(evidence_ref):
                raise ValueError(
                    "evidence_packet browser surface_qa_gate.evidence_ref must be a worktree file path, "
                    "not a prose description, when status is pass"
                )

    if artifact_kind == "worker_result":
        branch = str(normalized_payload.get("branch") or "").strip()
        if not branch:
            raise ValueError("worker_result payload requires non-empty string field 'branch'")

    if artifact_kind == "release_result":
        blocked_reason_aliases = {
            "human_approval_required": "release_human_approval_missing",
            "approval_override_mode is none": "release_human_approval_missing",
        }
        blocked_reason = str(normalized_payload.get("blocked_reason") or "").strip().lower()
        blocked_reason = blocked_reason_aliases.get(blocked_reason, blocked_reason)
        if blocked_reason:
            normalized_payload["blocked_reason"] = blocked_reason

        failure_kind = str(normalized_payload.get("failure_kind") or "").strip().lower()
        if not failure_kind and blocked_reason:
            if blocked_reason == "release_human_approval_missing":
                normalized_payload["failure_kind"] = "human_approval_pending"
            else:
                normalized_payload["failure_kind"] = blocked_reason

        if status in {"blocked", "fail", "failed"}:
            if not blocked_reason:
                raise ValueError("release_result payload requires non-empty string field 'blocked_reason' when status is blocked/failed")
            next_recommended_step = str(normalized_payload.get("next_recommended_step") or "").strip()
            if not next_recommended_step:
                raise ValueError(
                    "release_result payload requires non-empty string field 'next_recommended_step' when status is blocked/failed"
                )

        merge_gate_raw = normalized_payload.get("merge_gate")
        merge_gate = cast(dict[str, object], merge_gate_raw) if isinstance(merge_gate_raw, dict) else {}
        checks_state = str(merge_gate.get("checks_state") or "").strip().lower()
        if not checks_state:
            checks_state = (
                "pending"
                if blocked_reason == "required_checks_pending"
                else "failed"
                if blocked_reason == "required_checks_failed"
                else "passed"
            )
        mergeability_state = str(merge_gate.get("mergeability_state") or "").strip().lower()
        if not mergeability_state:
            mergeability_state = "conflicted" if blocked_reason == "pr_not_mergeable" else "clean"
        approval_state = str(merge_gate.get("approval_state") or "").strip().lower()
        if not approval_state:
            approval_state = "missing" if blocked_reason == "release_human_approval_missing" else "satisfied"
        normalized_payload["merge_gate"] = {
            "checks_state": checks_state,
            "mergeability_state": mergeability_state,
            "approval_state": approval_state,
            "blocked_reason": blocked_reason or "none",
            "next_action": str(normalized_payload.get("next_recommended_step") or "").strip(),
        }

        retryable_raw = normalized_payload.get("retryable")
        if retryable_raw is None:
            normalized_payload["retryable"] = blocked_reason in TRANSIENT_RELEASE_BLOCKERS or blocked_reason == "release_human_approval_missing"

        workspace_hygiene_raw = normalized_payload.get("workspace_hygiene")
        workspace_hygiene = (
            cast(dict[str, object], workspace_hygiene_raw) if isinstance(workspace_hygiene_raw, dict) else {}
        )
        cleanup_status = str(workspace_hygiene.get("cleanup_status") or "").strip().lower()
        if not cleanup_status:
            cleanup_status = "blocked" if blocked_reason == "workspace_hygiene_failed" else "pass"
        normalized_payload["workspace_hygiene"] = {
            "cleanup_status": cleanup_status,
            "blocked_reason": str(workspace_hygiene.get("blocked_reason") or (blocked_reason if cleanup_status == "blocked" else "none")),
            "primary_workspace_branch_before": str(workspace_hygiene.get("primary_workspace_branch_before") or ""),
            "primary_workspace_branch_after": str(workspace_hygiene.get("primary_workspace_branch_after") or ""),
            "dirty_state_detected": bool(workspace_hygiene.get("dirty_state_detected") or False),
            "workspace_clean_after": bool(workspace_hygiene.get("workspace_clean_after") or False),
            "issue_worktree_removed": str(workspace_hygiene.get("issue_worktree_removed") or ""),
        }

    return normalized_payload


def _release_gate_view(base_dir: Path, issue_number: str) -> JsonObject:
    release_result = read_artifact_fact(base_dir, issue_number, "release_result")
    if not bool(release_result.get("parse_ok")):
        return {}
    merge_gate_raw = release_result.get("merge_gate")
    merge_gate = cast(dict[str, object], merge_gate_raw) if isinstance(merge_gate_raw, dict) else {}
    workspace_hygiene_raw = release_result.get("workspace_hygiene")
    workspace_hygiene = cast(dict[str, object], workspace_hygiene_raw) if isinstance(workspace_hygiene_raw, dict) else {}
    return {
        "status": str(release_result.get("status") or ""),
        "blocked_reason": str(release_result.get("blocked_reason") or ""),
        "failure_kind": str(release_result.get("failure_kind") or ""),
        "next_recommended_step": str(release_result.get("next_recommended_step") or ""),
        "merge_gate": merge_gate,
        "workspace_hygiene": workspace_hygiene,
    }


def _load_session_helpers() -> ModuleType:
    module_path = Path(__file__).with_name("orchestrator_sessions.py")
    spec = importlib.util.spec_from_file_location("orchestrator_sessions", module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load session helpers from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


_session_helpers = _load_session_helpers()


def _load_lifecycle_helpers() -> ModuleType:
    module_path = Path(__file__).with_name("orchestrator_lifecycle.py")
    spec = importlib.util.spec_from_file_location("orchestrator_lifecycle", module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load lifecycle helpers from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


_lifecycle_helpers = _load_lifecycle_helpers()


def _load_request_helpers() -> ModuleType:
    module_path = Path(__file__).with_name("orchestrator_requests.py")
    spec = importlib.util.spec_from_file_location("orchestrator_requests", module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load request helpers from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


_request_helpers = _load_request_helpers()


def _load_selection_helpers() -> ModuleType:
    module_path = Path(__file__).with_name("orchestrator_selection.py")
    spec = importlib.util.spec_from_file_location("orchestrator_selection", module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load selection helpers from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


_selection_helpers = _load_selection_helpers()


def _load_reconcile_helpers() -> ModuleType:
    module_path = Path(__file__).with_name("orchestrator_reconcile.py")
    spec = importlib.util.spec_from_file_location("orchestrator_reconcile", module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load reconcile helpers from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


_reconcile_helpers = _load_reconcile_helpers()


def _load_policy_helpers() -> ModuleType:
    module_path = Path(__file__).with_name("orchestrator_policy.py")
    spec = importlib.util.spec_from_file_location("orchestrator_policy", module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load policy helpers from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


_policy_helpers = _load_policy_helpers()


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ISSUE_INTAKE_SCRIPT_PATH = ROOT / "scripts/issue_packet_intake.py"
DEFAULT_WORKFLOW_POLICY_PATH = str(ROOT / "docs/agents/autonomous-development-workflow.yaml")
DEFAULT_SUPERVISOR_DOC_PATH = str(ROOT / "docs/agents/runtime/nonstop-supervisor-loop.md")
DEFAULT_RELEASE_RESULT_TEMPLATE_PATH = str(ROOT / "docs/agents/release-result-template.yaml")
DEFAULT_ROOT_SESSION_AGENT = "build"
READY_FOR_AGENT_LABEL = "ready-for-agent"
AGENT_DISPATCHING_LABEL = "agent-dispatching"
AGENT_IN_PROGRESS_LABEL = "agent-in-progress"
AGENT_IN_REVIEW_LABEL = "agent-in-review"
AGENT_COMPLETED_LABEL = "agent-completed"
MAX_ROLE_ATTEMPTS = 3
ROLE_COUNTER_DEFAULTS: dict[str, int] = {
    "main_orchestrator": 0,
    "issue_worker": 0,
    "pr_verifier": 0,
    "release_worker": 0,
    "source_session_stop": 0,
}
ROLE_LIMIT_DEFAULTS: dict[str, int] = {
    "main_orchestrator": MAX_ROLE_ATTEMPTS,
    "issue_worker": MAX_ROLE_ATTEMPTS,
    "pr_verifier": MAX_ROLE_ATTEMPTS,
    "release_worker": MAX_ROLE_ATTEMPTS,
    "source_session_stop": MAX_ROLE_ATTEMPTS,
}
DEFAULT_DEVELOPMENT_CAPACITY = 1
DEFAULT_RELEASE_CAPACITY = 1
DEFAULT_RELEASE_BACKFILL_MODE = "auto"
DEFAULT_AUTO_RELEASE_APPROVAL_MODE = "human_required"
ROOT_HEARTBEAT_TIMEOUT_SECONDS = 900
DEFAULT_SAME_REPO_PROBE_DEGRADED_LIMIT = 2
TRANSIENT_RELEASE_BLOCKERS = {
    "required_checks_pending",
    "required_checks_failed",
    "pr_not_mergeable",
    "workspace_hygiene_failed",
    "transient_tool_failure",
}
NON_TERMINAL_RELEASE_FAILURE_KINDS = {
    "human_approval_pending",
    "approval_blocked",
    "policy_blocked",
}
RUNTIME_PHASE_PROJECTION_CLEAR_STATES = {"completed"}
RUNTIME_PHASE_PROJECTION_WHITELISTS: dict[str, set[tuple[str, str, str]]] = {
    "ready": {
        ("main_orchestrator", "orchestrator_bootstrap", "queued"),
        ("main_orchestrator", "orchestrator_bootstrap", "running"),
        ("main_orchestrator", "issue_selection_or_recovery", "queued"),
        ("issue_worker", "issue_worker_execution", "queued"),
        ("pr_verifier", "pr_verifier_execution", "queued"),
    },
    "verified": {("main_orchestrator", "issue_selection_or_recovery", "queued")},
    "failed": {("main_orchestrator", "issue_selection_or_recovery", "queued")},
    "release_pending": {
        ("main_orchestrator", "release_root_execution", "queued"),
        ("main_orchestrator", "release_root_execution", "running"),
        ("main_orchestrator", "release_root_execution", "pending_approval"),
    },
    "dispatching": {("issue_worker", "issue_worker_execution", "queued")},
    "running": {
        ("main_orchestrator", "orchestrator_bootstrap", "running"),
        ("issue_worker", "issue_worker_execution", "queued"),
        ("issue_worker", "issue_worker_repair", "queued"),
        ("pr_verifier", "pr_verifier_execution", "queued"),
    },
    "verifying": {("pr_verifier", "pr_verifier_execution", "queued")},
}


class SessionRequest(TypedDict):
    requestGeneration: int
    nonce: str
    requestID: str
    createdAt: str
    createdForLedgerRevision: str
    reason: str
    title: str
    agent: str
    prompt: str
    role: str
    stage: str
    issueNumber: str
    branch: str
    selectedIssueNumber: NotRequired[str]
    selectedIssueBranch: NotRequired[str]


class SessionResult(TypedDict, total=False):
    status: str
    sourceSessionID: str
    rootSessionID: str
    launchTitle: str
    title: str
    reason: str
    error: str
    tuiResumeCommand: str
    cliOpenCommand: str
    recommendedAction: str
    sessionReadabilityStatus: str
    stopContinuationStatus: str
    stopContinuationAttempts: int
    role: str
    stage: str
    issueNumber: str
    branch: str
    recordedAt: str
    executionMode: str
    childRole: str
    childSessionID: str
    childSessionStatus: str


def _session_request_body(request: SessionRequest) -> str:
    return json.dumps(dict(request), ensure_ascii=False, indent=2)


def _session_result_body(result: SessionResult) -> str:
    return json.dumps(dict(result), ensure_ascii=False, indent=2)


def _content_hash(text: str) -> str:
    return f"sha256:{hashlib.sha256(text.encode('utf-8')).hexdigest()}"


def _record_dispatch_request_history(
    *,
    base_dir: Path,
    request: SessionRequest,
    created_at: str,
) -> int:
    request_id = str(request.get("requestID") or "")
    body_text = _session_request_body(request)
    return append_issue_history(
        base_dir,
        issue_number=request["issueNumber"],
        entry_type="dispatch_request",
        created_at=created_at,
        role=request["role"],
        stage=request["stage"],
        status="queued",
        request_id=request_id,
        command_id=request_id,
        summary=request["reason"],
        payload=dict(request),
        body_text=body_text,
        content_hash=_content_hash(body_text),
        unique_key=f"dispatch-request:{request_id or created_at}",
    )


def _record_dispatch_result_history(
    *,
    base_dir: Path,
    session_result: SessionResult,
) -> int:
    issue_number = str(session_result.get("issueNumber") or "")
    recorded_at = str(session_result.get("recordedAt") or "")
    request_id = str(session_result.get("sourceSessionID") or "")
    root_session_id = str(session_result.get("rootSessionID") or "")
    status = str(session_result.get("status") or "")
    if not issue_number or not recorded_at:
        return 0
    body_text = _session_result_body(session_result)
    body_hash = _content_hash(body_text)
    return append_issue_history(
        base_dir,
        issue_number=issue_number,
        entry_type="dispatch_result",
        created_at=recorded_at,
        role=str(session_result.get("role") or ""),
        stage=str(session_result.get("stage") or ""),
        status=status,
        session_id=root_session_id,
        request_id=request_id,
        command_id=request_id,
        summary=str(session_result.get("reason") or status),
        payload=dict(session_result),
        body_text=body_text,
        content_hash=body_hash,
        unique_key=f"dispatch-result:{issue_number}:{body_hash}",
    )


def _is_same_repo_probe_degradation(session_result: SessionResult) -> bool:
    root_session_id = str(session_result.get("rootSessionID") or "")
    if not root_session_id:
        return False
    readability_status = str(session_result.get("sessionReadabilityStatus") or "")
    if readability_status in {"failed_same_repo_probe", "degraded_same_repo_probe"}:
        return True
    error_text = str(session_result.get("error") or "").lower()
    return "same-repo session_read probe" in error_text


def _promote_same_repo_probe_degraded_result(session_result: SessionResult) -> bool:
    if session_result.get("status") == "success":
        return False
    if not _is_same_repo_probe_degradation(session_result):
        return False
    degraded_root_session_id = str(session_result.get("rootSessionID") or "")
    if not degraded_root_session_id:
        return False
    session_result["status"] = "success"
    if not str(session_result.get("sessionReadabilityStatus") or ""):
        session_result["sessionReadabilityStatus"] = "degraded_same_repo_probe"
    if not str(session_result.get("cliOpenCommand") or ""):
        session_result["cliOpenCommand"] = f"opencode --session {degraded_root_session_id}"
    if not str(session_result.get("tuiResumeCommand") or ""):
        session_result["tuiResumeCommand"] = "/sessions"
    degraded_error = str(session_result.get("error") or "")
    resume_link = _default_host_adapter().resume_link(degraded_root_session_id)
    session_result["recommendedAction"] = (
        f"Resume the active root session with {resume_link}. "
        f"Dispatch returned a degraded status after the session was created: {degraded_error or 'same-repo session_read probe failed'}"
    )
    return True


def _session_result_recorded_at(session_result: JsonObject) -> str:
    return str(session_result.get("recordedAt") or "")


def _session_result_is_newer(*, candidate: JsonObject, current: JsonObject) -> bool:
    candidate_recorded_at = _session_result_recorded_at(candidate)
    current_recorded_at = _session_result_recorded_at(current)
    if not candidate_recorded_at:
        return False
    if not current_recorded_at:
        return True
    candidate_time = _parse_iso8601(candidate_recorded_at)
    current_time = _parse_iso8601(current_recorded_at)
    if candidate_time is not None and current_time is not None:
        return candidate_time > current_time
    return candidate_recorded_at > current_recorded_at


def _record_runtime_transition_history(
    *,
    base_dir: Path,
    issue_number: str,
    recorded_at: str,
    from_role: str,
    from_stage: str,
    to_role: str,
    to_stage: str,
    reason: str,
) -> int:
    body_text = f"{from_role}/{from_stage} -> {to_role}/{to_stage}"
    unique_key = f"runtime-transition:{issue_number}:{recorded_at}:{from_role}:{from_stage}:{to_role}:{to_stage}"
    payload = {
        "transition_type": "runtime_role_stage",
        "from_role": from_role,
        "from_stage": from_stage,
        "to_role": to_role,
        "to_stage": to_stage,
    }
    return append_issue_history(
        base_dir,
        issue_number=issue_number,
        entry_type="runtime_transition",
        created_at=recorded_at,
        role=to_role,
        stage=to_stage,
        status="queued",
        summary=reason,
        payload=payload,
        body_text=body_text,
        content_hash=_content_hash(body_text),
        unique_key=unique_key,
        update_issue_last_history=False,
    )


def read_latest_dispatch_result(base_dir: Path, *, issue_number: str | None = None) -> SessionResult | None:
    row = read_latest_history_entry(base_dir, issue_number=issue_number, entry_type="dispatch_result")
    if row is None:
        return None
    payload = json.loads(str(row.get("payload_json") or "{}"))
    return cast(SessionResult, cast(object, payload)) if isinstance(payload, dict) else None


def read_latest_session_result(base_dir: Path, *, issue_number: str | None = None) -> SessionResult | None:
    row = read_latest_history_entry(base_dir, issue_number=issue_number, entry_type="session_result")
    if row is None:
        return None
    payload = json.loads(str(row.get("payload_json") or "{}"))
    return cast(SessionResult, cast(object, payload)) if isinstance(payload, dict) else None


def _read_latest_session_payload(base_dir: Path, *, issue_number: str) -> SessionResult | None:
    dispatch_result = read_latest_dispatch_result(base_dir, issue_number=issue_number)
    session_result = read_latest_session_result(base_dir, issue_number=issue_number)
    if dispatch_result is None:
        return session_result
    if session_result is None:
        return dispatch_result
    if _session_result_is_newer(candidate=cast(JsonObject, cast(object, session_result)), current=cast(JsonObject, cast(object, dispatch_result))):
        return session_result
    return dispatch_result


def _runtime_requires_role_stage_dispatch(*, role: str, stage: str) -> bool:
    return role == "main_orchestrator" and stage == "release_root_execution"


def _has_role_stage_dispatch_evidence(*, base_dir: Path, issue_number: str, role: str, stage: str) -> bool:
    request_row = read_latest_history_entry(base_dir, issue_number=issue_number, entry_type="dispatch_request")
    if request_row is not None and str(request_row.get("role") or "") == role and str(request_row.get("stage") or "") == stage:
        return True
    result_row = read_latest_history_entry(base_dir, issue_number=issue_number, entry_type="dispatch_result")
    if result_row is not None and str(result_row.get("role") or "") == role and str(result_row.get("stage") or "") == stage:
        return True
    return False


def _show_session_for_issue(*, base_dir: Path, issue: dict[str, object], issue_number: str) -> SessionResult | None:
    current_session_id = str(issue.get("current_session_id") or "")
    current_role = str(issue.get("current_role") or "")
    current_stage = str(issue.get("current_stage") or "")
    result = _read_latest_session_payload(base_dir, issue_number=issue_number)
    if result is not None:
        result_role = str(result.get("role") or "")
        result_stage = str(result.get("stage") or "")
        if not current_role and not current_stage:
            return result
        if result_role == current_role and result_stage == current_stage:
            return result
        if _runtime_requires_role_stage_dispatch(role=current_role, stage=current_stage):
            return None
        return result
    if not current_session_id:
        return None
    if _runtime_requires_role_stage_dispatch(role=current_role, stage=current_stage):
        return None
    return cast(
        SessionResult,
        cast(
            object,
            {
                "status": "success",
                "rootSessionID": current_session_id,
                "issueNumber": issue_number,
                "branch": str(issue.get("branch") or ""),
                "role": current_role,
                "stage": current_stage,
                "recordedAt": str(issue.get("updated_at") or ""),
            },
        ),
    )


def _repair_stale_release_pending_fences(*, base_dir: Path, updated_at: str) -> list[str]:
    repaired: list[str] = []
    release_pending_issues = list_issues(base_dir, states=["release_pending"])
    for issue in release_pending_issues:
        issue_number = str(issue.get("issue_number") or "")
        if not issue_number:
            continue
        current_role = str(issue.get("current_role") or "")
        current_stage = str(issue.get("current_stage") or "")
        current_status = str(issue.get("current_status") or "")
        current_session_id = str(issue.get("current_session_id") or "")
        if current_role != "main_orchestrator" or current_stage != "release_root_execution" or (not current_status and not current_session_id):
            continue
        if current_status == "pending_approval":
            continue
        if _has_role_stage_dispatch_evidence(
            base_dir=base_dir,
            issue_number=issue_number,
            role="main_orchestrator",
            stage="release_root_execution",
        ):
            continue
        _ = sync_issue_runtime_context(
            base_dir,
            issue_number=issue_number,
            updated_at=updated_at,
            current_role="main_orchestrator",
            current_stage="release_root_execution",
            current_status="",
        )
        _ = upsert_issue_state(
            base_dir,
            issue_number=issue_number,
            state="release_pending",
            command_id=f"release-fence-repair:{issue_number}:{updated_at}",
            updated_at=updated_at,
            current_session_id="",
        )
        _ = append_issue_history(
            base_dir,
            issue_number=issue_number,
            entry_type="runtime_transition",
            created_at=updated_at,
            role="release_worker",
            stage="release_worker_execution",
            status="",
            summary=(
                f"Cleared stale release_worker fence for issue #{issue_number}: "
                "release_pending lacked matching release dispatch evidence."
            ),
            payload={
                "transition_type": "release_fence_repair",
                "issue_number": issue_number,
            },
            unique_key=f"release-fence-repair-event:{issue_number}:{updated_at}",
        )
        repaired.append(issue_number)
    return repaired


def _extract_pr_number_from_artifact_fact(fact: dict[str, object]) -> str:
    direct = str(fact.get("pr_number") or "").strip()
    if direct and direct.lower() != "none":
        return direct
    nested_raw = fact.get("pr")
    if isinstance(nested_raw, dict):
        nested = cast(dict[str, object], nested_raw)
        nested_number = str(nested.get("number") or "").strip()
        if nested_number and nested_number.lower() != "none":
            return nested_number
    return ""


def _ensure_release_pr_opened_fact(*, base_dir: Path, issue_number: str, updated_at: str) -> dict[str, object] | None:
    existing = read_latest_history_entry(base_dir, issue_number=issue_number, entry_type="pr_opened")
    if existing is not None:
        return existing

    evidence_packet = _read_db_artifact_fact(base_dir=base_dir, issue_number=issue_number, artifact_kind="evidence_packet")
    worker_result = _read_db_artifact_fact(base_dir=base_dir, issue_number=issue_number, artifact_kind="worker_result")
    pr_number = _extract_pr_number_from_artifact_fact(evidence_packet)
    source_artifact = "evidence_packet"
    if not pr_number:
        pr_number = _extract_pr_number_from_artifact_fact(worker_result)
        source_artifact = "worker_result"
    if not pr_number:
        return None

    verifier_session_id = str(evidence_packet.get("verifier_session_id") or "")
    if not verifier_session_id:
        verifier_session_id = str(worker_result.get("verifier_session_id") or "")
    if not verifier_session_id:
        verifier_session_id = str(evidence_packet.get("session_id") or "")

    issue = read_issue(base_dir, issue_number) or {}
    branch = str(issue.get("branch") or "")
    runtime_context = read_runtime_context(base_dir, issue_number)
    base_branch = str(runtime_context.get("resolved_base_branch") or "") if isinstance(runtime_context, dict) else ""

    _ = record_pr_opened(
        base_dir=base_dir,
        issue_number=issue_number,
        pr_number=pr_number,
        created_at=updated_at,
        verifier_session_id=verifier_session_id,
        command_id=f"release-pr-opened-backfill:{issue_number}",
        summary=(
            f"Backfill missing pr_opened fact for issue #{issue_number} "
            f"from {source_artifact} before independent release."
        ),
        payload={
            "issue_number": issue_number,
            "pr_number": pr_number,
            "source_artifact": f"release_{source_artifact}_fallback",
            "head_branch": branch,
            "base_branch": base_branch,
        },
    )

    return read_latest_history_entry(base_dir, issue_number=issue_number, entry_type="pr_opened")


def _latest_pr_opened_payload(base_dir: Path, issue_number: str) -> dict[str, object]:
    entry = read_latest_history_entry(base_dir, issue_number=issue_number, entry_type="pr_opened")
    if entry is None:
        return {}
    return _load_json_dict(entry.get("payload_json"))


def _read_release_pending_pr_merge_status(
    *,
    base_dir: Path,
    issue_number: str,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> dict[str, object]:
    repo = _read_project_github_repo(base_dir)
    if not repo:
        return {}
    payload = _latest_pr_opened_payload(base_dir, issue_number)
    pr_number = str(payload.get("pr_number") or "").strip()
    if not pr_number.isdigit():
        return {}
    owner, _, name = repo.partition("/")
    if not owner or not name:
        return {}

    query = (
        "query($owner:String!,$repo:String!,$number:Int!){repository(owner:$owner,name:$repo){pullRequest(number:$number){merged reviewDecision mergeStateStatus url mergedAt}}}"
    )
    result = run(
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
            f"number={pr_number}",
        ],
        cwd=base_dir,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return {}
    try:
        response = json.loads(result.stdout or "{}")
    except json.JSONDecodeError:
        return {}
    pr = response.get("data", {}).get("repository", {}).get("pullRequest", {}) if isinstance(response, dict) else {}
    if not isinstance(pr, dict):
        return {}
    return {
        "pr_number": pr_number,
        "merged": bool(pr.get("merged")),
        "reviewDecision": str(pr.get("reviewDecision") or "").strip(),
        "mergeStateStatus": str(pr.get("mergeStateStatus") or "").strip(),
        "url": str(pr.get("url") or "").strip(),
        "mergedAt": str(pr.get("mergedAt") or "").strip(),
    }


def _recover_approved_merged_release_pending_issue(
    *,
    base_dir: Path,
    issue_number: str,
    updated_at: str,
    run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> bool:
    issue = read_issue(base_dir, issue_number)
    if issue is None:
        return False
    if str(issue.get("state") or "") != "release_pending":
        return False
    if str(issue.get("current_status") or "") != "pending_approval":
        return False

    pr_status = _read_release_pending_pr_merge_status(
        base_dir=base_dir,
        issue_number=issue_number,
        run=run,
    )
    if not pr_status:
        return False
    if not bool(pr_status.get("merged")):
        return False
    if str(pr_status.get("reviewDecision") or "") != "APPROVED":
        return False

    _ = sync_issue_runtime_context(
        base_dir,
        issue_number=issue_number,
        updated_at=updated_at,
        current_role="main_orchestrator",
        current_stage="release_root_execution",
        current_status="",
    )
    _ = upsert_issue_state(
        base_dir,
        issue_number=issue_number,
        state="release_pending",
        command_id=f"release-approval-merge-recovery:{issue_number}",
        updated_at=updated_at,
        current_session_id="",
    )
    _ = append_issue_history(
        base_dir,
        issue_number=issue_number,
        entry_type="runtime_transition",
        created_at=updated_at,
        role="release_worker",
        stage="release_worker_execution",
        status="",
        summary=(
            f"Recovered pending approval release fence for issue #{issue_number} after GitHub confirmed PR #{pr_status['pr_number']} was approved and merged."
        ),
        payload={
            "transition_type": "release_approval_merge_recovery",
            "issue_number": issue_number,
            "pr_number": pr_status["pr_number"],
            "review_decision": pr_status.get("reviewDecision"),
            "merge_state_status": pr_status.get("mergeStateStatus"),
            "pr_url": pr_status.get("url"),
            "merged_at": pr_status.get("mergedAt"),
        },
        unique_key=f"release-approval-merge-recovery:{issue_number}:{updated_at}",
    )
    return True


def _recover_approved_merged_release_pending_issues(*, base_dir: Path, updated_at: str) -> list[str]:
    recovered: list[str] = []
    for issue in list_issues(base_dir, states=["release_pending"]):
        issue_number = str(issue.get("issue_number") or "")
        if not issue_number:
            continue
        if _recover_approved_merged_release_pending_issue(
            base_dir=base_dir,
            issue_number=issue_number,
            updated_at=updated_at,
        ):
            recovered.append(issue_number)
    return recovered


def show_latest_session(*, base_dir: Path, issue_number: str | None = None) -> SessionResult | None:
    base_dir = _canonical_supervisor_base_dir(base_dir)
    if issue_number:
        issue = read_issue(base_dir, issue_number)
        if issue is not None:
            return _show_session_for_issue(base_dir=base_dir, issue=cast(dict[str, object], issue), issue_number=issue_number)
    # Prefer currently active/fenced execution lanes first so stale quarantined
    # session fences do not overshadow the operator-facing latest runnable session.
    active_issues = list_issues(
        base_dir,
        states=["claimed", "dispatching", "running", "verifying", "release_pending"],
        require_current_session=True,
    )
    if not active_issues:
        active_issues = list_issues(base_dir, require_current_session=True)
    if active_issues:
        latest_issue = active_issues[0]
        latest_issue_number = str(latest_issue.get("issue_number") or "")
        if latest_issue_number:
            return _show_session_for_issue(
                base_dir=base_dir,
                issue=cast(dict[str, object], latest_issue),
                issue_number=latest_issue_number,
            )
        return None
    root_result = read_latest_dispatch_result(base_dir)
    latest_session_result = read_latest_session_result(base_dir)
    if root_result is None:
        return latest_session_result
    if latest_session_result is None:
        return root_result
    if _session_result_is_newer(
        candidate=cast(JsonObject, cast(object, latest_session_result)),
        current=cast(JsonObject, cast(object, root_result)),
    ):
        return latest_session_result
    return root_result


def start_issue(
    *,
    base_dir: Path,
    issue_number: str,
    source_session_id: str,
    updated_at: str | None = None,
) -> SessionResult:
    base_dir = _canonical_supervisor_base_dir(base_dir)
    ensure_control_plane_db(base_dir)
    normalized_issue_number = _normalize_requested_issue_number(issue_number)
    packet = _load_issue_packet_from_db(base_dir, normalized_issue_number)
    if packet is None:
        raise RuntimeError(
            f"issue packet not recorded in SQLite for issue #{normalized_issue_number}; sync the packet into the DB control plane before starting the issue"
        )

    issue_number = _validate_start_packet_issue_number(requested_issue_number=normalized_issue_number, packet=packet)
    timestamp = _now(updated_at)
    ensure_issue_row(base_dir, issue_number=issue_number, updated_at=timestamp)
    resolved_base_branch = _resolve_issue_base_branch(base_dir, packet)
    if not resolved_base_branch:
        raise RuntimeError(f"issue #{issue_number} has unresolved stacked dependencies; refusing start until exactly one parent PR is stackable or all dependencies are completed")
    claim_command_id = f"start-issue:{issue_number}:claimed"
    try:
        _ = claim_issue_if_ready(
            base_dir,
            issue_number=issue_number,
            command_id=claim_command_id,
            scheduler_id="upsert",
            reason=f"Upsert issue #{issue_number} state to claimed.",
            updated_at=timestamp,
        )
    except ValueError as error:
        raise RuntimeError(str(error)) from error
    try:
        issue_worktree = _ensure_issue_worktree(
            base_dir=base_dir,
            issue_number=issue_number,
            branch=packet.branch,
            base_branch=resolved_base_branch,
            updated_at=timestamp,
        )
    except RuntimeError as error:
        rollback_command_id = f"start-issue:{issue_number}:worktree-rollback"
        _ = upsert_issue_state(
            base_dir,
            issue_number=issue_number,
            state="ready",
            command_id=rollback_command_id,
            updated_at=timestamp,
            current_session_id="",
        )
        _ = sync_issue_runtime_context(
            base_dir,
            issue_number=issue_number,
            updated_at=timestamp,
            last_failure={
                "kind": "worktree_prepare_failed",
                "retryable": True,
                "summary": str(error),
                "rollback_reason": f"worktree_prepare_error:{str(error)}",
            },
        )
        _ = append_issue_history(
            base_dir,
            issue_number=issue_number,
            entry_type="admin_action",
            created_at=timestamp,
            status="rollback",
            command_id=rollback_command_id,
            from_state="claimed",
            to_state="ready",
            summary=f"Rollback issue #{issue_number} to ready after worktree prepare failure.",
            payload={
                "decision_type": "admin_worktree_prepare_failure",
                "rollback_reason": f"worktree_prepare_error:{str(error)}",
                "restored_ready_for_agent": True,
            },
            unique_key=f"admin-worktree-rollback:{issue_number}:{timestamp}",
        )
        raise RuntimeError(str(error)) from error
    _ = sync_issue_runtime_context(
        base_dir,
        issue_number=issue_number,
        updated_at=timestamp,
        runtime_context={
            "resolved_base_branch": resolved_base_branch,
            "target_branch": packet.branch,
            "issue_worktree_path": str(issue_worktree),
        },
        worktree_path=str(issue_worktree),
    )
    _ = append_issue_history(
        base_dir,
        issue_number=issue_number,
        entry_type="stack_base_resolved",
        created_at=timestamp,
        status="resolved",
        summary=f"Resolved issue #{issue_number} base branch to {resolved_base_branch}.",
        payload={"issue_number": issue_number, "target_branch": packet.branch, "base_branch": resolved_base_branch},
        unique_key=f"stack-base:{issue_number}:{resolved_base_branch}:{timestamp}",
    )
    try:
        _sync_projected_issue_labels_or_raise(
            base_dir=base_dir,
            issue_number=issue_number,
            command_id=f"start-issue:{issue_number}:labels",
            updated_at=timestamp,
        )
    except RuntimeError as error:
        _ = upsert_issue_state(
            base_dir,
            issue_number=issue_number,
            state="ready",
            command_id=f"start-issue:{issue_number}:labels-rollback",
            updated_at=timestamp,
            current_session_id="",
        )
        raise RuntimeError(str(error)) from error
    ledger = create_initial_ledger(
        issue_packet=packet,
        workflow_policy_path=DEFAULT_WORKFLOW_POLICY_PATH,
        primary_workspace_root=str(issue_worktree),
        root_session_agent=DEFAULT_ROOT_SESSION_AGENT,
        base_branch=resolved_base_branch,
        updated_at=timestamp,
    )
    request = build_orchestrator_request(ledger)
    _record_dispatch_request_history(base_dir=base_dir, request=request, created_at=str(request.get("createdAt") or timestamp))
    dispatch_command_id = str(request.get("requestID") or uuid4().hex)
    _transition_issue_state_if_possible(
        base_dir=base_dir,
        issue_number=issue_number,
        to_state="dispatching",
        command_id=dispatch_command_id,
        updated_at=timestamp,
        reason=f"Dispatch root session request for issue #{issue_number}.",
        from_state="claimed",
    )
    _sync_github_projection_bundle(
        base_dir=base_dir,
        issue_number=issue_number,
        phase="dispatching",
        updated_at=timestamp,
    )
    session_result = dispatch_session_request(
        request,
        workdir=issue_worktree,
        source_session_id=source_session_id,
        updated_at=timestamp,
    )
    _ = _promote_same_repo_probe_degraded_result(session_result)
    if session_result.get("status") == "success":
        root_session_id = str(session_result.get("rootSessionID") or "")
        recorded_at = str(session_result.get("recordedAt") or timestamp)
        _transition_issue_state_if_possible(
            base_dir=base_dir,
            issue_number=issue_number,
            to_state="running",
            command_id=f"{dispatch_command_id}:running",
            updated_at=recorded_at,
            reason=f"Root session {root_session_id} acknowledged for issue #{issue_number}.",
            from_state="dispatching",
            current_session_id=root_session_id,
        )
        _append_root_issue_event(
            base_dir=base_dir,
            issue_number=issue_number,
            root_session_id=root_session_id,
            event_type="root_session_started",
            created_at=recorded_at,
            payload=cast(JsonObject, cast(object, dict(session_result))),
            session_seq=1,
        )
        _record_same_repo_probe_degraded_event(
            base_dir=base_dir,
            issue_number=issue_number,
            session_result=session_result,
        )
        sync_error = _sync_projected_issue_labels(
            base_dir=base_dir,
            issue_number=issue_number,
            command_id=f"start-issue:{issue_number}:running-labels",
            updated_at=recorded_at,
        )
        if sync_error:
            session_result["recommendedAction"] = (
                f"Resume the active root session with {_default_host_adapter().resume_link(root_session_id)}. "
                f"GitHub running-label sync failed and may need retry: {sync_error}"
            )
        _sync_github_projection_bundle(
            base_dir=base_dir,
            issue_number=issue_number,
            phase="running",
            updated_at=recorded_at,
        )
        # Persist the initial bootstrap -> issue_worker handoff in SQLite before the
        # first issue_worker launch so verifier evidence can come from the control plane.
        ledger["current"] = {
            "role": "main_orchestrator",
            "stage": "orchestrator_bootstrap",
            "status": "running",
        }
        ledger["lastSessionResult"] = dict(session_result)
        _bump_ledger_revision(ledger, recorded_at)
        reconcile_ledger(
            ledger,
            artifact_base_dir=base_dir,
            updated_at=recorded_at,
        )
    else:
        prior_dispatch_result = read_latest_dispatch_result(base_dir, issue_number=issue_number)
        _record_dispatch_result_history(base_dir=base_dir, session_result=session_result)
        active_root_session_id = str((prior_dispatch_result or {}).get("rootSessionID") or "")
        active_dispatch_status = str((prior_dispatch_result or {}).get("status") or "")
        if active_root_session_id and active_dispatch_status == "success":
            raise RuntimeError(
                f"dispatch failure rollback blocked for issue #{issue_number}: latest dispatch_result has active root session {active_root_session_id}"
            )
        failure_updated_at = str(session_result.get("recordedAt") or timestamp)
        release_issue_execution(
            base_dir=base_dir,
            issue_number=issue_number,
            restore_ready_for_agent=True,
            updated_at=failure_updated_at,
            rollback_reason=(
                f"dispatch_error:{str(session_result.get('error') or 'unknown')}"
            ),
        )
    if session_result.get("status") == "success":
        _record_dispatch_result_history(base_dir=base_dir, session_result=session_result)
    return session_result


class SupervisorDecision(TypedDict):
    action: str
    next_role: str
    next_stage: str
    summary: str
    request_title: str
    subagent_prompt: NotRequired[str]


def _role_execution_mode(role: str) -> str:
    return "root_session" if role == "main_orchestrator" else "orchestrator_subagent"


def _root_session_agent(ledger: JsonObject) -> str:
    automation = cast(dict[str, object], ledger.get("automation", {}))
    configured = automation.get("rootSessionAgent")
    return configured if isinstance(configured, str) and configured else DEFAULT_ROOT_SESSION_AGENT


def _cli_agent_name(agent_name: str) -> str | None:
    normalized = agent_name.strip()
    if not normalized:
        return None
    if normalized.lower() == DEFAULT_ROOT_SESSION_AGENT:
        return None
    return normalized


def _now(updated_at: str | None = None) -> str:
    return updated_at or datetime.now().astimezone().isoformat(timespec="seconds")


def _development_capacity() -> int:
    raw = os.environ.get("AUTODEV_DEVELOPMENT_CAPACITY", "")
    if not raw:
        return DEFAULT_DEVELOPMENT_CAPACITY
    try:
        capacity = int(raw)
    except ValueError:
        return DEFAULT_DEVELOPMENT_CAPACITY
    return max(1, capacity)


def _release_capacity() -> int:
    raw = os.environ.get("AUTODEV_RELEASE_CAPACITY", "")
    if not raw:
        return DEFAULT_RELEASE_CAPACITY
    try:
        capacity = int(raw)
    except ValueError:
        return DEFAULT_RELEASE_CAPACITY
    return max(1, capacity)


def root_heartbeat_timeout_seconds() -> int:
    raw = os.environ.get("AUTODEV_ROOT_HEARTBEAT_TIMEOUT_SECONDS", "")
    if not raw:
        return ROOT_HEARTBEAT_TIMEOUT_SECONDS
    try:
        timeout_seconds = int(raw)
    except ValueError:
        return ROOT_HEARTBEAT_TIMEOUT_SECONDS
    return max(1, timeout_seconds)


def _same_repo_probe_degraded_limit() -> int:
    raw = os.environ.get("AUTODEV_SAME_REPO_PROBE_DEGRADED_LIMIT", "")
    if not raw:
        return DEFAULT_SAME_REPO_PROBE_DEGRADED_LIMIT
    try:
        limit = int(raw)
    except ValueError:
        return DEFAULT_SAME_REPO_PROBE_DEGRADED_LIMIT
    return max(1, limit)


def _release_backfill_mode() -> str:
    raw = os.environ.get("AUTODEV_RELEASE_BACKFILL_MODE", "")
    normalized = raw.strip().lower()
    if not normalized:
        return DEFAULT_RELEASE_BACKFILL_MODE
    if normalized in {"auto", "manual"}:
        return normalized
    return DEFAULT_RELEASE_BACKFILL_MODE


def _auto_release_approval_mode() -> str:
    raw = os.environ.get("AUTODEV_AUTO_RELEASE_APPROVAL_MODE", "")
    normalized = raw.strip().lower()
    if not normalized:
        return DEFAULT_AUTO_RELEASE_APPROVAL_MODE
    if normalized in {"human_required", "bypass_approval"}:
        return normalized
    return DEFAULT_AUTO_RELEASE_APPROVAL_MODE


def _release_backfill_source_session_id(source_session_id: str) -> str:
    normalized = source_session_id.strip()
    if not normalized:
        return "workspace_reconcile_release_backfill"
    return f"{normalized}:release_backfill"


def _parse_iso8601(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _root_event_id(*, issue_number: str, root_session_id: str, event_type: str) -> str:
    return ":".join(["issue", issue_number, root_session_id or "unknown-root", event_type])


def _append_root_issue_event(
    *,
    base_dir: Path,
    issue_number: str,
    root_session_id: str,
    event_type: str,
    created_at: str,
    payload: JsonObject,
    session_seq: int,
) -> None:
    if not root_session_id or not created_at:
        return
    append_issue_event(
        base_dir,
        event_id=_root_event_id(
            issue_number=issue_number,
            root_session_id=root_session_id,
            event_type=event_type,
        ),
        issue_number=issue_number,
        root_session_id=root_session_id,
        session_seq=session_seq,
        event_type=event_type,
        payload=dict(payload),
        created_at=created_at,
    )


def _sync_root_issue_event_from_session_result(ledger: JsonObject, *, base_dir: Path) -> None:
    last_session_result = cast(JsonObject, ledger.get("lastSessionResult", {}))
    root_session_id = str(last_session_result.get("rootSessionID") or "")
    recorded_at = str(last_session_result.get("recordedAt") or "")
    if not root_session_id or not recorded_at:
        return
    issue = cast(dict[str, str], ledger.get("issue", {}))
    issue_number = issue.get("number", "")
    if not issue_number:
        return
    runtime_issue = read_issue(base_dir, issue_number) or {}
    current_state = str(runtime_issue.get("state") or "")
    current_session_id = str(runtime_issue.get("current_session_id") or "")
    if current_state and not current_session_id:
        upsert_issue_state(
            base_dir,
            issue_number=issue_number,
            state=current_state,
            command_id=f"session-result-hydrate:{issue_number}:{recorded_at}",
            updated_at=recorded_at,
            current_session_id=root_session_id,
        )
    _append_root_issue_event(
        base_dir=base_dir,
        issue_number=issue_number,
        root_session_id=root_session_id,
        event_type="root_session_started",
        created_at=recorded_at,
        payload=last_session_result,
        session_seq=1,
    )


def _sync_last_session_result_from_db(ledger: JsonObject, *, base_dir: Path) -> None:
    issue = cast(dict[str, str], ledger.get("issue", {}))
    issue_number = str(issue.get("number") or "")
    if not issue_number:
        return
    persisted = cast(JsonObject | None, read_latest_dispatch_result(base_dir, issue_number=issue_number))
    if persisted is None:
        return
    current = cast(JsonObject, ledger.get("lastSessionResult", {}))
    if current and not _session_result_is_newer(candidate=persisted, current=current):
        return
    ledger["lastSessionResult"] = dict(persisted)
    stop_attempts = persisted.get("stopContinuationAttempts")
    if isinstance(stop_attempts, int):
        cast(dict[str, int], ledger["attempts"])["source_session_stop"] = stop_attempts
    recorded_at = _session_result_recorded_at(persisted)
    if recorded_at:
        _bump_ledger_revision(ledger, recorded_at)


def _append_root_terminal_event_for_verifier_handoff(
    *,
    base_dir: Path,
    ledger: JsonObject,
    runtime_issue: dict[str, object],
    updated_at: str,
) -> None:
    issue = cast(dict[str, str], ledger["issue"])
    current = cast(dict[str, str], ledger["current"])
    root_session_id = str(runtime_issue.get("current_session_id") or "")
    if not root_session_id:
        return
    _append_root_issue_event(
        base_dir=base_dir,
        issue_number=issue["number"],
        root_session_id=root_session_id,
        event_type="root_terminal",
        created_at=updated_at,
        payload={
            "issueNumber": issue["number"],
            "role": current["role"],
            "stage": current["stage"],
            "reason": "verifier_handoff",
        },
        session_seq=2,
    )


def _quarantine_stale_running_issue(
    *,
    base_dir: Path,
    ledger: JsonObject,
    runtime_issue: dict[str, object],
    updated_at: str,
) -> bool:
    if str(runtime_issue.get("state") or "") != "running":
        return False
    root_session_id = str(runtime_issue.get("current_session_id") or "")
    last_event_at = str(runtime_issue.get("last_event_at") or "")
    if not root_session_id or not last_event_at:
        return False
    current_time = _parse_iso8601(updated_at)
    last_event_time = _parse_iso8601(last_event_at)
    if current_time is None or last_event_time is None:
        return False
    if current_time - last_event_time <= timedelta(seconds=root_heartbeat_timeout_seconds()):
        return False

    issue = cast(dict[str, str], ledger["issue"])
    quarantine_issue_execution(
        base_dir=base_dir,
        issue_number=issue["number"],
        reason=(
            f"Root session {root_session_id} heartbeat timed out after last event at {last_event_at}; "
            "move issue into quarantined until fenced resume or terminal failure."
        ),
        updated_at=updated_at,
    )
    return True


def _quarantine_running_issue_without_root_session(
    *,
    base_dir: Path,
    ledger: JsonObject,
    runtime_issue: dict[str, object],
    updated_at: str,
) -> bool:
    if str(runtime_issue.get("state") or "") != "running":
        return False
    if str(runtime_issue.get("current_session_id") or ""):
        return False

    issue = cast(dict[str, str], ledger["issue"])
    quarantine_issue_execution(
        base_dir=base_dir,
        issue_number=issue["number"],
        reason=(
            f"Issue #{issue['number']} is marked running without a recorded root session id; "
            "treat it as an orphaned dispatch and require fenced resume or terminal failure."
        ),
        updated_at=updated_at,
    )
    return True


def _quarantine_stale_dispatching_issue_without_root_session(
    *,
    base_dir: Path,
    ledger: JsonObject,
    current: dict[str, str],
    runtime_issue: dict[str, object],
    updated_at: str,
) -> bool:
    if current.get("role") != "issue_worker" or current.get("status") != "queued":
        return False
    if str(runtime_issue.get("state") or "") != "dispatching":
        return False
    if str(runtime_issue.get("current_session_id") or ""):
        return False

    dispatching_at = str(runtime_issue.get("dispatching_at") or runtime_issue.get("updated_at") or "")
    current_time = _parse_iso8601(updated_at)
    dispatching_time = _parse_iso8601(dispatching_at)
    if current_time is None or dispatching_time is None:
        return False
    if current_time - dispatching_time <= timedelta(seconds=root_heartbeat_timeout_seconds()):
        return False

    issue = cast(dict[str, str], ledger["issue"])
    quarantine_issue_execution(
        base_dir=base_dir,
        issue_number=issue["number"],
        reason=(
            f"Issue #{issue['number']} stayed in dispatching without a recorded root session id since {dispatching_at}; "
            "treat it as an orphaned queued issue_worker dispatch and quarantine before redispatch."
        ),
        updated_at=updated_at,
    )
    return True


def _recover_stale_claimed_issue_without_dispatch_evidence(
    *,
    base_dir: Path,
    ledger: JsonObject,
    runtime_issue: dict[str, object],
    updated_at: str,
) -> bool:
    if str(runtime_issue.get("state") or "") != "claimed":
        return False
    if str(runtime_issue.get("current_session_id") or ""):
        return False
    issue = cast(dict[str, str], ledger["issue"])
    issue_number = issue.get("number", "")
    if not issue_number:
        return False
    if _has_role_stage_dispatch_evidence(
        base_dir=base_dir,
        issue_number=issue_number,
        role="main_orchestrator",
        stage="orchestrator_bootstrap",
    ):
        return False
    claimed_at = str(runtime_issue.get("claimed_at") or runtime_issue.get("updated_at") or "")
    current_time = _parse_iso8601(updated_at)
    claimed_time = _parse_iso8601(claimed_at)
    if current_time is None or claimed_time is None:
        return False
    if current_time - claimed_time <= timedelta(seconds=root_heartbeat_timeout_seconds()):
        return False

    release_issue_execution(
        base_dir=base_dir,
        issue_number=issue_number,
        restore_ready_for_agent=True,
        rollback_reason=(
            f"Issue #{issue_number} stayed in claimed without dispatch evidence since {claimed_at}; "
            "release stale claim fence back to ready so development capacity can continue."
        ),
        updated_at=updated_at,
    )
    return True


def _recover_stale_quarantined_dispatching_issue_without_live_session(
    *,
    base_dir: Path,
    ledger: JsonObject,
    runtime_issue: dict[str, object],
    updated_at: str,
) -> bool:
    if str(runtime_issue.get("state") or "") != "quarantined":
        return False
    if str(runtime_issue.get("current_session_id") or ""):
        return False

    issue = cast(dict[str, str], ledger["issue"])
    issue_number = issue.get("number", "")
    if not issue_number:
        return False

    latest_transition = read_latest_history_entry(
        base_dir,
        issue_number=issue_number,
        entry_type="state_transition",
    )
    if latest_transition is None:
        return False
    if str(latest_transition.get("to_state") or "") != "quarantined":
        return False
    if str(latest_transition.get("from_state") or "") != "dispatching":
        return False

    if _has_role_stage_dispatch_evidence(
        base_dir=base_dir,
        issue_number=issue_number,
        role="main_orchestrator",
        stage="orchestrator_bootstrap",
    ):
        return False

    latest_dispatch_result = read_latest_dispatch_result(base_dir, issue_number=issue_number)
    if latest_dispatch_result is not None:
        if str(latest_dispatch_result.get("status") or "") == "success" and str(
            latest_dispatch_result.get("rootSessionID") or ""
        ):
            return False

    command_id = f"auto-recover-quarantined-dispatching:{issue_number}:{updated_at}"
    clear_issue_execution_claim_projection(
        base_dir=base_dir,
        issue_number=issue_number,
        updated_at=updated_at,
    )
    _ = upsert_issue_state(
        base_dir,
        issue_number=issue_number,
        state="ready",
        command_id=command_id,
        updated_at=updated_at,
        current_session_id="",
    )
    clear_issue_runtime_phase_projection(
        base_dir=base_dir,
        issue_number=issue_number,
        updated_at=updated_at,
    )
    record_admin_decision(
        base_dir,
        command_id=f"{command_id}:admin",
        issue_number=issue_number,
        decision_type="admin_auto_recover_quarantined_dispatching_orphan",
        reason=(
            f"Auto-recover issue #{issue_number} from quarantined to ready after confirming "
            "stale dispatching quarantine has no live root session and no dispatch evidence."
        ),
        updated_at=updated_at,
        from_state="quarantined",
        to_state="ready",
    )
    sync_error = _sync_projected_issue_labels(
        base_dir=base_dir,
        issue_number=issue_number,
        command_id=f"{command_id}:labels",
        updated_at=updated_at,
    )
    if sync_error:
        record_admin_decision(
            base_dir,
            command_id=f"{command_id}:labels:admin-failed",
            issue_number=issue_number,
            decision_type="admin_github_projection_failure",
            reason=(
                f"GitHub projected label sync failed after quarantined dispatching auto-recovery for issue #{issue_number}: "
                f"{sync_error}"
            ),
            updated_at=updated_at,
        )
    return True


def _quarantine_stale_queued_subagent_with_stale_root(
    *,
    base_dir: Path,
    ledger: JsonObject,
    current: dict[str, str],
    runtime_issue: dict[str, object],
    updated_at: str,
) -> bool:
    if current.get("status") != "queued":
        return False
    if current.get("role") not in {"issue_worker", "pr_verifier"} and not (
        current.get("role") == "main_orchestrator" and current.get("stage") == "release_root_execution"
    ):
        return False
    if str(runtime_issue.get("state") or "") not in {"running", "verifying", "release_pending"}:
        return False
    root_session_id = str(runtime_issue.get("current_session_id") or "")
    last_event_at = str(runtime_issue.get("last_event_at") or "")
    if not root_session_id or not last_event_at:
        return False
    current_time = _parse_iso8601(updated_at)
    last_event_time = _parse_iso8601(last_event_at)
    if current_time is None or last_event_time is None:
        return False
    if current_time - last_event_time <= timedelta(seconds=root_heartbeat_timeout_seconds()):
        return False

    issue = cast(dict[str, str], ledger["issue"])
    quarantine_issue_execution(
        base_dir=base_dir,
        issue_number=issue["number"],
        reason=(
            f"Queued {current['role']} for issue #{issue['number']} outlived root session heartbeat {root_session_id} after last event at {last_event_at}; "
            "treat it as an orphaned queued subagent and quarantine before redispatch."
        ),
        updated_at=updated_at,
    )
    return True


def _refresh_running_issue_heartbeat_from_worker_result(
    *,
    base_dir: Path,
    issue_number: str,
    runtime_issue: dict[str, object],
    updated_at: str,
) -> bool:
    if str(runtime_issue.get("state") or "") != "running":
        return False
    current_session_id = str(runtime_issue.get("current_session_id") or "")
    if not current_session_id:
        return False
    worker_result = _read_db_artifact_fact(base_dir=base_dir, issue_number=issue_number, artifact_kind="worker_result")
    if not bool(worker_result.get("parse_ok")):
        return False
    completed_at = str(worker_result.get("completed_at") or "")
    if not completed_at:
        return False
    completed_at_time = _parse_iso8601(completed_at)
    if completed_at_time is None:
        return False
    last_event_at = str(runtime_issue.get("last_event_at") or "")
    last_event_time = _parse_iso8601(last_event_at) if last_event_at else None
    if last_event_time is not None and completed_at_time <= last_event_time:
        return False
    upsert_issue_state(
        base_dir,
        issue_number=issue_number,
        state="running",
        command_id=f"worker-result-heartbeat:{issue_number}:{completed_at}",
        updated_at=completed_at,
        current_session_id=current_session_id,
    )
    return True


def _projection_updated_at_from_worker_result(
    *,
    base_dir: Path,
    issue_number: str,
    fallback_updated_at: str,
) -> str:
    worker_result = _read_db_artifact_fact(base_dir=base_dir, issue_number=issue_number, artifact_kind="worker_result")
    completed_at = str(worker_result.get("completed_at") or "").strip()
    completed_time = _parse_iso8601(completed_at)
    if completed_time is None:
        return fallback_updated_at

    fallback_time = _parse_iso8601(fallback_updated_at)
    if fallback_time is None:
        return completed_at
    if completed_time <= fallback_time:
        return completed_at
    return fallback_updated_at


def _record_current_verifier_session(
    *,
    base_dir: Path,
    issue_number: str,
    verifier_session_id: str,
    updated_at: str,
    fallback_state: str = "verifying",
) -> None:
    if not verifier_session_id:
        return
    issue_state = read_issue(base_dir, issue_number)
    state = str(issue_state.get("state") or fallback_state) if issue_state else fallback_state
    upsert_issue_state(
        base_dir,
        issue_number=issue_number,
        state=state,
        command_id=uuid4().hex,
        updated_at=updated_at,
        current_session_id=verifier_session_id,
    )

def has_issue_execution_lock(base_dir: Path, issue_number: str) -> bool:
    has_lock = cast(Callable[[Path, str], bool], _lifecycle_helpers.has_issue_execution_lock)
    return has_lock(base_dir, issue_number)


def update_issue_execution_claim(*, base_dir: Path, issue_number: str, updates: JsonObject) -> None:
    update_claim = cast(Callable[..., None], _lifecycle_helpers.update_issue_execution_claim)
    update_claim(base_dir=base_dir, issue_number=issue_number, updates=updates, now=_now)


def clear_issue_execution_claim_projection(*, base_dir: Path, issue_number: str, updated_at: str) -> None:
    clear_claim = cast(Callable[..., None], _lifecycle_helpers.clear_issue_execution_claim_projection)
    clear_claim(base_dir=base_dir, issue_number=issue_number, updated_at=updated_at)


def clear_issue_runtime_phase_projection(*, base_dir: Path, issue_number: str, updated_at: str) -> None:
    clear_phase = cast(Callable[..., None], _lifecycle_helpers.clear_issue_runtime_phase_projection)
    clear_phase(base_dir=base_dir, issue_number=issue_number, updated_at=updated_at)


def _sync_issue_progress_label(
    *,
    base_dir: Path,
    issue_number: str,
    add_labels: list[str],
    remove_labels: list[str],
    command_id: str | None = None,
    updated_at: str | None = None,
) -> str:
    sync_labels = cast(Callable[..., str], _lifecycle_helpers.sync_issue_progress_label)
    return sync_labels(
        base_dir=base_dir,
        issue_number=issue_number,
        repo=_read_project_github_repo(base_dir),
        add_labels=add_labels,
        remove_labels=remove_labels,
        now=_now,
        run=subprocess.run,
        command_id=command_id,
        updated_at=updated_at,
    )


def _projected_issue_state_and_workflows(
    *,
    base_dir: Path,
    issue_number: str,
) -> tuple[str, str, str]:
    issue = read_issue(base_dir, issue_number) or {}
    issue_state = str(issue.get("state") or "")
    pr_opened = read_latest_history_entry(base_dir, issue_number=issue_number, entry_type="pr_opened")
    has_pr_opened = pr_opened is not None

    evidence_packet = _read_db_artifact_fact(base_dir=base_dir, issue_number=issue_number, artifact_kind="evidence_packet")
    evidence_status = str(evidence_packet.get("status") or "").strip().lower()

    release_result = _read_db_artifact_fact(base_dir=base_dir, issue_number=issue_number, artifact_kind="release_result")
    release_status = str(release_result.get("status") or "").strip().lower()
    merge_payload_raw = release_result.get("merge")
    merge_payload = cast(dict[str, object], merge_payload_raw) if isinstance(merge_payload_raw, dict) else {}
    release_merged = bool(release_result.get("merged")) or bool(merge_payload.get("merged"))

    projection = issue_projection(
        issue_state=issue_state,
        has_pr_opened=has_pr_opened,
        evidence_status=evidence_status,
        release_status=release_status,
        release_merged=release_merged,
        config=load_state_projection_config(base_dir),
    )
    return issue_state, projection.team_workflow, projection.pr_workflow_state


def _sync_projected_issue_labels(
    *,
    base_dir: Path,
    issue_number: str,
    command_id: str | None = None,
    updated_at: str | None = None,
) -> str:
    issue = read_issue(base_dir, issue_number) or {}
    pr_opened = read_latest_history_entry(base_dir, issue_number=issue_number, entry_type="pr_opened")
    has_pr_opened = pr_opened is not None
    evidence_packet = _read_db_artifact_fact(base_dir=base_dir, issue_number=issue_number, artifact_kind="evidence_packet")
    evidence_status = str(evidence_packet.get("status") or "").strip().lower()
    release_result = _read_db_artifact_fact(base_dir=base_dir, issue_number=issue_number, artifact_kind="release_result")
    release_status = str(release_result.get("status") or "").strip().lower()
    merge_payload_raw = release_result.get("merge")
    merge_payload = cast(dict[str, object], merge_payload_raw) if isinstance(merge_payload_raw, dict) else {}
    release_merged = bool(release_result.get("merged")) or bool(merge_payload.get("merged"))
    projection = issue_projection(
        issue_state=str(issue.get("state") or ""),
        has_pr_opened=has_pr_opened,
        evidence_status=evidence_status,
        release_status=release_status,
        release_merged=release_merged,
        config=load_state_projection_config(base_dir),
    )
    add_labels, remove_labels = label_delta_for_projection(projection)
    return _sync_issue_progress_label(
        base_dir=base_dir,
        issue_number=issue_number,
        add_labels=add_labels,
        remove_labels=remove_labels,
        command_id=command_id,
        updated_at=updated_at,
    )


def _sync_projected_issue_labels_or_raise(
    *,
    base_dir: Path,
    issue_number: str,
    command_id: str,
    updated_at: str,
) -> None:
    sync_error = _sync_projected_issue_labels(
        base_dir=base_dir,
        issue_number=issue_number,
        command_id=command_id,
        updated_at=updated_at,
    )
    if sync_error:
        raise RuntimeError(f"failed to sync projected issue labels for issue #{issue_number}: {sync_error}")


def _render_issue_body_projection_markdown(*, base_dir: Path, issue_number: str, updated_at: str) -> str:
    issue = read_issue(base_dir, issue_number) or {}
    issue_packet = _load_json_dict(issue.get("issue_packet_json"))

    state = str(issue.get("state") or "")
    role = str(issue.get("current_role") or "")
    stage = str(issue.get("current_stage") or "")
    status = str(issue.get("current_status") or "")
    dependencies_raw = issue_packet.get("dependencies")
    dependencies = [str(item).strip() for item in dependencies_raw] if isinstance(dependencies_raw, list) else []
    dependencies = [item for item in dependencies if item]
    parent_reference = str(issue_packet.get("parent_reference") or "none")

    def _db_ref(entry_type: str) -> str:
        ref = read_latest_ref(base_dir, issue_number, entry_type)
        command_id = str(ref.get("command_id") or "")
        history_id = str(ref.get("history_id") or "")
        pointer = command_id or history_id
        if not pointer:
            return "none"
        return f"db:issue-history/{entry_type}:{issue_number}:{pointer}"

    dependencies_display = ", ".join(f"#{dep}" if dep.isdigit() else dep for dep in dependencies) if dependencies else "none"
    lines = [
        "## Autodev status snapshot",
        f"- state: {state or 'unknown'}",
        f"- role/stage/status: {role or 'n/a'} / {stage or 'n/a'} / {status or 'n/a'}",
        f"- parent reference: {parent_reference}",
        f"- dependencies: {dependencies_display}",
        f"- latest pr ref: {_db_ref('pr_opened')}",
        f"- latest evidence ref: {_db_ref('evidence_packet')}",
        f"- latest release ref: {_db_ref('release_result')}",
        f"- updated_at: {updated_at}",
    ]
    return "\n".join(lines)


def _sync_issue_body_projection(
    *,
    base_dir: Path,
    issue_number: str,
    command_id: str | None = None,
    updated_at: str | None = None,
) -> str:
    sync_body = cast(Callable[..., str], _lifecycle_helpers.sync_issue_body_projection)
    timestamp = _now(updated_at)
    try:
        return sync_body(
            base_dir=base_dir,
            issue_number=issue_number,
            repo=_read_project_github_repo(base_dir),
            projection_markdown=_render_issue_body_projection_markdown(
                base_dir=base_dir,
                issue_number=issue_number,
                updated_at=timestamp,
            ),
            now=_now,
            run=subprocess.run,
            command_id=command_id,
            updated_at=timestamp,
        )
    except Exception as error:  # pragma: no cover - defensive subprocess/env guard
        return str(error)


def _sync_issue_status_comment(
    *,
    base_dir: Path,
    issue_number: str,
    command_id: str | None = None,
    updated_at: str | None = None,
) -> str:
    sync_comment = cast(Callable[..., str], _lifecycle_helpers.sync_issue_status_comment)
    timestamp = _now(updated_at)
    issue = read_issue(base_dir, issue_number) or {}
    body_lines = [
        "## Autodev status",
        f"- state: {str(issue.get('state') or 'unknown')}",
        (
            "- role/stage/status: "
            f"{str(issue.get('current_role') or 'n/a')} / "
            f"{str(issue.get('current_stage') or 'n/a')} / "
            f"{str(issue.get('current_status') or 'n/a')}"
        ),
        f"- updated_at: {timestamp}",
    ]
    try:
        return sync_comment(
            base_dir=base_dir,
            issue_number=issue_number,
            repo=_read_project_github_repo(base_dir),
            comment_markdown="\n".join(body_lines),
            now=_now,
            run=subprocess.run,
            command_id=command_id,
            updated_at=timestamp,
        )
    except Exception as error:  # pragma: no cover - defensive subprocess/env guard
        return str(error)


def _sync_project_fields_projection(
    *,
    base_dir: Path,
    issue_number: str,
    command_id: str | None = None,
    updated_at: str | None = None,
) -> str:
    sync_fields = cast(Callable[..., str], _lifecycle_helpers.sync_project_fields_projection)
    timestamp = _now(updated_at)
    runtime_context = read_runtime_context(base_dir, issue_number) or {}
    configured = _load_json_dict(runtime_context.get("github_project_field_ids"))
    configured_fallback = _configured_project_field_ids(base_dir)
    fields: dict[str, str] = {}
    state_field_id = str(configured.get("state") or configured_fallback.get("state") or "").strip()
    stage_field_id = str(configured.get("stage") or configured_fallback.get("stage") or "").strip()
    pr_workflow_field_id = str(configured.get("pr_workflow") or configured_fallback.get("pr_workflow") or "").strip()

    issue = read_issue(base_dir, issue_number) or {}
    issue_state, _team_workflow_state, pr_workflow_status = _projected_issue_state_and_workflows(
        base_dir=base_dir,
        issue_number=issue_number,
    )
    if state_field_id:
        fields[state_field_id] = issue_state
    if stage_field_id:
        fields[stage_field_id] = str(issue.get("current_stage") or "")
    if pr_workflow_field_id:
        fields[pr_workflow_field_id] = pr_workflow_status
    try:
        return sync_fields(
            base_dir=base_dir,
            issue_number=issue_number,
            repo=_read_project_github_repo(base_dir),
            fields=fields,
            now=_now,
            run=subprocess.run,
            command_id=command_id,
            updated_at=timestamp,
        )
    except Exception as error:  # pragma: no cover - defensive subprocess/env guard
        return str(error)


def _project_pr_workflow_projection(base_dir: Path, issue_number: str) -> dict[str, str]:
    issue_state, _team_workflow_state, status = _projected_issue_state_and_workflows(
        base_dir=base_dir,
        issue_number=issue_number,
    )
    detail = f"projected from issue state {issue_state}"

    evidence_packet = _read_db_artifact_fact(base_dir=base_dir, issue_number=issue_number, artifact_kind="evidence_packet")
    release_result = _read_db_artifact_fact(base_dir=base_dir, issue_number=issue_number, artifact_kind="release_result")

    return {
        "status": status,
        "detail": detail,
        "prNumber": _extract_pr_number_from_artifact_fact(evidence_packet) or _extract_pr_number_from_artifact_fact(release_result),
    }


def _sync_github_projection_bundle(
    *,
    base_dir: Path,
    issue_number: str,
    phase: str,
    updated_at: str,
) -> None:
    sync_commands = {
        "labels": f"start-issue:{issue_number}:{phase}:labels",
        "body": f"start-issue:{issue_number}:{phase}:body",
        "comment": f"start-issue:{issue_number}:{phase}:status-comment",
        "project": f"start-issue:{issue_number}:{phase}:project-fields",
    }
    sync_errors: list[tuple[str, str]] = []
    label_error = _sync_projected_issue_labels(
        base_dir=base_dir,
        issue_number=issue_number,
        command_id=sync_commands["labels"],
        updated_at=updated_at,
    )
    if label_error:
        sync_errors.append(("labels", label_error))
    body_error = _sync_issue_body_projection(
        base_dir=base_dir,
        issue_number=issue_number,
        command_id=sync_commands["body"],
        updated_at=updated_at,
    )
    if body_error:
        sync_errors.append(("issue_body", body_error))
    comment_error = _sync_issue_status_comment(
        base_dir=base_dir,
        issue_number=issue_number,
        command_id=sync_commands["comment"],
        updated_at=updated_at,
    )
    if comment_error:
        sync_errors.append(("status_comment", comment_error))
    project_error = _sync_project_fields_projection(
        base_dir=base_dir,
        issue_number=issue_number,
        command_id=sync_commands["project"],
        updated_at=updated_at,
    )
    if project_error:
        sync_errors.append(("project_fields", project_error))

    for target, error in sync_errors:
        record_admin_decision(
            base_dir,
            command_id=f"start-issue:{issue_number}:{phase}:{target}:admin-failed",
            issue_number=issue_number,
            decision_type="admin_github_projection_failure",
            reason=f"GitHub projection sync failed ({target}) for issue #{issue_number}: {error}",
            updated_at=updated_at,
        )


def _close_github_issue_after_release_merge(
    *,
    base_dir: Path,
    issue_number: str,
    command_id: str | None = None,
    updated_at: str | None = None,
) -> str:
    close_issue = cast(Callable[..., str], _lifecycle_helpers.close_github_issue_after_release_merge)
    return close_issue(
        base_dir=base_dir,
        issue_number=issue_number,
        repo=_read_project_github_repo(base_dir),
        now=_now,
        run=subprocess.run,
        command_id=command_id,
        updated_at=updated_at,
    )


def _default_host_adapter() -> HostAdapter:
    factory = cast(Callable[[], HostAdapter], _session_helpers.default_host_adapter)
    return factory()


def _dispatch_launch_title(request: SessionRequest) -> str:
    request_id = str(request.get("requestID") or uuid4().hex)
    return f"{request['title']} [{request_id}]"


def _scheduler_id(base_dir: Path) -> str:
    scheduler = cast(Callable[[Path], str], _lifecycle_helpers.scheduler_id)
    return scheduler(base_dir)


def _transition_issue_state_if_possible(
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
    transition = cast(Callable[..., None], _lifecycle_helpers.transition_issue_state_if_possible)
    transition(
        base_dir=base_dir,
        issue_number=issue_number,
        to_state=to_state,
        command_id=command_id,
        updated_at=updated_at,
        reason=reason,
        from_state=from_state,
        current_session_id=current_session_id,
    )


def _rebuild_issue_state_from_runtime_phase(
    *,
    base_dir: Path,
    issue_number: str,
    desired_state: str,
    updated_at: str,
) -> None:
    sequences = {
        "running": ["ready", "claimed", "dispatching", "running"],
        "verifying": ["ready", "claimed", "dispatching", "running", "verifying"],
        "release_pending": ["ready", "claimed", "dispatching", "running", "verifying", "verified", "release_pending"],
    }
    sequence = sequences.get(desired_state)
    if sequence is None:
        return

    runtime_issue = read_issue(base_dir, issue_number)
    current_state = str(runtime_issue.get("state") or "ready") if runtime_issue else "ready"
    if current_state == desired_state or current_state == "quarantined":
        return
    if current_state not in sequence:
        raise ValueError(f"cannot rebuild issue #{issue_number} from {current_state!r} to {desired_state!r}")

    start_index = sequence.index(current_state)
    for index in range(start_index + 1, len(sequence)):
        from_state = sequence[index - 1]
        to_state = sequence[index]
        _transition_issue_state_if_possible(
            base_dir=base_dir,
            issue_number=issue_number,
            to_state=to_state,
            command_id=f"runtime-rebuild:{issue_number}:{to_state}:{index}",
            updated_at=updated_at,
            reason=f"Rebuild control-plane state for issue #{issue_number} from runtime phase into {to_state}.",
            from_state=from_state,
        )


def _sync_runtime_phase_to_control_plane_state(
    *,
    base_dir: Path,
    issue_number: str,
    ledger: JsonObject,
    current: dict[str, str],
    updated_at: str,
) -> None:
    runtime_target_state = cast(Callable[[dict[str, str]], str], _policy_helpers.runtime_target_state)
    desired_state = runtime_target_state(current)

    if not desired_state:
        return

    runtime_issue = read_issue(base_dir, issue_number)
    current_state = str(runtime_issue.get("state") or "") if runtime_issue else ""
    current_session_id = str(runtime_issue.get("current_session_id") or "") if runtime_issue else ""
    last_session_result = cast(JsonObject, ledger.get("lastSessionResult", {}))
    session_result_root_session_id = str(last_session_result.get("rootSessionID") or "")
    if current_state in {"quarantined", "completed", "failed"}:
        return
    if current_state == desired_state:
        return
    if (
        current["role"] == "issue_worker"
        and current.get("status") == "queued"
        and current_state == "dispatching"
        and not current_session_id
        and not session_result_root_session_id
    ):
        return
    if desired_state == "running" and not current_session_id and not session_result_root_session_id:
        return
    _rebuild_issue_state_from_runtime_phase(
        base_dir=base_dir,
        issue_number=issue_number,
        desired_state=desired_state,
        updated_at=updated_at,
    )


def _sync_runtime_phase_metadata(
    *,
    base_dir: Path,
    issue_number: str,
    current: dict[str, str],
    attempts: dict[str, int],
    limits: dict[str, int],
    last_failure: dict[str, object],
    workflow: dict[str, object],
    automation: dict[str, object],
    artifacts: dict[str, object],
    queued_next_issue: dict[str, object] | None = None,
    updated_at: str,
) -> None:
    normalized_current, runtime_context = _normalize_runtime_phase_projection(
        base_dir=base_dir,
        issue_number=issue_number,
        current=current,
        queued_next_issue=queued_next_issue,
    )

    merged_runtime_context = dict(runtime_context or {})
    recovery_cursor = cast(dict[str, object], last_failure.get("recovery_cursor", {})) if isinstance(last_failure.get("recovery_cursor"), dict) else {}
    merged_runtime_context["failure_context"] = dict(last_failure)
    if recovery_cursor:
        merged_runtime_context["recovery_cursor"] = dict(recovery_cursor)
    else:
        merged_runtime_context["recovery_cursor"] = None

    _ = sync_issue_runtime_context(
        base_dir,
        issue_number=issue_number,
        updated_at=updated_at,
        current_role=normalized_current.get("role", ""),
        current_stage=normalized_current.get("stage", ""),
        current_status=normalized_current.get("status", ""),
        attempts=attempts,
        limits=limits,
        last_failure=last_failure,
        resume_snapshot=workflow,
        runtime_context=merged_runtime_context,
        automation_flags=automation,
        artifact_refs=artifacts,
    )


def _normalize_runtime_phase_projection(
    *,
    base_dir: Path,
    issue_number: str,
    current: dict[str, str],
    queued_next_issue: dict[str, object] | None,
) -> tuple[dict[str, str], dict[str, object] | None]:
    normalized_current = dict(current)
    runtime_issue = read_issue(base_dir, issue_number) or {}
    runtime_state = str(runtime_issue.get("state") or "")
    current_projection = (
        normalized_current.get("role", ""),
        normalized_current.get("stage", ""),
        normalized_current.get("status", ""),
    )

    if runtime_state in RUNTIME_PHASE_PROJECTION_CLEAR_STATES:
        normalized_current = {"role": "", "stage": "", "status": ""}
    else:
        allowed_projections = RUNTIME_PHASE_PROJECTION_WHITELISTS.get(runtime_state)
        if allowed_projections is not None and current_projection not in allowed_projections:
            normalized_current = {"role": "", "stage": "", "status": ""}

    runtime_context: dict[str, object] | None = None
    if queued_next_issue:
        runtime_context = {"queuedNextIssue": queued_next_issue}
    else:
        existing_runtime_context = read_runtime_context(base_dir, issue_number)
        if isinstance(existing_runtime_context, dict) and "queuedNextIssue" in existing_runtime_context:
            runtime_context = {"queuedNextIssue": None}
    return normalized_current, runtime_context


def _json_dict(raw: object) -> dict[str, object]:
    return cast(dict[str, object], raw) if isinstance(raw, dict) else {}


def _load_json_dict(raw: object) -> dict[str, object]:
    if isinstance(raw, str):
        try:
            return _json_dict(json.loads(raw))
        except json.JSONDecodeError:
            return {}
    return _json_dict(raw)


def _to_int(raw: object, default: int) -> int:
    if isinstance(raw, bool):
        return int(raw)
    if isinstance(raw, int):
        return raw
    if isinstance(raw, float):
        return int(raw)
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return default
        try:
            return int(text)
        except ValueError:
            return default
    return default


def _normalize_role_attempt_counters(raw: dict[str, object]) -> dict[str, int]:
    normalized: dict[str, int] = {
        str(key): _to_int(value, 0)
        for key, value in raw.items()
        if str(key or "").strip()
    }
    for role, fallback in ROLE_COUNTER_DEFAULTS.items():
        normalized.setdefault(role, fallback)
    return normalized


def _normalize_role_attempt_limits(raw: dict[str, object]) -> dict[str, int]:
    normalized: dict[str, int] = {
        str(key): _to_int(value, MAX_ROLE_ATTEMPTS)
        for key, value in raw.items()
        if str(key or "").strip()
    }
    for role, fallback in ROLE_LIMIT_DEFAULTS.items():
        candidate = normalized.get(role, fallback)
        normalized[role] = candidate if candidate > 0 else fallback
    return normalized


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


def _read_autodev_config(base_dir: Path) -> dict[str, object]:
    config_path = _canonical_supervisor_base_dir(base_dir) / ".autodev.yaml"
    if not config_path.exists():
        return {}
    try:
        parsed = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError):
        return {}
    return cast(dict[str, object], parsed) if isinstance(parsed, dict) else {}


def _configured_project_field_ids(base_dir: Path) -> dict[str, str]:
    return _string_map(_read_autodev_config(base_dir).get("github_project_field_ids"))


def _configured_project_id(base_dir: Path) -> str:
    return str(_read_autodev_config(base_dir).get("github_project_id") or "").strip()


def _project_fields_sync_enabled(*, base_dir: Path, issue_number: str) -> bool:
    runtime_context = read_runtime_context(base_dir, issue_number) or {}
    runtime_field_ids = _string_map(runtime_context.get("github_project_field_ids"))
    configured_field_ids = _configured_project_field_ids(base_dir)
    has_field_ids = any(
        str(runtime_field_ids.get(key) or configured_field_ids.get(key) or "").strip()
        for key in ("state", "stage", "pr_workflow")
    )
    if not has_field_ids:
        return False

    project_id = str(runtime_context.get("github_project_id") or "").strip()
    if not project_id:
        project_id = str(os.environ.get("AUTODEV_GITHUB_PROJECT_ID", "")).strip()
    if not project_id:
        project_id = _configured_project_id(base_dir)
    return bool(project_id)


def _db_issue_to_ledger(issue: dict[str, object], *, runtime_context: dict[str, object]) -> JsonObject:
    issue_number = str(issue.get("issue_number") or "")
    issue_packet = _load_json_dict(issue.get("issue_packet_json"))
    attempts = _normalize_role_attempt_counters(_load_json_dict(issue.get("attempts_json")))
    limits = _normalize_role_attempt_limits(_load_json_dict(issue.get("limits_json")))
    last_failure = _load_json_dict(issue.get("last_failure_json"))
    resume_snapshot = _load_json_dict(issue.get("resume_snapshot_json"))
    automation_flags = _load_json_dict(issue.get("automation_flags_json"))
    artifact_refs = _load_json_dict(issue.get("artifact_refs_json"))
    resolved_base_branch = str(runtime_context.get("resolved_base_branch") or issue_packet.get("resolved_base_branch") or issue_packet.get("base_branch") or "main")
    issue_worktree_path = str(
        runtime_context.get("issue_worktree_path")
        or issue.get("worktree_path")
        or automation_flags.get("primaryWorkspaceRoot")
        or ""
    )
    ledger: JsonObject = {
        "schemaVersion": "1.0",
        "automation": {
            "continueWithoutHuman": bool(automation_flags.get("continueWithoutHuman", True)),
            "queueNextSessionOnIdle": bool(automation_flags.get("queueNextSessionOnIdle", True)),
            "primaryWorkspaceRoot": issue_worktree_path,
            "rootSessionAgent": str(automation_flags.get("rootSessionAgent") or DEFAULT_ROOT_SESSION_AGENT),
            "supervisorDocPath": str(automation_flags.get("supervisorDocPath") or DEFAULT_SUPERVISOR_DOC_PATH),
        },
        "issue": {
            "number": issue_number,
            "title": str(issue.get("title") or issue_packet.get("title") or ""),
            "branch": str(issue.get("branch") or issue_packet.get("branch") or ""),
            "baseBranch": resolved_base_branch,
            "backingType": str(issue_packet.get("backing_type") or "github"),
            "priorHandoffPath": str(issue_packet.get("prior_handoff") or ""),
            "parentReference": str(issue_packet.get("parent_reference") or ""),
        },
        "workflow": resume_snapshot,
        "artifacts": artifact_refs,
        "current": {
            "role": str(issue.get("current_role") or ""),
            "stage": str(issue.get("current_stage") or ""),
            "status": str(issue.get("current_status") or ""),
        },
        "attempts": attempts,
        "limits": limits,
        "lastFailure": last_failure,
        "lastSessionResult": {},
        "history": [],
        "ledgerRevision": str(issue.get("updated_at") or issue.get("last_event_at") or ""),
        "updatedAt": str(issue.get("updated_at") or issue.get("last_event_at") or ""),
        "runtimeContext": runtime_context,
    }
    queued_next_issue = _json_dict(runtime_context.get("queuedNextIssue"))
    if queued_next_issue:
        ledger["queuedNextIssue"] = queued_next_issue

    workflow = cast(dict[str, object], ledger.get("workflow", {}))
    workflow_policy_path = str(workflow.get("workflowPolicyPath") or "").strip()
    if not workflow_policy_path:
        workflow["workflowPolicyPath"] = DEFAULT_WORKFLOW_POLICY_PATH
    release_template_path = str(workflow.get("releaseResultTemplatePath") or "").strip()
    if not release_template_path:
        workflow["releaseResultTemplatePath"] = DEFAULT_RELEASE_RESULT_TEMPLATE_PATH
    ledger["workflow"] = workflow

    return ledger


def reconcile_issue_from_db(*, base_dir: Path, issue_number: str, updated_at: str | None = None) -> tuple[JsonObject, SupervisorDecision, SessionRequest | None]:
    base_dir = _canonical_supervisor_base_dir(base_dir)
    issue = read_issue(base_dir, issue_number)
    if issue is None:
        raise RuntimeError(f"issue #{issue_number} not found in control plane")
    runtime_context = read_runtime_context(base_dir, issue_number)
    return reconcile_ledger(
        _db_issue_to_ledger(cast(dict[str, object], issue), runtime_context=cast(dict[str, object], runtime_context)),
        artifact_base_dir=base_dir,
        updated_at=updated_at,
    )


def reconcile_workspace_from_db(
    *,
    base_dir: Path,
    updated_at: str | None = None,
    source_session_id: str = "workspace_reconcile",
) -> JsonObject:
    base_dir = _canonical_supervisor_base_dir(base_dir)
    _ensure_control_plane_db_with_diagnostic(
        base_dir=base_dir,
        command_label="reconcile",
        allow_create=False,
    )
    timestamp = _now(updated_at)
    active_states = ["claimed", "dispatching", "running", "verifying", "verified", "release_pending", "quarantined", "failed"]
    active_issues = list_issues(base_dir, states=active_states)
    issue_results: list[JsonObject] = []
    for issue in active_issues:
        issue_number = str(issue.get("issue_number") or "")
        if not issue_number:
            continue
        ledger, decision, request = reconcile_issue_from_db(base_dir=base_dir, issue_number=issue_number, updated_at=timestamp)
        issue_results.append(
            {
                "issue_number": issue_number,
                "decision": decision,
                "request": request,
                "current": ledger.get("current", {}),
            }
        )

    intake_error = ""
    try:
        intake_ok = run_issue_packet_intake(base_dir)
    except Exception as error:  # pragma: no cover - defensive best-effort guard
        intake_ok = False
        intake_error = str(error)

    if intake_ok:
        for ready_issue in list_issues(base_dir, states=["ready"]):
            ready_issue_number = str(ready_issue.get("issue_number") or "")
            if not ready_issue_number:
                continue
            if not _project_fields_sync_enabled(base_dir=base_dir, issue_number=ready_issue_number):
                continue
            project_error = _sync_project_fields_projection(
                base_dir=base_dir,
                issue_number=ready_issue_number,
                command_id=f"intake:{ready_issue_number}:ready:project-fields",
                updated_at=timestamp,
            )
            if project_error:
                record_admin_decision(
                    base_dir,
                    command_id=f"intake:{ready_issue_number}:ready:project-fields:admin-failed",
                    issue_number=ready_issue_number,
                    decision_type="admin_github_projection_failure",
                    reason=(
                        "GitHub project field sync failed after intake for issue "
                        f"#{ready_issue_number}: {project_error}"
                    ),
                    updated_at=timestamp,
                )

    started_issues: list[JsonObject] = []
    capacity = _development_capacity()
    current_issue = {"number": "", "parentReference": ""}
    ready_candidates = select_issue_candidates_for_capacity(
        base_dir,
        current_issue_number=current_issue["number"],
        current_parent_reference=current_issue["parentReference"],
        development_capacity=capacity,
    )
    free_slots = available_development_slots(base_dir, capacity)
    for candidate in ready_candidates[:free_slots]:
        session_result = start_issue(
            base_dir=base_dir,
            issue_number=candidate.issue_number,
            source_session_id=source_session_id,
            updated_at=timestamp,
        )
        started_issues.append(
            {
                "issue_number": candidate.issue_number,
                "branch": candidate.branch,
                "session_result": session_result,
            }
        )

    release_capacity = _release_capacity()
    release_backfill_mode = _release_backfill_mode()
    auto_release_approval_mode = _auto_release_approval_mode()
    started_releases: list[JsonObject] = []
    release_backfill_errors: list[JsonObject] = []
    if release_backfill_mode == "auto":
        _ = _recover_approved_merged_release_pending_issues(base_dir=base_dir, updated_at=timestamp)
        free_release_slots = available_release_slots(base_dir, release_capacity)
        release_candidates: list[str] = []
        for issue in list_issues(base_dir, states=["verified"]):
            issue_number = str(issue.get("issue_number") or "")
            if issue_number:
                release_candidates.append(issue_number)
        for issue in list_issues(base_dir, states=["release_pending"]):
            issue_number = str(issue.get("issue_number") or "")
            if not issue_number or issue_number in release_candidates:
                continue
            if str(issue.get("current_session_id") or "") or str(issue.get("current_status") or ""):
                continue
            release_candidates.append(issue_number)
        for issue_number in release_candidates:
            if free_release_slots <= 0:
                break
            try:
                session_result = start_release(
                    base_dir=base_dir,
                    issue_number=issue_number,
                    source_session_id=_release_backfill_source_session_id(source_session_id),
                    approval_override_mode=(
                        "bypass_approval" if auto_release_approval_mode == "bypass_approval" else None
                    ),
                    override_source=(
                        "workspace_reconcile_auto_release"
                        if auto_release_approval_mode == "bypass_approval"
                        else None
                    ),
                    human_approval_skipped=auto_release_approval_mode == "bypass_approval",
                    updated_at=timestamp,
                    start_reason=f"Workspace reconcile auto-backfill claimed issue #{issue_number} for release_worker dispatch.",
                )
            except RuntimeError as error:
                release_backfill_errors.append({"issue_number": issue_number, "error": str(error)})
                continue
            started_releases.append(
                {
                    "issue_number": issue_number,
                    "session_result": session_result,
                }
            )
            free_release_slots -= 1

    return {
        "status": "success",
        "intake_status": "success" if intake_ok else "failed",
        "intake_error": intake_error,
        "development_capacity": capacity,
        "release_capacity": release_capacity,
        "release_backfill_mode": release_backfill_mode,
        "auto_release_approval_mode": auto_release_approval_mode,
        "active_issue_numbers": [str(issue.get("issue_number") or "") for issue in active_issues],
        "reconciled_issues": issue_results,
        "started_issues": started_issues,
        "started_releases": started_releases,
        "release_backfill_errors": release_backfill_errors,
    }


def _select_release_issue_number(base_dir: Path, requested_issue_number: str | None) -> str:
    select_release_issue_number = cast(Callable[..., str], _policy_helpers.select_release_issue_number)
    normalized_request = _normalize_requested_issue_number(requested_issue_number) if requested_issue_number else None
    verified = [str(issue.get("issue_number") or "") for issue in list_issues(base_dir, states=["verified"]) if str(issue.get("issue_number") or "")]
    idle_release_pending = [
        issue
        for issue in list_issues(base_dir, states=["release_pending"])
        if not str(issue.get("current_session_id") or "") and not str(issue.get("current_status") or "")
    ]
    idle_release_pending_numbers = [str(issue.get("issue_number") or "") for issue in idle_release_pending if str(issue.get("issue_number") or "")]
    return select_release_issue_number(
        requested_issue_number=normalized_request,
        verified_issue_numbers=verified,
        idle_release_pending_issue_numbers=idle_release_pending_numbers,
    )


def start_release(
    *,
    base_dir: Path,
    issue_number: str | None = None,
    source_session_id: str,
    approval_override_mode: str | None = None,
    override_source: str | None = None,
    human_approval_skipped: bool | None = None,
    updated_at: str | None = None,
    start_reason: str | None = None,
) -> SessionResult:
    base_dir = _canonical_supervisor_base_dir(base_dir)
    _ensure_control_plane_db_with_diagnostic(
        base_dir=base_dir,
        command_label="release",
        allow_create=False,
    )
    timestamp = _now(updated_at)
    _ = _repair_stale_release_pending_fences(base_dir=base_dir, updated_at=timestamp)
    if issue_number:
        _ = _recover_approved_merged_release_pending_issue(
            base_dir=base_dir,
            issue_number=_normalize_requested_issue_number(issue_number),
            updated_at=timestamp,
        )
    else:
        _ = _recover_approved_merged_release_pending_issues(base_dir=base_dir, updated_at=timestamp)
    release_capacity = _release_capacity()
    if available_release_slots(base_dir, release_capacity) <= 0:
        raise RuntimeError(f"release capacity is full ({release_capacity}); wait for an active release_worker to finish")
    selected_issue_number = _select_release_issue_number(base_dir, issue_number)
    issue = read_issue(base_dir, selected_issue_number)
    if issue is None:
        raise RuntimeError(f"issue #{selected_issue_number} not found in control plane")
    state = str(issue.get("state") or "")
    current_session_id = str(issue.get("current_session_id") or "")
    current_status = str(issue.get("current_status") or "")
    pr_opened = _ensure_release_pr_opened_fact(
        base_dir=base_dir,
        issue_number=selected_issue_number,
        updated_at=timestamp,
    )
    if pr_opened is None:
        raise RuntimeError(f"issue #{selected_issue_number} has no verifier-owned pr_opened fact; release command refuses to merge")
    release_reason = start_reason or f"Independent release command claimed issue #{selected_issue_number} for a dedicated release root session."
    release_command_id = f"release:{selected_issue_number}:claim"
    record_admin_decision(
        base_dir,
        command_id=release_command_id,
        issue_number=selected_issue_number,
        decision_type="admin_release_start",
        reason=release_reason,
        updated_at=timestamp,
        from_state=state,
        to_state="release_pending",
    )
    release_admission_decision = cast(Callable[..., str], _policy_helpers.release_admission_decision)
    admission = release_admission_decision(
        state=state,
        current_session_id=current_session_id,
        current_status=current_status,
    )
    if admission == "transition_to_release_pending":
        _transition_issue_state_if_possible(
            base_dir=base_dir,
            issue_number=selected_issue_number,
            to_state="release_pending",
            command_id=release_command_id,
            updated_at=timestamp,
            reason=release_reason,
            from_state="verified",
            current_session_id="",
        )
    elif admission == "reject_active_fence":
        raise RuntimeError(f"issue #{selected_issue_number} already has an active release_worker session fence")
    elif admission == "reject_invalid_state":
        raise RuntimeError(f"issue #{selected_issue_number} is {state or 'unknown'}; only verified or idle release_pending issues can be released")

    refreshed_issue = read_issue(base_dir, selected_issue_number)
    if refreshed_issue is None:
        raise RuntimeError(f"issue #{selected_issue_number} disappeared from control plane")
    runtime_context = read_runtime_context(base_dir, selected_issue_number)
    release_worktree = _ensure_issue_worktree(
        base_dir=base_dir,
        issue_number=selected_issue_number,
        branch=str(refreshed_issue.get("branch") or ""),
        base_branch=str(runtime_context.get("resolved_base_branch") or "main"),
        updated_at=timestamp,
    )
    ledger = _db_issue_to_ledger(cast(dict[str, object], refreshed_issue), runtime_context=cast(dict[str, object], runtime_context))
    workflow = cast(dict[str, object], ledger.get("workflow", {}))
    workflow.setdefault("workflowPolicyPath", DEFAULT_WORKFLOW_POLICY_PATH)
    workflow.setdefault("releaseResultTemplatePath", DEFAULT_RELEASE_RESULT_TEMPLATE_PATH)
    workflow["runtimeControls"] = {
        "approval_override_mode": approval_override_mode or "",
        "default_merge_approval_mode": "human_required",
        "override_source": override_source or "none",
        "human_approval_skipped": bool(human_approval_skipped),
    }
    _ = sync_issue_runtime_context(
        base_dir,
        issue_number=selected_issue_number,
        updated_at=timestamp,
        runtime_context={
            "release_runtime_controls": cast(dict[str, object], workflow["runtimeControls"]),
            "issue_worktree_path": str(release_worktree),
        },
        worktree_path=str(release_worktree),
    )
    automation = cast(dict[str, object], ledger.get("automation", {}))
    if not str(automation.get("primaryWorkspaceRoot") or ""):
        automation["primaryWorkspaceRoot"] = str(release_worktree)
    automation.setdefault("rootSessionAgent", DEFAULT_ROOT_SESSION_AGENT)
    automation.setdefault("supervisorDocPath", DEFAULT_SUPERVISOR_DOC_PATH)
    attempts = cast(dict[str, int], ledger.get("attempts", {}))
    attempts["release_worker"] = int(attempts.get("release_worker", 0)) + 1
    current = {"role": "main_orchestrator", "stage": "release_root_execution", "status": "queued"}
    ledger["current"] = current
    summary = (
        f"Release flow is launching an independent release root session for issue #{selected_issue_number}; "
        "that root session must run release_worker as a foreground subagent before returning."
    )
    _sync_runtime_phase_metadata(
        base_dir=base_dir,
        issue_number=selected_issue_number,
        current=current,
        attempts=attempts,
        limits=cast(dict[str, int], ledger.get("limits", {})),
        last_failure=cast(dict[str, object], ledger.get("lastFailure", {})),
        workflow=workflow,
        automation=automation,
        artifacts=cast(dict[str, object], ledger.get("artifacts", {})),
        queued_next_issue=cast(dict[str, object], ledger.get("queuedNextIssue", {})),
        updated_at=timestamp,
    )
    request = build_session_request(
        ledger,
        role="main_orchestrator",
        stage="release_root_execution",
        reason=f"independent release root-session dispatch for issue #{selected_issue_number}",
        title=f"Release issue #{selected_issue_number} on {str(refreshed_issue.get('branch') or '')}",
        decision_summary=summary,
    )
    _record_dispatch_request_history(base_dir=base_dir, request=request, created_at=str(request.get("createdAt") or timestamp))
    return dispatch_request_from_db(
        request,
        base_dir=base_dir,
        source_session_id=source_session_id,
        updated_at=timestamp,
        failure_restore_state="release_pending",
    )


def _run_reconcile_issue_cli(*, base_dir: Path, issue_number: str, updated_at: str | None) -> int:
    _, decision, request = reconcile_issue_from_db(base_dir=base_dir, issue_number=issue_number, updated_at=updated_at)
    payload: JsonObject = {"status": "success", "decision": decision}
    if request is not None:
        payload["request"] = request
    print(json.dumps(payload, ensure_ascii=False))
    return 0


def _run_reconcile_db_cli(*, base_dir: Path, issue_number: str, updated_at: str | None, child_only: bool) -> int:
    _ensure_control_plane_db_with_diagnostic(
        base_dir=base_dir,
        command_label="advance-child" if child_only else "reconcile",
        allow_create=False,
    )
    if child_only:
        issue = read_issue(base_dir, issue_number)
        current_role = str(issue.get("current_role") or "") if issue is not None else ""
        if current_role not in {"issue_worker", "pr_verifier", "release_worker"}:
            print(
                f"advance-child requires the DB-backed issue to already be queued on a child role (found {current_role or 'unknown'}).",
                file=sys.stderr,
            )
            return 2
    return _run_reconcile_issue_cli(base_dir=base_dir, issue_number=issue_number, updated_at=updated_at)


def _recover_stale_bootstrap_with_worker_artifact(
    *,
    ledger: JsonObject,
    base_dir: Path,
    updated_at: str,
) -> bool:
    current = cast(dict[str, str], ledger.get("current", {}))
    if current.get("role") != "main_orchestrator" or current.get("stage") != "orchestrator_bootstrap":
        return False
    issue = cast(dict[str, str], ledger.get("issue", {}))
    if not bool(_read_db_artifact_fact(base_dir=base_dir, issue_number=str(issue.get("number") or ""), artifact_kind="worker_result").get("parse_ok")):
        return False

    history = cast(list[JsonObject], ledger.get("history", []))
    history.append(
        {
            "recordedAt": updated_at,
            "fromRole": current.get("role", "main_orchestrator"),
            "fromStage": current.get("stage", "orchestrator_bootstrap"),
            "toRole": "issue_worker",
            "toStage": "issue_worker_execution",
            "reason": (
                f"Recovered stale bootstrap ledger for issue #{issue.get('number', '')} after detecting an existing worker_result artifact."
            ),
        }
    )
    ledger["current"] = {
        "role": "issue_worker",
        "stage": "issue_worker_execution",
        "status": "queued",
    }
    attempts = cast(dict[str, int], ledger.get("attempts", {}))
    attempts["issue_worker"] = max(int(attempts.get("issue_worker", 0)), 1)
    _bump_ledger_revision(ledger, updated_at)
    ledger["updatedAt"] = updated_at
    return True


def _ledger_issue_number(ledger: JsonObject, fallback_issue_number: str) -> str:
    issue = cast(dict[str, str], ledger.get("issue", {}))
    return issue.get("number", "") or fallback_issue_number


def _sync_issue_packet_to_db(base_dir: Path, packet: IssuePacketRecord, *, updated_at: str | None = None) -> None:
    sync_packet = cast(Callable[..., None], _selection_helpers.sync_issue_packet_to_db)
    sync_packet(
        base_dir,
        packet,
        issue_packet_record_to_json=issue_packet_record_to_json,
        now=_now,
        updated_at=updated_at,
    )


def _load_issue_packet_from_db(base_dir: Path, issue_number: str) -> IssuePacketRecord | None:
    load_packet = cast(Callable[..., IssuePacketRecord | None], _selection_helpers.load_issue_packet_from_db)
    return load_packet(base_dir, issue_number, issue_packet_record_from_json=issue_packet_record_from_json)


def _canonical_artifact_base_dir(ledger: JsonObject, *, default_base_dir: Path) -> Path:
    automation = cast(dict[str, object], ledger.get("automation", {}))
    primary_workspace_root = str(automation.get("primaryWorkspaceRoot") or "")
    if not primary_workspace_root:
        return default_base_dir
    path = Path(primary_workspace_root)
    return path if path.exists() else default_base_dir


def _completed_issue_numbers(base_dir: Path) -> set[str]:
    completed_func = cast(Callable[[Path], set[str]], _selection_helpers.completed_issue_numbers_from_control_plane)
    return completed_func(base_dir)


def _queued_next_issue_fields(ledger: JsonObject) -> tuple[str, str, str]:
    queued_next_issue = cast(dict[str, object], ledger.get("queuedNextIssue", {}))
    issue_number = str(queued_next_issue.get("issue_number") or "")
    branch = str(queued_next_issue.get("branch") or "")
    base_branch = str(queued_next_issue.get("base_branch") or "")
    if issue_number:
        return issue_number, branch, base_branch

    queued_next_issue_record = cast(dict[str, object], queued_next_issue.get("record", {}))
    return (
        str(queued_next_issue_record.get("issue_number") or ""),
        str(queued_next_issue_record.get("branch") or ""),
        str(queued_next_issue_record.get("base_branch") or ""),
    )


def _dependency_issue_numbers(issue_number: str, dependencies: list[str]) -> list[str]:
    resolver = cast(Callable[[str, list[str]], list[str]], _selection_helpers.dependency_issue_numbers_for_selection)
    return resolver(issue_number, dependencies)


def select_next_issue_packet(
    base_dir: Path,
    *,
    current_issue_number: str,
    current_parent_reference: str,
) -> IssuePacketRecord | None:
    select_packet = cast(Callable[..., IssuePacketRecord | None], _selection_helpers.select_next_issue_packet)
    return select_packet(
        base_dir,
        current_issue_number=current_issue_number,
        current_parent_reference=current_parent_reference,
        issue_packet_record_from_json=issue_packet_record_from_json,
        dependency_issue_numbers=_dependency_issue_numbers,
    )


def select_next_issue_candidate(
    base_dir: Path,
    *,
    current_issue_number: str,
    current_parent_reference: str,
) -> IssueSelectionCandidate | None:
    select_candidate = cast(Callable[..., IssueSelectionCandidate | None], _selection_helpers.select_next_issue_candidate)
    return select_candidate(
        base_dir,
        current_issue_number=current_issue_number,
        current_parent_reference=current_parent_reference,
        issue_packet_record_from_json=issue_packet_record_from_json,
        dependency_issue_numbers=_dependency_issue_numbers,
    )


def select_issue_packets_for_capacity(
    base_dir: Path,
    *,
    current_issue_number: str,
    current_parent_reference: str,
    development_capacity: int | None,
) -> list[IssuePacketRecord]:
    select_packets = cast(Callable[..., list[IssuePacketRecord]], _selection_helpers.select_issue_packets_for_capacity)
    return select_packets(
        base_dir,
        current_issue_number=current_issue_number,
        current_parent_reference=current_parent_reference,
        issue_packet_record_from_json=issue_packet_record_from_json,
        dependency_issue_numbers=_dependency_issue_numbers,
        development_capacity=development_capacity,
    )


def select_issue_candidates_for_capacity(
    base_dir: Path,
    *,
    current_issue_number: str,
    current_parent_reference: str,
    development_capacity: int | None,
) -> list[IssueSelectionCandidate]:
    select_candidates = cast(
        Callable[..., list[IssueSelectionCandidate]],
        _selection_helpers.select_issue_candidates_for_capacity,
    )
    return select_candidates(
        base_dir,
        current_issue_number=current_issue_number,
        current_parent_reference=current_parent_reference,
        issue_packet_record_from_json=issue_packet_record_from_json,
        dependency_issue_numbers=_dependency_issue_numbers,
        development_capacity=development_capacity,
    )


def _resolve_issue_base_branch(base_dir: Path, packet: IssuePacketRecord) -> str:
    resolver = cast(Callable[..., str], _selection_helpers.resolve_issue_base_branch)
    return resolver(
        base_dir,
        issue_number=packet.issue_number,
        dependencies=packet.dependencies,
        default_base_branch=packet.base_branch,
        dependency_issue_numbers=_dependency_issue_numbers,
    )


def run_issue_packet_intake(base_dir: Path) -> bool:
    run_intake = cast(Callable[..., bool], _selection_helpers.run_issue_packet_intake)
    return run_intake(
        base_dir,
        read_project_github_repo=_read_project_github_repo,
        run=subprocess.run,
    )


def _read_project_github_repo(base_dir: Path) -> str:
    base_dir = _canonical_supervisor_base_dir(base_dir)
    config_path = base_dir / ".autodev.yaml"
    if not config_path.exists():
        return ""
    in_project = False
    for line in config_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        indent = len(line) - len(line.lstrip(" "))
        if indent == 0 and stripped == "project:":
            in_project = True
            continue
        if in_project and indent == 0:
            break
        if in_project and indent == 2 and stripped.startswith("github_repo:"):
            _, value = stripped.split(":", 1)
            return value.strip().strip('"')
    return ""


def claim_issue_execution(
    *,
    base_dir: Path,
    issue_number: str,
    branch: str,
    source_session_id: str,
    updated_at: str | None = None,
) -> None:
    claim = cast(Callable[..., None], _lifecycle_helpers.claim_issue_execution)
    claim(
        base_dir=base_dir,
        issue_number=issue_number,
        branch=branch,
        source_session_id=source_session_id,
        now=_now,
        sync_progress_label=_sync_projected_issue_labels,
        transition_state=_transition_issue_state_if_possible,
        updated_at=updated_at,
    )


def release_issue_execution(
    *,
    base_dir: Path,
    issue_number: str,
    restore_ready_for_agent: bool,
    rollback_reason: str = "",
    final_state: str | None = None,
    updated_at: str | None = None,
) -> None:
    release = cast(Callable[..., None], _lifecycle_helpers.release_issue_execution)
    timestamp = _now(updated_at)
    expected_target_state = str(final_state or ("ready" if restore_ready_for_agent else "failed") or "").strip() or "state-sync"
    release(
        base_dir=base_dir,
        issue_number=issue_number,
        restore_ready_for_agent=restore_ready_for_agent,
        now=_now,
        sync_progress_label=_sync_projected_issue_labels,
        sync_local_main_after_release_merge=cast(Callable[..., str], _lifecycle_helpers.sync_local_main_after_release_merge),
        close_github_issue_after_release_merge=_close_github_issue_after_release_merge,
        cleanup_issue_worktree_after_release_merge=cast(
            Callable[..., str], _lifecycle_helpers.cleanup_issue_worktree_after_release_merge
        ),
        transition_state=_transition_issue_state_if_possible,
        rollback_reason=rollback_reason,
        final_state=final_state,
        updated_at=updated_at,
    )
    if _project_fields_sync_enabled(base_dir=base_dir, issue_number=issue_number):
        runtime_issue = read_issue(base_dir, issue_number) or {}
        state_after_release = str(runtime_issue.get("state") or "").strip() or expected_target_state
        project_error = _sync_project_fields_projection(
            base_dir=base_dir,
            issue_number=issue_number,
            command_id=f"release:{issue_number}:{state_after_release}:project-fields",
            updated_at=timestamp,
        )
        if project_error:
            record_admin_decision(
                base_dir,
                command_id=f"release:{issue_number}:{state_after_release}:project-fields:admin-failed",
                issue_number=issue_number,
                decision_type="admin_github_projection_failure",
                reason=(
                    f"GitHub project field sync failed after release_issue_execution for issue #{issue_number}: "
                    f"{project_error}"
                ),
                updated_at=timestamp,
            )


def quarantine_issue_execution(
    *,
    base_dir: Path,
    issue_number: str,
    reason: str,
    updated_at: str | None = None,
) -> None:
    quarantine = cast(Callable[..., None], _lifecycle_helpers.quarantine_issue_execution)
    quarantine(
        base_dir=base_dir,
        issue_number=issue_number,
        reason=reason,
        now=_now,
        sync_progress_label=_sync_projected_issue_labels,
        transition_state=_transition_issue_state_if_possible,
        updated_at=updated_at,
    )


def resume_quarantined_issue_execution(
    *,
    base_dir: Path,
    issue_number: str,
    reason: str,
    updated_at: str | None = None,
) -> None:
    resume = cast(Callable[..., None], _lifecycle_helpers.resume_quarantined_issue_execution)
    resume(
        base_dir=base_dir,
        issue_number=issue_number,
        reason=reason,
        now=_now,
        sync_progress_label=_sync_projected_issue_labels,
        transition_state=_transition_issue_state_if_possible,
        updated_at=updated_at,
    )


def redispatch_quarantined_issue_execution(
    *,
    base_dir: Path,
    issue_number: str,
    branch: str,
    source_session_id: str,
    reason: str,
    updated_at: str | None = None,
) -> None:
    redispatch = cast(Callable[..., None], _lifecycle_helpers.redispatch_quarantined_issue_execution)
    redispatch(
        base_dir=base_dir,
        issue_number=issue_number,
        branch=branch,
        source_session_id=source_session_id,
        reason=reason,
        now=_now,
        sync_progress_label=_sync_projected_issue_labels,
        transition_state=_transition_issue_state_if_possible,
        updated_at=updated_at,
    )


def fail_quarantined_issue_execution(
    *,
    base_dir: Path,
    issue_number: str,
    reason: str,
    updated_at: str | None = None,
) -> None:
    fail = cast(Callable[..., None], _lifecycle_helpers.fail_quarantined_issue_execution)
    fail(
        base_dir=base_dir,
        issue_number=issue_number,
        reason=reason,
        now=_now,
        transition_state=_transition_issue_state_if_possible,
        release_issue=release_issue_execution,
        updated_at=updated_at,
    )


def create_initial_ledger(
    *,
    issue_packet: IssuePacketRecord,
    workflow_policy_path: str = DEFAULT_WORKFLOW_POLICY_PATH,
    primary_workspace_root: str | None = None,
    root_session_agent: str = DEFAULT_ROOT_SESSION_AGENT,
    base_branch: str | None = None,
    updated_at: str | None = None,
) -> JsonObject:
    timestamp = _now(updated_at)
    return {
        "schemaVersion": "1.0",
        "automation": {
            "continueWithoutHuman": True,
            "queueNextSessionOnIdle": True,
            "primaryWorkspaceRoot": primary_workspace_root or "",
            "rootSessionAgent": root_session_agent,
            "supervisorDocPath": DEFAULT_SUPERVISOR_DOC_PATH,
        },
        "issue": {
            "number": issue_packet.issue_number,
            "title": issue_packet.title,
            "branch": issue_packet.branch,
            "baseBranch": base_branch or issue_packet.base_branch or "main",
            "backingType": issue_packet.backing_type,
            "priorHandoffPath": issue_packet.prior_handoff,
            "parentReference": issue_packet.parent_reference,
        },
        "workflow": {
            "workflowPolicyPath": workflow_policy_path,
            "releaseResultTemplatePath": DEFAULT_RELEASE_RESULT_TEMPLATE_PATH,
        },
        "artifacts": {
            "worker_result_ref": "",
            "evidence_packet_ref": "",
            "release_result_ref": "",
        },
        "current": {
            "role": "main_orchestrator",
            "stage": "orchestrator_bootstrap",
            "status": "queued",
        },
        "attempts": {
            "main_orchestrator": 1,
            "issue_worker": 0,
            "pr_verifier": 0,
            "release_worker": 0,
            "source_session_stop": 0,
        },
        "limits": {
            "main_orchestrator": MAX_ROLE_ATTEMPTS,
            "issue_worker": MAX_ROLE_ATTEMPTS,
            "pr_verifier": MAX_ROLE_ATTEMPTS,
            "release_worker": MAX_ROLE_ATTEMPTS,
            "source_session_stop": MAX_ROLE_ATTEMPTS,
        },
        "lastFailure": {
            "kind": "none",
            "summary": "",
            "retryable": True,
            "failure_class": "",
            "artifact_kind": "",
            "owner_role": "",
            "owner_stage": "",
            "owner_state": "",
            "recovery_cursor": {},
        },
        "lastSessionResult": {},
        "history": [
            {
                "recordedAt": timestamp,
                "fromRole": "system",
                "toRole": "main_orchestrator",
                "toStage": "orchestrator_bootstrap",
                "reason": f"Initialize nonstop supervisor ledger for issue #{issue_packet.issue_number}.",
            }
        ],
        "ledgerRevision": timestamp,
        "updatedAt": timestamp,
    }


def _reject_session_request(
    request: SessionRequest,
    *,
    source_session_id: str,
    error: str,
    updated_at: str | None = None,
) -> SessionResult:
    timestamp = _now(updated_at)
    return {
        "status": "rejected",
        "sourceSessionID": source_session_id,
        "title": request["title"],
        "reason": request["reason"],
        "role": request["role"],
        "stage": request["stage"],
        "issueNumber": request["issueNumber"],
        "branch": request["branch"],
        "error": error,
        "recordedAt": timestamp,
    }


def validate_session_request_for_dispatch(
    request: SessionRequest,
    ledger: JsonObject,
    *,
    base_dir: Path,
) -> str:
    issue = cast(dict[str, str], ledger["issue"])
    queued_issue_number, queued_issue_branch, _queued_issue_base_branch = _queued_next_issue_fields(ledger)
    selected_issue_number = str(request.get("selectedIssueNumber") or "")
    selected_issue_branch = str(request.get("selectedIssueBranch") or "")
    ledger_revision = str(ledger.get("ledgerRevision") or ledger.get("updatedAt") or "")
    request_revision = str(request.get("createdForLedgerRevision", ""))
    completed = _completed_issue_numbers(base_dir)
    packet = _load_issue_packet_from_db(base_dir, request["issueNumber"])
    validate_dispatch_admission = cast(Callable[..., str], _policy_helpers.validate_dispatch_admission)
    return validate_dispatch_admission(
        request_issue_number=request["issueNumber"],
        request_branch=request["branch"],
        ledger_issue_number=issue.get("number", ""),
        ledger_branch=issue.get("branch", ""),
        request_revision=request_revision,
        ledger_revision=ledger_revision,
        queued_issue_number=queued_issue_number,
        queued_issue_branch=queued_issue_branch,
        selected_issue_number=selected_issue_number,
        selected_issue_branch=selected_issue_branch,
        role=str(request.get("role") or ""),
        stage=str(request.get("stage") or ""),
        issue_is_completed=request["issueNumber"] in completed,
        packet_exists=packet is not None,
        packet_is_ready_for_agent=bool(packet and READY_FOR_AGENT_LABEL in packet.labels),
        packet_issue_number=str(packet.issue_number) if packet else "",
    )


def _validation_ledger_from_db(*, base_dir: Path, issue_number: str) -> JsonObject | None:
    issue = read_issue(base_dir, issue_number)
    if issue is None:
        return None
    runtime_context = read_runtime_context(base_dir, issue_number)
    return _db_issue_to_ledger(cast(dict[str, object], issue), runtime_context=cast(dict[str, object], runtime_context))


def _dispatch_request_via_db(
    request: SessionRequest,
    *,
    base_dir: Path,
    source_session_id: str,
    updated_at: str | None,
    failure_restore_state: str = "ready",
) -> SessionResult:
    ensure_control_plane_db(base_dir)
    validation_ledger = _validation_ledger_from_db(
        base_dir=base_dir,
        issue_number=request["issueNumber"],
    )
    dispatch_command_id = request.get("requestID") or uuid4().hex
    dispatch_timestamp = _now(updated_at)
    _ = append_issue_history(
        base_dir,
        issue_number=request["issueNumber"],
        entry_type="session_request",
        created_at=str(request.get("createdAt") or dispatch_timestamp),
        role=request["role"],
        stage=request["stage"],
        status="queued",
        request_id=request.get("requestID", ""),
        command_id=request.get("requestID", ""),
        summary=request["reason"],
        payload=dict(request),
        unique_key=f"session-request:{request.get('requestID', '')}",
    )
    validation_error = (
        validate_session_request_for_dispatch(request, validation_ledger, base_dir=base_dir)
        if validation_ledger is not None
        else "issue not found in SQLite control plane"
    )
    if validation_error:
        session_result = _reject_session_request(
            request,
            source_session_id=source_session_id,
            error=validation_error,
            updated_at=updated_at,
        )
    else:
        runtime_issue = read_issue(base_dir, request["issueNumber"])
        runtime_state = str(runtime_issue.get("state") or "") if runtime_issue else ""
        classify_bootstrap_dispatch = cast(Callable[..., bool], _policy_helpers.is_bootstrap_dispatch)
        is_bootstrap_dispatch = classify_bootstrap_dispatch(
            role=str(request.get("role") or ""),
            stage=str(request.get("stage") or ""),
        )
        if is_bootstrap_dispatch:
            if runtime_issue is None:
                _ = ensure_issue_row(base_dir, issue_number=request["issueNumber"], state="claimed", updated_at=dispatch_timestamp)
            elif runtime_state == "ready":
                _ = upsert_issue_state(
                    base_dir,
                    issue_number=request["issueNumber"],
                    state="claimed",
                    command_id=f"{dispatch_command_id}:seed-claimed",
                    updated_at=dispatch_timestamp,
                )
            _transition_issue_state_if_possible(
                base_dir=base_dir,
                issue_number=request["issueNumber"],
                to_state="dispatching",
                command_id=dispatch_command_id,
                updated_at=dispatch_timestamp,
                reason=f"Dispatch root session request for issue #{request['issueNumber']}.",
                from_state="claimed",
            )
        session_result = dispatch_session_request(
            request,
            workdir=_issue_dispatch_workdir(
                base_dir=base_dir,
                issue_number=request["issueNumber"],
                branch=request["branch"],
                base_branch=str(request.get("baseBranch") or "main"),
                updated_at=dispatch_timestamp,
            ),
            source_session_id=source_session_id,
            updated_at=updated_at,
        )
        root_session_id = session_result.get("rootSessionID")
        if isinstance(root_session_id, str) and root_session_id and is_bootstrap_dispatch:
            recorded_at = str(session_result.get("recordedAt") or dispatch_timestamp)
            _transition_issue_state_if_possible(
                base_dir=base_dir,
                issue_number=request["issueNumber"],
                to_state="running",
                command_id=f"{dispatch_command_id}:running",
                updated_at=recorded_at,
                reason=f"Root session {root_session_id} acknowledged for issue #{request['issueNumber']}.",
                from_state="dispatching",
                current_session_id=root_session_id,
            )
            _append_root_issue_event(
                base_dir=base_dir,
                issue_number=request["issueNumber"],
                root_session_id=root_session_id,
                event_type="root_session_started",
                created_at=recorded_at,
                payload=cast(JsonObject, cast(object, dict(session_result))),
                session_seq=1,
            )
            _record_same_repo_probe_degraded_event(
                base_dir=base_dir,
                issue_number=request["issueNumber"],
                session_result=session_result,
            )
            update_issue_execution_claim(
                base_dir=base_dir,
                issue_number=request["issueNumber"],
                updates={
                    "rootSessionID": root_session_id,
                    "status": "root_session_started",
                    "recordedAt": recorded_at,
                },
            )
            sync_error = _sync_projected_issue_labels(
                base_dir=base_dir,
                issue_number=request["issueNumber"],
                command_id=f"{dispatch_command_id}:running-labels",
                updated_at=dispatch_timestamp,
            )
            if sync_error:
                resume_link = _default_host_adapter().resume_link(root_session_id)
                session_result["recommendedAction"] = (
                    f"Resume the active root session with {resume_link}. "
                    f"GitHub running-label sync failed and may need retry: {sync_error}"
                )
        classify_release_root_execution = cast(Callable[..., bool], _policy_helpers.is_release_root_execution)
        is_release_root_dispatch = classify_release_root_execution(
            role=str(request.get("role") or ""),
            stage=str(request.get("stage") or ""),
        )
        if isinstance(root_session_id, str) and root_session_id and is_release_root_dispatch:
            recorded_at = str(session_result.get("recordedAt") or dispatch_timestamp)
            _ = upsert_issue_state(
                base_dir,
                issue_number=request["issueNumber"],
                state="release_pending",
                command_id=f"{dispatch_command_id}:release-running",
                updated_at=recorded_at,
                current_session_id=root_session_id,
            )
            _ = sync_issue_runtime_context(
                base_dir,
                issue_number=request["issueNumber"],
                updated_at=recorded_at,
                current_role="main_orchestrator",
                current_stage=str(request.get("stage") or "release_root_execution"),
                current_status="running",
                )
        _ = _promote_same_repo_probe_degraded_result(session_result)
    if session_result.get("status") != "success":
        current_issue_state = read_issue(base_dir, request["issueNumber"])
        current_state = str(current_issue_state.get("state") or "") if current_issue_state else ""
        dispatch_restore_strategy = cast(Callable[..., str], _policy_helpers.dispatch_restore_strategy)
        restore_strategy = dispatch_restore_strategy(
            failure_restore_state=failure_restore_state,
            current_state=current_state,
        )
        if restore_strategy != "skip":
            failure_updated_at = str(session_result.get("recordedAt") or dispatch_timestamp)
            if restore_strategy == "quarantined":
                clear_issue_execution_claim_projection(
                    base_dir=base_dir,
                    issue_number=request["issueNumber"],
                    updated_at=failure_updated_at,
                )
                _ = upsert_issue_state(
                    base_dir,
                    issue_number=request["issueNumber"],
                    state="quarantined",
                    command_id=f"{dispatch_command_id}:quarantine-rollback",
                    updated_at=failure_updated_at,
                    current_session_id="",
                )
                _ = _sync_projected_issue_labels(
                    base_dir=base_dir,
                    issue_number=request["issueNumber"],
                    command_id=f"{dispatch_command_id}:quarantine-labels",
                    updated_at=failure_updated_at,
                )
            elif restore_strategy == "release_pending":
                _ = upsert_issue_state(
                    base_dir,
                    issue_number=request["issueNumber"],
                    state="release_pending",
                    command_id=f"{dispatch_command_id}:release-dispatch-failed",
                    updated_at=failure_updated_at,
                    current_session_id="",
                )
                _ = sync_issue_runtime_context(
                    base_dir,
                    issue_number=request["issueNumber"],
                    updated_at=failure_updated_at,
                    current_role="main_orchestrator",
                    current_stage=str(request.get("stage") or "release_root_execution"),
                    current_status="",
                )
            else:
                release_issue_execution(
                    base_dir=base_dir,
                    issue_number=request["issueNumber"],
                    restore_ready_for_agent=True,
                    updated_at=failure_updated_at,
                )
    _record_session_result_history(
        base_dir,
        validation_ledger if validation_ledger is not None else {"issue": {"number": request["issueNumber"]}},
        cast(JsonObject, cast(object, dict(session_result))),
    )
    return session_result


def dispatch_request_from_db(
    request: SessionRequest,
    *,
    base_dir: Path,
    source_session_id: str,
    updated_at: str | None = None,
    failure_restore_state: str = "ready",
) -> SessionResult:
    return _dispatch_request_via_db(
        request,
        base_dir=base_dir,
        source_session_id=source_session_id,
        updated_at=updated_at,
        failure_restore_state=failure_restore_state,
    )


def dispatch_session_request(
    request: SessionRequest,
    *,
    workdir: Path,
    source_session_id: str,
    updated_at: str | None = None,
) -> SessionResult:
    timestamp = _now(updated_at)
    launch_title = _dispatch_launch_title(request)
    adapter = _default_host_adapter()
    start_context = SessionStartContext(
        title=launch_title,
        prompt=request["prompt"],
        agent=request["agent"],
        workdir=workdir,
        source_session_id=source_session_id,
        role=request["role"],
        stage=request["stage"],
        issue_number=request["issueNumber"],
        branch=request["branch"],
        started_at_iso=timestamp,
    )
    is_release_root_execution = request.get("role") == "main_orchestrator" and request.get("stage") == "release_root_execution"
    if is_release_root_execution:
        start_result = adapter.start_child_role("release_worker", start_context)
    else:
        start_result = adapter.start_root_session(start_context)

    # Host-level prefill continuation errors can be recovered by launching a
    # fresh root session without source-session affinity once.
    if start_result.status != "success" and start_result.should_retry_without_source_session:
        retry_context = SessionStartContext(
            title=start_context.title,
            prompt=start_context.prompt,
            agent=start_context.agent,
            workdir=start_context.workdir,
            source_session_id="",
            role=start_context.role,
            stage=start_context.stage,
            issue_number=start_context.issue_number,
            branch=start_context.branch,
            started_at_iso=start_context.started_at_iso,
        )
        if is_release_root_execution:
            start_result = adapter.start_child_role("release_worker", retry_context)
        else:
            start_result = adapter.start_root_session(retry_context)
    stop_attempts_raw = session_result_field(start_result, "stop_continuation_attempts", "stopContinuationAttempts", 0)
    stop_attempts = (
        int(stop_attempts_raw)
        if isinstance(stop_attempts_raw, (int, float, str)) and not isinstance(stop_attempts_raw, bool)
        else 0
    )
    if start_result.status != "success":
        return {
            "status": "error",
            "sourceSessionID": source_session_id,
            "launchTitle": launch_title,
            "title": request["title"],
            "reason": request["reason"],
            "role": request["role"],
            "stage": request["stage"],
            "issueNumber": request["issueNumber"],
            "branch": request["branch"],
            "error": start_result.error,
            "rootSessionID": start_result.session_id,
            "sessionReadabilityStatus": start_result.readability_status,
            "executionMode": str(session_result_field(start_result, "execution_mode", "executionMode", "root_session") or "root_session"),
            "childRole": str(session_result_field(start_result, "child_role", "childRole", "") or ""),
            "childSessionID": str(session_result_field(start_result, "child_session_id", "childSessionID", "") or ""),
            "childSessionStatus": str(session_result_field(start_result, "child_session_status", "childSessionStatus", "") or ""),
            "recordedAt": timestamp,
        }
    root_session_id = start_result.session_id
    return {
        "status": "success",
        "sourceSessionID": source_session_id,
        "rootSessionID": root_session_id,
        "launchTitle": launch_title,
        "title": request["title"],
        "reason": request["reason"],
        "role": request["role"],
        "stage": request["stage"],
        "issueNumber": request["issueNumber"],
        "branch": request["branch"],
        "tuiResumeCommand": str(session_result_field(start_result, "tui_resume_command", "tuiResumeCommand", "/sessions") or "/sessions"),
        "cliOpenCommand": start_result.resume_command,
        "recommendedAction": start_result.resume_hint,
        "sessionReadabilityStatus": start_result.readability_status,
        "stopContinuationStatus": str(session_result_field(start_result, "stop_continuation_status", "stopContinuationStatus", "root_session_detached") or "root_session_detached"),
        "stopContinuationAttempts": stop_attempts,
        "executionMode": str(session_result_field(start_result, "execution_mode", "executionMode", "root_session") or "root_session"),
        "childRole": str(session_result_field(start_result, "child_role", "childRole", "") or ""),
        "childSessionID": str(session_result_field(start_result, "child_session_id", "childSessionID", "") or ""),
        "childSessionStatus": str(session_result_field(start_result, "child_session_status", "childSessionStatus", "") or ""),
        "recordedAt": timestamp,
    }


def _cli_option_was_provided(argv: list[str], option: str) -> bool:
    return option in argv or any(argument.startswith(f"{option}=") for argument in argv)

def _handoff_to_selected_issue(
    current_ledger: JsonObject,
    *,
    selected_issue: IssuePacketRecord,
    base_dir: Path,
    updated_at: str,
    summary: str,
) -> tuple[JsonObject, SupervisorDecision, SessionRequest]:
    workflow = cast(dict[str, str], current_ledger["workflow"])
    resolved_base_branch = _resolve_issue_base_branch(base_dir, selected_issue) or selected_issue.base_branch or "main"
    next_ledger = create_initial_ledger(
        issue_packet=selected_issue,
        workflow_policy_path=workflow["workflowPolicyPath"],
        primary_workspace_root=str(base_dir),
        root_session_agent=_root_session_agent(current_ledger),
        base_branch=resolved_base_branch,
        updated_at=updated_at,
    )
    cast(list[JsonObject], next_ledger["history"]).append(
        {
            "recordedAt": updated_at,
            "fromRole": "main_orchestrator",
            "fromStage": "issue_selection_or_recovery",
            "toRole": "main_orchestrator",
                "toStage": "orchestrator_bootstrap",
            "reason": summary,
        }
    )
    request = build_orchestrator_request(next_ledger)
    return next_ledger, {
        "action": "queue_next_issue",
        "next_role": "main_orchestrator",
        "next_stage": "orchestrator_bootstrap",
        "summary": summary,
        "request_title": request["title"],
    }, request


def _record_session_result_history(base_dir: Path, ledger: JsonObject, session_result: JsonObject) -> None:
    issue = cast(dict[str, str], ledger.get("issue", {}))
    issue_number = str(session_result.get("issueNumber") or issue.get("number") or "")
    recorded_at = str(session_result.get("recordedAt") or "")
    if not issue_number or not recorded_at:
        return
    request_id = str(session_result.get("sourceSessionID") or "")
    root_session_id = str(session_result.get("rootSessionID") or "")
    status = str(session_result.get("status") or "")
    history_id = append_issue_history(
        base_dir,
        issue_number=issue_number,
        entry_type="session_result",
        created_at=recorded_at,
        role=str(session_result.get("role") or ""),
        stage=str(session_result.get("stage") or ""),
        status=status,
        session_id=root_session_id,
        request_id=request_id,
        command_id=request_id,
        summary=str(session_result.get("reason") or status),
        payload=dict(session_result),
        unique_key=f"session-result:{issue_number}:{request_id}:{recorded_at}",
    )
    if (
        str(session_result.get("role") or "") != "main_orchestrator"
        or str(session_result.get("stage") or "") != "release_root_execution"
    ):
        return
    child_role = str(session_result.get("childRole") or "")
    child_session_id = str(session_result.get("childSessionID") or "")
    child_session_status = str(session_result.get("childSessionStatus") or "")
    if not any((child_role, child_session_id, child_session_status)):
        return
    release_child_session = {
        "childRole": child_role,
        "childSessionID": child_session_id,
        "childSessionStatus": child_session_status,
        "rootSessionID": root_session_id,
        "recordedAt": recorded_at,
    }
    _ = sync_issue_runtime_context(
        base_dir,
        issue_number=issue_number,
        updated_at=recorded_at,
        runtime_context={"release_child_session": release_child_session},
    )
    record_latest_ref_snapshot(
        base_dir,
        issue_number=issue_number,
        entry_type="release_child_session",
        history_id=history_id,
        created_at=recorded_at,
        command_id=request_id,
        session_id=root_session_id,
        status=child_session_status or status,
        extra=release_child_session,
    )


def _record_same_repo_probe_degraded_event(
    *,
    base_dir: Path,
    issue_number: str,
    session_result: SessionResult,
) -> None:
    root_session_id = str(session_result.get("rootSessionID") or "")
    recorded_at = str(session_result.get("recordedAt") or "")
    if not root_session_id or not recorded_at:
        return
    readability_status = str(session_result.get("sessionReadabilityStatus") or "")
    if readability_status != "degraded_same_repo_probe":
        return
    error_text = str(session_result.get("error") or "").strip()
    summary = (
        f"Root session {root_session_id} started with degraded same-repo session_read probe status; "
        f"session remains active and reconcile should monitor follow-up health checks."
    )
    if error_text:
        summary = f"{summary} Detail: {error_text}"
    _ = append_issue_history(
        base_dir,
        issue_number=issue_number,
        entry_type="admin_action",
        created_at=recorded_at,
        status="same_repo_probe_degraded",
        session_id=root_session_id,
        command_id=f"same-repo-probe-degraded:{issue_number}:{root_session_id}:{recorded_at}",
        summary=summary,
        payload={
            "decision_type": "same_repo_probe_degraded",
            "root_session_id": root_session_id,
            "session_readability_status": readability_status,
            "error": error_text,
        },
        unique_key=f"same-repo-probe-degraded:{issue_number}:{root_session_id}:{recorded_at}",
    )


def _same_repo_probe_degraded_event_count(
    *,
    base_dir: Path,
    issue_number: str,
) -> int:
    ensure_control_plane_db(base_dir)
    control_plane_path = control_plane_db_path(base_dir)
    connection = sqlite3.connect(control_plane_path)
    try:
        row = connection.execute(
            (
                "SELECT COUNT(*) AS count FROM issue_history "
                "WHERE issue_number = ? AND entry_type = 'admin_action' AND status = 'same_repo_probe_degraded'"
            ),
            (issue_number,),
        ).fetchone()
    finally:
        connection.close()
    return int(row[0]) if row else 0


def _same_repo_probe_degraded_streak(
    *,
    base_dir: Path,
    issue_number: str,
) -> int:
    ensure_control_plane_db(base_dir)
    control_plane_path = control_plane_db_path(base_dir)
    connection = sqlite3.connect(control_plane_path)
    try:
        rows = connection.execute(
            (
                "SELECT payload_json FROM issue_history "
                "WHERE issue_number = ? AND entry_type = 'dispatch_result' "
                "ORDER BY created_at DESC, history_id DESC LIMIT 32"
            ),
            (issue_number,),
        ).fetchall()
    finally:
        connection.close()

    streak = 0
    for row in rows:
        payload_raw = str(row[0] or "{}")
        try:
            payload = cast(dict[str, object], json.loads(payload_raw))
        except json.JSONDecodeError:
            break
        if not isinstance(payload, dict):
            break
        root_session_id = str(payload.get("rootSessionID") or "")
        if not root_session_id:
            continue
        readability_status = str(payload.get("sessionReadabilityStatus") or "")
        if readability_status == "degraded_same_repo_probe":
            streak += 1
            continue
        break
    return streak


def _quarantine_on_repeated_same_repo_probe_degradation(
    *,
    base_dir: Path,
    ledger: JsonObject,
    runtime_issue: dict[str, object],
    updated_at: str,
) -> bool:
    issue = cast(dict[str, str], ledger["issue"])
    issue_number = issue["number"]
    runtime_state = str(runtime_issue.get("state") or "")
    if runtime_state in {"quarantined", "completed", "failed"}:
        return False
    degraded_streak = _same_repo_probe_degraded_streak(base_dir=base_dir, issue_number=issue_number)
    degraded_limit = _same_repo_probe_degraded_limit()
    if degraded_streak < degraded_limit:
        return False
    degraded_count = _same_repo_probe_degraded_event_count(base_dir=base_dir, issue_number=issue_number)
    summary = (
        f"Issue #{issue_number} recorded {degraded_streak} consecutive same-repo session_read degraded starts "
        f"({degraded_count} total degraded events); "
        f"quarantine after threshold {degraded_limit} to require controlled recovery."
    )
    quarantine_issue_execution(
        base_dir=base_dir,
        issue_number=issue_number,
        reason=summary,
        updated_at=updated_at,
    )
    return True


def _bump_ledger_revision(ledger: JsonObject, updated_at: str) -> None:
    ledger["ledgerRevision"] = updated_at


def inspect_control_plane(
    *,
    base_dir: Path,
    issue_number: str,
) -> JsonObject:
    base_dir = _canonical_supervisor_base_dir(base_dir)
    ensure_control_plane_db(base_dir)
    release_capacity = _release_capacity()
    release_backfill_mode = _release_backfill_mode()
    available_slots = available_release_slots(base_dir, release_capacity)
    verified_waiting_count = len(list_issues(base_dir, states=["verified"]))
    runtime_context = read_runtime_context(base_dir, issue_number)
    return {
        "schema": describe_control_plane_schema(base_dir),
        "issue": read_issue(base_dir, issue_number) or {},
        "latestDecision": read_latest_decision(base_dir, issue_number) or {},
        "latestGitHubSyncAttempt": read_latest_github_sync_attempt(base_dir, issue_number) or {},
        "failureContext": cast(dict[str, object], runtime_context.get("failure_context", {})) if isinstance(runtime_context.get("failure_context"), dict) else {},
        "recoveryCursor": cast(dict[str, object], runtime_context.get("recovery_cursor", {})) if isinstance(runtime_context.get("recovery_cursor"), dict) else {},
        "projectPrWorkflow": _project_pr_workflow_projection(base_dir, issue_number),
        "releaseGate": _release_gate_view(base_dir, issue_number),
        "releaseChildSession": read_release_child_session(base_dir, issue_number),
        "latestReleaseChildSession": read_latest_ref(base_dir, issue_number, "release_child_session"),
        "releaseBackfill": {
            "mode": release_backfill_mode,
            "releaseCapacity": release_capacity,
            "availableReleaseSlots": available_slots,
            "verifiedWaitingCount": verified_waiting_count,
        },
    }


def retry_failed_issue_execution(
    *,
    base_dir: Path,
    issue_number: str,
    reason: str,
    updated_at: str | None = None,
) -> JsonObject:
    base_dir = _canonical_supervisor_base_dir(base_dir)
    ensure_control_plane_db(base_dir)
    issue = read_issue(base_dir, issue_number)
    if issue is None:
        raise ValueError(f"unknown issue #{issue_number}")
    if str(issue.get("state") or "") != "failed":
        raise ValueError(f"issue #{issue_number} is not failed")

    last_failure = json.loads(str(issue.get("last_failure_json") or "{}"))
    if not isinstance(last_failure, dict) or not bool(last_failure.get("retryable")):
        raise ValueError(f"issue #{issue_number} is not retryable")

    timestamp = _now(updated_at)
    command_id = uuid4().hex
    clear_issue_execution_claim_projection(base_dir=base_dir, issue_number=issue_number, updated_at=timestamp)
    _ = upsert_issue_state(
        base_dir,
        issue_number=issue_number,
        state="ready",
        command_id=command_id,
        updated_at=timestamp,
        current_session_id="",
    )
    clear_issue_runtime_phase_projection(base_dir=base_dir, issue_number=issue_number, updated_at=timestamp)
    sync_error = _sync_projected_issue_labels(
        base_dir=base_dir,
        issue_number=issue_number,
        command_id=command_id,
        updated_at=timestamp,
    )
    if sync_error:
        _ = upsert_issue_state(
            base_dir,
            issue_number=issue_number,
            state="failed",
            command_id=f"{command_id}:rollback",
            updated_at=timestamp,
        )

    record_admin_decision(
        base_dir,
        command_id=f"{command_id}:retry-failed",
        issue_number=issue_number,
        decision_type="admin_retry_failed_issue",
        reason=(
            f"Retry failed issue #{issue_number}: {reason}"
            if not sync_error
            else f"Retry failed issue #{issue_number} failed during label sync: {sync_error}"
        ),
        updated_at=timestamp,
        from_state="failed",
        to_state="ready" if not sync_error else "failed",
    )
    return {
        "issue_number": issue_number,
        "status": "success" if not sync_error else "failed",
        "last_error": sync_error,
        "issue": read_issue(base_dir, issue_number) or {},
    }


def clear_ready_issue_session_fence(
    *,
    base_dir: Path,
    issue_number: str,
    reason: str,
    updated_at: str | None = None,
) -> JsonObject:
    base_dir = _canonical_supervisor_base_dir(base_dir)
    ensure_control_plane_db(base_dir)
    issue = read_issue(base_dir, issue_number)
    if issue is None:
        raise ValueError(f"unknown issue #{issue_number}")
    if str(issue.get("state") or "") != "ready":
        raise ValueError(f"issue #{issue_number} is not ready")
    stale_session_id = str(issue.get("current_session_id") or "")
    if not stale_session_id:
        raise ValueError(f"issue #{issue_number} does not have a current session fence")

    timestamp = _now(updated_at)
    command_id = uuid4().hex
    clear_issue_execution_claim_projection(base_dir=base_dir, issue_number=issue_number, updated_at=timestamp)
    _ = upsert_issue_state(
        base_dir,
        issue_number=issue_number,
        state="ready",
        command_id=command_id,
        updated_at=timestamp,
        current_session_id="",
    )
    clear_issue_runtime_phase_projection(base_dir=base_dir, issue_number=issue_number, updated_at=timestamp)
    sync_error = _sync_projected_issue_labels(
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
            current_session_id=stale_session_id,
        )

    record_admin_decision(
        base_dir,
        command_id=f"{command_id}:clear-ready-session-fence",
        issue_number=issue_number,
        decision_type="admin_clear_ready_session_fence",
        reason=(
            f"Clear stale ready session fence for issue #{issue_number}: {reason}"
            if not sync_error
            else f"Clear stale ready session fence for issue #{issue_number} failed during label sync: {sync_error}"
        ),
        updated_at=timestamp,
        from_state="ready",
        to_state="ready",
    )
    return {
        "issue_number": issue_number,
        "status": "success" if not sync_error else "failed",
        "cleared_session_id": stale_session_id if not sync_error else "",
        "last_error": sync_error,
        "issue": read_issue(base_dir, issue_number) or {},
    }


def retry_github_sync_attempt(
    *,
    base_dir: Path,
    command_id: str,
    updated_at: str | None = None,
) -> JsonObject:
    base_dir = _canonical_supervisor_base_dir(base_dir)
    attempt = read_github_sync_attempt_by_command_id(base_dir, command_id)
    if attempt is None:
        raise ValueError(f"unknown github sync attempt {command_id!r}")
    if str(attempt.get("status") or "") != "failed":
        raise ValueError(f"github sync attempt {command_id!r} is not failed")
    issue_number = str(attempt.get("issue_number") or "")
    latest_attempt = read_latest_github_sync_attempt(base_dir, issue_number)
    if latest_attempt is not None and str(latest_attempt.get("command_id") or "") != command_id:
        raise ValueError(f"github sync attempt {command_id!r} is stale for issue #{issue_number}")

    projection_target = str(attempt.get("projection_target") or "labels").strip() or "labels"
    if projection_target == "labels":
        delta = json.loads(str(attempt.get("intended_label_delta") or "{}"))
        add_labels = delta.get("add", [])
        remove_labels = delta.get("remove", [])
        if not isinstance(add_labels, list) or not isinstance(remove_labels, list):
            raise ValueError(f"github sync attempt {command_id!r} has invalid intended_label_delta")

        sync_error = _sync_issue_progress_label(
            base_dir=base_dir,
            issue_number=issue_number,
            add_labels=[str(label) for label in add_labels],
            remove_labels=[str(label) for label in remove_labels],
            command_id=command_id,
            updated_at=updated_at,
        )
    elif projection_target == "project_fields":
        sync_error = _sync_project_fields_projection(
            base_dir=base_dir,
            issue_number=issue_number,
            command_id=command_id,
            updated_at=updated_at,
        )
    else:
        raise ValueError(
            f"github sync attempt {command_id!r} projection_target {projection_target!r} is not retryable"
        )
    record_admin_decision(
        base_dir,
        command_id=f"{command_id}:retry",
        issue_number=issue_number,
        decision_type="admin_github_sync_retry",
        reason=(
            f"Retry GitHub sync-safe command {command_id} ({projection_target}) for issue #{issue_number}."
            if not sync_error
            else f"Retry GitHub sync-safe command {command_id} ({projection_target}) for issue #{issue_number} failed again: {sync_error}"
        ),
        updated_at=_now(updated_at),
    )
    refreshed = read_github_sync_attempt_by_command_id(base_dir, command_id) or {}
    return {
        "command_id": command_id,
        "issue_number": issue_number,
        "status": "success" if not sync_error else "failed",
        "last_error": sync_error,
        "attempt": refreshed,
    }


def _build_common_prompt_lines(ledger: JsonObject) -> list[str]:
    build_common = cast(Callable[..., list[str]], _request_helpers.build_common_prompt_lines)
    automation = cast(dict[str, object], ledger.get("automation", {}))
    supervisor_doc_path = str(automation.get("supervisorDocPath") or DEFAULT_SUPERVISOR_DOC_PATH)
    return build_common(ledger, default_supervisor_doc_path=supervisor_doc_path)


def _build_prompt(ledger: JsonObject, role: str, stage: str, decision_summary: str) -> str:
    build_prompt_impl = cast(Callable[..., str], _request_helpers.build_prompt)
    automation = cast(dict[str, object], ledger.get("automation", {}))
    workflow = cast(dict[str, object], ledger.get("workflow", {}))
    return build_prompt_impl(
        ledger,
        role,
        stage,
        decision_summary,
        default_supervisor_doc_path=str(automation.get("supervisorDocPath") or DEFAULT_SUPERVISOR_DOC_PATH),
        default_release_result_template_path=str(workflow.get("releaseResultTemplatePath") or DEFAULT_RELEASE_RESULT_TEMPLATE_PATH),
    )


def build_session_request(
    ledger: JsonObject,
    *,
    role: str,
    stage: str,
    reason: str,
    title: str,
    decision_summary: str,
) -> SessionRequest:
    build_request = cast(Callable[..., JsonObject], _request_helpers.build_session_request)
    request = build_request(
        ledger,
        role=role,
        stage=stage,
        reason=reason,
        title=title,
        decision_summary=decision_summary,
        now=_now,
        root_session_agent=_root_session_agent,
        build_prompt=_build_prompt,
    )
    return cast(SessionRequest, cast(object, request))


def build_orchestrator_request(ledger: JsonObject) -> SessionRequest:
    build_orchestrator = cast(Callable[..., JsonObject], _request_helpers.build_orchestrator_request)
    request = build_orchestrator(ledger, build_session_request=build_session_request)
    return cast(SessionRequest, cast(object, request))


def submit_artifact(
    *,
    base_dir: Path,
    issue_number: str,
    artifact_kind: str,
    payload: JsonObject,
    body_text: str = "",
    updated_at: str | None = None,
) -> dict[str, object]:
    base_dir = _canonical_supervisor_base_dir(base_dir)
    timestamp = _now(updated_at)
    if artifact_kind not in {"worker_result", "evidence_packet", "release_result"}:
        raise ValueError(f"unsupported artifact kind {artifact_kind!r}")
    db_path = control_plane_db_path(base_dir)
    if not db_path.exists():
        raise RuntimeError(
            f"control-plane DB missing at {db_path}; refusing to recreate during active submit-artifact workflow"
        )
    issue = read_issue(base_dir, issue_number)
    if issue is None:
        raise ValueError(f"unknown issue #{issue_number}")
    normalized_payload = _validated_artifact_payload(
        base_dir=base_dir,
        issue_number=issue_number,
        artifact_kind=artifact_kind,
        payload=payload,
    )
    if artifact_kind in {"worker_result", "evidence_packet"}:
        pr_payload_raw = normalized_payload.get("pr")
        if isinstance(pr_payload_raw, dict):
            pr_payload = cast(dict[str, object], pr_payload_raw)
            pr_number = str(pr_payload.get("number") or "")
            if pr_number and not str(normalized_payload.get("pr_number") or ""):
                normalized_payload["pr_number"] = pr_number
            pr_url = str(pr_payload.get("url") or "")
            if pr_url and not str(normalized_payload.get("pr_url") or ""):
                normalized_payload["pr_url"] = pr_url
    persisted = _record_db_artifact_fact(
        base_dir=base_dir,
        issue_number=issue_number,
        artifact_kind=artifact_kind,
        parsed=normalized_payload,
        observed_at=timestamp,
        body_text=body_text,
    )

    ref_key = ARTIFACT_REF_KEYS.get(artifact_kind)
    if ref_key:
        issue_after = read_issue(base_dir, issue_number) or {}
        artifact_refs = _load_json_dict(issue_after.get("artifact_refs_json"))
        artifact_refs[ref_key] = _artifact_fact_ref(artifact_kind, persisted)
        _ = sync_issue_runtime_context(
            base_dir,
            issue_number=issue_number,
            updated_at=timestamp,
            artifact_refs=artifact_refs,
        )

    return persisted


def _queue_transition(
    ledger: JsonObject,
    *,
    next_role: str,
    next_stage: str,
    summary: str,
    updated_at: str,
) -> None:
    issue = cast(dict[str, str], ledger["issue"])
    current_before = cast(dict[str, str], ledger["current"])
    queue_transition = cast(Callable[..., None], _reconcile_helpers.queue_transition)
    queue_transition(
        ledger,
        next_role=next_role,
        next_stage=next_stage,
        summary=summary,
        updated_at=updated_at,
        bump_ledger_revision=_bump_ledger_revision,
    )
    automation = cast(dict[str, object], ledger.get("automation", {}))
    primary_workspace_root = str(automation.get("primaryWorkspaceRoot") or "")
    transition_base_dir = Path(primary_workspace_root) if primary_workspace_root else ROOT
    _record_runtime_transition_history(
        base_dir=transition_base_dir,
        issue_number=issue.get("number", ""),
        recorded_at=updated_at,
        from_role=current_before.get("role", "unknown"),
        from_stage=current_before.get("stage", "unknown"),
        to_role=next_role,
        to_stage=next_stage,
        reason=summary,
    )


def _set_failure(
    ledger: JsonObject,
    *,
    kind: str,
    summary: str,
    retryable: bool,
    failure_class: str = "",
    artifact_kind: str = "",
    owner_role: str = "",
    owner_stage: str = "",
    owner_state: str = "",
    resume_role: str = "",
    resume_stage: str = "",
    resume_state: str = "",
    resume_strategy: str = "",
) -> None:
    set_failure = cast(Callable[..., None], _reconcile_helpers.set_failure)
    set_failure(
        ledger,
        kind=kind,
        summary=summary,
        retryable=retryable,
        failure_class=failure_class,
        artifact_kind=artifact_kind,
        owner_role=owner_role,
        owner_stage=owner_stage,
        owner_state=owner_state,
        resume_role=resume_role,
        resume_stage=resume_stage,
        resume_state=resume_state,
        resume_strategy=resume_strategy,
    )


def _request_for_transition(
    ledger: JsonObject,
    *,
    next_role: str,
    next_stage: str,
    summary: str,
) -> SessionRequest:
    request_for_transition = cast(Callable[..., JsonObject], _reconcile_helpers.request_for_transition)
    request = request_for_transition(
        ledger,
        next_role=next_role,
        next_stage=next_stage,
        summary=summary,
        build_session_request=build_session_request,
    )
    return cast(SessionRequest, cast(object, request))


def _subagent_decision(ledger: JsonObject, *, next_role: str, next_stage: str, summary: str) -> SupervisorDecision:
    subagent_decision = cast(Callable[..., JsonObject], _reconcile_helpers.subagent_decision)
    decision = subagent_decision(
        ledger,
        next_role=next_role,
        next_stage=next_stage,
        summary=summary,
        build_prompt=_build_prompt,
    )
    return cast(SupervisorDecision, cast(object, decision))


def _requeue_issue_worker(
    ledger: JsonObject,
    *,
    base_dir: Path,
    issue_number: str,
    updated_at: str,
    summary: str,
    next_stage: str,
) -> tuple[SupervisorDecision, None]:
    requeue_issue_worker = cast(Callable[..., tuple[JsonObject, None]], _reconcile_helpers.requeue_issue_worker)
    decision, request = requeue_issue_worker(
        ledger,
        base_dir=base_dir,
        issue_number=issue_number,
        updated_at=updated_at,
        summary=summary,
        next_stage=next_stage,
        read_issue=read_issue,
        transition_issue_state_if_possible=_transition_issue_state_if_possible,
        queue_transition_func=_queue_transition,
        subagent_decision_func=_subagent_decision,
    )
    return cast(SupervisorDecision, cast(object, decision)), request


def _queue_orchestrator_recovery(
    ledger: JsonObject,
    *,
    base_dir: Path,
    updated_at: str,
    summary: str,
    final_state: str | None = None,
) -> tuple[JsonObject, SupervisorDecision, SessionRequest]:
    queue_orchestrator_recovery = cast(Callable[..., tuple[JsonObject, JsonObject, JsonObject]], _reconcile_helpers.queue_orchestrator_recovery)
    next_ledger, decision, request = queue_orchestrator_recovery(
        ledger,
        base_dir=base_dir,
        updated_at=updated_at,
        summary=summary,
        release_issue_execution=release_issue_execution,
        select_next_issue_candidate=select_next_issue_candidate,
        load_issue_packet_from_db=_load_issue_packet_from_db,
        handoff_to_selected_issue=_handoff_to_selected_issue,
        request_for_transition_func=_request_for_transition,
        queue_transition_func=_queue_transition,
        final_state=final_state,
    )
    return (
        next_ledger,
        cast(SupervisorDecision, cast(object, decision)),
        cast(SessionRequest, cast(object, request)),
    )


def _consume_queued_next_issue(
    ledger: JsonObject,
    *,
    base_dir: Path,
    updated_at: str,
    summary: str,
) -> tuple[JsonObject, SupervisorDecision, SessionRequest] | None:
    issue_number, _issue_branch, _issue_base_branch = _queued_next_issue_fields(ledger)
    if not issue_number:
        return None
    selected_issue = _load_issue_packet_from_db(base_dir, issue_number)
    if selected_issue is None:
        return None
    if issue_number in _completed_issue_numbers(base_dir):
        return None
    revalidated_candidate = select_next_issue_candidate(
        base_dir,
        current_issue_number=str(cast(dict[str, str], ledger["issue"]).get("number", "")),
        current_parent_reference=str(cast(dict[str, str], ledger["issue"]).get("parentReference", "")),
    )
    if revalidated_candidate is None:
        ledger.pop("queuedNextIssue", None)
        return None
    if revalidated_candidate.issue_number != selected_issue.issue_number:
        ledger.pop("queuedNextIssue", None)
        revalidated_issue = _load_issue_packet_from_db(base_dir, revalidated_candidate.issue_number)
        if revalidated_issue is None:
            return None
        next_ledger, decision, request = _handoff_to_selected_issue(
            ledger,
            selected_issue=revalidated_issue,
            base_dir=base_dir,
            updated_at=updated_at,
            summary=(
                f"Queued next issue #{selected_issue.issue_number} is no longer ready. Continue automatically with revalidated issue #{revalidated_candidate.issue_number}."
            ),
        )
        return next_ledger, decision, request
    next_ledger, decision, request = _handoff_to_selected_issue(
        ledger,
        selected_issue=selected_issue,
        base_dir=base_dir,
        updated_at=updated_at,
        summary=summary,
    )
    return next_ledger, decision, request


def _quarantine_decision(ledger: JsonObject, *, summary: str) -> tuple[JsonObject, SupervisorDecision, None]:
    quarantine_decision = cast(Callable[..., tuple[JsonObject, JsonObject, None]], _reconcile_helpers.quarantine_decision)
    next_ledger, decision, request = quarantine_decision(ledger, summary=summary)
    return next_ledger, cast(SupervisorDecision, cast(object, decision)), request


def reconcile_ledger(
    ledger: JsonObject,
    *,
    artifact_base_dir: Path | None = None,
    updated_at: str | None = None,
) -> tuple[JsonObject, SupervisorDecision, SessionRequest | None]:
    timestamp = _now(updated_at)
    base_dir = _canonical_supervisor_base_dir(artifact_base_dir or ROOT)
    artifact_lookup_base_dir = _canonical_artifact_base_dir(ledger, default_base_dir=base_dir)
    ensure_control_plane_db(base_dir)
    automation = cast(dict[str, object], ledger.get("automation", {}))
    if not str(automation.get("primaryWorkspaceRoot") or ""):
        runtime_issue = read_issue(base_dir, str(cast(dict[str, str], ledger.get("issue", {})).get("number") or "")) or {}
        issue_worktree = str(runtime_issue.get("worktree_path") or "")
        automation["primaryWorkspaceRoot"] = issue_worktree or str(base_dir)
    _sync_last_session_result_from_db(ledger, base_dir=base_dir)
    issue = cast(dict[str, str], ledger["issue"])
    ensure_issue_row(base_dir, issue_number=issue["number"], updated_at=timestamp)
    _sync_runtime_phase_metadata(
        base_dir=base_dir,
        issue_number=issue["number"],
        current=cast(dict[str, str], ledger["current"]),
        attempts=cast(dict[str, int], ledger.get("attempts", {})),
        limits=cast(dict[str, int], ledger.get("limits", {})),
        last_failure=cast(dict[str, object], ledger.get("lastFailure", {})),
        workflow=cast(dict[str, object], ledger.get("workflow", {})),
        automation=cast(dict[str, object], ledger.get("automation", {})),
        artifacts=cast(dict[str, object], ledger.get("artifacts", {})),
        updated_at=timestamp,
    )
    _sync_root_issue_event_from_session_result(ledger, base_dir=base_dir)
    pre_sync_runtime_issue = read_issue(base_dir, issue["number"])
    current = cast(dict[str, str], ledger["current"])
    if _recover_stale_bootstrap_with_worker_artifact(
        ledger=ledger,
        base_dir=base_dir,
        updated_at=timestamp,
    ):
        current = cast(dict[str, str], ledger["current"])
    attempts = cast(dict[str, int], ledger["attempts"])
    limits = cast(dict[str, int], ledger["limits"])
    artifacts = cast(dict[str, str], ledger["artifacts"])
    _sync_runtime_phase_to_control_plane_state(
        base_dir=base_dir,
        issue_number=issue["number"],
        ledger=ledger,
        current=current,
        updated_at=timestamp,
    )
    runtime_issue = read_issue(base_dir, issue["number"])
    try:
        if runtime_issue and _quarantine_running_issue_without_root_session(
            base_dir=base_dir,
            ledger=ledger,
            runtime_issue=cast(dict[str, object], runtime_issue),
            updated_at=timestamp,
        ):
            runtime_issue = read_issue(base_dir, issue["number"])

        if runtime_issue and _quarantine_stale_dispatching_issue_without_root_session(
            base_dir=base_dir,
            ledger=ledger,
            current=current,
            runtime_issue=cast(dict[str, object], runtime_issue),
            updated_at=timestamp,
        ):
            runtime_issue = read_issue(base_dir, issue["number"])

        if runtime_issue and _recover_stale_claimed_issue_without_dispatch_evidence(
            base_dir=base_dir,
            ledger=ledger,
            runtime_issue=cast(dict[str, object], runtime_issue),
            updated_at=timestamp,
        ):
            runtime_issue = read_issue(base_dir, issue["number"])

        if (
            runtime_issue
            and current["role"] == "issue_worker"
            and _refresh_running_issue_heartbeat_from_worker_result(
                base_dir=artifact_lookup_base_dir,
                issue_number=issue["number"],
                runtime_issue=cast(dict[str, object], runtime_issue),
                updated_at=timestamp,
            )
        ):
            runtime_issue = read_issue(base_dir, issue["number"])

        if runtime_issue and _quarantine_stale_running_issue(
            base_dir=base_dir,
            ledger=ledger,
            runtime_issue=cast(dict[str, object], runtime_issue),
            updated_at=timestamp,
        ):
            runtime_issue = read_issue(base_dir, issue["number"])

        if runtime_issue and _quarantine_stale_queued_subagent_with_stale_root(
            base_dir=base_dir,
            ledger=ledger,
            current=current,
            runtime_issue=cast(dict[str, object], runtime_issue),
            updated_at=timestamp,
        ):
            runtime_issue = read_issue(base_dir, issue["number"])

        if runtime_issue and _quarantine_on_repeated_same_repo_probe_degradation(
            base_dir=base_dir,
            ledger=ledger,
            runtime_issue=cast(dict[str, object], runtime_issue),
            updated_at=timestamp,
        ):
            runtime_issue = read_issue(base_dir, issue["number"])

        if (
            runtime_issue
            and runtime_issue.get("state") == "quarantined"
            and pre_sync_runtime_issue is not None
            and str(pre_sync_runtime_issue.get("state") or "") == "quarantined"
            and _recover_stale_quarantined_dispatching_issue_without_live_session(
                base_dir=base_dir,
                ledger=ledger,
                runtime_issue=cast(dict[str, object], runtime_issue),
                updated_at=timestamp,
            )
        ):
            runtime_issue = read_issue(base_dir, issue["number"])

        if runtime_issue and runtime_issue.get("state") == "quarantined":
            summary = (
                f"Issue #{issue['number']} is quarantined. Hold automatic retries and require an explicit fenced resume or terminal failure decision."
            )
            return _quarantine_decision(ledger, summary=summary)

        if runtime_issue and runtime_issue.get("state") == "running" and current["role"] == "pr_verifier":
            _transition_issue_state_if_possible(
                base_dir=base_dir,
                issue_number=issue["number"],
                to_state="verifying",
                command_id=uuid4().hex,
                updated_at=timestamp,
                reason=f"Issue #{issue['number']} is now waiting on verifier-owned evidence.",
                from_state="running",
            )

        def _route_orchestrator_bootstrap() -> tuple[JsonObject, JsonObject, JsonObject | None]:
            reconcile_orchestrator_bootstrap = cast(Callable[..., tuple[JsonObject, JsonObject, JsonObject | None]], _reconcile_helpers.reconcile_orchestrator_bootstrap)
            next_ledger, decision, request = reconcile_orchestrator_bootstrap(
                ledger,
                issue=issue,
                attempts=attempts,
                updated_at=timestamp,
                set_failure_func=_set_failure,
                queue_transition_func=_queue_transition,
                subagent_decision_func=_subagent_decision,
            )
            _sync_runtime_phase_metadata(base_dir=base_dir, issue_number=_ledger_issue_number(next_ledger, issue["number"]), current=cast(dict[str, str], next_ledger["current"]), attempts=cast(dict[str, int], next_ledger.get("attempts", {})), limits=cast(dict[str, int], next_ledger.get("limits", {})), last_failure=cast(dict[str, object], next_ledger.get("lastFailure", {})), workflow=cast(dict[str, object], next_ledger.get("workflow", {})), automation=cast(dict[str, object], next_ledger.get("automation", {})), artifacts=cast(dict[str, object], next_ledger.get("artifacts", {})), queued_next_issue=cast(dict[str, object], next_ledger.get("queuedNextIssue", {})), updated_at=timestamp)
            return next_ledger, decision, request

        def _route_issue_worker() -> tuple[JsonObject, JsonObject, JsonObject | None]:
            reconcile_issue_worker = cast(Callable[..., tuple[JsonObject, JsonObject, JsonObject | None]], _reconcile_helpers.reconcile_issue_worker)
            next_ledger, decision, request = reconcile_issue_worker(
                ledger,
                base_dir=base_dir,
                issue=issue,
                current=current,
                attempts=attempts,
                limits=limits,
                artifacts=artifacts,
                updated_at=timestamp,
                is_successful_release_status=_is_successful_release_status,
                read_artifact_fact=lambda entry_type: _read_db_artifact_fact(
                    base_dir=artifact_lookup_base_dir,
                    issue_number=issue["number"],
                    artifact_kind=entry_type,
                ),
                record_pr_opened=record_pr_opened,
                set_failure_func=_set_failure,
                requeue_issue_worker_func=_requeue_issue_worker,
                queue_orchestrator_recovery_func=_queue_orchestrator_recovery,
                queue_transition_func=_queue_transition,
                subagent_decision_func=_subagent_decision,
            )
            _sync_runtime_phase_metadata(base_dir=base_dir, issue_number=_ledger_issue_number(next_ledger, issue["number"]), current=cast(dict[str, str], next_ledger["current"]), attempts=cast(dict[str, int], next_ledger.get("attempts", {})), limits=cast(dict[str, int], next_ledger.get("limits", {})), last_failure=cast(dict[str, object], next_ledger.get("lastFailure", {})), workflow=cast(dict[str, object], next_ledger.get("workflow", {})), automation=cast(dict[str, object], next_ledger.get("automation", {})), artifacts=cast(dict[str, object], next_ledger.get("artifacts", {})), queued_next_issue=cast(dict[str, object], next_ledger.get("queuedNextIssue", {})), updated_at=timestamp)
            if str(cast(dict[str, object], decision).get("next_role") or "") == "pr_verifier":
                projection_updated_at = _projection_updated_at_from_worker_result(
                    base_dir=base_dir,
                    issue_number=issue["number"],
                    fallback_updated_at=timestamp,
                )
                sync_error = _sync_projected_issue_labels(
                    base_dir=base_dir,
                    issue_number=issue["number"],
                    command_id=f"reconcile:{issue['number']}:issue-worker-pr-opened:labels",
                    updated_at=projection_updated_at,
                )
                if sync_error:
                    record_admin_decision(
                        base_dir,
                        command_id=f"reconcile:{issue['number']}:issue-worker-pr-opened:labels:admin-failed",
                        issue_number=issue["number"],
                        decision_type="admin_github_projection_failure",
                        reason=(
                            f"GitHub projected label sync failed after worker_result success for issue #{issue['number']}: "
                            f"{sync_error}"
                        ),
                        updated_at=projection_updated_at,
                    )
                if _project_fields_sync_enabled(base_dir=base_dir, issue_number=issue["number"]):
                    project_error = _sync_project_fields_projection(
                        base_dir=base_dir,
                        issue_number=issue["number"],
                        command_id=f"reconcile:{issue['number']}:issue-worker-pr-opened:project-fields",
                        updated_at=projection_updated_at,
                    )
                    if project_error:
                        record_admin_decision(
                            base_dir,
                            command_id=f"reconcile:{issue['number']}:issue-worker-pr-opened:project-fields:admin-failed",
                            issue_number=issue["number"],
                            decision_type="admin_github_projection_failure",
                            reason=(
                                f"GitHub project field sync failed after worker_result success for issue #{issue['number']}: "
                                f"{project_error}"
                            ),
                            updated_at=projection_updated_at,
                        )
            return next_ledger, decision, request

        def _route_pr_verifier() -> tuple[JsonObject, JsonObject, JsonObject | None]:
            reconcile_pr_verifier = cast(Callable[..., tuple[JsonObject, JsonObject, JsonObject | None]], _reconcile_helpers.reconcile_pr_verifier)
            next_ledger, decision, request = reconcile_pr_verifier(
                ledger,
                # Use artifact lookup base so browser artifact evidence_ref is resolved
                # against the issue worktree when verifier stores relative paths.
                base_dir=artifact_lookup_base_dir,
                issue=issue,
                current=current,
                attempts=attempts,
                limits=limits,
                artifacts=artifacts,
                updated_at=timestamp,
                read_issue=read_issue,
                read_issue_packet=read_issue_packet,
                read_artifact_fact=lambda entry_type: _read_db_artifact_fact(
                    base_dir=artifact_lookup_base_dir,
                    issue_number=issue["number"],
                    artifact_kind=entry_type,
                ),
                read_session_outcome=lambda runtime_session_id: _default_host_adapter().read_session_outcome(runtime_session_id),
                record_pr_opened=record_pr_opened,
                record_current_verifier_session=_record_current_verifier_session,
                transition_issue_state_if_possible=_transition_issue_state_if_possible,
                set_failure_func=_set_failure,
                requeue_issue_worker_func=_requeue_issue_worker,
                queue_orchestrator_recovery_func=_queue_orchestrator_recovery,
                queue_transition_func=_queue_transition,
                subagent_decision_func=_subagent_decision,
            )
            should_emit_root_terminal = (
                str(cast(dict[str, object], decision).get("action") or "") == "release_waiting"
                and pre_sync_runtime_issue is not None
                and str(pre_sync_runtime_issue.get("state") or "") == "running"
            )
            if should_emit_root_terminal:
                _append_root_terminal_event_for_verifier_handoff(
                    base_dir=base_dir,
                    ledger=ledger,
                    runtime_issue=cast(dict[str, object], pre_sync_runtime_issue),
                    updated_at=timestamp,
                )
            _sync_runtime_phase_metadata(base_dir=base_dir, issue_number=_ledger_issue_number(next_ledger, issue["number"]), current=cast(dict[str, str], next_ledger["current"]), attempts=cast(dict[str, int], next_ledger.get("attempts", {})), limits=cast(dict[str, int], next_ledger.get("limits", {})), last_failure=cast(dict[str, object], next_ledger.get("lastFailure", {})), workflow=cast(dict[str, object], next_ledger.get("workflow", {})), automation=cast(dict[str, object], next_ledger.get("automation", {})), artifacts=cast(dict[str, object], next_ledger.get("artifacts", {})), queued_next_issue=cast(dict[str, object], next_ledger.get("queuedNextIssue", {})), updated_at=timestamp)
            if str(cast(dict[str, object], decision).get("action") or "") == "release_waiting":
                sync_error = _sync_projected_issue_labels(
                    base_dir=base_dir,
                    issue_number=issue["number"],
                    command_id=f"reconcile:{issue['number']}:pr-verifier-release-waiting:labels",
                    updated_at=timestamp,
                )
                if sync_error:
                    record_admin_decision(
                        base_dir,
                        command_id=f"reconcile:{issue['number']}:pr-verifier-release-waiting:labels:admin-failed",
                        issue_number=issue["number"],
                        decision_type="admin_github_projection_failure",
                        reason=(
                            f"GitHub projected label sync failed after verifier pass for issue #{issue['number']}: "
                            f"{sync_error}"
                        ),
                        updated_at=timestamp,
                    )
                if _project_fields_sync_enabled(base_dir=base_dir, issue_number=issue["number"]):
                    project_error = _sync_project_fields_projection(
                        base_dir=base_dir,
                        issue_number=issue["number"],
                        command_id=f"reconcile:{issue['number']}:pr-verifier-release-waiting:project-fields",
                        updated_at=timestamp,
                    )
                    if project_error:
                        record_admin_decision(
                            base_dir,
                            command_id=f"reconcile:{issue['number']}:pr-verifier-release-waiting:project-fields:admin-failed",
                            issue_number=issue["number"],
                            decision_type="admin_github_projection_failure",
                            reason=(
                                f"GitHub project field sync failed after verifier pass for issue #{issue['number']}: "
                                f"{project_error}"
                            ),
                            updated_at=timestamp,
                        )
            return next_ledger, decision, request

        def _route_release_root_execution() -> tuple[JsonObject, JsonObject, JsonObject | None]:
            reconcile_release_worker = cast(Callable[..., tuple[JsonObject, JsonObject, JsonObject | None]], _reconcile_helpers.reconcile_release_worker)
            next_ledger, decision, request = reconcile_release_worker(
                ledger,
                base_dir=base_dir,
                issue=issue,
                current=current,
                attempts=attempts,
                limits=limits,
                artifacts=artifacts,
                updated_at=timestamp,
                transient_release_blockers=TRANSIENT_RELEASE_BLOCKERS,
                non_terminal_release_failure_kinds=NON_TERMINAL_RELEASE_FAILURE_KINDS,
                read_issue=read_issue,
                read_artifact_fact=lambda entry_type: _read_db_artifact_fact(
                    base_dir=artifact_lookup_base_dir,
                    issue_number=issue["number"],
                    artifact_kind=entry_type,
                ),
                read_session_outcome=lambda runtime_session_id: _default_host_adapter().read_session_outcome(runtime_session_id),
                transition_issue_state_if_possible=_transition_issue_state_if_possible,
                set_failure_func=_set_failure,
                queue_orchestrator_recovery_func=_queue_orchestrator_recovery,
                queue_transition_func=_queue_transition,
                subagent_decision_func=_subagent_decision,
                sync_issue_runtime_context=sync_issue_runtime_context,
            )
            _sync_runtime_phase_metadata(base_dir=base_dir, issue_number=_ledger_issue_number(next_ledger, issue["number"]), current=cast(dict[str, str], next_ledger["current"]), attempts=cast(dict[str, int], next_ledger.get("attempts", {})), limits=cast(dict[str, int], next_ledger.get("limits", {})), last_failure=cast(dict[str, object], next_ledger.get("lastFailure", {})), workflow=cast(dict[str, object], next_ledger.get("workflow", {})), automation=cast(dict[str, object], next_ledger.get("automation", {})), artifacts=cast(dict[str, object], next_ledger.get("artifacts", {})), queued_next_issue=cast(dict[str, object], next_ledger.get("queuedNextIssue", {})), updated_at=timestamp)
            return next_ledger, decision, request

        def _route_issue_selection_or_recovery() -> tuple[JsonObject, JsonObject, JsonObject | None]:
            queued_next_issue_result = _consume_queued_next_issue(
                ledger,
                base_dir=base_dir,
                updated_at=timestamp,
                summary=(
                    f"Resume deterministic next-issue handoff selected earlier for issue #{issue['number']}."
                ),
            )
            if queued_next_issue_result is not None:
                next_ledger, decision, request = queued_next_issue_result
                _sync_runtime_phase_metadata(base_dir=base_dir, issue_number=_ledger_issue_number(next_ledger, issue["number"]), current=cast(dict[str, str], next_ledger["current"]), attempts=cast(dict[str, int], next_ledger.get("attempts", {})), limits=cast(dict[str, int], next_ledger.get("limits", {})), last_failure=cast(dict[str, object], next_ledger.get("lastFailure", {})), workflow=cast(dict[str, object], next_ledger.get("workflow", {})), automation=cast(dict[str, object], next_ledger.get("automation", {})), artifacts=cast(dict[str, object], next_ledger.get("artifacts", {})), queued_next_issue=cast(dict[str, object], next_ledger.get("queuedNextIssue", {})), updated_at=timestamp)
                return next_ledger, cast(JsonObject, cast(object, decision)), cast(JsonObject | None, cast(object, request))
            reconcile_issue_selection_or_recovery = cast(Callable[..., tuple[JsonObject, JsonObject, JsonObject | None] | None], _reconcile_helpers.reconcile_issue_selection_or_recovery)
            recovery_result = reconcile_issue_selection_or_recovery(
                ledger,
                base_dir=base_dir,
                issue=issue,
                artifacts=artifacts,
                updated_at=timestamp,
                read_issue=read_issue,
                read_artifact_fact=lambda entry_type: _read_db_artifact_fact(
                    base_dir=artifact_lookup_base_dir,
                    issue_number=issue["number"],
                    artifact_kind=entry_type,
                ),
                is_successful_release_status=_is_successful_release_status,
                set_failure_func=_set_failure,
                queue_orchestrator_recovery_func=_queue_orchestrator_recovery,
                upsert_issue_state=upsert_issue_state,
                request_for_transition_func=_request_for_transition,
                queue_transition_func=_queue_transition,
                subagent_decision_func=_subagent_decision,
                attempts=attempts,
            )
            if recovery_result is not None:
                next_ledger, decision, request = recovery_result
                _sync_runtime_phase_metadata(base_dir=base_dir, issue_number=_ledger_issue_number(next_ledger, issue["number"]), current=cast(dict[str, str], next_ledger["current"]), attempts=cast(dict[str, int], next_ledger.get("attempts", {})), limits=cast(dict[str, int], next_ledger.get("limits", {})), last_failure=cast(dict[str, object], next_ledger.get("lastFailure", {})), workflow=cast(dict[str, object], next_ledger.get("workflow", {})), automation=cast(dict[str, object], next_ledger.get("automation", {})), artifacts=cast(dict[str, object], next_ledger.get("artifacts", {})), queued_next_issue=cast(dict[str, object], next_ledger.get("queuedNextIssue", {})), updated_at=timestamp)
                return next_ledger, decision, request
            return _route_no_change()

        def _route_no_change() -> tuple[JsonObject, JsonObject, JsonObject | None]:
            persisted_release = _read_db_artifact_fact(
                base_dir=artifact_lookup_base_dir,
                issue_number=issue["number"],
                artifact_kind="release_result",
            )
            persisted_release_status = str(persisted_release.get("status") or "").strip().lower()
            if (
                not str(current.get("role") or "")
                and not str(current.get("stage") or "")
                and runtime_issue is not None
                and str(runtime_issue.get("state") or "") in {"failed", "ready"}
                and bool(artifacts.get("release_result_ref"))
                and persisted_release_status in {"success", "completed"}
            ):
                summary = (
                    f"Issue #{issue['number']} has no queued role/stage, but SQLite already has a successful release_result. "
                    "Queue issue_selection_or_recovery so late release recovery can reconcile the control plane to completed."
                )
                queue_transition = {"role": "main_orchestrator", "stage": "issue_selection_or_recovery", "status": "queued"}
                cast(dict[str, str], ledger["current"]).update(queue_transition)
                _sync_runtime_phase_metadata(
                    base_dir=base_dir,
                    issue_number=_ledger_issue_number(ledger, issue["number"]),
                    current=cast(dict[str, str], ledger["current"]),
                    attempts=cast(dict[str, int], ledger.get("attempts", {})),
                    limits=cast(dict[str, int], ledger.get("limits", {})),
                    last_failure=cast(dict[str, object], ledger.get("lastFailure", {})),
                    workflow=cast(dict[str, object], ledger.get("workflow", {})),
                    automation=cast(dict[str, object], ledger.get("automation", {})),
                    artifacts=cast(dict[str, object], ledger.get("artifacts", {})),
                    queued_next_issue=cast(dict[str, object], ledger.get("queuedNextIssue", {})),
                    updated_at=timestamp,
                )
                request = {
                    "role": "main_orchestrator",
                    "stage": "issue_selection_or_recovery",
                    "title": f"Recover issue #{issue['number']} from persisted release result",
                    "issueNumber": issue["number"],
                }
                return (
                    ledger,
                    {
                        "action": "queue_next_session",
                        "next_role": "main_orchestrator",
                        "next_stage": "issue_selection_or_recovery",
                        "summary": summary,
                        "request_title": cast(str, request["title"]),
                    },
                    cast(JsonObject, request),
                )
            no_change_decision = cast(Callable[..., tuple[JsonObject, JsonObject, JsonObject | None]], _reconcile_helpers.no_change_decision)
            next_ledger, decision, request = no_change_decision(
                ledger,
                current=current,
                runtime_issue_state=str(runtime_issue.get("state") or "") if runtime_issue is not None else "",
                updated_at=timestamp,
                bump_ledger_revision=_bump_ledger_revision,
            )
            _sync_runtime_phase_metadata(base_dir=base_dir, issue_number=_ledger_issue_number(next_ledger, issue["number"]), current=cast(dict[str, str], next_ledger["current"]), attempts=cast(dict[str, int], next_ledger.get("attempts", {})), limits=cast(dict[str, int], next_ledger.get("limits", {})), last_failure=cast(dict[str, object], next_ledger.get("lastFailure", {})), workflow=cast(dict[str, object], next_ledger.get("workflow", {})), automation=cast(dict[str, object], next_ledger.get("automation", {})), artifacts=cast(dict[str, object], next_ledger.get("artifacts", {})), queued_next_issue=cast(dict[str, object], next_ledger.get("queuedNextIssue", {})), updated_at=timestamp)
            return next_ledger, decision, request

        dispatch_reconcile_route = cast(Callable[..., tuple[JsonObject, JsonObject, JsonObject | None]], _policy_helpers.dispatch_reconcile_route)
        next_ledger, decision, request = dispatch_reconcile_route(
            current=current,
            handlers={
                "orchestrator_bootstrap": _route_orchestrator_bootstrap,
                "issue_worker": _route_issue_worker,
                "pr_verifier": _route_pr_verifier,
                "release_root_execution": _route_release_root_execution,
                "issue_selection_or_recovery": _route_issue_selection_or_recovery,
                "no_change": _route_no_change,
            },
        )
        return (
            next_ledger,
            cast(SupervisorDecision, cast(object, decision)),
            cast(SessionRequest | None, cast(object, request)),
        )
    finally:
        pass


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init", help="Start a root orchestrator session from DB-backed issue state")
    _ = init_parser.add_argument("--base-dir", default=".", help="Consumer project root")
    _ = init_parser.add_argument("--issue-number", required=True, help="Issue number already recorded in the SQLite control plane")
    _ = init_parser.add_argument("--source-session-id", default="supervisor_init", help="Source session id to record when dispatching immediately")
    _ = init_parser.add_argument("--updated-at")

    reconcile_parser = subparsers.add_parser("reconcile", help="Read DB-backed issue state and report the next supervisor action")
    _ = reconcile_parser.add_argument("--base-dir", default=".", help="Consumer project root")
    _ = reconcile_parser.add_argument("--issue-number", required=True, help="Issue number to reconcile from the SQLite control plane")
    _ = reconcile_parser.add_argument("--updated-at")

    reconcile_workspace_parser = subparsers.add_parser(
        "reconcile-workspace",
        help="Reconcile all active DB-backed issues and fill free development capacity",
    )
    _ = reconcile_workspace_parser.add_argument("--base-dir", default=".", help="Consumer project root")
    _ = reconcile_workspace_parser.add_argument("--source-session-id", default="workspace_reconcile")
    _ = reconcile_workspace_parser.add_argument("--updated-at")

    release_parser = subparsers.add_parser(
        "release",
        help="Launch an independent release_worker session for PR merge/release",
    )
    _ = release_parser.add_argument("--base-dir", default=".", help="Consumer project root")
    _ = release_parser.add_argument("--issue-number", help="Verified or idle release_pending issue number to release")
    _ = release_parser.add_argument("--source-session-id", default="manual_release", help="Source session id to record in the dispatch result")
    _ = release_parser.add_argument("--approval-override-mode", help="Release-only merge approval override mode")
    _ = release_parser.add_argument("--override-source", help="Release-only approval override source")
    _ = release_parser.add_argument(
        "--human-approval-skipped",
        action="store_true",
        help="Record that human approval is intentionally skipped for this release run",
    )
    _ = release_parser.add_argument("--updated-at")

    reconcile_issue_parser = subparsers.add_parser("reconcile-issue", help=argparse.SUPPRESS)
    _ = reconcile_issue_parser.add_argument("--base-dir", default=".", help=argparse.SUPPRESS)
    _ = reconcile_issue_parser.add_argument("--issue-number", required=True, help=argparse.SUPPRESS)
    _ = reconcile_issue_parser.add_argument("--updated-at", help=argparse.SUPPRESS)

    advance_child_parser = subparsers.add_parser(
        "advance-child",
        help="Advance a DB-backed child role after its result has been recorded in SQLite",
    )
    _ = advance_child_parser.add_argument("--base-dir", default=".", help="Consumer project root")
    _ = advance_child_parser.add_argument("--issue-number", required=True, help="Issue number already queued on a child role")
    _ = advance_child_parser.add_argument("--updated-at")

    dispatch_parser = subparsers.add_parser("dispatch", help="Launch the next session explicitly without relying on session.idle plugins")
    _ = dispatch_parser.add_argument("--base-dir", default=".", help="Consumer project root")
    _ = dispatch_parser.add_argument("--issue-number", required=True, help="Issue number whose latest DB-backed dispatch_request should be launched")
    _ = dispatch_parser.add_argument("--source-session-id", default="manual_dispatch", help="Source session id to record in the session result")
    _ = dispatch_parser.add_argument("--updated-at")

    start_issue_parser = subparsers.add_parser(
        "start-issue",
        help="Start a root orchestrator session from DB-backed issue state",
    )
    _ = start_issue_parser.add_argument("--issue-number", required=True, help="Issue number to dispatch")
    _ = start_issue_parser.add_argument(
        "--source-session-id",
        default="supervisor_start_issue",
        help="Source session id to record in the dispatch result",
    )
    _ = start_issue_parser.add_argument("--base-dir", default=".", help="Consumer project root")
    _ = start_issue_parser.add_argument("--updated-at")

    submit_artifact_parser = subparsers.add_parser(
        "submit-artifact",
        help="Record worker/verifier/release result directly into the SQLite control plane",
    )
    _ = submit_artifact_parser.add_argument("--base-dir", default=".", help="Consumer project root")
    _ = submit_artifact_parser.add_argument("--issue-number", required=True, help="Issue number owning the artifact")
    _ = submit_artifact_parser.add_argument(
        "--artifact-kind",
        required=True,
        choices=["worker_result", "evidence_packet", "release_result"],
        help="Artifact/result kind to persist",
    )
    _ = submit_artifact_parser.add_argument(
        "--payload-json",
        required=True,
        help="Normalized JSON payload to persist in issue_history.payload_json",
    )
    _ = submit_artifact_parser.add_argument(
        "--body-text",
        default="",
        help="Optional original human-readable body to preserve in issue_history.body_text",
    )
    _ = submit_artifact_parser.add_argument("--updated-at")

    show_session_parser = subparsers.add_parser(
        "show-session",
        help="Show the latest DB-backed dispatch result for an issue or workspace",
    )
    _ = show_session_parser.add_argument("--base-dir", default=".", help="Consumer project root")
    _ = show_session_parser.add_argument("--issue-number", help="Optional issue number filter")

    quarantine_parser = subparsers.add_parser("quarantine", help="Move an issue into quarantined state")
    _ = quarantine_parser.add_argument("--base-dir", default=".", help="Consumer project root")
    _ = quarantine_parser.add_argument("--issue-number", help="Explicit issue number override")
    _ = quarantine_parser.add_argument("--reason", required=True, help="Why the issue is being quarantined")
    _ = quarantine_parser.add_argument("--updated-at")

    resume_parser = subparsers.add_parser("resume-quarantined", help="Fenced resume for a quarantined issue")
    _ = resume_parser.add_argument("--base-dir", default=".", help="Consumer project root")
    _ = resume_parser.add_argument("--issue-number", help="Explicit issue number override")
    _ = resume_parser.add_argument("--reason", required=True, help="Why the issue is allowed to resume")
    _ = resume_parser.add_argument("--updated-at")

    redispatch_parser = subparsers.add_parser(
        "redispatch-quarantined",
        help="Create a fresh root session for a quarantined issue",
    )
    _ = redispatch_parser.add_argument("--base-dir", default=".", help="Consumer project root")
    _ = redispatch_parser.add_argument("--issue-number", required=True, help="Quarantined issue number to redispatch")
    _ = redispatch_parser.add_argument("--reason", required=True, help="Why the quarantined issue is safe to redispatch")
    _ = redispatch_parser.add_argument(
        "--source-session-id",
        default="supervisor_redispatch_quarantined",
        help="Source session id to record in the new session result",
    )
    _ = redispatch_parser.add_argument("--updated-at")

    fail_quarantine_parser = subparsers.add_parser("fail-quarantined", help="Mark a quarantined issue as failed")
    _ = fail_quarantine_parser.add_argument("--base-dir", default=".", help="Consumer project root")
    _ = fail_quarantine_parser.add_argument("--issue-number", help="Explicit issue number override")
    _ = fail_quarantine_parser.add_argument("--reason", required=True, help="Why the quarantined issue is terminally failed")
    _ = fail_quarantine_parser.add_argument("--updated-at")

    inspect_parser = subparsers.add_parser("inspect", help="Inspect control-plane issue, decision, and GitHub sync state")
    _ = inspect_parser.add_argument("--base-dir", default=".", help="Consumer project root")
    _ = inspect_parser.add_argument("--issue-number", help="Explicit issue number override")

    retry_sync_parser = subparsers.add_parser("retry-github-sync", help="Retry a failed GitHub projection sync attempt by command id")
    _ = retry_sync_parser.add_argument("--base-dir", default=".", help="Consumer project root")
    _ = retry_sync_parser.add_argument("--command-id", required=True, help="Failed GitHub sync command id to replay")
    _ = retry_sync_parser.add_argument("--updated-at")

    retry_failed_parser = subparsers.add_parser("retry-failed", help="Move a retryable failed issue back to ready-for-agent")
    _ = retry_failed_parser.add_argument("--base-dir", default=".", help="Consumer project root")
    _ = retry_failed_parser.add_argument("--issue-number", help="Explicit issue number override")
    _ = retry_failed_parser.add_argument("--reason", required=True, help="Why the failed issue is safe to retry")
    _ = retry_failed_parser.add_argument("--updated-at")

    clear_ready_fence_parser = subparsers.add_parser(
        "clear-ready-session-fence",
        help="Clear a stale current_session_id from a ready issue",
    )
    _ = clear_ready_fence_parser.add_argument("--base-dir", default=".", help="Consumer project root")
    _ = clear_ready_fence_parser.add_argument("--issue-number", help="Explicit issue number override")
    _ = clear_ready_fence_parser.add_argument("--reason", required=True, help="Why the ready issue fence is stale")
    _ = clear_ready_fence_parser.add_argument("--updated-at")

    return parser


def main(argv: list[str] | None = None) -> int:
    raw_argv = list(argv) if argv is not None else sys.argv[1:]
    args = build_parser().parse_args(raw_argv)

    if cast(str, args.command) == "init":
        base_dir = Path(cast(str, args.base_dir)).resolve()
        issue_number = _normalize_requested_issue_number(cast(str, args.issue_number))
        record = _load_issue_packet_from_db(base_dir, issue_number)
        if record is None:
            raise RuntimeError(
                f"issue packet not recorded in SQLite for issue #{issue_number}; sync the packet into the DB control plane before initializing the supervisor"
        )
        del record
        session_result = start_issue(
            base_dir=base_dir,
            issue_number=issue_number,
            source_session_id=cast(str, args.source_session_id),
            updated_at=cast(str | None, args.updated_at),
        )
        print(f"delegated supervisor init to DB-backed start-issue for issue #{issue_number}")
        print(json.dumps(session_result, indent=2, ensure_ascii=False))
        return 0

    if cast(str, args.command) == "dispatch":
        base_dir = Path(cast(str, getattr(args, "base_dir", "."))).resolve()
        issue_number = _normalize_requested_issue_number(cast(str, args.issue_number))
        request_row = read_latest_history_entry(base_dir, issue_number=issue_number, entry_type="dispatch_request")
        if request_row is None:
            raise ValueError(f"no DB-backed dispatch_request found for issue #{issue_number}")
        payload = json.loads(str(request_row.get("payload_json") or "{}"))
        if not isinstance(payload, dict):
            raise ValueError(f"invalid DB-backed dispatch_request payload for issue #{issue_number}")
        session_result = dispatch_request_from_db(
            cast(SessionRequest, cast(object, payload)),
            base_dir=base_dir,
            source_session_id=cast(str, args.source_session_id),
            updated_at=cast(str | None, args.updated_at),
        )
        print(json.dumps(session_result, indent=2, ensure_ascii=False))
        return 0

    if cast(str, args.command) == "start-issue":
        base_dir = Path(cast(str, args.base_dir)).resolve()
        session_result = start_issue(
            base_dir=base_dir,
            issue_number=cast(str, args.issue_number),
            source_session_id=cast(str, args.source_session_id),
            updated_at=cast(str | None, args.updated_at),
        )
        print(json.dumps(session_result, indent=2, ensure_ascii=False))
        return 0

    if cast(str, args.command) == "submit-artifact":
        base_dir = Path(cast(str, args.base_dir)).resolve()
        try:
            payload_raw = json.loads(cast(str, args.payload_json))
        except json.JSONDecodeError as error:
            raise ValueError(f"invalid --payload-json: {error}") from error
        if not isinstance(payload_raw, dict):
            raise ValueError("--payload-json must decode to a JSON object")
        persisted = submit_artifact(
            base_dir=base_dir,
            issue_number=cast(str, args.issue_number),
            artifact_kind=cast(str, args.artifact_kind),
            payload=cast(JsonObject, payload_raw),
            body_text=cast(str, args.body_text),
            updated_at=cast(str | None, args.updated_at),
        )
        print(json.dumps(persisted, indent=2, ensure_ascii=False))
        return 0

    if cast(str, args.command) == "show-session":
        base_dir = Path(cast(str, args.base_dir)).resolve()
        session_result = show_latest_session(
            base_dir=base_dir,
            issue_number=cast(str | None, getattr(args, "issue_number", None)),
        )
        if session_result is None:
            print(json.dumps({"status": "missing", "error": "no DB-backed dispatch result found"}, ensure_ascii=False))
            return 1
        print(json.dumps(session_result, ensure_ascii=False))
        return 0

    if cast(str, args.command) == "reconcile-workspace":
        base_dir = Path(cast(str, args.base_dir)).resolve()
        payload = reconcile_workspace_from_db(
            base_dir=base_dir,
            updated_at=cast(str | None, args.updated_at),
            source_session_id=cast(str, args.source_session_id),
        )
        print(json.dumps(payload, ensure_ascii=False))
        return 0

    if cast(str, args.command) == "release":
        base_dir = Path(cast(str, args.base_dir)).resolve()
        session_result = start_release(
            base_dir=base_dir,
            issue_number=cast(str | None, getattr(args, "issue_number", None)),
            source_session_id=cast(str, args.source_session_id),
            approval_override_mode=cast(str | None, getattr(args, "approval_override_mode", None)),
            override_source=cast(str | None, getattr(args, "override_source", None)),
            human_approval_skipped=cast(bool, getattr(args, "human_approval_skipped", False)),
            updated_at=cast(str | None, args.updated_at),
        )
        print(json.dumps(session_result, indent=2, ensure_ascii=False))
        return 0

    if cast(str, args.command) in {"reconcile", "reconcile-issue", "advance-child"}:
        return _run_reconcile_db_cli(
            base_dir=Path(cast(str, args.base_dir)).resolve(),
            issue_number=cast(str, args.issue_number),
            updated_at=cast(str | None, args.updated_at),
            child_only=cast(str, args.command) == "advance-child",
        )

    if cast(str, args.command) == "redispatch-quarantined":
        base_dir = Path(cast(str, getattr(args, "base_dir", "."))).resolve()
        issue_number = _normalize_requested_issue_number(cast(str, args.issue_number))
        issue = read_issue(base_dir, issue_number)
        if issue is None:
            raise ValueError(f"unknown issue #{issue_number}")
        branch = str(issue.get("branch") or "")
        if not branch:
            raise ValueError(f"issue #{issue_number} is missing branch metadata")
        redispatch_quarantined_issue_execution(
            base_dir=base_dir,
            issue_number=issue_number,
            branch=branch,
            reason=cast(str, args.reason),
            source_session_id=cast(str, args.source_session_id),
            updated_at=cast(str | None, args.updated_at),
        )
        validation_ledger = _validation_ledger_from_db(base_dir=base_dir, issue_number=issue_number)
        if validation_ledger is None:
            raise ValueError(f"unknown issue #{issue_number}")
        request = build_orchestrator_request(validation_ledger)
        session_result = dispatch_request_from_db(
            request,
            base_dir=base_dir,
            source_session_id=cast(str, args.source_session_id),
            updated_at=cast(str | None, args.updated_at),
            failure_restore_state="quarantined",
        )
        print(json.dumps(session_result, indent=2, ensure_ascii=False))
        return 0

    if cast(str, args.command) in {"quarantine", "resume-quarantined", "fail-quarantined"}:
        base_dir = Path(cast(str, args.base_dir)).resolve()
        issue_number = cast(str | None, getattr(args, "issue_number", None))
        if not issue_number:
            raise ValueError("--issue-number is required for DB-backed operator commands")
        reason = cast(str, args.reason)
        updated_at = cast(str | None, args.updated_at)
        if cast(str, args.command) == "quarantine":
            quarantine_issue_execution(base_dir=base_dir, issue_number=issue_number, reason=reason, updated_at=updated_at)
            print(f"quarantined issue #{issue_number}")
            return 0
        if cast(str, args.command) == "resume-quarantined":
            resume_quarantined_issue_execution(base_dir=base_dir, issue_number=issue_number, reason=reason, updated_at=updated_at)
            print(f"resumed quarantined issue #{issue_number}")
            return 0
        fail_quarantined_issue_execution(base_dir=base_dir, issue_number=issue_number, reason=reason, updated_at=updated_at)
        print(f"failed quarantined issue #{issue_number}")
        return 0

    if cast(str, args.command) == "inspect":
        base_dir = Path(cast(str, args.base_dir)).resolve()
        issue_number = cast(str | None, getattr(args, "issue_number", None))
        if not issue_number:
            raise ValueError("--issue-number is required for DB-backed operator commands")
        print(json.dumps(inspect_control_plane(base_dir=base_dir, issue_number=issue_number), indent=2, ensure_ascii=False))
        return 0

    if cast(str, args.command) == "retry-github-sync":
        base_dir = Path(cast(str, args.base_dir)).resolve()
        payload = retry_github_sync_attempt(
            base_dir=base_dir,
            command_id=cast(str, args.command_id),
            updated_at=cast(str | None, args.updated_at),
        )
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return 0

    if cast(str, args.command) == "retry-failed":
        base_dir = Path(cast(str, args.base_dir)).resolve()
        issue_number = cast(str | None, getattr(args, "issue_number", None))
        if not issue_number:
            raise ValueError("--issue-number is required for DB-backed operator commands")
        payload = retry_failed_issue_execution(
            base_dir=base_dir,
            issue_number=issue_number,
            reason=cast(str, args.reason),
            updated_at=cast(str | None, args.updated_at),
        )
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return 0

    if cast(str, args.command) == "clear-ready-session-fence":
        base_dir = Path(cast(str, args.base_dir)).resolve()
        issue_number = cast(str | None, getattr(args, "issue_number", None))
        if not issue_number:
            raise ValueError("--issue-number is required for DB-backed operator commands")
        payload = clear_ready_issue_session_fence(
            base_dir=base_dir,
            issue_number=issue_number,
            reason=cast(str, args.reason),
            updated_at=cast(str | None, args.updated_at),
        )
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return 0

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
