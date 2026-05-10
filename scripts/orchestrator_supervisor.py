#!/usr/bin/env python3
"""Runtime supervisor for nonstop autonomous issue dispatch."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from uuid import uuid4
from typing import NotRequired, TypedDict, cast

from scripts.orchestrator_compact_payload import write_checkpoint_file


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LEDGER_PATH = ROOT / ".opencode/runtime/orchestrator-ledger.json"
DEFAULT_REQUEST_PATH = ROOT / ".opencode/runtime/new-session-request.json"
DEFAULT_SESSION_RESULT_PATH = ROOT / ".opencode/runtime/new-session-result.json"
DEFAULT_ISSUE_INTAKE_SCRIPT_PATH = ROOT / "scripts/issue_packet_intake.py"
DEFAULT_CHECKPOINT_PATH = "docs/agents/runtime/context-checkpoint.yaml"
DEFAULT_WORKFLOW_POLICY_PATH = "docs/agents/autonomous-development-workflow.yaml"
DEFAULT_SUPERVISOR_DOC_PATH = "docs/agents/runtime/nonstop-supervisor-loop.md"
DEFAULT_RELEASE_RESULT_TEMPLATE_PATH = "docs/agents/release-result-template.yaml"
DEFAULT_ROOT_SESSION_AGENT = "hephaestus"
MAX_ROLE_ATTEMPTS = 3
TRANSIENT_RELEASE_BLOCKERS = {
    "required_checks_pending",
    "required_checks_failed",
    "pr_not_mergeable",
    "workspace_hygiene_failed",
    "transient_tool_failure",
}


@dataclass
class IssuePacketRecord:
    issue_number: str
    title: str
    branch: str
    issue_packet_path: str
    prior_handoff: str
    labels: list[str]
    parent_reference: str
    dependencies: list[str]


JsonObject = dict[str, object]


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


class SessionResult(TypedDict, total=False):
    status: str
    sourceSessionID: str
    rootSessionID: str
    title: str
    reason: str
    error: str
    tuiResumeCommand: str
    cliOpenCommand: str
    recommendedAction: str
    stopContinuationStatus: str
    stopContinuationAttempts: int
    role: str
    stage: str
    issueNumber: str
    branch: str
    recordedAt: str


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


def _now(updated_at: str | None = None) -> str:
    return updated_at or datetime.now().astimezone().isoformat(timespec="seconds")


def _parse_scalar(value: str) -> str:
    value = value.strip()
    if not value:
        return ""
    if value.startswith('"') and value.endswith('"'):
        loaded = cast(object, json.loads(value))
        return loaded if isinstance(loaded, str) else str(loaded)
    return value


def _parse_bool(value: str) -> bool:
    normalized = value.strip().lower()
    return normalized in {"true", "yes", "1"}


def _extract_top_level_scalar(text: str, key: str) -> str:
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or line.startswith(" "):
            continue
        if stripped.startswith(f"{key}:"):
            _, value = stripped.split(":", 1)
            return _parse_scalar(value)
    raise ValueError(f"missing top-level scalar {key!r}")


def _extract_nested_scalar(text: str, block_name: str, nested_key: str) -> str:
    in_block = False
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        indent = len(line) - len(line.lstrip(" "))
        if indent == 0 and stripped == f"{block_name}:":
            in_block = True
            continue
        if in_block and indent == 0:
            break
        if in_block and indent == 2 and stripped.startswith(f"{nested_key}:"):
            _, value = stripped.split(":", 1)
            return _parse_scalar(value)
    raise ValueError(f"missing nested scalar {nested_key!r} in block {block_name!r}")


def _extract_nested_scalar_optional(text: str, block_name: str, nested_key: str) -> str:
    try:
        return _extract_nested_scalar(text, block_name, nested_key)
    except ValueError:
        return ""


def _extract_issue_inline_reference(text: str, nested_key: str) -> str:
    in_issue = False
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        indent = len(line) - len(line.lstrip(" "))
        if indent == 0 and stripped == "issue:":
            in_issue = True
            continue
        if in_issue and indent == 0:
            break
        if in_issue and indent == 2 and stripped.startswith(f"{nested_key}:") and "{" in stripped and "}" in stripped:
            body = stripped.split("{", 1)[1].rsplit("}", 1)[0]
            for part in [part.strip() for part in body.split(",")]:
                if ":" not in part:
                    continue
                found_key, value = part.split(":", 1)
                if found_key.strip() == "reference":
                    return _parse_scalar(value)
    return ""


def _extract_issue_labels(text: str) -> list[str]:
    in_issue = False
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        indent = len(line) - len(line.lstrip(" "))
        if indent == 0 and stripped == "issue:":
            in_issue = True
            continue
        if in_issue and indent == 0:
            break
        if in_issue and indent == 2 and stripped.startswith("labels:"):
            _, value = stripped.split(":", 1)
            value = value.strip()
            if value.startswith("[") and value.endswith("]"):
                body = value[1:-1].strip()
                if not body:
                    return []
                return [_parse_scalar(part.strip()) for part in body.split(",")]
    return []


def _extract_list_block(text: str, block_name: str) -> list[str]:
    in_block = False
    values: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        indent = len(line) - len(line.lstrip(" "))
        if indent == 0 and stripped == f"{block_name}:":
            in_block = True
            continue
        if in_block and indent == 0:
            break
        if in_block and indent == 2 and stripped.endswith(": []"):
            return []
        if in_block and indent == 2 and stripped.startswith("- "):
            values.append(_parse_scalar(stripped[2:]))
    return values


def _extract_nested_list(text: str, block_name: str, nested_key: str) -> list[str]:
    in_block = False
    in_nested = False
    values: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        indent = len(line) - len(line.lstrip(" "))
        if indent == 0 and stripped == f"{block_name}:":
            in_block = True
            in_nested = False
            continue
        if in_block and indent == 0:
            if in_nested and values:
                return values
            in_block = False
            in_nested = False
        if not in_block:
            continue
        if indent == 2 and stripped.startswith(f"{nested_key}:"):
            _, value = stripped.split(":", 1)
            value = value.strip()
            if value.startswith("[") and value.endswith("]"):
                body = value[1:-1].strip()
                if not body:
                    return []
                return [_parse_scalar(part.strip()) for part in body.split(",")]
            in_nested = True
            values = []
            continue
        if in_nested and indent <= 2:
            if values:
                return values
            in_nested = False
            continue
        if in_nested and indent == 4 and stripped.startswith("- "):
            values.append(_parse_scalar(stripped[2:]))
    return values


def _parse_issue_numbers(text: str) -> list[str]:
    return [match.group(1) for match in re.finditer(r"(?i)issue\s*#(\d+)", text)]


def _dependency_issue_numbers(issue_number: str, dependencies: list[str]) -> list[str]:
    numbers: list[str] = []
    for dependency in dependencies:
        lowered = dependency.lower()
        blocked_match = re.search(r"blocked by issue\s*#(\d+)", lowered)
        if blocked_match:
            blocked_by = blocked_match.group(1)
            if blocked_by != issue_number and blocked_by not in numbers:
                numbers.append(blocked_by)
            continue
        if not any(token in lowered for token in ["released", "closed", "complete", "depends on", "requires"]):
            continue
        for found in _parse_issue_numbers(dependency):
            if found != issue_number and found not in numbers:
                numbers.append(found)
    return numbers


def _extract_inline_mapping_value(text: str, prefix: str, key: str) -> str:
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith(prefix):
            body = stripped.split("{", 1)[1].rsplit("}", 1)[0]
            parts = [part.strip() for part in body.split(",")]
            for part in parts:
                if ":" not in part:
                    continue
                found_key, value = part.split(":", 1)
                if found_key.strip() == key:
                    return _parse_scalar(value)
    raise ValueError(f"missing {key!r} in inline mapping {prefix!r}")


def _extract_inline_mapping_value_optional(text: str, prefix: str, key: str) -> str:
    try:
        return _extract_inline_mapping_value(text, prefix, key)
    except (IndexError, ValueError):
        return ""


def _extract_inline_bool_optional(text: str, prefix: str, key: str) -> bool | None:
    value = _extract_inline_mapping_value_optional(text, prefix, key)
    if not value:
        return None
    return _parse_bool(value)


def parse_issue_packet_text(text: str, issue_packet_path: str) -> IssuePacketRecord:
    issue_number = _extract_nested_scalar(text, "issue", "number")
    title = _extract_nested_scalar_optional(text, "issue", "title")
    branch = _extract_inline_mapping_value(text, "branch:", "name")
    prior_handoff = _extract_nested_scalar_optional(text, "bootstrap_context", "prior_handoff")
    return IssuePacketRecord(
        issue_number=issue_number,
        title=title,
        branch=branch,
        issue_packet_path=issue_packet_path,
        prior_handoff="" if prior_handoff == "none" else prior_handoff,
        labels=_extract_issue_labels(text),
        parent_reference=_extract_issue_inline_reference(text, "parent"),
        dependencies=_extract_nested_list(text, "implementation_notes", "dependencies") or _extract_list_block(text, "dependencies"),
    )


def default_worker_result_path(issue_number: str) -> str:
    return f"docs/agents/worker-results/issue-{issue_number}.yaml"


def default_evidence_packet_path(issue_number: str, pr_number: str) -> str:
    return f"docs/agents/evidence/issue-{issue_number}-pr-{pr_number}.yaml"


def default_release_result_path(issue_number: str, pr_number: str) -> str:
    return f"docs/agents/release-results/issue-{issue_number}-pr-{pr_number}.yaml"


def parse_worker_result_file(path: Path) -> JsonObject:
    text = path.read_text(encoding="utf-8")
    return {
        "status": _extract_top_level_scalar(text, "status"),
        "pr_number": _extract_nested_scalar_optional(text, "pr", "number"),
        "next_recommended_step": _extract_top_level_scalar(text, "next_recommended_step"),
        "failure_kind": _extract_inline_mapping_value_optional(text, "failure_classification:", "kind"),
        "retryable": _extract_inline_bool_optional(text, "failure_classification:", "retryable"),
    }


def parse_evidence_packet_file(path: Path) -> JsonObject:
    text = path.read_text(encoding="utf-8")
    return {
        "status": _extract_top_level_scalar(text, "status"),
        "pr_number": _extract_nested_scalar_optional(text, "subject", "pr_number"),
        "next_recommended_step": _extract_top_level_scalar(text, "next_recommended_step"),
        "failure_kind": _extract_inline_mapping_value_optional(text, "failure_classification:", "kind"),
        "retryable": _extract_inline_bool_optional(text, "failure_classification:", "retryable"),
    }


def parse_release_result_file(path: Path) -> JsonObject:
    text = path.read_text(encoding="utf-8")
    return {
        "status": _extract_top_level_scalar(text, "status"),
        "blocked_reason": _extract_top_level_scalar(text, "blocked_reason"),
        "next_recommended_step": _extract_nested_scalar(text, "summary", "next_recommended_step"),
        "failure_kind": _extract_inline_mapping_value_optional(text, "failure_classification:", "kind"),
        "retryable": _extract_inline_bool_optional(text, "failure_classification:", "retryable"),
    }


def _read_json(path: Path) -> JsonObject:
    return cast(JsonObject, json.loads(path.read_text(encoding="utf-8")))


def _write_json(path: Path, payload: JsonObject) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    _ = path.write_text(f"{json.dumps(payload, indent=2, ensure_ascii=False)}\n", encoding="utf-8")


def _consume_session_request(request_path: Path) -> SessionRequest:
    payload = cast(object, _read_json(request_path))
    request_path.unlink(missing_ok=True)
    return cast(SessionRequest, payload)


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
    if request["issueNumber"] != issue.get("number"):
        return f"stale request issue #{request['issueNumber']} does not match ledger issue #{issue.get('number', '')}"
    if request["branch"] != issue.get("branch"):
        return f"stale request branch {request['branch']} does not match ledger branch {issue.get('branch', '')}"

    ledger_revision = str(ledger.get("ledgerRevision") or ledger.get("updatedAt") or "")
    request_revision = request.get("createdForLedgerRevision", "")
    if request_revision and ledger_revision and request_revision != ledger_revision:
        return f"stale request revision {request_revision} does not match ledger revision {ledger_revision}"

    completed = _completed_issue_numbers(base_dir, cast(dict[str, str], ledger["workflow"])["checkpointPath"])
    if request["issueNumber"] in completed:
        return f"issue #{request['issueNumber']} is already completed or released; refusing to dispatch stale request"

    issue_packet_path = _resolve_artifact_path(issue["issuePacketPath"], base_dir=base_dir)
    if not issue_packet_path.exists():
        return f"issue packet not found for issue #{request['issueNumber']}: {issue['issuePacketPath']}"
    packet = parse_issue_packet_text(issue_packet_path.read_text(encoding="utf-8"), issue["issuePacketPath"])
    if "ready-for-agent" not in packet.labels:
        return f"issue #{request['issueNumber']} is not ready-for-agent; refusing to dispatch"
    if packet.issue_number != request["issueNumber"]:
        return f"issue packet {issue['issuePacketPath']} belongs to issue #{packet.issue_number}, not request issue #{request['issueNumber']}"
    return ""


def read_session_request(request_path: Path) -> SessionRequest:
    payload = cast(object, _read_json(request_path))
    return cast(SessionRequest, payload)


def consume_session_request(request_path: Path) -> SessionRequest:
    return _consume_session_request(request_path)


def _extract_session_id_from_run_output(output: str) -> str:
    for line in output.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        try:
            event = cast(dict[str, object], json.loads(stripped))
        except json.JSONDecodeError:
            continue
        session_id = event.get("sessionID")
        if isinstance(session_id, str) and session_id:
            return session_id
    raise RuntimeError("opencode run --format json did not emit a sessionID")


def dispatch_session_request(
    request: SessionRequest,
    *,
    workdir: Path,
    source_session_id: str,
    updated_at: str | None = None,
) -> SessionResult:
    completed = subprocess.run(
        [
            "opencode",
            "run",
            "--format",
            "json",
            "--title",
            request["title"],
            "--agent",
            request["agent"],
            request["prompt"],
        ],
        cwd=str(workdir),
        check=False,
        capture_output=True,
        text=True,
    )
    timestamp = _now(updated_at)
    if completed.returncode != 0:
        return {
            "status": "error",
            "sourceSessionID": source_session_id,
            "title": request["title"],
            "reason": request["reason"],
            "role": request["role"],
            "stage": request["stage"],
            "issueNumber": request["issueNumber"],
            "branch": request["branch"],
            "error": (completed.stderr or completed.stdout).strip() or f"opencode run failed with exit code {completed.returncode}",
            "recordedAt": timestamp,
        }

    root_session_id = _extract_session_id_from_run_output(completed.stdout)
    return {
        "status": "success",
        "sourceSessionID": source_session_id,
        "rootSessionID": root_session_id,
        "title": request["title"],
        "reason": request["reason"],
        "role": request["role"],
        "stage": request["stage"],
        "issueNumber": request["issueNumber"],
        "branch": request["branch"],
        "tuiResumeCommand": "/sessions",
        "cliOpenCommand": f"opencode --session {root_session_id}",
        "recommendedAction": (
            f"Open /sessions in OpenCode TUI and switch to {root_session_id}, or run opencode --session {root_session_id}."
        ),
        "stopContinuationStatus": "not_applicable",
        "stopContinuationAttempts": 0,
        "recordedAt": timestamp,
    }


def write_session_result(session_result_path: Path, session_result: SessionResult) -> None:
    _write_json(session_result_path, dict(session_result))


def default_session_result_path_for_request(request_path: Path) -> Path:
    if request_path == DEFAULT_REQUEST_PATH:
        return DEFAULT_SESSION_RESULT_PATH
    return request_path.parent / "new-session-result.json"


def sync_session_result_into_ledger(ledger_path: Path, session_result_path: Path) -> JsonObject:
    ledger = _read_json(ledger_path)
    _sync_session_result(ledger, session_result_path)
    write_ledger_file(ledger_path, ledger)
    return ledger


def _resolve_artifact_path(path_text: str, *, base_dir: Path) -> Path:
    path = Path(path_text)
    if path.is_absolute():
        return path
    candidate = base_dir / path
    if candidate.exists():
        return candidate
    return ROOT / path


def _infer_artifact_base_dir(ledger_path: Path) -> Path:
    parents = ledger_path.parents
    if len(parents) >= 3:
        return parents[2]
    if len(parents) >= 1:
        return parents[len(parents) - 1]
    return ROOT


def create_initial_ledger(
    *,
    issue_packet: IssuePacketRecord,
    checkpoint_path: str = DEFAULT_CHECKPOINT_PATH,
    workflow_policy_path: str = DEFAULT_WORKFLOW_POLICY_PATH,
    root_session_agent: str = DEFAULT_ROOT_SESSION_AGENT,
    updated_at: str | None = None,
) -> JsonObject:
    timestamp = _now(updated_at)
    return {
        "schemaVersion": "1.0",
        "automation": {
            "continueWithoutHuman": True,
            "queueNextSessionOnIdle": True,
            "rootSessionAgent": root_session_agent,
            "supervisorDocPath": DEFAULT_SUPERVISOR_DOC_PATH,
        },
        "issue": {
            "number": issue_packet.issue_number,
            "title": issue_packet.title,
            "branch": issue_packet.branch,
            "issuePacketPath": issue_packet.issue_packet_path,
            "priorHandoffPath": issue_packet.prior_handoff,
            "parentReference": issue_packet.parent_reference,
        },
        "workflow": {
            "checkpointPath": checkpoint_path,
            "workflowPolicyPath": workflow_policy_path,
            "releaseResultTemplatePath": DEFAULT_RELEASE_RESULT_TEMPLATE_PATH,
        },
        "artifacts": {
            "workerResultPath": default_worker_result_path(issue_packet.issue_number),
            "evidencePacketPath": "",
            "releaseResultPath": "",
            "lastSessionResultPath": str(DEFAULT_SESSION_RESULT_PATH.relative_to(ROOT)),
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


def write_ledger_file(ledger_path: Path, ledger: JsonObject) -> None:
    _write_json(ledger_path, ledger)


def _completed_issue_numbers(base_dir: Path, checkpoint_path: str) -> set[str]:
    completed: set[str] = set()
    checkpoint_file = _resolve_artifact_path(checkpoint_path, base_dir=base_dir)
    if checkpoint_file.exists():
        completed.update(_checkpoint_completed_issue_numbers(checkpoint_file.read_text(encoding="utf-8")))

    catalog_path = base_dir / "docs/agents/e2e/test-case-catalog.yaml"
    if catalog_path.exists():
        current_issue = ""
        for line in catalog_path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if stripped.startswith("issue: {"):
                current_issue = _extract_inline_mapping_value(stripped, "issue:", "number")
            if stripped.startswith("status:"):
                status = _parse_scalar(stripped.split(":", 1)[1])
                if current_issue and status in {"verified", "released"}:
                    completed.add(current_issue)

    release_results_dir = base_dir / "docs/agents/release-results"
    if release_results_dir.exists():
        for path in sorted(release_results_dir.glob("*.yaml")):
            try:
                release = parse_release_result_file(path)
            except ValueError:
                continue
            if release["status"] == "success":
                completed.update(_parse_issue_numbers(path.stem))

    return completed


def _checkpoint_completed_issue_numbers(text: str) -> set[str]:
    completed: set[str] = set()
    in_completed = False
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        indent = len(line) - len(line.lstrip(" "))
        if indent == 2 and stripped == "completed:":
            in_completed = True
            continue
        if in_completed and indent <= 2 and not stripped.startswith("- "):
            break
        if in_completed and stripped.startswith("- "):
            completed.update(_parse_issue_numbers(stripped))
    return completed


def select_next_issue_packet(base_dir: Path, *, workflow: dict[str, str], current_issue: dict[str, str]) -> IssuePacketRecord | None:
    packets_dir = base_dir / "docs/agents/issue-packets"
    if not packets_dir.exists():
        return None
    completed = _completed_issue_numbers(base_dir, workflow["checkpointPath"])
    current_number = current_issue.get("number", "")
    current_parent = current_issue.get("parentReference", "")
    def issue_sort_key(item: Path) -> int:
        match = re.search(r"(\d+)", item.stem)
        return int(match.group(1)) if match else 10**9

    candidates = sorted(packets_dir.glob("issue-*.yaml"), key=issue_sort_key)
    for path in candidates:
        packet = parse_issue_packet_text(path.read_text(encoding="utf-8"), str(path.relative_to(base_dir)))
        if packet.issue_number == current_number:
            continue
        if "ready-for-agent" not in packet.labels:
            continue
        if current_parent and packet.parent_reference != current_parent:
            continue
        if any(number not in completed for number in _dependency_issue_numbers(packet.issue_number, packet.dependencies)):
            continue
        return packet
    return None


def run_issue_packet_intake(base_dir: Path) -> bool:
    script_path = DEFAULT_ISSUE_INTAKE_SCRIPT_PATH
    try:
        _ = subprocess.run(
            [
                "python3",
                str(script_path),
                "--output-dir",
                str(base_dir / "docs/agents/issue-packets"),
            ],
            cwd=base_dir,
            check=True,
            capture_output=True,
            text=True,
        )
        return True
    except subprocess.CalledProcessError:
        return False


def _handoff_to_selected_issue(
    current_ledger: JsonObject,
    *,
    selected_issue: IssuePacketRecord,
    base_dir: Path,
    updated_at: str,
    summary: str,
) -> tuple[JsonObject, SupervisorDecision, SessionRequest]:
    workflow = cast(dict[str, str], current_ledger["workflow"])
    checkpoint_path = _resolve_artifact_path(workflow["checkpointPath"], base_dir=base_dir)
    _ = write_checkpoint_file(
        checkpoint_path,
        issue_number=selected_issue.issue_number,
        branch=selected_issue.branch,
        role="main_orchestrator",
        agent=_root_session_agent(current_ledger),
        issue_packet=selected_issue.issue_packet_path,
        handoff=selected_issue.prior_handoff,
        workflow_policy_path=workflow["workflowPolicyPath"],
        updated_at=updated_at,
    )
    next_ledger = create_initial_ledger(
        issue_packet=selected_issue,
        checkpoint_path=workflow["checkpointPath"],
        workflow_policy_path=workflow["workflowPolicyPath"],
        root_session_agent=_root_session_agent(current_ledger),
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


def _sync_session_result(ledger: JsonObject, session_result_path: Path) -> None:
    if not session_result_path.exists():
        return
    session_result = _read_json(session_result_path)
    previous = cast(JsonObject, ledger.get("lastSessionResult", {}))
    if previous.get("recordedAt") == session_result.get("recordedAt"):
        return
    ledger["lastSessionResult"] = session_result
    stop_attempts = session_result.get("stopContinuationAttempts")
    if isinstance(stop_attempts, int):
        cast(dict[str, int], ledger["attempts"])["source_session_stop"] = stop_attempts
    recorded_at = session_result.get("recordedAt")
    if isinstance(recorded_at, str) and recorded_at:
        _bump_ledger_revision(ledger, recorded_at)


def _bump_ledger_revision(ledger: JsonObject, updated_at: str) -> None:
    ledger["ledgerRevision"] = updated_at


def _dispatch_consumed_request(
    request_path: Path,
    *,
    ledger_path: Path,
    session_result_path: Path,
    source_session_id: str,
    updated_at: str | None,
) -> SessionResult:
    request = _consume_session_request(request_path)
    ledger = _read_json(ledger_path) if ledger_path.exists() else {}
    base_dir = _infer_artifact_base_dir(ledger_path)
    validation_error = validate_session_request_for_dispatch(request, ledger, base_dir=base_dir) if ledger else "ledger not found"
    if validation_error:
        session_result = _reject_session_request(
            request,
            source_session_id=source_session_id,
            error=validation_error,
            updated_at=updated_at,
        )
    else:
        session_result = dispatch_session_request(
            request,
            workdir=base_dir,
            source_session_id=source_session_id,
            updated_at=updated_at,
        )
    write_session_result(session_result_path, session_result)
    if ledger_path.exists():
        synced_ledger = _read_json(ledger_path)
        _sync_session_result(synced_ledger, session_result_path)
        write_ledger_file(ledger_path, synced_ledger)
    return session_result


def _build_common_prompt_lines(ledger: JsonObject) -> list[str]:
    issue = cast(dict[str, str], ledger["issue"])
    workflow = cast(dict[str, str], ledger["workflow"])
    return [
        "Bootstrap from checkpoint and runtime artifacts only.",
        f"Read {workflow['checkpointPath']} first.",
        "Read .opencode/runtime/orchestrator-ledger.json second.",
        f"Read {workflow['workflowPolicyPath']} for role boundaries and gates.",
        f"Read {DEFAULT_SUPERVISOR_DOC_PATH} for the nonstop supervisor contract.",
        f"Active issue: #{issue['number']} on branch {issue['branch']}.",
        "Do not wait for a user reply before advancing the workflow.",
    ]


def _build_prompt(ledger: JsonObject, role: str, stage: str, decision_summary: str) -> str:
    issue = cast(dict[str, str], ledger["issue"])
    artifacts = cast(dict[str, str], ledger["artifacts"])
    common = _build_common_prompt_lines(ledger)
    if role == "main_orchestrator" and stage == "orchestrator_bootstrap":
        lines = common + [
            "You are the fresh main_orchestrator session for the selected AFK issue.",
            f"Read {issue['issuePacketPath']} and any prior handoff when present.",
            "Confirm the issue packet and branch are still the correct target.",
            "Do not implement issue scope directly.",
            "You own orchestration for the whole selected issue inside this root session.",
            "Run issue_worker, pr_verifier, and release_worker as subagents from this root orchestrator session; do not create root sessions for those roles.",
            "After each subagent writes its compact artifact, run:",
            "PYTHONPATH=. python3 scripts/orchestrator_supervisor.py reconcile --ledger .opencode/runtime/orchestrator-ledger.json",
            "Use the supervisor decision to choose the next subagent role. Only main_orchestrator recovery or next-issue handoff may create another root session.",
        ]
    elif role == "issue_worker":
        lines = common + [
            f"You are the issue_worker subagent for issue #{issue['number']}.",
            f"Read {issue['issuePacketPath']} and implement only that issue scope.",
            f"Write {artifacts['workerResultPath']} using docs/agents/worker-result-template.yaml.",
            "If the worker is blocked or failed, include failure_classification with kind, retryable, routed_to, and root_cause_signature.",
            "Do not claim final acceptance; that belongs to pr_verifier.",
            "When the worker_result is written, return control to the main_orchestrator root session; do not launch a root session.",
        ]
    elif role == "pr_verifier":
        lines = common + [
            f"You are the pr_verifier subagent for issue #{issue['number']}.",
            f"Read {issue['issuePacketPath']} and {artifacts['workerResultPath']} before touching anything else.",
            f"Write {artifacts['evidencePacketPath']} using docs/agents/evidence-packet-template.yaml.",
            "If verification is blocked or fails, include failure_classification with kind, retryable, routed_to, and root_cause_signature.",
            "Final acceptance belongs to this verifier role; keep raw logs outside repo docs.",
            "When the evidence packet is written, return control to the main_orchestrator root session; do not launch a root session.",
        ]
    elif role == "release_worker":
        lines = common + [
            f"You are the release_worker subagent for issue #{issue['number']}.",
            f"Read {artifacts['evidencePacketPath']} before evaluating merge or release decisions.",
            f"Write {artifacts['releaseResultPath']} using {DEFAULT_RELEASE_RESULT_TEMPLATE_PATH}.",
            "If release is blocked or fails, include failure_classification with kind, retryable, routed_to, and root_cause_signature.",
            "Respect required checks, mergeability, approval policy, and workspace hygiene.",
            "When the release_result is written, return control to the main_orchestrator root session; do not launch a root session.",
        ]
    else:
        lines = common + [
            "You are a recovery/select-next-issue main_orchestrator session.",
            decision_summary,
            "Advance the broader workflow without waiting for a human reply.",
            "If the current issue is blocked, create or link the blocker and continue to the next ready issue when possible.",
            "If another issue is ready, run orchestrator bootstrap for it directly with:",
            "python3 scripts/orchestrator_bootstrap_runner.py --issue-packet <path-to-selected-issue-packet> --dispatch-now",
            "That command will refresh the checkpoint, supervisor ledger, and create the next main_orchestrator root session.",
            "If no issue is ready, stop cleanly and report the blocking reason in compact form.",
        ]
    lines.append(f"Decision summary: {decision_summary}")
    return "\n".join(lines)


def build_session_request(
    ledger: JsonObject,
    *,
    role: str,
    stage: str,
    reason: str,
    title: str,
    decision_summary: str,
) -> SessionRequest:
    issue = cast(dict[str, str], ledger["issue"])
    created_at = str(ledger.get("updatedAt") or _now())
    ledger_revision = str(ledger.get("ledgerRevision") or ledger.get("updatedAt") or created_at)
    nonce = uuid4().hex
    return {
        "requestGeneration": len(cast(list[JsonObject], ledger.get("history", []))) + 1,
        "nonce": nonce,
        "requestID": nonce,
        "createdAt": created_at,
        "createdForLedgerRevision": ledger_revision,
        "reason": reason,
        "title": title,
        "agent": _root_session_agent(ledger),
        "prompt": _build_prompt(ledger, role, stage, decision_summary),
        "role": role,
        "stage": stage,
        "issueNumber": issue["number"],
        "branch": issue["branch"],
    }


def build_orchestrator_request(ledger: JsonObject) -> SessionRequest:
    issue = cast(dict[str, str], ledger["issue"])
    immediate_next_action = (
        f"Continue per_issue_flow for issue #{issue['number']} by creating or switching the issue branch."
    )
    return build_session_request(
        ledger,
        role="main_orchestrator",
        stage="orchestrator_bootstrap",
        reason=f"orchestrator bootstrap continuation for issue #{issue['number']}",
        title=f"Continue issue #{issue['number']} on {issue['branch']}",
        decision_summary=(
            "Fresh orchestrator session must validate the selected issue target and queue the first issue_worker "
            f"without waiting for a human reply. Immediate next action: {immediate_next_action}"
        ),
    )


def write_session_request(request_path: Path, request: SessionRequest) -> None:
    _write_json(request_path, dict(request))


def _queue_transition(
    ledger: JsonObject,
    *,
    next_role: str,
    next_stage: str,
    summary: str,
    updated_at: str,
) -> None:
    current = cast(JsonObject, ledger["current"])
    history = cast(list[JsonObject], ledger["history"])
    history.append(
        {
            "recordedAt": updated_at,
            "fromRole": current.get("role", "unknown"),
            "fromStage": current.get("stage", "unknown"),
            "toRole": next_role,
            "toStage": next_stage,
            "reason": summary,
        }
    )
    ledger["current"] = {
        "role": next_role,
        "stage": next_stage,
        "status": "queued",
    }
    _bump_ledger_revision(ledger, updated_at)
    ledger["updatedAt"] = updated_at


def _set_failure(ledger: JsonObject, *, kind: str, summary: str, retryable: bool) -> None:
    ledger["lastFailure"] = {
        "kind": kind,
        "summary": summary,
        "retryable": retryable,
    }


def _request_for_transition(
    ledger: JsonObject,
    *,
    next_role: str,
    next_stage: str,
    summary: str,
) -> SessionRequest:
    issue = cast(dict[str, str], ledger["issue"])
    if next_role == "issue_worker":
        return build_session_request(
            ledger,
            role="issue_worker",
            stage=next_stage,
            reason=f"issue_worker dispatch for issue #{issue['number']}",
            title=f"Issue #{issue['number']} worker on {issue['branch']}",
            decision_summary=summary,
        )
    if next_role == "pr_verifier":
        artifacts = cast(dict[str, str], ledger["artifacts"])
        evidence_path = artifacts["evidencePacketPath"] or f"issue #{issue['number']} verifier evidence"
        return build_session_request(
            ledger,
            role="pr_verifier",
            stage=next_stage,
            reason=f"pr_verifier dispatch for issue #{issue['number']}",
            title=f"Verify issue #{issue['number']} using {evidence_path}",
            decision_summary=summary,
        )
    if next_role == "release_worker":
        return build_session_request(
            ledger,
            role="release_worker",
            stage=next_stage,
            reason=f"release_worker dispatch for issue #{issue['number']}",
            title=f"Release issue #{issue['number']} on {issue['branch']}",
            decision_summary=summary,
        )
    return build_session_request(
        ledger,
        role="main_orchestrator",
        stage=next_stage,
        reason=f"main_orchestrator recovery for issue #{issue['number']}",
        title=f"Recover or continue after issue #{issue['number']}",
        decision_summary=summary,
    )


def _subagent_decision(ledger: JsonObject, *, next_role: str, next_stage: str, summary: str) -> SupervisorDecision:
    return {
        "action": "delegate_subagent",
        "next_role": next_role,
        "next_stage": next_stage,
        "summary": summary,
        "request_title": "",
        "subagent_prompt": _build_prompt(ledger, next_role, next_stage, summary),
    }


def _requeue_issue_worker(
    ledger: JsonObject,
    *,
    updated_at: str,
    summary: str,
    next_stage: str,
) -> tuple[SupervisorDecision, None]:
    attempts = cast(dict[str, int], ledger["attempts"])
    attempts["issue_worker"] += 1
    _queue_transition(ledger, next_role="issue_worker", next_stage=next_stage, summary=summary, updated_at=updated_at)
    return _subagent_decision(ledger, next_role="issue_worker", next_stage=next_stage, summary=summary), None


def _queue_orchestrator_recovery(
    ledger: JsonObject,
    *,
    base_dir: Path,
    updated_at: str,
    summary: str,
) -> tuple[JsonObject, SupervisorDecision, SessionRequest]:
    selected_issue = select_next_issue_packet(
        base_dir,
        workflow=cast(dict[str, str], ledger["workflow"]),
        current_issue=cast(dict[str, str], ledger["issue"]),
    )
    if selected_issue is None and run_issue_packet_intake(base_dir):
        selected_issue = select_next_issue_packet(
            base_dir,
            workflow=cast(dict[str, str], ledger["workflow"]),
            current_issue=cast(dict[str, str], ledger["issue"]),
        )
    if selected_issue is not None:
        next_summary = (
            f"{summary} Continue automatically with issue #{selected_issue.issue_number} via {selected_issue.issue_packet_path}."
        )
        return _handoff_to_selected_issue(
            ledger,
            selected_issue=selected_issue,
            base_dir=base_dir,
            updated_at=updated_at,
            summary=next_summary,
        )
    attempts = cast(dict[str, int], ledger["attempts"])
    attempts["main_orchestrator"] += 1
    _queue_transition(
        ledger,
        next_role="main_orchestrator",
        next_stage="issue_selection_or_recovery",
        summary=summary,
        updated_at=updated_at,
    )
    request = _request_for_transition(
        ledger,
        next_role="main_orchestrator",
        next_stage="issue_selection_or_recovery",
        summary=summary,
    )
    return ledger, {
        "action": "queue_next_session",
        "next_role": "main_orchestrator",
        "next_stage": "issue_selection_or_recovery",
        "summary": summary,
        "request_title": request["title"],
    }, request


def reconcile_ledger(
    ledger: JsonObject,
    *,
    session_result_path: Path = DEFAULT_SESSION_RESULT_PATH,
    artifact_base_dir: Path | None = None,
    updated_at: str | None = None,
) -> tuple[JsonObject, SupervisorDecision, SessionRequest | None]:
    timestamp = _now(updated_at)
    _sync_session_result(ledger, session_result_path)
    base_dir = artifact_base_dir or ROOT
    current = cast(dict[str, str], ledger["current"])
    attempts = cast(dict[str, int], ledger["attempts"])
    limits = cast(dict[str, int], ledger["limits"])
    artifacts = cast(dict[str, str], ledger["artifacts"])
    issue = cast(dict[str, str], ledger["issue"])

    if current["role"] == "main_orchestrator" and current["stage"] == "orchestrator_bootstrap":
        summary = (
            f"Issue #{issue['number']} passed orchestrator bootstrap. The main_orchestrator should delegate an issue_worker subagent and keep the workflow moving without waiting for a human reply."
        )
        attempts["issue_worker"] += 1
        _set_failure(ledger, kind="none", summary="", retryable=True)
        _queue_transition(
            ledger,
            next_role="issue_worker",
            next_stage="issue_worker_execution",
            summary=summary,
            updated_at=timestamp,
        )
        return ledger, _subagent_decision(ledger, next_role="issue_worker", next_stage="issue_worker_execution", summary=summary), None

    if current["role"] == "issue_worker":
        worker_result_path = _resolve_artifact_path(artifacts["workerResultPath"], base_dir=base_dir)
        if not worker_result_path.exists():
            if current.get("status") == "queued":
                summary = (
                    f"Issue worker for issue #{issue['number']} is queued and has not produced {artifacts['workerResultPath']} yet. Keep the queued dispatch state unchanged."
                )
                return ledger, {
                    "action": "no_change",
                    "next_role": current["role"],
                    "next_stage": current["stage"],
                    "summary": summary,
                    "request_title": "",
                }, None
            summary = (
                f"Issue worker for issue #{issue['number']} ended without writing {artifacts['workerResultPath']}. Retry the worker session as a contract repair."
            )
            _set_failure(ledger, kind="contract_invalid", summary=summary, retryable=True)
            if attempts["issue_worker"] < limits["issue_worker"]:
                return (ledger, *_requeue_issue_worker(ledger, updated_at=timestamp, summary=summary, next_stage="issue_worker_repair"))
            return _queue_orchestrator_recovery(ledger, base_dir=base_dir, updated_at=timestamp, summary=summary)

        worker = parse_worker_result_file(worker_result_path)
        status = cast(str, worker["status"])
        if status == "success":
            pr_number = cast(str, worker["pr_number"])
            if not pr_number or pr_number == "none":
                summary = (
                    f"Issue worker for issue #{issue['number']} reported success without a PR number. Route to main_orchestrator recovery instead of stalling."
                )
                _set_failure(ledger, kind="contract_invalid", summary=summary, retryable=True)
                return _queue_orchestrator_recovery(ledger, base_dir=base_dir, updated_at=timestamp, summary=summary)
            artifacts["evidencePacketPath"] = default_evidence_packet_path(issue["number"], pr_number)
            attempts["pr_verifier"] += 1
            summary = (
                f"Issue worker for issue #{issue['number']} succeeded. The main_orchestrator should delegate a pr_verifier subagent for PR #{pr_number}."
            )
            _set_failure(ledger, kind="none", summary="", retryable=True)
            _queue_transition(
                ledger,
                next_role="pr_verifier",
                next_stage="pr_verifier_execution",
                summary=summary,
                updated_at=timestamp,
            )
            return ledger, _subagent_decision(ledger, next_role="pr_verifier", next_stage="pr_verifier_execution", summary=summary), None

        summary = cast(str, worker["next_recommended_step"])
        failure_kind = cast(str, worker["failure_kind"] or "issue_worker_retry")
        retryable = cast(bool | None, worker["retryable"])
        _set_failure(
            ledger,
            kind=failure_kind,
            summary=summary,
            retryable=True if retryable is None else retryable,
        )
        if attempts["issue_worker"] < limits["issue_worker"] and (retryable is None or retryable):
            retry_summary = (
                f"Issue worker for issue #{issue['number']} returned {status}. The main_orchestrator should retry with a fresh issue_worker subagent and keep the workflow moving."
            )
            return (ledger, *_requeue_issue_worker(ledger, updated_at=timestamp, summary=retry_summary, next_stage="issue_worker_repair"))
        recovery_summary = (
            f"Issue worker for issue #{issue['number']} exhausted retries after status {status}. Route to main_orchestrator recovery so the workflow can classify the blocker or move to another ready issue."
        )
        return _queue_orchestrator_recovery(ledger, base_dir=base_dir, updated_at=timestamp, summary=recovery_summary)

    if current["role"] == "pr_verifier":
        evidence_packet_path = _resolve_artifact_path(artifacts["evidencePacketPath"], base_dir=base_dir)
        if not artifacts["evidencePacketPath"] or not evidence_packet_path.exists():
            summary = (
                f"pr_verifier for issue #{issue['number']} ended without writing {artifacts['evidencePacketPath'] or 'an evidence packet'}. Retry the verifier once before recovery."
            )
            _set_failure(ledger, kind="contract_invalid", summary=summary, retryable=True)
            if attempts["pr_verifier"] < limits["pr_verifier"]:
                attempts["pr_verifier"] += 1
                _queue_transition(
                    ledger,
                    next_role="pr_verifier",
                    next_stage="pr_verifier_execution",
                    summary=summary,
                    updated_at=timestamp,
                )
                return ledger, _subagent_decision(ledger, next_role="pr_verifier", next_stage="pr_verifier_execution", summary=summary), None
            return _queue_orchestrator_recovery(ledger, base_dir=base_dir, updated_at=timestamp, summary=summary)

        evidence = parse_evidence_packet_file(evidence_packet_path)
        status = cast(str, evidence["status"])
        if status == "pass":
            pr_number = cast(str, evidence["pr_number"])
            if not pr_number or pr_number == "none":
                summary = (
                    f"Verifier for issue #{issue['number']} passed without a PR number. Route to main_orchestrator recovery instead of waiting."
                )
                _set_failure(ledger, kind="contract_invalid", summary=summary, retryable=True)
                return _queue_orchestrator_recovery(ledger, base_dir=base_dir, updated_at=timestamp, summary=summary)
            artifacts["releaseResultPath"] = default_release_result_path(issue["number"], pr_number)
            attempts["release_worker"] += 1
            summary = f"Verifier for issue #{issue['number']} passed. The main_orchestrator should delegate release_worker for PR #{pr_number}."
            _set_failure(ledger, kind="none", summary="", retryable=True)
            _queue_transition(
                ledger,
                next_role="release_worker",
                next_stage="release_worker_execution",
                summary=summary,
                updated_at=timestamp,
            )
            return ledger, _subagent_decision(ledger, next_role="release_worker", next_stage="release_worker_execution", summary=summary), None

        failure_kind = cast(str, evidence["failure_kind"] or "verifier_retry")
        retryable = cast(bool | None, evidence["retryable"])
        summary = cast(str, evidence["next_recommended_step"])
        _set_failure(
            ledger,
            kind=failure_kind,
            summary=summary,
            retryable=True if retryable is None else retryable,
        )
        if status == "fail" and attempts["issue_worker"] < limits["issue_worker"]:
            retry_summary = (
                f"Verifier for issue #{issue['number']} failed. Return the issue to a fresh issue_worker subagent instead of waiting for human intervention."
            )
            return (ledger, *_requeue_issue_worker(ledger, updated_at=timestamp, summary=retry_summary, next_stage="issue_worker_repair"))
        if status == "blocked" and attempts["pr_verifier"] < limits["pr_verifier"] and retryable:
            attempts["pr_verifier"] += 1
            retry_summary = (
                f"Verifier for issue #{issue['number']} is retryable-blocked. Rerun a fresh pr_verifier subagent once more before escalating."
            )
            _queue_transition(
                ledger,
                next_role="pr_verifier",
                next_stage="pr_verifier_execution",
                summary=retry_summary,
                updated_at=timestamp,
            )
            return ledger, _subagent_decision(ledger, next_role="pr_verifier", next_stage="pr_verifier_execution", summary=retry_summary), None
        recovery_summary = (
            f"Verifier for issue #{issue['number']} ended with status {status}. Route to main_orchestrator recovery so the workflow can classify the blocker and continue with another ready issue when possible."
        )
        return _queue_orchestrator_recovery(ledger, base_dir=base_dir, updated_at=timestamp, summary=recovery_summary)

    if current["role"] == "release_worker":
        release_result_path = _resolve_artifact_path(artifacts["releaseResultPath"], base_dir=base_dir)
        if not artifacts["releaseResultPath"] or not release_result_path.exists():
            summary = (
                f"release_worker for issue #{issue['number']} ended without writing {artifacts['releaseResultPath'] or 'a release result'}. Retry release once before recovery."
            )
            _set_failure(ledger, kind="contract_invalid", summary=summary, retryable=True)
            if attempts["release_worker"] < limits["release_worker"]:
                attempts["release_worker"] += 1
                _queue_transition(
                    ledger,
                    next_role="release_worker",
                    next_stage="release_worker_execution",
                    summary=summary,
                    updated_at=timestamp,
                )
                return ledger, _subagent_decision(ledger, next_role="release_worker", next_stage="release_worker_execution", summary=summary), None
            return _queue_orchestrator_recovery(ledger, base_dir=base_dir, updated_at=timestamp, summary=summary)

        release = parse_release_result_file(release_result_path)
        status = cast(str, release["status"])
        if status == "success":
            summary = (
                f"Release worker completed issue #{issue['number']}. Hand off to main_orchestrator to select the next ready issue and keep the workflow moving."
            )
            _set_failure(ledger, kind="none", summary="", retryable=True)
            return _queue_orchestrator_recovery(ledger, base_dir=base_dir, updated_at=timestamp, summary=summary)

        blocked_reason = cast(str, release["blocked_reason"])
        retryable = cast(bool | None, release["retryable"])
        failure_kind = cast(str, release["failure_kind"] or blocked_reason or "release_blocked")
        summary = cast(str, release["next_recommended_step"])
        _set_failure(
            ledger,
            kind=failure_kind,
            summary=summary,
            retryable=True if retryable is None else retryable,
        )
        if blocked_reason in TRANSIENT_RELEASE_BLOCKERS and attempts["release_worker"] < limits["release_worker"] and (retryable is None or retryable):
            attempts["release_worker"] += 1
            retry_summary = (
                f"Release worker for issue #{issue['number']} hit transient blocker {blocked_reason}. Retry the release_worker subagent instead of stalling."
            )
            _queue_transition(
                ledger,
                next_role="release_worker",
                next_stage="release_worker_execution",
                summary=retry_summary,
                updated_at=timestamp,
            )
            return ledger, _subagent_decision(ledger, next_role="release_worker", next_stage="release_worker_execution", summary=retry_summary), None
        recovery_summary = (
            f"Release worker for issue #{issue['number']} is blocked by {blocked_reason or status}. Route to main_orchestrator recovery so the broader workflow can continue without waiting for a human reply."
        )
        return _queue_orchestrator_recovery(ledger, base_dir=base_dir, updated_at=timestamp, summary=recovery_summary)

    summary = (
        f"Supervisor found role={current['role']} stage={current['stage']} with no automatic transition. Keep the current state unchanged."
    )
    ledger["updatedAt"] = timestamp
    _bump_ledger_revision(ledger, timestamp)
    return ledger, {
        "action": "no_change",
        "next_role": current["role"],
        "next_stage": current["stage"],
        "summary": summary,
        "request_title": "",
    }, None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init", help="Create the nonstop supervisor ledger for a selected issue")
    _ = init_parser.add_argument("--issue-packet", required=True, help="Path to the selected AFK issue packet")
    _ = init_parser.add_argument("--ledger", default=str(DEFAULT_LEDGER_PATH), help="Path to orchestrator-ledger.json")
    _ = init_parser.add_argument("--request", default=str(DEFAULT_REQUEST_PATH), help="Optional path to new-session-request.json")
    _ = init_parser.add_argument("--write-request", action="store_true", help="Also write the initial orchestrator bootstrap request")
    _ = init_parser.add_argument("--dispatch-now", action="store_true", help="Immediately launch the fresh session after writing the request")
    _ = init_parser.add_argument("--source-session-id", default="supervisor_init", help="Source session id to record when dispatching immediately")
    _ = init_parser.add_argument("--workflow-policy-path", default=DEFAULT_WORKFLOW_POLICY_PATH)
    _ = init_parser.add_argument("--checkpoint-path", default=DEFAULT_CHECKPOINT_PATH)
    _ = init_parser.add_argument("--updated-at")

    reconcile_parser = subparsers.add_parser("reconcile", help="Read runtime artifacts and queue the next session")
    _ = reconcile_parser.add_argument("--ledger", default=str(DEFAULT_LEDGER_PATH), help="Path to orchestrator-ledger.json")
    _ = reconcile_parser.add_argument("--request", default=str(DEFAULT_REQUEST_PATH), help="Path to new-session-request.json")
    _ = reconcile_parser.add_argument("--session-result", default=str(DEFAULT_SESSION_RESULT_PATH), help="Path to new-session-result.json")
    _ = reconcile_parser.add_argument("--write-request", action="store_true", help="Persist the computed next-session request")
    _ = reconcile_parser.add_argument("--dispatch-now", action="store_true", help="Immediately launch the fresh session after writing the request")
    _ = reconcile_parser.add_argument("--source-session-id", default="supervisor_reconcile", help="Source session id to record when dispatching immediately")
    _ = reconcile_parser.add_argument("--updated-at")

    dispatch_parser = subparsers.add_parser("dispatch", help="Launch the next session explicitly without relying on session.idle plugins")
    _ = dispatch_parser.add_argument("--request", default=str(DEFAULT_REQUEST_PATH), help="Path to new-session-request.json")
    _ = dispatch_parser.add_argument("--session-result", default=str(DEFAULT_SESSION_RESULT_PATH), help="Path to new-session-result.json")
    _ = dispatch_parser.add_argument("--ledger", default=str(DEFAULT_LEDGER_PATH), help="Path to orchestrator-ledger.json")
    _ = dispatch_parser.add_argument("--source-session-id", default="manual_dispatch", help="Source session id to record in the session result")
    _ = dispatch_parser.add_argument("--updated-at")

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if cast(str, args.command) == "init":
        issue_packet_path = Path(cast(str, args.issue_packet))
        ledger_path = Path(cast(str, args.ledger))
        request_path = Path(cast(str, args.request))
        record = parse_issue_packet_text(
            issue_packet_path.read_text(encoding="utf-8"),
            str(issue_packet_path),
        )
        ledger = create_initial_ledger(
            issue_packet=record,
            checkpoint_path=cast(str, args.checkpoint_path),
            workflow_policy_path=cast(str, args.workflow_policy_path),
            updated_at=cast(str | None, args.updated_at),
        )
        write_ledger_file(ledger_path, ledger)
        print(f"wrote supervisor ledger {ledger_path}")
        if cast(bool, args.write_request):
            request = build_orchestrator_request(ledger)
            write_session_request(request_path, request)
            print(f"wrote session request {request_path}")
            if cast(bool, args.dispatch_now):
                session_result_path = default_session_result_path_for_request(request_path)
                _ = _dispatch_consumed_request(
                    request_path,
                    ledger_path=ledger_path,
                    session_result_path=session_result_path,
                    source_session_id=cast(str, args.source_session_id),
                    updated_at=cast(str | None, args.updated_at),
                )
                print(f"wrote session result {session_result_path}")
        return 0

    if cast(str, args.command) == "dispatch":
        request_path = Path(cast(str, args.request))
        session_result_path = Path(cast(str, args.session_result))
        ledger_path = Path(cast(str, args.ledger))
        session_result = _dispatch_consumed_request(
            request_path,
            ledger_path=ledger_path,
            session_result_path=session_result_path,
            source_session_id=cast(str, args.source_session_id),
            updated_at=cast(str | None, args.updated_at),
        )
        print(f"wrote session result {session_result_path}")
        print(json.dumps(session_result, indent=2, ensure_ascii=False))
        return 0

    ledger_path = Path(cast(str, args.ledger))
    request_path = Path(cast(str, args.request))
    session_result_path = Path(cast(str, args.session_result))
    ledger = _read_json(ledger_path)
    updated_ledger, decision, request = reconcile_ledger(
        ledger,
        session_result_path=session_result_path,
        artifact_base_dir=_infer_artifact_base_dir(ledger_path),
        updated_at=cast(str | None, args.updated_at),
    )
    write_ledger_file(ledger_path, updated_ledger)
    if cast(bool, args.write_request) and request is not None:
        write_session_request(request_path, request)
        print(f"wrote session request {request_path}")
        if cast(bool, args.dispatch_now):
            _ = _dispatch_consumed_request(
                request_path,
                ledger_path=ledger_path,
                session_result_path=session_result_path,
                source_session_id=cast(str, args.source_session_id),
                updated_at=cast(str | None, args.updated_at),
            )
            print(f"wrote session result {session_result_path}")
    print(json.dumps(decision, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
