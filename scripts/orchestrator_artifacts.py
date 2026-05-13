"""Artifact and packet parsing helpers for the autodev supervisor."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import cast


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


def _extract_top_level_scalar_optional(text: str, key: str) -> str:
    try:
        return _extract_top_level_scalar(text, key)
    except ValueError:
        return ""


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


def _extract_mapping_value_optional(text: str, prefix: str, key: str) -> str:
    inline_value = _extract_inline_mapping_value_optional(text, prefix, key)
    if inline_value:
        return inline_value

    in_block = False
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        indent = len(line) - len(line.lstrip(" "))
        if indent == 0 and stripped == prefix:
            in_block = True
            continue
        if in_block and indent == 0:
            break
        if in_block and indent == 2 and stripped.startswith(f"{key}:"):
            _, value = stripped.split(":", 1)
            return _parse_scalar(value)
    return ""


def _extract_inline_bool_optional(text: str, prefix: str, key: str) -> bool | None:
    value = _extract_inline_mapping_value_optional(text, prefix, key)
    if not value:
        return None
    return _parse_bool(value)


def parse_issue_packet_text(text: str, issue_packet_path: str) -> IssuePacketRecord:
    issue_number = _extract_nested_scalar(text, "issue", "number")
    title = _extract_nested_scalar_optional(text, "issue", "title")
    branch = _extract_mapping_value_optional(text, "branch:", "name")
    if not branch:
        raise ValueError("missing 'name' in mapping 'branch:'")
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


def issue_packet_record_to_json(record: IssuePacketRecord) -> JsonObject:
    return {
        "issue_number": record.issue_number,
        "title": record.title,
        "branch": record.branch,
        "issue_packet_path": record.issue_packet_path,
        "prior_handoff": record.prior_handoff,
        "labels": list(record.labels),
        "parent_reference": record.parent_reference,
        "dependencies": list(record.dependencies),
    }


def issue_packet_record_from_json(payload: dict[str, object]) -> IssuePacketRecord | None:
    issue_number = str(payload.get("issue_number") or "")
    branch = str(payload.get("branch") or "")
    issue_packet_path = str(payload.get("issue_packet_path") or "")
    if not issue_number or not branch or not issue_packet_path:
        return None
    labels_raw = payload.get("labels", [])
    dependencies_raw = payload.get("dependencies", [])
    labels = [str(label) for label in labels_raw] if isinstance(labels_raw, list) else []
    dependencies = [str(item) for item in dependencies_raw] if isinstance(dependencies_raw, list) else []
    return IssuePacketRecord(
        issue_number=issue_number,
        title=str(payload.get("title") or ""),
        branch=branch,
        issue_packet_path=issue_packet_path,
        prior_handoff=str(payload.get("prior_handoff") or ""),
        labels=labels,
        parent_reference=str(payload.get("parent_reference") or ""),
        dependencies=dependencies,
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
        "pr_number": _extract_mapping_value_optional(text, "pr:", "number"),
        "next_recommended_step": _extract_top_level_scalar(text, "next_recommended_step"),
        "failure_kind": _extract_inline_mapping_value_optional(text, "failure_classification:", "kind"),
        "retryable": _extract_inline_bool_optional(text, "failure_classification:", "retryable"),
        "completed_at": _extract_mapping_value_optional(text, "metadata:", "completed_at") or _extract_nested_scalar_optional(text, "metadata", "completed_at"),
    }


def parse_evidence_packet_file(path: Path) -> JsonObject:
    text = path.read_text(encoding="utf-8")
    return {
        "status": _extract_top_level_scalar(text, "status"),
        "pr_number": _extract_mapping_value_optional(text, "subject:", "pr_number") or _extract_nested_scalar_optional(text, "subject", "pr_number"),
        "verifier_session_id": _extract_mapping_value_optional(text, "verifier:", "verifier_session_id") or _extract_nested_scalar_optional(text, "verifier", "verifier_session_id"),
        "next_recommended_step": _extract_top_level_scalar(text, "next_recommended_step"),
        "failure_kind": _extract_inline_mapping_value_optional(text, "failure_classification:", "kind"),
        "retryable": _extract_inline_bool_optional(text, "failure_classification:", "retryable"),
    }


def parse_release_result_file(path: Path) -> JsonObject:
    text = path.read_text(encoding="utf-8")
    return {
        "status": _extract_top_level_scalar(text, "status"),
        "blocked_reason": _extract_top_level_scalar_optional(text, "blocked_reason") or "none",
        "next_recommended_step": _extract_mapping_value_optional(text, "summary:", "next_recommended_step") or _extract_nested_scalar(text, "summary", "next_recommended_step"),
        "failure_kind": _extract_inline_mapping_value_optional(text, "failure_classification:", "kind"),
        "retryable": _extract_inline_bool_optional(text, "failure_classification:", "retryable"),
    }


def _is_successful_release_status(status: str) -> bool:
    return status in {"success", "completed"}
