#!/usr/bin/env python3
"""Derive and persist the orchestrator compact payload for checkpoints."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TypedDict, cast


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CHECKPOINT_PATH = ROOT / "docs/agents/runtime/context-checkpoint.yaml"
DEFAULT_WORKFLOW_POLICY_PATH = "docs/agents/autonomous-development-workflow.yaml"
DEFAULT_ROOT_SESSION_AGENT = "build"
CHECKPOINT_LINE_CAP = 80


@dataclass
class CheckpointRecord:
    issue_number: str
    branch: str
    role: str
    agent: str
    checkpoint_reason: str
    issue_packet: str
    worker_result: str
    evidence_packet: str
    handoff: str
    artifact_bundle: str
    updated_by: str
    completed: list[str]
    in_progress: list[str]
    next_steps: list[str]
    blockers: list[str]
    approval_override_mode: str
    default_merge_approval_mode: str
    override_source: str
    human_approval_skipped: bool


class ActiveTarget(TypedDict):
    issue_number: str
    branch: str
    role: str
    agent: str
    next_flow: str


class StateSnapshot(TypedDict):
    completed: list[str]
    in_progress: list[str]
    next: list[str]
    blockers: list[str]


class CompactPayload(TypedDict):
    active_target: ActiveTarget
    authoritative_refs: list[str]
    state_snapshot: StateSnapshot
    resume_rules: list[str]
    immediate_next_action: str


def _quote(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def _parse_scalar(value: str) -> str:
    value = value.strip()
    if not value:
        return ""
    if value.startswith('"') and value.endswith('"'):
        loaded = cast(object, json.loads(value))
        return loaded if isinstance(loaded, str) else str(loaded)
    return value


def _find_top_level_block_bounds(lines: list[str], block_name: str) -> tuple[int, int]:
    start = None
    for index, line in enumerate(lines):
        if line.startswith(f"{block_name}:") and not line.startswith(" "):
            start = index
            break
    if start is None:
        raise ValueError(f"missing top-level block: {block_name}")

    end = len(lines)
    for index in range(start + 1, len(lines)):
        line = lines[index]
        if line and not line.startswith(" "):
            end = index
            break
    return start, end


def _extract_block_lines(lines: list[str], block_name: str) -> list[str]:
    start, end = _find_top_level_block_bounds(lines, block_name)
    return lines[start:end]


def _parse_mapping_block(block_lines: list[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in block_lines[1:]:
        stripped = line.strip()
        if not stripped:
            continue
        if line.startswith("  ") and not line.startswith("    ") and ":" in stripped:
            key, value = stripped.split(":", 1)
            result[key] = _parse_scalar(value)
    return result


def _parse_state_block(block_lines: list[str]) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {
        "completed": [],
        "in_progress": [],
        "next": [],
        "blockers": [],
    }
    current_key: str | None = None
    for line in block_lines[1:]:
        stripped = line.strip()
        if not stripped:
            continue
        indent = len(line) - len(line.lstrip(" "))
        if indent == 2 and stripped.endswith(": []"):
            key, _ = stripped.split(":", 1)
            result[key] = []
            current_key = None
            continue
        if indent == 2 and stripped.endswith(":"):
            current_key = stripped[:-1]
            if current_key not in result:
                result[current_key] = []
            continue
        if indent == 4 and stripped.startswith("- ") and current_key:
            result[current_key].append(_parse_scalar(stripped[2:]))
    return result


def _parse_inline_mapping_value(line: str, key: str) -> str:
    if "{" not in line or "}" not in line:
        return ""
    body = line.split("{", 1)[1].rsplit("}", 1)[0]
    for part in [part.strip() for part in body.split(",")]:
        if ":" not in part:
            continue
        found_key, value = part.split(":", 1)
        if found_key.strip() == key:
            return _parse_scalar(value)
    return ""


def _parse_compact_payload_agent(lines: list[str]) -> str:
    try:
        compact_payload_lines = _extract_block_lines(lines, "compact_payload")
    except ValueError:
        return ""
    for line in compact_payload_lines[1:]:
        stripped = line.strip()
        if stripped.startswith("active_target:"):
            return _parse_inline_mapping_value(stripped, "agent")
    return ""


def parse_checkpoint_text(text: str) -> CheckpointRecord:
    lines = text.splitlines()
    subject = _parse_mapping_block(_extract_block_lines(lines, "subject"))
    refs = _parse_mapping_block(_extract_block_lines(lines, "refs"))
    metadata = _parse_mapping_block(_extract_block_lines(lines, "metadata"))
    state = _parse_state_block(_extract_block_lines(lines, "state"))
    try:
        runtime_controls = _parse_mapping_block(_extract_block_lines(lines, "runtime_controls"))
    except ValueError:
        runtime_controls = {}

    return CheckpointRecord(
        issue_number=subject["issue_number"],
        branch=subject["branch"],
        role=subject["role"],
        agent=_parse_compact_payload_agent(lines) or DEFAULT_ROOT_SESSION_AGENT,
        checkpoint_reason=subject.get("checkpoint_reason", ""),
        issue_packet=refs.get("issue_packet", ""),
        worker_result=refs.get("worker_result", ""),
        evidence_packet=refs.get("evidence_packet", ""),
        handoff=refs.get("handoff", ""),
        artifact_bundle=refs.get("artifact_bundle", ""),
        updated_by=metadata.get("updated_by", "Build"),
        completed=state.get("completed", []),
        in_progress=state.get("in_progress", []),
        next_steps=state.get("next", []),
        blockers=state.get("blockers", []) or ["none"],
        approval_override_mode=runtime_controls.get("approval_override_mode", ""),
        default_merge_approval_mode=runtime_controls.get("default_merge_approval_mode", "human_required"),
        override_source=runtime_controls.get("override_source", "none"),
        human_approval_skipped=_parse_scalar(runtime_controls.get("human_approval_skipped", "false")).lower() == "true",
    )


def apply_overrides(
    record: CheckpointRecord,
    *,
    issue_number: str | None = None,
    branch: str | None = None,
    role: str | None = None,
    agent: str | None = None,
    issue_packet: str | None = None,
    handoff: str | None = None,
    worker_result: str | None = None,
    evidence_packet: str | None = None,
    artifact_bundle: str | None = None,
    completed: list[str] | None = None,
    in_progress: list[str] | None = None,
    next_steps: list[str] | None = None,
    blockers: list[str] | None = None,
    approval_override_mode: str | None = None,
    override_source: str | None = None,
    human_approval_skipped: bool | None = None,
) -> CheckpointRecord:
    target_changed = any(
        value is not None and value != current
        for value, current in (
            (issue_number, record.issue_number),
            (branch, record.branch),
            (role, record.role),
        )
    )
    preserve_runtime_state = any(value is not None for value in (in_progress, next_steps, blockers))
    return CheckpointRecord(
        issue_number=issue_number or record.issue_number,
        branch=branch or record.branch,
        role=role or record.role,
        agent=agent or record.agent,
        checkpoint_reason=record.checkpoint_reason,
        issue_packet=issue_packet or record.issue_packet,
        worker_result=worker_result if worker_result is not None else record.worker_result,
        evidence_packet=evidence_packet if evidence_packet is not None else record.evidence_packet,
        handoff=handoff if handoff is not None else record.handoff,
        artifact_bundle=artifact_bundle if artifact_bundle is not None else record.artifact_bundle,
        updated_by=record.updated_by,
        completed=list(record.completed) if completed is None else list(completed),
        in_progress=(list(record.in_progress) if preserve_runtime_state and not target_changed else []) if in_progress is None else list(in_progress),
        next_steps=(list(record.next_steps) if preserve_runtime_state and not target_changed else []) if next_steps is None else list(next_steps),
        blockers=(list(record.blockers) or ["none"]) if preserve_runtime_state and blockers is None else ((list(blockers) or ["none"]) if blockers is not None else ["none"]),
        approval_override_mode=record.approval_override_mode if approval_override_mode is None else approval_override_mode,
        default_merge_approval_mode=record.default_merge_approval_mode,
        override_source=record.override_source if override_source is None else override_source,
        human_approval_skipped=record.human_approval_skipped if human_approval_skipped is None else human_approval_skipped,
    )


def build_orchestrator_state(record: CheckpointRecord) -> StateSnapshot:
    in_progress = list(record.in_progress)
    next_steps = list(record.next_steps)
    if not in_progress:
        in_progress = [f"Prepare the orchestrator session to enter issue #{record.issue_number} PR flow."]
    if not next_steps:
        next_steps = [
            f"Run supervisor reconcile for issue #{record.issue_number} to persist issue_worker_execution before creating or switching the issue branch and launching the first issue_worker subagent."
        ]
    return {
        "completed": list(record.completed),
        "in_progress": in_progress,
        "next": next_steps,
        "blockers": list(record.blockers) or ["none"],
    }


def derive_compact_payload(
    record: CheckpointRecord,
    *,
    workflow_policy_path: str = DEFAULT_WORKFLOW_POLICY_PATH,
) -> CompactPayload:
    orchestrator_state = build_orchestrator_state(record)
    authoritative_refs = [record.issue_packet]
    if record.handoff:
        authoritative_refs.append(record.handoff)
    authoritative_refs.append(workflow_policy_path)

    return {
        "active_target": {
            "issue_number": record.issue_number,
            "branch": record.branch,
            "role": record.role,
            "agent": record.agent,
            "next_flow": "per_issue_flow",
        },
        "authoritative_refs": authoritative_refs,
        "state_snapshot": orchestrator_state,
        "resume_rules": [
            "Resume from checkpoint and compact payload, not full chat history.",
            "Keep raw evidence as refs only; do not inline logs or traces.",
        ],
        "immediate_next_action": orchestrator_state["next"][0],
    }


def _render_list_block(key: str, items: list[str], *, indent: int) -> list[str]:
    prefix = " " * indent
    if not items:
        return [f"{prefix}{key}: []"]
    lines = [f"{prefix}{key}:"]
    lines.extend(f"{prefix}  - {_quote(item)}" for item in items)
    return lines


def _render_inline_string_list(items: list[str]) -> str:
    if not items:
        return "[]"
    return f"[{', '.join(_quote(item) for item in items)}]"


def render_compact_payload_block(payload: CompactPayload) -> list[str]:
    active_target = payload["active_target"]
    state_snapshot = payload["state_snapshot"]
    lines = ["compact_payload:"]
    lines.append(
        "  active_target: {"
        + ", ".join(
            [
                f"issue_number: {_quote(active_target['issue_number'])}",
                f"branch: {_quote(active_target['branch'])}",
                f"role: {_quote(active_target['role'])}",
                f"agent: {_quote(active_target['agent'])}",
                f"next_flow: {_quote(active_target['next_flow'])}",
            ]
        )
        + "}"
    )
    lines.append(f"  authoritative_refs: {_render_inline_string_list(payload['authoritative_refs'])}")
    lines.append("  state_snapshot:")
    lines.append(f"    completed: {_render_inline_string_list(state_snapshot['completed'])}")
    lines.append(f"    in_progress: {_render_inline_string_list(state_snapshot['in_progress'])}")
    lines.append(f"    next: {_render_inline_string_list(state_snapshot['next'])}")
    lines.append(f"    blockers: {_render_inline_string_list(state_snapshot['blockers'])}")
    lines.append(f"  resume_rules: {_render_inline_string_list(payload['resume_rules'])}")
    lines.append(f"  immediate_next_action: {_quote(payload['immediate_next_action'])}")
    return lines


def _render_subject_block(record: CheckpointRecord) -> list[str]:
    return [
        "subject:",
        f"  issue_number: {_quote(record.issue_number)}",
        f"  branch: {_quote(record.branch)}",
        f"  role: {_quote(record.role)}",
        f"  checkpoint_reason: {_quote(record.checkpoint_reason)}",
    ]


def _render_state_block(record: CheckpointRecord) -> list[str]:
    orchestrator_state = build_orchestrator_state(record)
    lines = ["state:"]
    lines.extend(_render_list_block("completed", orchestrator_state["completed"], indent=2))
    lines.extend(_render_list_block("in_progress", orchestrator_state["in_progress"], indent=2))
    lines.extend(_render_list_block("next", orchestrator_state["next"], indent=2))
    lines.extend(_render_list_block("blockers", orchestrator_state["blockers"], indent=2))
    return lines


def _render_refs_block(record: CheckpointRecord) -> list[str]:
    return [
        "refs:",
        f"  issue_packet: {_quote(record.issue_packet)}",
        f"  worker_result: {_quote(record.worker_result)}",
        f"  evidence_packet: {_quote(record.evidence_packet)}",
        f"  handoff: {_quote(record.handoff)}",
        f"  artifact_bundle: {_quote(record.artifact_bundle)}",
    ]


def _render_runtime_controls_block(record: CheckpointRecord) -> list[str]:
    human_approval_skipped = "true" if record.human_approval_skipped else "false"
    return [
        "runtime_controls:",
        f"  approval_override_mode: {_quote(record.approval_override_mode)}",
        f"  default_merge_approval_mode: {_quote(record.default_merge_approval_mode)}",
        '  set_only_at_workflow_start: true',
        '  mutable_after_start: false',
        '  scope: "workflow_run_only"',
        '  applies_to: "all_prs_created_by_this_run"',
        '  affects_stage: "release_worker_only"',
        f"  override_source: {_quote(record.override_source)}",
        f"  human_approval_skipped: {human_approval_skipped}",
    ]


def _render_metadata_block(updated_by: str, updated_at: str) -> list[str]:
    return [
        "metadata:",
        f"  updated_by: {_quote(updated_by)}",
        f"  updated_at: {_quote(updated_at)}",
    ]


def _replace_or_insert_block(
    lines: list[str],
    block_name: str,
    new_block_lines: list[str],
    *,
    insert_before: str | None = None,
) -> list[str]:
    try:
        start, end = _find_top_level_block_bounds(lines, block_name)
    except ValueError:
        if insert_before is None:
            raise
        insert_at, _ = _find_top_level_block_bounds(lines, insert_before)
        return lines[:insert_at] + new_block_lines + [""] + lines[insert_at:]

    suffix = [""] if end < len(lines) else []
    return lines[:start] + new_block_lines + suffix + lines[end:]


def update_checkpoint_text(
    text: str,
    *,
    issue_number: str | None = None,
    branch: str | None = None,
    role: str | None = None,
    agent: str | None = None,
    issue_packet: str | None = None,
    handoff: str | None = None,
    worker_result: str | None = None,
    evidence_packet: str | None = None,
    artifact_bundle: str | None = None,
    completed: list[str] | None = None,
    in_progress: list[str] | None = None,
    next_steps: list[str] | None = None,
    blockers: list[str] | None = None,
    approval_override_mode: str | None = None,
    override_source: str | None = None,
    human_approval_skipped: bool | None = None,
    workflow_policy_path: str = DEFAULT_WORKFLOW_POLICY_PATH,
    updated_at: str | None = None,
) -> str:
    record = apply_overrides(
        parse_checkpoint_text(text),
        issue_number=issue_number,
        branch=branch,
        role=role,
        agent=agent,
        issue_packet=issue_packet,
        handoff=handoff,
        worker_result=worker_result,
        evidence_packet=evidence_packet,
        artifact_bundle=artifact_bundle,
        completed=completed,
        in_progress=in_progress,
        next_steps=next_steps,
        blockers=blockers,
        approval_override_mode=approval_override_mode,
        override_source=override_source,
        human_approval_skipped=human_approval_skipped,
    )
    payload = derive_compact_payload(record, workflow_policy_path=workflow_policy_path)
    timestamp = updated_at or datetime.now().astimezone().isoformat(timespec="seconds")

    lines = text.splitlines()
    lines = _replace_or_insert_block(lines, "subject", _render_subject_block(record))
    lines = _replace_or_insert_block(
        lines,
        "runtime_controls",
        _render_runtime_controls_block(record),
        insert_before="state",
    )
    lines = _replace_or_insert_block(lines, "state", _render_state_block(record))
    lines = _replace_or_insert_block(lines, "refs", _render_refs_block(record))
    lines = _replace_or_insert_block(
        lines,
        "compact_payload",
        render_compact_payload_block(payload),
        insert_before="metadata",
    )
    lines = _replace_or_insert_block(lines, "metadata", _render_metadata_block(record.updated_by, timestamp))

    updated_text = "\n".join(lines) + "\n"
    if len(updated_text.splitlines()) > CHECKPOINT_LINE_CAP:
        raise ValueError(
            f"updated checkpoint exceeds line cap: {len(updated_text.splitlines())} > {CHECKPOINT_LINE_CAP}"
        )
    return updated_text


def write_checkpoint_file(
    checkpoint_path: Path,
    *,
    issue_number: str | None = None,
    branch: str | None = None,
    role: str | None = None,
    agent: str | None = None,
    issue_packet: str | None = None,
    handoff: str | None = None,
    worker_result: str | None = None,
    evidence_packet: str | None = None,
    artifact_bundle: str | None = None,
    completed: list[str] | None = None,
    in_progress: list[str] | None = None,
    next_steps: list[str] | None = None,
    blockers: list[str] | None = None,
    approval_override_mode: str | None = None,
    override_source: str | None = None,
    human_approval_skipped: bool | None = None,
    workflow_policy_path: str = DEFAULT_WORKFLOW_POLICY_PATH,
    updated_at: str | None = None,
) -> str:
    original_text = checkpoint_path.read_text(encoding="utf-8")
    updated_text = update_checkpoint_text(
        original_text,
        issue_number=issue_number,
        branch=branch,
        role=role,
        agent=agent,
        issue_packet=issue_packet,
        handoff=handoff,
        worker_result=worker_result,
        evidence_packet=evidence_packet,
        artifact_bundle=artifact_bundle,
        completed=completed,
        in_progress=in_progress,
        next_steps=next_steps,
        blockers=blockers,
        approval_override_mode=approval_override_mode,
        override_source=override_source,
        human_approval_skipped=human_approval_skipped,
        workflow_policy_path=workflow_policy_path,
        updated_at=updated_at,
    )
    _ = checkpoint_path.write_text(updated_text, encoding="utf-8")
    return updated_text


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    _ = parser.add_argument("--checkpoint", default=str(DEFAULT_CHECKPOINT_PATH), help="Path to context-checkpoint.yaml")
    _ = parser.add_argument("--issue-number", help="Override the selected issue number")
    _ = parser.add_argument("--branch", help="Override the selected issue branch")
    _ = parser.add_argument("--role", help="Override the checkpoint role")
    _ = parser.add_argument("--agent", help="Override the root-session agent")
    _ = parser.add_argument("--issue-packet", help="Override the issue packet ref")
    _ = parser.add_argument("--handoff", help="Override the prior handoff ref; use empty string to clear")
    _ = parser.add_argument(
        "--workflow-policy-path",
        default=DEFAULT_WORKFLOW_POLICY_PATH,
        help="Canonical workflow policy ref for authoritative_refs",
    )
    _ = parser.add_argument("--updated-at", help="Fixed timestamp for deterministic updates")
    _ = parser.add_argument("--write", action="store_true", help="Persist the updated checkpoint file")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    checkpoint_path = Path(cast(str, args.checkpoint))
    checkpoint_text = checkpoint_path.read_text(encoding="utf-8")
    updated_text = update_checkpoint_text(
        checkpoint_text,
        issue_number=cast(str | None, args.issue_number),
        branch=cast(str | None, args.branch),
        role=cast(str | None, args.role),
        agent=cast(str | None, args.agent),
        issue_packet=cast(str | None, args.issue_packet),
        handoff=cast(str | None, args.handoff),
        workflow_policy_path=cast(str, args.workflow_policy_path),
        updated_at=cast(str | None, args.updated_at),
    )
    if cast(bool, args.write):
        _ = checkpoint_path.write_text(updated_text, encoding="utf-8")
        print(f"updated checkpoint: {checkpoint_path}")
        return 0

    payload = derive_compact_payload(
        apply_overrides(
            parse_checkpoint_text(updated_text),
            issue_number=cast(str | None, args.issue_number),
            branch=cast(str | None, args.branch),
            role=cast(str | None, args.role),
            agent=cast(str | None, args.agent),
            issue_packet=cast(str | None, args.issue_packet),
            handoff=cast(str | None, args.handoff),
        ),
        workflow_policy_path=cast(str, args.workflow_policy_path),
    )
    print("\n".join(render_compact_payload_block(payload)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
