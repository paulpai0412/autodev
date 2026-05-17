#!/usr/bin/env python3
"""Validate autonomous workflow context-budget artifact caps."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

EXPECTED_LINE_CAP_BY_KIND = {
    "context_checkpoint": 80,
    "evidence_packet": 60,
    "failure_registry": 80,
    "issue_handoff": 35,
    "issue_packet": 80,
    "prd_phase_audit": 80,
    "refactor_candidate_audit": 80,
    "release_result": 60,
    "test_case_catalog": 80,
    "worker_result": 80,
}

REQUIRED_TEXT = {
    "docs/agents/autonomous-development-workflow.yaml": [
        "checkpoint_warning_at_percent: 45",
        "stop_and_rotate_at_percent: 50",
        "checkpoint_only_cross_session_resume: true",
        "release_worker_policy:",
        "human_merge_approval_policy_satisfied",
        "default_mode: human_required",
        "allowed_override_modes:",
        "approval_override_mode_is_bypass_approval",
        "human_required_mode:",
        "bypass_approval_mode:",
        "run_post_merge_workspace_hygiene",
        "wip_branch_name_template:",
        "blocked_reason_enum:",
        "post_merge_workspace_hygiene_comment_fields:",
        "release_summary_override_fields:",
        "allow_parallel_ready_issues: false",
        "forbid_orchestrator_multi_issue_ultrawork: true",
        "main_agent_contract_validation_only:",
        "main_agent_direct_issue_or_phase_qa",
        "script_gate: scripts/agent_context_budget_check.py",
    ],
    "docs/agents/issue-tracker.md": [
        "release_worker",
        "verifier-owned evidence packet",
        "human merge approval",
        "Default merge approval mode is `human_required`.",
        "approval_override_mode: bypass_approval",
        "human_approval_skipped",
        "post-merge workspace hygiene",
        "primary_workspace_branch_before",
        "blocked_reason",
        "one selected issue, one branch, one root orchestrator coordinating worker/verifier/release subagents",
        "## Post-merge workspace hygiene",
        "cleanup_status:",
    ],
}


def rel(path: str, *, root: Path = ROOT) -> Path:
    return root / path


def line_count(path: Path) -> int:
    return len(path.read_text(encoding="utf-8").splitlines())


def top_level_scalar(text: str, key: str) -> str:
    prefix = f"{key}:"
    for line in text.splitlines():
        if line.startswith(prefix):
            return line.split(":", 1)[1].strip().strip('"')
    return ""


def is_pre_enforcement_artifact(text: str) -> bool:
    return "pre_enforcement: true" in text


def iter_governed_artifacts(root: Path = ROOT) -> list[Path]:
    agents_root = root / "docs/agents"
    if not agents_root.exists():
        return []
    return sorted(path for path in agents_root.glob("**/*.yaml") if path.is_file())


def artifact_cap_failures(root: Path = ROOT) -> list[str]:
    failures: list[str] = []

    for path in iter_governed_artifacts(root):
        text = path.read_text(encoding="utf-8")
        kind = top_level_scalar(text, "kind")
        if kind not in EXPECTED_LINE_CAP_BY_KIND:
            continue
        if is_pre_enforcement_artifact(text):
            continue

        expected_cap = EXPECTED_LINE_CAP_BY_KIND[kind]
        relative_path = str(path.relative_to(root))
        declared_cap = top_level_scalar(text, "line_cap")
        if not declared_cap:
            failures.append(
                f"missing line_cap in {relative_path}: expected {expected_cap} for kind {kind}"
            )
            continue

        try:
            parsed_cap = int(declared_cap)
        except ValueError:
            failures.append(f"invalid line_cap in {relative_path}: {declared_cap}")
            continue

        if parsed_cap != expected_cap:
            failures.append(
                f"wrong line_cap in {relative_path}: declared {parsed_cap}, expected {expected_cap} for kind {kind}"
            )

        count = line_count(path)
        if count > expected_cap:
            failures.append(f"over cap: {relative_path} has {count} lines > {expected_cap}")

    return failures


def required_text_failures(root: Path = ROOT) -> list[str]:
    failures: list[str] = []

    for artifact, required_strings in REQUIRED_TEXT.items():
        path = rel(artifact, root=root)
        if not path.exists():
            failures.append(f"missing: {artifact}")
            continue
        text = path.read_text(encoding="utf-8")
        for required in required_strings:
            if required not in text:
                failures.append(f"missing text in {artifact}: {required}")

    return failures


def main() -> int:
    failures = artifact_cap_failures() + required_text_failures()

    if failures:
        print("context budget gate: fail")
        for failure in failures:
            print(f"- {failure}")
        return 1

    governed_artifacts = [
        path
        for path in iter_governed_artifacts()
        if top_level_scalar(path.read_text(encoding="utf-8"), "kind") in EXPECTED_LINE_CAP_BY_KIND
        and not is_pre_enforcement_artifact(path.read_text(encoding="utf-8"))
    ]
    print("context budget gate: pass")
    print(
        f"checked {len(governed_artifacts)} governed artifacts and {len(REQUIRED_TEXT)} policy files"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
