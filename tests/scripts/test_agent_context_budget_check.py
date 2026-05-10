from __future__ import annotations

from pathlib import Path

from scripts import agent_context_budget_check as gate


def write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    _ = path.write_text(text, encoding="utf-8")


def test_artifact_cap_failures_require_line_cap_for_non_legacy_artifact(tmp_path: Path):
    write(
        tmp_path / "docs/agents/issue-packets/issue-1.yaml",
        'schema_version: "1.0"\nkind: issue_packet\nissue:\n  number: "1"\n',
    )

    failures = gate.artifact_cap_failures(tmp_path)

    assert any(
        "missing line_cap in docs/agents/issue-packets/issue-1.yaml: expected 80 for kind issue_packet" in failure
        for failure in failures
    )


def test_artifact_cap_failures_skip_pre_enforcement_artifact(tmp_path: Path):
    write(
        tmp_path / "docs/agents/evidence/issue-1-pr-1.yaml",
        'schema_version: "1.0"\nkind: evidence_packet\nlegacy_note:\n  pre_enforcement: true\n',
    )

    assert gate.artifact_cap_failures(tmp_path) == []


def test_artifact_cap_failures_reject_wrong_declared_cap(tmp_path: Path):
    write(
        tmp_path / "docs/agents/handoffs/issue-1.yaml",
        'schema_version: "1.0"\nkind: issue_handoff\nline_cap: 60\nissue_identity:\n  number: "1"\n',
    )

    failures = gate.artifact_cap_failures(tmp_path)

    assert any(
        "wrong line_cap in docs/agents/handoffs/issue-1.yaml: declared 60, expected 35 for kind issue_handoff"
        in failure
        for failure in failures
    )


def test_artifact_cap_failures_govern_release_result_artifacts(tmp_path: Path):
    write(
        tmp_path / "docs/agents/release-results/issue-1-pr-1.yaml",
        'schema_version: "1.0"\nkind: release_result\nsubject:\n  issue_number: "1"\n',
    )

    failures = gate.artifact_cap_failures(tmp_path)

    assert any(
        "missing line_cap in docs/agents/release-results/issue-1-pr-1.yaml: expected 60 for kind release_result"
        in failure
        for failure in failures
    )
