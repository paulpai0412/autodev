#!/usr/bin/env python3
"""Smoke-check artifact contract enforcement in orchestrator_supervisor.submit_artifact.

Run:
  PYTHONPATH=. python3 scripts/test_artifact_contracts.py
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import scripts.orchestrator_supervisor as orchestrator_supervisor
from scripts.control_plane_db import read_issue


def _artifact_refs(base_dir: Path, issue_number: str) -> dict[str, object]:
    issue = read_issue(base_dir, issue_number)
    if issue is None:
        raise RuntimeError(f"issue #{issue_number} not found")
    return json.loads(str(issue.get("artifact_refs_json") or "{}"))


def _expect_value_error(callable_name: str, fn) -> str:
    try:
        fn()
    except ValueError as error:
        return str(error)
    raise AssertionError(f"{callable_name} expected ValueError but succeeded")


def run() -> None:
    with tempfile.TemporaryDirectory(prefix="autodev-artifact-contract-") as tmp:
        base_dir = Path(tmp)
        issue_number = "999"
        now = "2026-05-18T14:30:00+08:00"

        orchestrator_supervisor.ensure_issue_row(base_dir, issue_number=issue_number, updated_at=now)

        # 1) Enum enforcement: worker_result status cannot be "pass".
        msg = _expect_value_error(
            "worker_result invalid status",
            lambda: orchestrator_supervisor.submit_artifact(
                base_dir=base_dir,
                issue_number=issue_number,
                artifact_kind="worker_result",
                payload={"issue": 999, "branch": "agent/issue-999-demo", "status": "pass"},
                updated_at=now,
            ),
        )
        assert "allowed statuses" in msg

        # 2) Evidence contract enforcement: pass requires surface_qa_gate.evidence_ref.
        msg = _expect_value_error(
            "evidence_packet missing evidence_ref",
            lambda: orchestrator_supervisor.submit_artifact(
                base_dir=base_dir,
                issue_number=issue_number,
                artifact_kind="evidence_packet",
                payload={
                    "subject": "issue #999",
                    "verifier": "pr_verifier",
                    "status": "pass",
                    "gates": {"surface_qa_gate": {"status": "pass"}},
                },
                updated_at=now,
            ),
        )
        assert "gates.surface_qa_gate.evidence_ref" in msg

        # 3) Successful insert updates artifact_refs_json.worker_result_ref.
        orchestrator_supervisor.submit_artifact(
            base_dir=base_dir,
            issue_number=issue_number,
            artifact_kind="worker_result",
            payload={
                "issue": 999,
                "branch": "agent/issue-999-demo",
                "status": "success",
                "pr_number": "123",
            },
            updated_at=now,
        )
        refs = _artifact_refs(base_dir, issue_number)
        worker_ref = str(refs.get("worker_result_ref") or "")
        assert worker_ref.startswith("db:worker_result:history:")

        # 4) Successful pass evidence updates artifact_refs_json.evidence_packet_ref.
        browser_report = base_dir / "artifacts/browser/report.html"
        browser_report.parent.mkdir(parents=True, exist_ok=True)
        browser_report.write_text("<html>browser evidence</html>", encoding="utf-8")
        orchestrator_supervisor.submit_artifact(
            base_dir=base_dir,
            issue_number=issue_number,
            artifact_kind="evidence_packet",
            payload={
                "subject": "issue #999",
                "verifier": "pr_verifier",
                "status": "pass",
                "gates": {
                    "surface_qa_gate": {
                        "status": "pass",
                        "evidence_ref": "artifacts/browser/report.html",
                        "evidence_kind": "browser",
                    }
                },
            },
            updated_at=now,
        )
        refs = _artifact_refs(base_dir, issue_number)
        evidence_ref = str(refs.get("evidence_packet_ref") or "")
        assert evidence_ref.startswith("db:evidence_packet:history:")

        # 5) release_result ref sync.
        orchestrator_supervisor.submit_artifact(
            base_dir=base_dir,
            issue_number=issue_number,
            artifact_kind="release_result",
            payload={"status": "success", "pr_number": "123"},
            updated_at=now,
        )
        refs = _artifact_refs(base_dir, issue_number)
        release_ref = str(refs.get("release_result_ref") or "")
        assert release_ref.startswith("db:release_result:history:")

    print("artifact_contract_check: PASS")


if __name__ == "__main__":
    run()
