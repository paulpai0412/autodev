from __future__ import annotations

from pathlib import Path

from scripts.orchestrator_artifacts import (
    parse_evidence_packet_file,
    parse_release_result_file,
    parse_worker_result_file,
)


def test_parse_worker_result_reads_next_recommended_step_from_summary(tmp_path: Path) -> None:
    path = tmp_path / "worker.yaml"
    path.write_text(
        """schema_version: "1.0"
kind: worker_result
line_cap: 80
status: "success"
summary:
  objective: "demo"
  outcome: "done"
  next_recommended_step: "Run verifier next"
pr:
  number: "77"
metadata:
  completed_at: "2026-05-07T17:10:00+08:00"
failure_classification: {kind: "none", retryable: true, routed_to: "pr_verifier", root_cause_signature: "none"}
""",
        encoding="utf-8",
    )

    parsed = parse_worker_result_file(path)

    assert parsed["next_recommended_step"] == "Run verifier next"


def test_parse_evidence_packet_reads_next_recommended_step_from_compact_summary(tmp_path: Path) -> None:
    path = tmp_path / "evidence.yaml"
    path.write_text(
        """schema_version: "1.0"
kind: evidence_packet
line_cap: 60
status: "pass"
subject:
  pr_number: "77"
verifier:
  verifier_session_id: "ses-v"
compact_summary:
  outcome: "ok"
  next_recommended_step: "Proceed to release"
failure_classification: {kind: "none", retryable: true, routed_to: "none", root_cause_signature: "none"}
""",
        encoding="utf-8",
    )

    parsed = parse_evidence_packet_file(path)

    assert parsed["next_recommended_step"] == "Proceed to release"


def test_parse_release_result_reads_next_recommended_step_from_top_level_or_summary(tmp_path: Path) -> None:
    path = tmp_path / "release.yaml"
    path.write_text(
        """schema_version: "1.0"
kind: release_result
line_cap: 60
status: "success"
blocked_reason: "none"
summary:
  outcome: "merged"
  next_recommended_step: "Queue next issue"
failure_classification: {kind: "none", retryable: true, routed_to: "none", root_cause_signature: "none"}
""",
        encoding="utf-8",
    )

    parsed = parse_release_result_file(path)

    assert parsed["next_recommended_step"] == "Queue next issue"
