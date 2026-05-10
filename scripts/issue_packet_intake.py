#!/usr/bin/env python3
"""Sync ready-for-agent GitHub issues into compact repo-local issue packets."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import cast


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REPO = os.environ.get("AUTODEV_GITHUB_REPO", "paulpai0412/wferp")
DEFAULT_ISSUE_PACKETS_DIR = ROOT / "docs/agents/issue-packets"


@dataclass
class GitHubIssue:
    number: str
    title: str
    body: str
    url: str
    labels: list[str]


def slugify(text: str) -> str:
    lowered = text.lower()
    lowered = re.sub(r"[^a-z0-9]+", "-", lowered)
    lowered = lowered.strip("-")
    return lowered[:48] or "ready-issue"


def build_branch_name(issue: GitHubIssue) -> str:
    return f"agent/issue-{issue.number}-{slugify(issue.title)}"


def infer_parent_reference(issue: GitHubIssue) -> str:
    match = re.search(r"(?im)^parent\s*:\s*(.+)$", issue.body)
    if match:
        return match.group(1).strip()
    issue_ref = re.search(r"(?i)#(\d+)", issue.body)
    return f"https://github.com/{DEFAULT_REPO}/issues/{issue_ref.group(1)}" if issue_ref else "none"


def infer_dependencies(issue: GitHubIssue) -> list[str]:
    dependencies: list[str] = []
    for line in issue.body.splitlines():
        if re.search(r"(?i)depends on|blocked by|requires issue", line):
            dependencies.append(line.strip())
    return dependencies or ["none"]


def infer_acceptance_criteria(issue: GitHubIssue) -> list[tuple[str, str]]:
    criteria: list[tuple[str, str]] = []
    for line in issue.body.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if re.match(r"^[-*]\s+", stripped) or re.match(r"^\d+[.)]\s+", stripped):
            text = re.sub(r"^[-*]\s+", "", stripped)
            text = re.sub(r"^\d+[.)]\s+", "", text)
            criteria.append((f"AC{len(criteria) + 1}", text))
    if criteria:
        return criteria[:5]
    return [("AC1", issue.title)]


def infer_objective(issue: GitHubIssue) -> str:
    first_sentence = issue.body.strip().splitlines()[0].strip() if issue.body.strip() else issue.title
    return first_sentence[:160]


def fetch_ready_issues(repo: str) -> list[GitHubIssue]:
    result = subprocess.run(
        [
            "gh",
            "issue",
            "list",
            "--repo",
            repo,
            "--state",
            "open",
            "--label",
            "ready-for-agent",
            "--json",
            "number,title,body,url,labels",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = cast(list[dict[str, object]], json.loads(result.stdout))
    issues: list[GitHubIssue] = []
    for item in payload:
        labels_payload = cast(list[dict[str, object]], item.get("labels", []))
        issues.append(
            GitHubIssue(
                number=str(item["number"]),
                title=str(item.get("title", "")),
                body=str(item.get("body", "")),
                url=str(item.get("url", "")),
                labels=[str(label.get("name", "")) for label in labels_payload],
            )
        )
    return issues


def render_issue_packet(issue: GitHubIssue, *, actor: str = "Hephaestus", prepared_at: str | None = None) -> str:
    timestamp = prepared_at or datetime.now().astimezone().isoformat(timespec="seconds")
    branch_name = build_branch_name(issue)
    acceptance_criteria = infer_acceptance_criteria(issue)
    parent_reference = infer_parent_reference(issue)
    dependencies = infer_dependencies(issue)

    lines = [
        'schema_version: "1.0"',
        "kind: issue_packet",
        "line_cap: 80",
        "raw_evidence_policy: index_only_refs_no_raw_logs_or_transcripts",
        "",
        "issue:",
        f'  number: "{issue.number}"',
        f'  title: {json.dumps(issue.title, ensure_ascii=False)}',
        f'  url: "{issue.url}"',
        f"  labels: [{', '.join(json.dumps(label, ensure_ascii=False) for label in issue.labels)}]",
        f'  parent: {{type: "github-issue", reference: {json.dumps(parent_reference, ensure_ascii=False)}}}',
        "",
        f'branch: {{name: "{branch_name}", base: "main"}}',
        f"objective: {json.dumps(infer_objective(issue), ensure_ascii=False)}",
        "",
        "acceptance_criteria:",
    ]
    lines.extend(
        f'  - {{id: "{ac_id}", text: {json.dumps(text, ensure_ascii=False)}, evidence_hint: "Verifier confirms observable outcome from issue scope."}}'
        for ac_id, text in acceptance_criteria
    )
    lines.extend(
        [
            "test_case:",
            "  applies: false",
            '  id: ""',
            '  source: {type: "none", ref: ""}',
            '  surface: "none"',
            '  scenario: ""',
            '  expected_outcome: ""',
            "  regression_bucket: []",
            "scope:",
            '  in: ["<fill-from-issue-or-worker-discovery>"]',
            '  out: ["Human-only scope decisions", "Raw logs in repo docs"]',
            "bootstrap_context:",
            '  required_reads: ["AGENTS.md", "docs/agents/autonomous-development-workflow.yaml", "docs/agents/issue-tracker.md"]',
            '  relevant_paths: ["<fill-from-issue-or-worker-discovery>"]',
            '  prior_handoff: "none"',
            '  context_budget: {checkpoint_warning_at_percent: 45, stop_and_rotate_at_percent: 50}',
            "implementation_notes:",
            '  constraints: ["Keep artifacts compact and index-only."]',
            f"  risks: [{json.dumps('Issue body requires worker verification for exact scope.', ensure_ascii=False)}]",
            f"  dependencies: [{', '.join(json.dumps(item, ensure_ascii=False) for item in dependencies)}]",
            "verification_plan:",
            "  worker_self_checks:",
            '    - {command: "<worker fills this after reading scope>", purpose: "implementation feedback only; not final acceptance QA", success_criteria: "Focused issue checks pass before worker returns."}',
            "  verifier_acceptance_checks:",
            '    required_gates: ["diagnostics_and_build_gate", "surface_qa_gate", "review_gate"]',
            "    automated_checks:",
            '      - {command: "<verifier fills this from issue scope>", success_criteria: "Verifier confirms acceptance criteria through observable behavior."}',
            '  verifier_manual_qa: {owner: "pr_verifier", surface: "cli|api|browser|library|sql|static-html", happy_path: "Exercise the issue\'s primary user path.", refusal_or_error_path: "Confirm one refusal, edge, or blocked path when applicable."}',
            f'handoff_contract: {{repo_path: "docs/agents/handoffs/issue-{issue.number}.yaml", github_summary_required: true}}',
            'result_contract: {template_path: "docs/agents/worker-result-template.yaml", verifier_template_path: "docs/agents/evidence-packet-template.yaml", allowed_statuses: [success, blocked, failed]}',
            'role_boundary: {orchestrator_may_validate_contract_only: true, worker_may_emit_final_acceptance: false, verifier_packet_required_for_completion: true}',
            f'prepared_by: {{actor: "{actor}", prepared_at: "{timestamp}"}}',
        ]
    )
    return "\n".join(lines) + "\n"


def sync_issue_packets(issues: list[GitHubIssue], *, output_dir: Path = DEFAULT_ISSUE_PACKETS_DIR) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for issue in issues:
        packet_path = output_dir / f"issue-{issue.number}.yaml"
        _ = packet_path.write_text(render_issue_packet(issue), encoding="utf-8")
        written.append(packet_path)
    return written


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    _ = parser.add_argument("--repo", default=DEFAULT_REPO, help="GitHub repo name for gh issue list")
    _ = parser.add_argument("--issues-json", help="Path to a JSON fixture with gh issue list output")
    _ = parser.add_argument("--output-dir", default=str(DEFAULT_ISSUE_PACKETS_DIR), help="Directory for issue packets")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    output_dir = Path(cast(str, args.output_dir))
    if cast(str | None, args.issues_json):
        payload = cast(list[dict[str, object]], json.loads(Path(cast(str, args.issues_json)).read_text(encoding="utf-8")))
        issues = [
            GitHubIssue(
                number=str(item["number"]),
                title=str(item.get("title", "")),
                body=str(item.get("body", "")),
                url=str(item.get("url", "")),
                labels=[str(label) for label in cast(list[object], item.get("labels", []))],
            )
            for item in payload
        ]
    else:
        issues = fetch_ready_issues(cast(str, args.repo))
    written = sync_issue_packets(issues, output_dir=output_dir)
    for path in written:
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
