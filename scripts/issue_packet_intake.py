#!/usr/bin/env python3
"""Sync ready-for-agent GitHub issues into compact consumer-project issue packets."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import cast

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.control_plane_db import ingest_issue_packet
from scripts.autodev_project import doctor_project


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REPO = os.environ.get("AUTODEV_GITHUB_REPO", "paulpai0412/autodev")
AUTODEV_CONFIG_NAME = ".autodev.yaml"
TRACKED_RUNTIME_BLOCK_PREFIX = "tracked autodev runtime files must be removed from git index:"


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


def infer_base_branch(issue: GitHubIssue) -> str:
    match = re.search(r"(?im)^(?:base branch|base_branch|base)\s*:\s*(.+)$", issue.body)
    if match:
        return match.group(1).strip()
    return "main"


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


def _strip_markdown_prefix(text: str) -> str:
    cleaned = text.strip()
    cleaned = re.sub(r"^#{1,6}\s*", "", cleaned)
    cleaned = re.sub(r"^[-*]\s+", "", cleaned)
    cleaned = re.sub(r"^\d+[.)]\s+", "", cleaned)
    return cleaned.strip()


def infer_objective(issue: GitHubIssue) -> str:
    if issue.body.strip():
        for line in issue.body.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            cleaned = _strip_markdown_prefix(stripped)
            if cleaned:
                return cleaned[:160]
    return issue.title[:160]


def infer_scope_in(issue: GitHubIssue) -> list[str]:
    scope_items: list[str] = []
    in_scope_section = False
    for line in issue.body.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if re.match(r"^#{1,6}\s*scope\b", stripped, flags=re.IGNORECASE):
            in_scope_section = True
            continue
        if in_scope_section and re.match(r"^#{1,6}\s+", stripped):
            break
        if not in_scope_section:
            continue
        if re.match(r"^[-*]\s+", stripped) or re.match(r"^\d+[.)]\s+", stripped):
            text = re.sub(r"^[-*]\s+", "", stripped)
            text = re.sub(r"^\d+[.)]\s+", "", text)
            if text:
                scope_items.append(text)
    if scope_items:
        return scope_items[:5]
    return [infer_objective(issue)]


def infer_relevant_paths(issue: GitHubIssue) -> list[str]:
    candidates: list[str] = []
    seen: set[str] = set()
    for match in re.finditer(r"`([^`]+)`", issue.body):
        token = match.group(1).strip()
        if not token:
            continue
        if token.startswith(("http://", "https://")):
            continue
        if "/" not in token and "." not in token:
            continue
        if token in seen:
            continue
        seen.add(token)
        candidates.append(token)
    if candidates:
        return candidates[:5]
    return ["."]


def infer_fallback_relevant_paths(issue: GitHubIssue) -> list[str]:
    inferred: list[str] = []
    seen: set[str] = set()
    body = issue.body.lower()
    title = issue.title.lower()

    def add(path: str) -> None:
        if path in seen:
            return
        seen.add(path)
        inferred.append(path)

    if "index.html" in body or "index.html" in title:
        add("index.html")

    if any(keyword in body or keyword in title for keyword in ["web", "ui", "frontend", "html", "badge", "filter", "dashboard", "page"]):
        add("index.html")
        add("smoke_test.js")

    if any(keyword in body or keyword in title for keyword in ["quiz", "vocab", "word", "review", "spaced repetition", "practice"]):
        add("index.html")
        add("smoke_test.js")

    if not inferred:
        add("index.html")
        add("smoke_test.js")

    return inferred[:5]


def _looks_like_placeholder(value: str) -> bool:
    lowered = value.strip().lower()
    if not lowered:
        return True
    if "<fill-" in lowered or "<file-or-directory-path>" in lowered:
        return True
    if lowered in {".", "./", "todo", "tbd", "none", "unknown"}:
        return True
    return False


def validate_issue_packet_handoff_fields(issue: GitHubIssue, *, scope_in: list[str], relevant_paths: list[str]) -> list[str]:
    problems: list[str] = []
    if not scope_in:
        problems.append("scope.in is empty")
    if any(_looks_like_placeholder(item) for item in scope_in):
        problems.append(f"scope.in contains placeholder-like item(s): {scope_in}")

    if not relevant_paths:
        problems.append("bootstrap_context.relevant_paths is empty")
    if any(_looks_like_placeholder(item) for item in relevant_paths):
        problems.append(f"bootstrap_context.relevant_paths contains placeholder-like item(s): {relevant_paths}")
    if any(path.strip() == "." for path in relevant_paths):
        problems.append("bootstrap_context.relevant_paths contains '.' which is too broad")

    issue_context = f"issue #{issue.number}"
    return [f"{issue_context}: {problem}" for problem in problems]


def resolve_worker_handoff_fields(issue: GitHubIssue) -> tuple[list[str], list[str]]:
    scope_in = infer_scope_in(issue)
    relevant_paths = infer_relevant_paths(issue)
    fallback_relevant_paths = infer_fallback_relevant_paths(issue)
    worker_handoff_scope_in = scope_in[:5]
    worker_handoff_relevant_paths = relevant_paths[:5]
    if worker_handoff_scope_in == [infer_objective(issue)] and fallback_relevant_paths:
        worker_handoff_scope_in = [
            f"Implement issue behavior in {path}."
            for path in fallback_relevant_paths[:2]
        ]
    if worker_handoff_relevant_paths == ["."] and fallback_relevant_paths:
        worker_handoff_relevant_paths = fallback_relevant_paths
    return worker_handoff_scope_in, worker_handoff_relevant_paths


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


def _project_root(path: str | None) -> Path:
    return Path(path or ".").resolve()


def _consumer_project_root(path: str | None) -> Path | None:
    candidate = _project_root(path)
    for current in (candidate, *candidate.parents):
        if (current / AUTODEV_CONFIG_NAME).exists():
            return current
    return None


def render_issue_packet(issue: GitHubIssue, *, actor: str = "Hephaestus", prepared_at: str | None = None) -> str:
    timestamp = prepared_at or datetime.now().astimezone().isoformat(timespec="seconds")
    branch_name = build_branch_name(issue)
    acceptance_criteria = infer_acceptance_criteria(issue)
    parent_reference = infer_parent_reference(issue)
    dependencies = infer_dependencies(issue)
    base_branch = infer_base_branch(issue)
    worker_handoff_scope_in, worker_handoff_relevant_paths = resolve_worker_handoff_fields(issue)

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
        f'branch: {{name: "{branch_name}", base: {json.dumps(base_branch, ensure_ascii=False)}}}',
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
            f"  in: [{', '.join(json.dumps(item, ensure_ascii=False) for item in worker_handoff_scope_in)}]",
            '  out: ["Human-only scope decisions", "Raw logs in repo docs"]',
            "bootstrap_context:",
            '  required_reads: ["AGENTS.md", "docs/agents/autonomous-development-workflow.yaml", "docs/agents/issue-tracker.md"]',
            f"  relevant_paths: [{', '.join(json.dumps(item, ensure_ascii=False) for item in worker_handoff_relevant_paths)}]",
            '  prior_handoff: "none"',
            '  context_budget: {checkpoint_warning_at_percent: 45, stop_and_rotate_at_percent: 50}',
            "implementation_notes:",
            '  constraints: ["Keep artifacts compact and index-only."]',
            f"  risks: [{json.dumps('Scope inferred from issue text; verifier must confirm acceptance behavior against user surface.', ensure_ascii=False)}]",
            f"  dependencies: [{', '.join(json.dumps(item, ensure_ascii=False) for item in dependencies)}]",
            "verification_plan:",
            "  worker_self_checks:",
            '    - {command: "<worker fills this after reading scope>", purpose: "implementation feedback only; not final acceptance QA", success_criteria: "Focused issue checks pass before worker returns."}',
            "  verifier_acceptance_checks:",
            '    required_gates: ["diagnostics_and_build_gate", "surface_qa_gate", "review_gate"]',
            "    automated_checks:",
            '      - {command: "<verifier fills this from issue scope>", success_criteria: "Verifier confirms acceptance criteria through observable behavior."}',
            '  verifier_manual_qa: {owner: "pr_verifier", surface: "cli|api|browser|library|sql|static-html", happy_path: "Exercise the issue\'s primary user path.", refusal_or_error_path: "Confirm one refusal, edge, or blocked path when applicable."}',
            'handoff_contract: {storage: "issue_history", github_summary_required: true}',
            'result_contract: {submission: "scripts/orchestrator_supervisor.py submit-artifact", allowed_statuses: [success, blocked, failed]}',
            'role_boundary: {orchestrator_may_validate_contract_only: true, worker_may_emit_final_acceptance: false, verifier_packet_required_for_completion: true}',
            f'prepared_by: {{actor: "{actor}", prepared_at: "{timestamp}"}}',
        ]
    )
    return "\n".join(lines) + "\n"


def issue_packet_payload(issue: GitHubIssue, *, actor: str = "Hephaestus", prepared_at: str | None = None) -> dict[str, object]:
    timestamp = prepared_at or datetime.now().astimezone().isoformat(timespec="seconds")
    return {
        "issue_number": issue.number,
        "title": issue.title,
        "branch": build_branch_name(issue),
        "base_branch": infer_base_branch(issue),
        "backing_type": "github",
        "prior_handoff": "",
        "labels": list(issue.labels),
        "parent_reference": infer_parent_reference(issue),
        "dependencies": infer_dependencies(issue),
        "raw_text": render_issue_packet(issue, actor=actor, prepared_at=timestamp),
    }


def sync_issue_packets_to_db(issues: list[GitHubIssue], *, project_root: Path, actor: str = "Hephaestus", prepared_at: str | None = None) -> list[str]:
    ingested: list[str] = []
    for issue in issues:
        worker_handoff_scope_in, worker_handoff_relevant_paths = resolve_worker_handoff_fields(issue)

        validation_problems = validate_issue_packet_handoff_fields(
            issue,
            scope_in=worker_handoff_scope_in,
            relevant_paths=worker_handoff_relevant_paths,
        )
        if validation_problems:
            details = "\n".join(f"- {problem}" for problem in validation_problems)
            raise ValueError(
                "Issue packet intake validation failed; refusing to ingest placeholder/ambiguous handoff fields:\n"
                f"{details}"
            )

        payload = issue_packet_payload(issue, actor=actor, prepared_at=prepared_at)
        _ = ingest_issue_packet(
            project_root,
            issue_number=issue.number,
            issue_packet=payload,
            updated_at=prepared_at or datetime.now().astimezone().isoformat(timespec="seconds"),
        )
        ingested.append(issue.number)
    return ingested


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    _ = parser.add_argument("--repo", default=DEFAULT_REPO, help="GitHub repo name for gh issue list")
    _ = parser.add_argument("--issues-json", help="Path to a JSON fixture with gh issue list output")
    _ = parser.add_argument("--project-root", default=".", help="Consumer project root or a nested path inside it")
    _ = parser.add_argument("--output-dir", help="Deprecated compatibility flag; DB-backed intake ignores packet file output")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    consumer_root = _consumer_project_root(cast(str, args.project_root))
    if consumer_root is None:
        message = (
            "ERROR: could not find .autodev.yaml from --project-root. "
            "Run intake from a consumer project or pass --project-root <project>."
        )
        print(message)
        return 1
    runtime_db = consumer_root / ".opencode/runtime/control-plane.sqlite3"
    print(f"[issue-packet-intake] project-root={consumer_root}")
    print(f"[issue-packet-intake] runtime-db={runtime_db}")
    report = doctor_project(consumer_root)
    tracked_findings = [
        finding
        for finding in report.findings
        if finding.startswith(TRACKED_RUNTIME_BLOCK_PREFIX)
    ]
    if tracked_findings:
        for finding in tracked_findings:
            print(f"[issue-packet-intake] BLOCKED: {finding}")
        print(
            f"[issue-packet-intake] Run `PYTHONPATH=. python3 scripts/autodev_project.py doctor --project-root \"{consumer_root}\"` and untrack runtime DB before retrying."
        )
        return 1
    print("[issue-packet-intake] doctor tracked-runtime check: pass")
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
    ingested = sync_issue_packets_to_db(issues, project_root=consumer_root)
    for issue_number in ingested:
        print(f"issue-{issue_number}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
