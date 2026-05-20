from __future__ import annotations

from typing import cast

from scripts import orchestrator_requests


def test_build_prompt_for_main_orchestrator_excludes_release_worker_child_subagent() -> None:
    ledger = cast(dict[str, object], {
        "automation": {"primaryWorkspaceRoot": "/tmp/project"},
        "issue": {
            "number": "6",
            "branch": "agent/issue-6-demo",
        },
        "workflow": {
            "workflowPolicyPath": "docs/agents/autonomous-development-workflow.yaml",
        },
        "artifacts": {},
    })

    prompt = orchestrator_requests.build_prompt(
        ledger,
        role="main_orchestrator",
        stage="orchestrator_bootstrap",
        decision_summary="launch child subagents now",
        default_supervisor_doc_path="docs/agents/runtime/nonstop-supervisor-loop.md",
        default_release_result_template_path="docs/agents/release-result-template.yaml",
    )

    assert 'task(subagent_type="general", ..., run_in_background=false)' in prompt
    assert "Run issue_worker and pr_verifier as subagents" in prompt
    assert "Do not launch release_worker from this root session" in prompt
    assert "Run issue_worker, pr_verifier, and release_worker" not in prompt
    assert "DB-backed control plane" in prompt
    assert "Use the DB-backed supervisor reconcile flow before the first issue_worker launch" in prompt
    assert "Use the first supervisor decision to confirm the issue_worker dispatch" in prompt
    assert "Wait for each child task call to finish in the foreground before continuing." in prompt
    assert 'load_skills=["tdd", "karpathy-guidelines", "git-master"]' in prompt
    assert 'load_skills=["tdd", "karpathy-guidelines", "git-master", "web-design-engineer"]' in prompt
    assert "Do not omit tdd for code-changing issue work." in prompt
    assert 'load_skills=["review-work", "karpathy-guidelines"]' in prompt
    assert 'load_skills=["review-work", "karpathy-guidelines", "browser-qa", "e2e-testing"]' in prompt
    assert "Do not include karpathy-guidelines in load_skills for child subagents" not in prompt


def test_build_prompt_for_main_orchestrator_uses_authoritative_shared_runtime_paths() -> None:
    ledger = cast(dict[str, object], {
        "automation": {"primaryWorkspaceRoot": "/tmp/project"},
        "issue": {
            "number": "6",
            "branch": "agent/issue-6-demo",
        },
        "workflow": {
            "workflowPolicyPath": "/shared/autodev/docs/agents/autonomous-development-workflow.yaml",
        },
        "artifacts": {},
    })

    prompt = orchestrator_requests.build_prompt(
        ledger,
        role="main_orchestrator",
        stage="orchestrator_bootstrap",
        decision_summary="launch child subagents now",
        default_supervisor_doc_path="/shared/autodev/docs/agents/runtime/nonstop-supervisor-loop.md",
        default_release_result_template_path="/shared/autodev/docs/agents/release-result-template.yaml",
    )

    assert "The authoritative shared workflow policy is /shared/autodev/docs/agents/autonomous-development-workflow.yaml." in prompt
    assert "The authoritative nonstop supervisor doc is /shared/autodev/docs/agents/runtime/nonstop-supervisor-loop.md." in prompt
    assert "Bootstrap from the SQLite-backed control plane" in prompt
    assert "Use the DB-backed supervisor reconcile flow before the first issue_worker launch" in prompt
    assert 'PYTHONPATH="/shared/autodev" python3 "/shared/autodev/scripts/orchestrator_supervisor.py" submit-artifact' in prompt
    assert 'python3 "/tmp/project/scripts/orchestrator_supervisor.py"' not in prompt


def test_build_prompt_uses_primary_workspace_absolute_artifact_paths() -> None:
    ledger = cast(dict[str, object], {
        "automation": {"primaryWorkspaceRoot": "/tmp/project"},
        "issue": {
            "number": "8",
            "branch": "agent/issue-8-demo",
        },
        "workflow": {
            "workflowPolicyPath": "docs/agents/autonomous-development-workflow.yaml",
        },
        "artifacts": {
            "worker_result_ref": "docs/agents/worker-results/issue-8.yaml",
            "evidence_packet_ref": "docs/agents/evidence/issue-8-pr-12.yaml",
            "release_result_ref": "docs/agents/release-results/issue-8-pr-12.yaml",
        },
    })

    verifier_prompt = orchestrator_requests.build_prompt(
        ledger,
        role="pr_verifier",
        stage="pr_verifier_execution",
        decision_summary="verify now",
        default_supervisor_doc_path="docs/agents/runtime/nonstop-supervisor-loop.md",
        default_release_result_template_path="docs/agents/release-result-template.yaml",
    )
    release_prompt = orchestrator_requests.build_prompt(
        ledger,
        role="main_orchestrator",
        stage="release_root_execution",
        decision_summary="release now",
        default_supervisor_doc_path="docs/agents/runtime/nonstop-supervisor-loop.md",
        default_release_result_template_path="docs/agents/release-result-template.yaml",
    )

    assert "submit-artifact" in verifier_prompt
    assert "submit-artifact" in release_prompt
    assert "python3 scripts/orchestrator_supervisor.py" in verifier_prompt
    assert "must not be required for progress" not in verifier_prompt
    assert "must not be required for progress" not in release_prompt


def test_build_prompt_for_release_worker_mentions_release_approval_override() -> None:
    ledger = cast(dict[str, object], {
        "issue": {
            "number": "7",
            "branch": "agent/issue-7-demo",
        },
        "workflow": {
            "workflowPolicyPath": "docs/agents/autonomous-development-workflow.yaml",
            "runtimeControls": {
                "approval_override_mode": "bypass_approval",
                "override_source": "user_requested_autodev_release",
                "human_approval_skipped": True,
            },
        },
        "artifacts": {
            "evidence_packet_ref": "docs/agents/evidence/issue-7-pr-11.yaml",
            "release_result_ref": "docs/agents/release-results/issue-7-pr-11.yaml",
        },
    })

    prompt = orchestrator_requests.build_prompt(
        ledger,
        role="main_orchestrator",
        stage="release_root_execution",
        decision_summary="release now",
        default_supervisor_doc_path="docs/agents/runtime/nonstop-supervisor-loop.md",
        default_release_result_template_path="docs/agents/release-result-template.yaml",
    )

    assert "You are the independent release root session" in prompt
    assert 'task(subagent_type="general", ..., run_in_background=false)' in prompt
    assert "foreground release_worker subagent" in prompt
    assert "Read the release runtime controls from the DB-backed control-plane context" in prompt
    assert "approval_override_mode" in prompt
    assert "bypass_approval" in prompt
    assert "human approval requirement" in prompt
    assert "merge_approval_mode" in prompt
    assert "override_scope" in prompt
    assert "git fetch origin --prune" in prompt
    assert "git pull --ff-only origin main" in prompt
    assert "release_result.merge_gate" in prompt
    assert "release_result.workspace_hygiene" in prompt
    assert "single merge authority" in prompt
    assert "return control to the supervisor/release command result" in prompt
    assert "Do not perform the merge/close workflow directly from the release root shell" in prompt
    assert "return control to the main_orchestrator root session" not in prompt


def test_build_prompt_for_pr_verifier_requires_evidence_packet_before_completion() -> None:
    ledger = cast(dict[str, object], {
        "issue": {
            "number": "8",
            "branch": "agent/issue-8-demo",
        },
        "workflow": {
            "workflowPolicyPath": "docs/agents/autonomous-development-workflow.yaml",
        },
        "artifacts": {
            "worker_result_ref": "docs/agents/worker-results/issue-8.yaml",
            "evidence_packet_ref": "docs/agents/evidence/issue-8-pr-12.yaml",
        },
    })

    prompt = orchestrator_requests.build_prompt(
        ledger,
        role="pr_verifier",
        stage="pr_verifier_execution",
        decision_summary="verify now",
        default_supervisor_doc_path="docs/agents/runtime/nonstop-supervisor-loop.md",
        default_release_result_template_path="docs/agents/release-result-template.yaml",
    )

    assert "Persist verifier acceptance or failure as an evidence_packet via submit-artifact" in prompt
    assert "Evidence payload contract" in prompt
    assert "gates.surface_qa_gate must be an object {status: 'pass', evidence_ref: '<non-empty>', evidence_kind: 'browser'}" in prompt
    assert "Do not stop, summarize, or report verification progress until the evidence_packet payload is stored in SQLite" in prompt
    assert "Immediately after submit-artifact for evidence_packet, run inspect" in prompt
    assert "Inspect command: python3 scripts/orchestrator_supervisor.py inspect --base-dir" in prompt
    assert "If inspect does not show persisted evidence_packet" in prompt
    assert "Only return after SQLite persistence is visible via inspect" in prompt
    assert 'load_skills containing "review-work"' in prompt
    assert 'include both "browser-qa" and "e2e-testing" in pr_verifier load_skills' in prompt


def test_build_prompt_for_issue_worker_requires_pr_metadata_before_success() -> None:
    ledger = cast(dict[str, object], {
        "automation": {"primaryWorkspaceRoot": "/tmp/project"},
        "issue": {
            "number": "18",
            "branch": "agent/issue-18-demo",
            "baseBranch": "agent/issue-17-parent",
        },
        "workflow": {
            "workflowPolicyPath": "docs/agents/autonomous-development-workflow.yaml",
        },
        "artifacts": {
            "worker_result_ref": "docs/agents/worker-results/issue-18.yaml",
        },
    })

    prompt = orchestrator_requests.build_prompt(
        ledger,
        role="issue_worker",
        stage="issue_worker_execution",
        decision_summary="implement now",
        default_supervisor_doc_path="docs/agents/runtime/nonstop-supervisor-loop.md",
        default_release_result_template_path="docs/agents/release-result-template.yaml",
    )

    assert "Persist the normalized worker_result into SQLite with submit-artifact" in prompt
    assert "fetch and merge the latest base branch" in prompt
    assert "Do not report status=success until the implementation branch is pushed" in prompt
    assert 'include "web-design-engineer" in issue_worker load_skills before coding.' in prompt
    assert "from base branch agent/issue-17-parent" in prompt
    assert "Do not create the formal PR" in prompt
    assert "submit blocked or failed instead of success" in prompt


def test_build_prompt_for_pr_verifier_owns_formal_pr_creation() -> None:
    ledger = cast(dict[str, object], {
        "issue": {
            "number": "18",
            "branch": "agent/issue-18-demo",
            "baseBranch": "agent/issue-17-parent",
        },
        "workflow": {
            "workflowPolicyPath": "docs/agents/autonomous-development-workflow.yaml",
        },
        "artifacts": {},
    })

    prompt = orchestrator_requests.build_prompt(
        ledger,
        role="pr_verifier",
        stage="pr_verifier_execution",
        decision_summary="verify now",
        default_supervisor_doc_path="docs/agents/runtime/nonstop-supervisor-loop.md",
        default_release_result_template_path="docs/agents/release-result-template.yaml",
    )

    assert "create or record the formal PR" in prompt
    assert "include pr_number in the evidence_packet payload" in prompt
    assert "head branch agent/issue-18-demo and base branch agent/issue-17-parent" in prompt
    assert "include base_branch in the evidence_packet payload" in prompt


def test_build_prompt_for_local_seeded_issue_avoids_github_issue_assumptions() -> None:
    ledger = cast(dict[str, object], {
        "automation": {"primaryWorkspaceRoot": "/tmp/project"},
        "issue": {
            "number": "31",
            "branch": "agent/issue-31-demo",
            "backingType": "local_seeded",
        },
        "workflow": {
            "workflowPolicyPath": "docs/agents/autonomous-development-workflow.yaml",
        },
        "artifacts": {
            "worker_result_ref": "docs/agents/worker-results/issue-31.yaml",
            "evidence_packet_ref": "docs/agents/evidence/issue-31-pr-28.yaml",
            "release_result_ref": "docs/agents/release-results/issue-31-pr-28.yaml",
        },
    })

    worker_prompt = orchestrator_requests.build_prompt(
        ledger,
        role="issue_worker",
        stage="issue_worker_execution",
        decision_summary="implement now",
        default_supervisor_doc_path="docs/agents/runtime/nonstop-supervisor-loop.md",
        default_release_result_template_path="docs/agents/release-result-template.yaml",
    )
    release_prompt = orchestrator_requests.build_prompt(
        ledger,
        role="main_orchestrator",
        stage="release_root_execution",
        decision_summary="release now",
        default_supervisor_doc_path="docs/agents/runtime/nonstop-supervisor-loop.md",
        default_release_result_template_path="docs/agents/release-result-template.yaml",
    )

    assert "This issue is local-seeded." in worker_prompt
    assert "do not rely on `gh issue view` succeeding" in worker_prompt
    assert "This issue is local-seeded." in release_prompt
    assert "Skip GitHub issue operations such as `gh issue close`" in release_prompt


def test_build_prompt_for_github_backed_issue_keeps_github_issue_language() -> None:
    ledger = cast(dict[str, object], {
        "issue": {
            "number": "42",
            "branch": "agent/issue-42-demo",
            "backingType": "github",
        },
        "workflow": {
            "workflowPolicyPath": "docs/agents/autonomous-development-workflow.yaml",
        },
        "artifacts": {
            "evidence_packet_ref": "docs/agents/evidence/issue-42-pr-77.yaml",
            "release_result_ref": "docs/agents/release-results/issue-42-pr-77.yaml",
        },
    })

    prompt = orchestrator_requests.build_prompt(
        ledger,
        role="main_orchestrator",
        stage="release_root_execution",
        decision_summary="release now",
        default_supervisor_doc_path="docs/agents/runtime/nonstop-supervisor-loop.md",
        default_release_result_template_path="docs/agents/release-result-template.yaml",
    )

    assert "This issue is GitHub-backed." in prompt
    assert "Apply the normal GitHub issue close/update workflow" in prompt


def test_build_prompt_spec_renders_decision_summary_as_last_line() -> None:
    ledger = cast(dict[str, object], {
        "issue": {"number": "42", "branch": "agent/issue-42-demo"},
        "workflow": {"workflowPolicyPath": "docs/agents/autonomous-development-workflow.yaml"},
        "artifacts": {},
    })

    spec = orchestrator_requests.build_prompt_spec(
        ledger,
        role="main_orchestrator",
        stage="orchestrator_bootstrap",
        decision_summary="summary tail check",
        default_supervisor_doc_path="docs/agents/runtime/nonstop-supervisor-loop.md",
        default_release_result_template_path="docs/agents/release-result-template.yaml",
    )

    rendered = spec.render()
    assert rendered.splitlines()[-1] == "Decision summary: summary tail check"


def test_build_session_request_spec_projects_selected_issue_fields() -> None:
    ledger = cast(dict[str, object], {
        "issue": {
            "number": "31",
            "branch": "agent/issue-31-demo",
            "baseBranch": "main",
        },
        "workflow": {
            "workflowPolicyPath": "docs/agents/autonomous-development-workflow.yaml",
        },
        "artifacts": {},
        "queuedNextIssue": {
            "issue_number": "32",
            "branch": "agent/issue-32-demo",
            "base_branch": "main",
        },
        "history": [],
        "updatedAt": "2026-05-07T17:00:00+08:00",
    })

    spec = orchestrator_requests.build_session_request_spec(
        ledger,
        role="main_orchestrator",
        stage="issue_selection_or_recovery",
        reason="recover",
        title="Recover",
        decision_summary="resume",
        now=lambda _value: "2026-05-07T17:00:00+08:00",
        root_session_agent=lambda _ledger: "build",
        build_prompt=lambda _ledger, _role, _stage, _summary: "prompt",
    )
    payload = spec.to_json()

    assert payload["selectedIssueNumber"] == "32"
    assert payload["selectedIssueBranch"] == "agent/issue-32-demo"
    assert "selectedIssueBaseBranch" not in payload
