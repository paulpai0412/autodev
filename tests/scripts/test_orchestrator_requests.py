from __future__ import annotations

from typing import cast

from scripts import orchestrator_requests


def test_build_prompt_for_main_orchestrator_requires_foreground_child_subagents() -> None:
    ledger = cast(dict[str, object], {
        "automation": {"primaryWorkspaceRoot": "/tmp/project"},
        "issue": {
            "number": "6",
            "branch": "agent/issue-6-demo",
            "issuePacketPath": "docs/agents/issue-packets/issue-6.yaml",
        },
        "workflow": {
            "checkpointPath": "docs/agents/runtime/context-checkpoint.yaml",
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
    assert "Before launching the first child subagent, run the supervisor reconcile command once" in prompt
    assert "After each child artifact is written, advance the queued child role with:" in prompt
    assert "advance-child --ledger .opencode/runtime/orchestrator-ledger.json" in prompt
    assert "Use the first supervisor decision to confirm the issue_worker dispatch" in prompt
    assert "Wait for each child task call to finish in the foreground before continuing." in prompt
    assert "Do not include karpathy-guidelines in load_skills for child subagents" not in prompt


def test_build_prompt_for_main_orchestrator_uses_authoritative_shared_runtime_paths() -> None:
    ledger = cast(dict[str, object], {
        "automation": {"primaryWorkspaceRoot": "/tmp/project"},
        "issue": {
            "number": "6",
            "branch": "agent/issue-6-demo",
            "issuePacketPath": "docs/agents/issue-packets/issue-6.yaml",
        },
        "workflow": {
            "checkpointPath": "docs/agents/runtime/context-checkpoint.yaml",
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
    assert 'PYTHONPATH="/shared/autodev" python3 "/shared/autodev/scripts/orchestrator_supervisor.py" reconcile --ledger .opencode/runtime/orchestrator-ledger.json' in prompt
    assert 'PYTHONPATH="/shared/autodev" python3 "/shared/autodev/scripts/orchestrator_supervisor.py" advance-child --ledger .opencode/runtime/orchestrator-ledger.json' in prompt


def test_build_prompt_uses_primary_workspace_absolute_artifact_paths() -> None:
    ledger = cast(dict[str, object], {
        "automation": {"primaryWorkspaceRoot": "/tmp/project"},
        "issue": {
            "number": "8",
            "branch": "agent/issue-8-demo",
            "issuePacketPath": "docs/agents/issue-packets/issue-8.yaml",
        },
        "workflow": {
            "checkpointPath": "docs/agents/runtime/context-checkpoint.yaml",
            "workflowPolicyPath": "docs/agents/autonomous-development-workflow.yaml",
        },
        "artifacts": {
            "workerResultPath": "docs/agents/worker-results/issue-8.yaml",
            "evidencePacketPath": "docs/agents/evidence/issue-8-pr-12.yaml",
            "releaseResultPath": "docs/agents/release-results/issue-8-pr-12.yaml",
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
        role="release_worker",
        stage="release_worker_execution",
        decision_summary="release now",
        default_supervisor_doc_path="docs/agents/runtime/nonstop-supervisor-loop.md",
        default_release_result_template_path="docs/agents/release-result-template.yaml",
    )

    assert "/tmp/project/docs/agents/worker-results/issue-8.yaml" in verifier_prompt
    assert "/tmp/project/docs/agents/evidence/issue-8-pr-12.yaml" in verifier_prompt
    assert "/tmp/project/docs/agents/evidence/issue-8-pr-12.yaml" in release_prompt
    assert "/tmp/project/docs/agents/release-results/issue-8-pr-12.yaml" in release_prompt


def test_build_prompt_for_release_worker_mentions_workflow_start_approval_override() -> None:
    ledger = cast(dict[str, object], {
        "issue": {
            "number": "7",
            "branch": "agent/issue-7-demo",
        },
        "workflow": {
            "checkpointPath": "docs/agents/runtime/context-checkpoint.yaml",
            "workflowPolicyPath": "docs/agents/autonomous-development-workflow.yaml",
        },
        "artifacts": {
            "evidencePacketPath": "docs/agents/evidence/issue-7-pr-11.yaml",
            "releaseResultPath": "docs/agents/release-results/issue-7-pr-11.yaml",
        },
    })

    prompt = orchestrator_requests.build_prompt(
        ledger,
        role="release_worker",
        stage="release_worker_execution",
        decision_summary="release now",
        default_supervisor_doc_path="docs/agents/runtime/nonstop-supervisor-loop.md",
        default_release_result_template_path="docs/agents/release-result-template.yaml",
    )

    assert "Read the runtime_controls block in the checkpoint" in prompt
    assert "approval_override_mode" in prompt
    assert "bypass_approval" in prompt
    assert "human approval requirement" in prompt
    assert "merge_approval_mode" in prompt
    assert "override_scope" in prompt


def test_build_prompt_for_pr_verifier_requires_evidence_packet_before_completion() -> None:
    ledger = cast(dict[str, object], {
        "issue": {
            "number": "8",
            "branch": "agent/issue-8-demo",
            "issuePacketPath": "docs/agents/issue-packets/issue-8.yaml",
        },
        "workflow": {
            "checkpointPath": "docs/agents/runtime/context-checkpoint.yaml",
            "workflowPolicyPath": "docs/agents/autonomous-development-workflow.yaml",
        },
        "artifacts": {
            "workerResultPath": "docs/agents/worker-results/issue-8.yaml",
            "evidencePacketPath": "docs/agents/evidence/issue-8-pr-12.yaml",
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

    assert "Write docs/agents/evidence/issue-8-pr-12.yaml using docs/agents/evidence-packet-template.yaml." in prompt
    assert "Do not stop, summarize, or report verification progress until that evidence packet exists" in prompt


def test_build_prompt_for_issue_worker_requires_pr_metadata_before_success() -> None:
    ledger = cast(dict[str, object], {
        "automation": {"primaryWorkspaceRoot": "/tmp/project"},
        "issue": {
            "number": "18",
            "branch": "agent/issue-18-demo",
            "issuePacketPath": "docs/agents/issue-packets/issue-18.yaml",
        },
        "workflow": {
            "checkpointPath": "docs/agents/runtime/context-checkpoint.yaml",
            "workflowPolicyPath": "docs/agents/autonomous-development-workflow.yaml",
        },
        "artifacts": {
            "workerResultPath": "docs/agents/worker-results/issue-18.yaml",
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

    assert "Do not write status: success until the branch is pushed, the PR exists" in prompt
    assert "pr.number plus pr.url are populated in the worker_result" in prompt
    assert "write blocked or failed instead of success" in prompt


def test_build_prompt_for_local_seeded_issue_avoids_github_issue_assumptions() -> None:
    ledger = cast(dict[str, object], {
        "automation": {"primaryWorkspaceRoot": "/tmp/project"},
        "issue": {
            "number": "31",
            "branch": "agent/issue-31-demo",
            "issuePacketPath": "docs/agents/issue-packets/issue-31.yaml",
            "backingType": "local_seeded",
        },
        "workflow": {
            "checkpointPath": "docs/agents/runtime/context-checkpoint.yaml",
            "workflowPolicyPath": "docs/agents/autonomous-development-workflow.yaml",
        },
        "artifacts": {
            "workerResultPath": "docs/agents/worker-results/issue-31.yaml",
            "evidencePacketPath": "docs/agents/evidence/issue-31-pr-28.yaml",
            "releaseResultPath": "docs/agents/release-results/issue-31-pr-28.yaml",
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
        role="release_worker",
        stage="release_worker_execution",
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
            "issuePacketPath": "docs/agents/issue-packets/issue-42.yaml",
            "backingType": "github",
        },
        "workflow": {
            "checkpointPath": "docs/agents/runtime/context-checkpoint.yaml",
            "workflowPolicyPath": "docs/agents/autonomous-development-workflow.yaml",
        },
        "artifacts": {
            "evidencePacketPath": "docs/agents/evidence/issue-42-pr-77.yaml",
            "releaseResultPath": "docs/agents/release-results/issue-42-pr-77.yaml",
        },
    })

    prompt = orchestrator_requests.build_prompt(
        ledger,
        role="release_worker",
        stage="release_worker_execution",
        decision_summary="release now",
        default_supervisor_doc_path="docs/agents/runtime/nonstop-supervisor-loop.md",
        default_release_result_template_path="docs/agents/release-result-template.yaml",
    )

    assert "This issue is GitHub-backed." in prompt
    assert "Apply the normal GitHub issue close/update workflow" in prompt
