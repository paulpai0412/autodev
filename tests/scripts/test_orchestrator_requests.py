from __future__ import annotations

from typing import cast

from scripts import orchestrator_requests


def test_build_prompt_for_main_orchestrator_requires_background_child_subagents() -> None:
    ledger = cast(dict[str, object], {
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

    assert 'task(subagent_type="general", ..., run_in_background=true)' in prompt
    assert "collect its result with background_output(...) before continuing" in prompt
    assert "Do not include karpathy-guidelines in load_skills for child subagents" not in prompt


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
