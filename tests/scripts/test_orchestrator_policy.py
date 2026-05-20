from __future__ import annotations

from typing import cast

from scripts import orchestrator_policy


def test_runtime_target_state_maps_known_role_stage_pairs() -> None:
    assert orchestrator_policy.runtime_target_state({"role": "main_orchestrator", "stage": "orchestrator_bootstrap"}) == "running"
    assert orchestrator_policy.runtime_target_state({"role": "issue_worker", "stage": "issue_worker_execution"}) == "running"
    assert orchestrator_policy.runtime_target_state({"role": "pr_verifier", "stage": "pr_verifier_execution"}) == "verifying"
    assert orchestrator_policy.runtime_target_state({"role": "main_orchestrator", "stage": "release_root_execution"}) == "release_pending"
    assert orchestrator_policy.runtime_target_state({"role": "main_orchestrator", "stage": "issue_selection_or_recovery"}) == ""


def test_reconcile_route_maps_role_stage_to_expected_route_key() -> None:
    assert orchestrator_policy.reconcile_route({"role": "main_orchestrator", "stage": "orchestrator_bootstrap"}) == "orchestrator_bootstrap"
    assert orchestrator_policy.reconcile_route({"role": "issue_worker", "stage": "issue_worker_execution"}) == "issue_worker"
    assert orchestrator_policy.reconcile_route({"role": "pr_verifier", "stage": "pr_verifier_execution"}) == "pr_verifier"
    assert orchestrator_policy.reconcile_route({"role": "main_orchestrator", "stage": "release_root_execution"}) == "release_root_execution"
    assert orchestrator_policy.reconcile_route({"role": "main_orchestrator", "stage": "issue_selection_or_recovery"}) == "issue_selection_or_recovery"
    assert orchestrator_policy.reconcile_route({"role": "unknown", "stage": "unknown"}) == "no_change"


def test_dispatch_reconcile_route_uses_matching_handler_and_falls_back_to_no_change() -> None:
    def issue_worker_handler() -> tuple[object, object, object | None]:
        return ({"route": "issue_worker"}, {"action": "continue"}, None)

    def no_change_handler() -> tuple[object, object, object | None]:
        return ({"route": "no_change"}, {"action": "no_change"}, None)

    handlers = {
        "issue_worker": issue_worker_handler,
        "no_change": no_change_handler,
    }

    next_ledger, decision, request = orchestrator_policy.dispatch_reconcile_route(
        current={"role": "issue_worker", "stage": "issue_worker_execution"},
        handlers=handlers,
    )
    next_ledger_map = cast(dict[str, object], next_ledger)
    decision_map = cast(dict[str, object], decision)
    assert next_ledger_map["route"] == "issue_worker"
    assert decision_map["action"] == "continue"
    assert request is None

    fallback_ledger, fallback_decision, fallback_request = orchestrator_policy.dispatch_reconcile_route(
        current={"role": "main_orchestrator", "stage": "orchestrator_bootstrap"},
        handlers={"no_change": no_change_handler},
    )
    fallback_ledger_map = cast(dict[str, object], fallback_ledger)
    fallback_decision_map = cast(dict[str, object], fallback_decision)
    assert fallback_ledger_map["route"] == "no_change"
    assert fallback_decision_map["action"] == "no_change"
    assert fallback_request is None


def test_select_release_issue_number_prefers_requested_then_verified_then_idle_release_pending() -> None:
    assert (
        orchestrator_policy.select_release_issue_number(
            requested_issue_number="42",
            verified_issue_numbers=["10"],
            idle_release_pending_issue_numbers=["11"],
        )
        == "42"
    )
    assert (
        orchestrator_policy.select_release_issue_number(
            requested_issue_number=None,
            verified_issue_numbers=["10"],
            idle_release_pending_issue_numbers=["11"],
        )
        == "10"
    )
    assert (
        orchestrator_policy.select_release_issue_number(
            requested_issue_number=None,
            verified_issue_numbers=[],
            idle_release_pending_issue_numbers=["11"],
        )
        == "11"
    )


def test_select_release_issue_number_raises_when_no_candidate() -> None:
    try:
        orchestrator_policy.select_release_issue_number(
            requested_issue_number=None,
            verified_issue_numbers=[],
            idle_release_pending_issue_numbers=[],
        )
    except RuntimeError as error:
        assert "no verified issue is waiting for independent release" in str(error)
    else:
        raise AssertionError("expected no-candidate release selection to fail")


def test_release_admission_decision_classifies_verified_idle_pending_and_rejects() -> None:
    assert (
        orchestrator_policy.release_admission_decision(
            state="verified",
            current_session_id="",
            current_status="",
        )
        == "transition_to_release_pending"
    )
    assert (
        orchestrator_policy.release_admission_decision(
            state="release_pending",
            current_session_id="",
            current_status="",
        )
        == "allow_idle_release_pending"
    )
    assert (
        orchestrator_policy.release_admission_decision(
            state="release_pending",
            current_session_id="ses-1",
            current_status="running",
        )
        == "reject_active_fence"
    )
    assert (
        orchestrator_policy.release_admission_decision(
            state="running",
            current_session_id="",
            current_status="",
        )
        == "reject_invalid_state"
    )


def test_validate_selected_issue_alignment_requires_match_with_queued_next_issue() -> None:
    assert (
        orchestrator_policy.validate_selected_issue_alignment(
            queued_issue_number="32",
            queued_issue_branch="agent/issue-32-demo",
            selected_issue_number="32",
            selected_issue_branch="agent/issue-32-demo",
        )
        == ""
    )
    assert (
        orchestrator_policy.validate_selected_issue_alignment(
            queued_issue_number="",
            queued_issue_branch="",
            selected_issue_number="32",
            selected_issue_branch="",
        )
        == "stale selected issue request no longer matches queued next issue state"
    )
    mismatch = orchestrator_policy.validate_selected_issue_alignment(
        queued_issue_number="31",
        queued_issue_branch="agent/issue-31-demo",
        selected_issue_number="32",
        selected_issue_branch="",
    )
    assert "stale selected issue #32" in mismatch


def test_recovery_and_dispatch_classifiers() -> None:
    assert orchestrator_policy.is_selected_issue_recovery_request(
        role="main_orchestrator",
        stage="issue_selection_or_recovery",
        selected_issue_number="32",
    )
    assert not orchestrator_policy.is_selected_issue_recovery_request(
        role="main_orchestrator",
        stage="issue_selection_or_recovery",
        selected_issue_number="",
    )
    assert orchestrator_policy.is_bootstrap_dispatch(
        role="main_orchestrator",
        stage="orchestrator_bootstrap",
    )
    assert not orchestrator_policy.is_bootstrap_dispatch(role="issue_worker", stage="issue_worker_execution")
    assert orchestrator_policy.is_release_root_execution(
        role="main_orchestrator",
        stage="release_root_execution",
    )
    assert not orchestrator_policy.is_release_root_execution(role="main_orchestrator", stage="orchestrator_bootstrap")


def test_validate_request_issue_branch_and_revision() -> None:
    assert (
        orchestrator_policy.validate_request_issue_branch(
            request_issue_number="42",
            request_branch="agent/issue-42-demo",
            ledger_issue_number="42",
            ledger_branch="agent/issue-42-demo",
        )
        == ""
    )
    issue_mismatch = orchestrator_policy.validate_request_issue_branch(
        request_issue_number="42",
        request_branch="agent/issue-42-demo",
        ledger_issue_number="41",
        ledger_branch="agent/issue-41-demo",
    )
    assert "stale request issue #42" in issue_mismatch
    branch_mismatch = orchestrator_policy.validate_request_issue_branch(
        request_issue_number="42",
        request_branch="agent/issue-42-demo",
        ledger_issue_number="42",
        ledger_branch="agent/issue-42-alt",
    )
    assert "stale request branch" in branch_mismatch

    assert orchestrator_policy.validate_request_revision(
        request_revision="2026-05-07T17:00:00+08:00",
        ledger_revision="2026-05-07T17:00:00+08:00",
    ) == ""
    assert "stale request revision" in orchestrator_policy.validate_request_revision(
        request_revision="2026-05-07T17:00:00+08:00",
        ledger_revision="2026-05-07T17:10:00+08:00",
    )


def test_validate_completed_issue_dispatch_and_restore_strategy() -> None:
    assert (
        orchestrator_policy.validate_completed_issue_dispatch(
            issue_number="42",
            completed_issue_numbers={"43"},
            is_recovery_request=False,
        )
        == ""
    )
    assert "already completed or released" in orchestrator_policy.validate_completed_issue_dispatch(
        issue_number="42",
        completed_issue_numbers={"42"},
        is_recovery_request=False,
    )
    assert (
        orchestrator_policy.validate_completed_issue_dispatch(
            issue_number="42",
            completed_issue_numbers={"42"},
            is_recovery_request=True,
        )
        == ""
    )

    assert orchestrator_policy.dispatch_restore_strategy(
        failure_restore_state="ready",
        current_state="running",
    ) == "ready"
    assert orchestrator_policy.dispatch_restore_strategy(
        failure_restore_state="release_pending",
        current_state="running",
    ) == "release_pending"
    assert orchestrator_policy.dispatch_restore_strategy(
        failure_restore_state="quarantined",
        current_state="running",
    ) == "quarantined"
    assert orchestrator_policy.dispatch_restore_strategy(
        failure_restore_state="ready",
        current_state="completed",
    ) == "skip"
