from scripts.issue_state_machine import transition


def test_issue_state_machine_accepts_verified_release_path() -> None:
    assert transition("verifying", "verified").ok is True
    assert transition("verified", "release_pending").ok is True
    assert transition("release_pending", "verified").ok is True
    assert transition("release_pending", "quarantined").ok is True
    assert transition("release_pending", "completed").ok is True


def test_issue_state_machine_preserves_legacy_repair_path_during_rewrite() -> None:
    result = transition("verifying", "running")

    assert result.ok is True


def test_issue_state_machine_preserves_legacy_release_and_repair_paths_for_ongoing_rewrite() -> None:
    assert transition("verifying", "completed").ok is True
    assert transition("verifying", "running").ok is True
