from scripts.issue_state_machine import transition


def test_transition_allows_canonical_control_plane_paths():
    assert transition("ready", "claimed").ok
    assert transition("claimed", "dispatching").ok
    assert transition("dispatching", "running").ok
    assert transition("dispatching", "quarantined").ok
    assert transition("running", "verifying").ok
    assert transition("verifying", "quarantined").ok
    assert transition("quarantined", "claimed").ok
    assert transition("verifying", "running").ok
    assert transition("verifying", "completed").ok


def test_transition_rejects_invalid_paths():
    result = transition("ready", "completed")

    assert not result.ok
    assert "invalid issue transition" in result.error
