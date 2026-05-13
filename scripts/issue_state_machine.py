#!/usr/bin/env python3
"""Canonical control-plane issue lifecycle transitions."""

from __future__ import annotations

from dataclasses import dataclass


ISSUE_STATES = (
    "ready",
    "claimed",
    "dispatching",
    "running",
    "verifying",
    "completed",
    "failed",
    "quarantined",
)

_ALLOWED_TRANSITIONS = {
    "ready": {"claimed"},
    "claimed": {"dispatching", "ready"},
    "dispatching": {"running", "ready"},
    "running": {"verifying", "quarantined"},
    "verifying": {"running", "completed", "failed"},
    "quarantined": {"claimed", "running", "failed"},
    "completed": set(),
    "failed": set(),
}


@dataclass(frozen=True)
class TransitionResult:
    ok: bool
    error: str = ""


def is_known_issue_state(state: str) -> bool:
    return state in ISSUE_STATES


def transition(from_state: str, to_state: str) -> TransitionResult:
    if not is_known_issue_state(from_state):
        return TransitionResult(False, f"unknown issue state {from_state!r}")
    if not is_known_issue_state(to_state):
        return TransitionResult(False, f"unknown issue state {to_state!r}")
    if from_state == to_state:
        return TransitionResult(True)
    if to_state not in _ALLOWED_TRANSITIONS[from_state]:
        return TransitionResult(False, f"invalid issue transition {from_state!r} -> {to_state!r}")
    return TransitionResult(True)


def require_transition(from_state: str, to_state: str) -> None:
    result = transition(from_state, to_state)
    if not result.ok:
        raise ValueError(result.error)
