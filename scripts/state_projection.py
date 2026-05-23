"""Centralized, config-driven state projection for team workflow and GitHub labels."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml


CANONICAL_SQLITE_STATES = [
    "ready",
    "claimed",
    "dispatching",
    "running",
    "verifying",
    "verified",
    "release_pending",
    "failed",
    "quarantined",
    "completed",
]

PR_WORKFLOW_STATES = [
    "not_opened",
    "opened",
    "verifier_passed",
    "verifier_fail",
    "verifier_blocked",
    "release_failed",
    "release_blocked",
    "merged",
]

DEFAULT_PR_WORKFLOW_TO_SQLITE_STATE: dict[str, str] = {
    "merged": "completed",
    "release_blocked": "quarantined",
    "release_failed": "failed",
    "verifier_fail": "failed",
    "verifier_blocked": "quarantined",
}

DEFAULT_PR_WORKFLOW_TO_LABEL: dict[str, str] = {
    "not_opened": "pr-not-opened",
    "opened": "pr-opened",
    "verifier_passed": "pr-verifier-passed",
    "verifier_fail": "pr-verifier-failed",
    "verifier_blocked": "pr-verifier-blocked",
    "release_failed": "pr-release-failed",
    "release_blocked": "pr-release-blocked",
    "merged": "pr-merged",
}

DEFAULT_SQLITE_TO_TEAM_WORKFLOW: dict[str, str] = {
    "ready": "ready",
    "claimed": "in progress",
    "dispatching": "in progress",
    "running": "in progress",
    "verifying": "in progress",
    "verified": "in progress",
    "release_pending": "in review",
    "failed": "in review",
    "quarantined": "in review",
    "completed": "done",
}

DEFAULT_SQLITE_TO_PRIMARY_LABEL: dict[str, str] = {
    "ready": "ready-for-agent",
    "claimed": "agent-dispatching",
    "dispatching": "agent-dispatching",
    "running": "agent-in-progress",
    "verifying": "agent-in-progress",
    "verified": "agent-in-progress",
    "release_pending": "agent-in-progress",
    "failed": "agent-in-review",
    "quarantined": "agent-in-review",
    "completed": "agent-completed",
}

DEFAULT_SQLITE_TO_PR_WORKFLOW: dict[str, str] = {
    "ready": "not_opened",
    "claimed": "not_opened",
    "dispatching": "not_opened",
    "running": "not_opened",
    "verifying": "opened",
    "verified": "verifier_passed",
    "release_pending": "verifier_passed",
    "failed": "verifier_fail",
    "quarantined": "verifier_blocked",
    "completed": "merged",
}

TEAM_WORKFLOW_STATES = ["ready", "in progress", "in review", "done"]


def _coerce_dict_of_strings(raw: object) -> dict[str, str]:
    if not isinstance(raw, dict):
        return {}
    result: dict[str, str] = {}
    for key, value in raw.items():
        normalized_key = str(key or "").strip()
        normalized_value = str(value or "").strip()
        if normalized_key and normalized_value:
            result[normalized_key] = normalized_value
    return result


@dataclass(frozen=True)
class StateProjectionConfig:
    pr_workflow_to_sqlite_state: dict[str, str]
    sqlite_to_team_workflow: dict[str, str]
    sqlite_to_primary_label: dict[str, str]
    sqlite_to_pr_workflow: dict[str, str]
    pr_workflow_to_label: dict[str, str]

    @property
    def primary_label_order(self) -> list[str]:
        labels: list[str] = []
        for state in CANONICAL_SQLITE_STATES:
            label = str(self.sqlite_to_primary_label.get(state) or "")
            if label and label not in labels:
                labels.append(label)
        return labels

    @property
    def pr_label_order(self) -> list[str]:
        labels: list[str] = []
        for state in PR_WORKFLOW_STATES:
            label = str(self.pr_workflow_to_label.get(state) or "")
            if label and label not in labels:
                labels.append(label)
        return labels

    @property
    def managed_issue_labels(self) -> list[str]:
        return [*self.primary_label_order, *self.pr_label_order]


DEFAULT_STATE_PROJECTION_CONFIG = StateProjectionConfig(
    pr_workflow_to_sqlite_state=dict(DEFAULT_PR_WORKFLOW_TO_SQLITE_STATE),
    sqlite_to_team_workflow=dict(DEFAULT_SQLITE_TO_TEAM_WORKFLOW),
    sqlite_to_primary_label=dict(DEFAULT_SQLITE_TO_PRIMARY_LABEL),
    sqlite_to_pr_workflow=dict(DEFAULT_SQLITE_TO_PR_WORKFLOW),
    pr_workflow_to_label=dict(DEFAULT_PR_WORKFLOW_TO_LABEL),
)

PR_WORKFLOW_LABELS = dict(DEFAULT_STATE_PROJECTION_CONFIG.pr_workflow_to_label)


def default_state_projection_config_lines() -> list[str]:
    lines = [
        "state_projection:",
        "  pr_workflow_to_sqlite_state:",
    ]
    for state in ["merged", "release_blocked", "release_failed", "verifier_fail", "verifier_blocked"]:
        lines.append(f"    {state}: {DEFAULT_PR_WORKFLOW_TO_SQLITE_STATE[state]}")

    lines.extend(
        [
            "  sqlite_to_team_workflow:",
        ]
    )
    for state in CANONICAL_SQLITE_STATES:
        lines.append(f"    {state}: {DEFAULT_SQLITE_TO_TEAM_WORKFLOW[state]}")

    lines.extend(
        [
            "  sqlite_to_primary_label:",
        ]
    )
    for state in CANONICAL_SQLITE_STATES:
        lines.append(f"    {state}: {DEFAULT_SQLITE_TO_PRIMARY_LABEL[state]}")

    lines.extend(
        [
            "  sqlite_to_pr_workflow:",
        ]
    )
    for state in CANONICAL_SQLITE_STATES:
        lines.append(f"    {state}: {DEFAULT_SQLITE_TO_PR_WORKFLOW[state]}")

    lines.extend(
        [
            "  pr_workflow_to_label:",
        ]
    )
    for state in PR_WORKFLOW_STATES:
        lines.append(f"    {state}: {DEFAULT_PR_WORKFLOW_TO_LABEL[state]}")

    return lines


def load_state_projection_config(base_dir: Path) -> StateProjectionConfig:
    config_path = base_dir / ".autodev.yaml"
    if not config_path.exists():
        return DEFAULT_STATE_PROJECTION_CONFIG
    try:
        parsed = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError):
        return DEFAULT_STATE_PROJECTION_CONFIG
    payload = parsed if isinstance(parsed, dict) else {}
    state_projection = payload.get("state_projection")
    raw = state_projection if isinstance(state_projection, dict) else {}

    pr_workflow_to_sqlite_state = dict(DEFAULT_STATE_PROJECTION_CONFIG.pr_workflow_to_sqlite_state)
    pr_workflow_to_sqlite_state.update(_coerce_dict_of_strings(raw.get("pr_workflow_to_sqlite_state")))

    sqlite_to_team_workflow = dict(DEFAULT_STATE_PROJECTION_CONFIG.sqlite_to_team_workflow)
    sqlite_to_team_workflow.update(_coerce_dict_of_strings(raw.get("sqlite_to_team_workflow")))

    sqlite_to_primary_label = dict(DEFAULT_STATE_PROJECTION_CONFIG.sqlite_to_primary_label)
    sqlite_to_primary_label.update(_coerce_dict_of_strings(raw.get("sqlite_to_primary_label")))

    sqlite_to_pr_workflow = dict(DEFAULT_STATE_PROJECTION_CONFIG.sqlite_to_pr_workflow)
    sqlite_to_pr_workflow.update(_coerce_dict_of_strings(raw.get("sqlite_to_pr_workflow")))

    pr_workflow_to_label = dict(DEFAULT_STATE_PROJECTION_CONFIG.pr_workflow_to_label)
    pr_workflow_to_label.update(_coerce_dict_of_strings(raw.get("pr_workflow_to_label")))

    return StateProjectionConfig(
        pr_workflow_to_sqlite_state=pr_workflow_to_sqlite_state,
        sqlite_to_team_workflow=sqlite_to_team_workflow,
        sqlite_to_primary_label=sqlite_to_primary_label,
        sqlite_to_pr_workflow=sqlite_to_pr_workflow,
        pr_workflow_to_label=pr_workflow_to_label,
    )


@dataclass(frozen=True)
class IssueProjection:
    team_workflow: str
    primary_label: str
    pr_workflow_state: str
    pr_workflow_label: str
    managed_labels_order: tuple[str, ...]

    @property
    def desired_labels(self) -> list[str]:
        labels = {self.primary_label, self.pr_workflow_label}
        return [label for label in self.managed_labels_order if label in labels]


def team_workflow_for_issue_state(issue_state: str, *, config: StateProjectionConfig) -> str:
    return str(config.sqlite_to_team_workflow.get(issue_state) or "in progress")


def primary_label_for_issue_state(issue_state: str, *, config: StateProjectionConfig) -> str:
    return str(config.sqlite_to_primary_label.get(issue_state) or config.sqlite_to_primary_label.get("running") or "agent-in-progress")


def pr_workflow_state_for_issue(
    *,
    issue_state: str,
    has_pr_opened: bool,
    evidence_status: str,
    release_status: str,
    release_merged: bool,
    config: StateProjectionConfig,
) -> str:
    normalized_evidence = evidence_status.strip().lower()
    normalized_release = release_status.strip().lower()
    base_state = str(config.sqlite_to_pr_workflow.get(issue_state) or "not_opened")

    if issue_state == "completed" or release_merged or normalized_release in {"success", "completed"}:
        return "merged"

    if issue_state == "release_pending":
        return base_state

    if issue_state == "failed":
        if normalized_release == "failed" and "release_failed" in config.pr_workflow_to_label:
            return "release_failed"
        if normalized_evidence == "fail" and "verifier_fail" in config.pr_workflow_to_label:
            return "verifier_fail"
        return base_state

    if issue_state == "quarantined":
        if normalized_release == "blocked" and "release_blocked" in config.pr_workflow_to_label:
            return "release_blocked"
        if normalized_evidence == "blocked" and "verifier_blocked" in config.pr_workflow_to_label:
            return "verifier_blocked"
        return base_state

    if normalized_evidence == "pass" and "verifier_passed" in config.pr_workflow_to_label:
        return "verifier_passed"
    if normalized_evidence == "fail" and "verifier_fail" in config.pr_workflow_to_label:
        return "verifier_fail"
    if normalized_evidence == "blocked" and "verifier_blocked" in config.pr_workflow_to_label:
        return "verifier_blocked"
    if has_pr_opened and "opened" in config.pr_workflow_to_label:
        return "opened"
    return base_state


def issue_projection(
    *,
    issue_state: str,
    has_pr_opened: bool,
    evidence_status: str,
    release_status: str,
    release_merged: bool,
    base_dir: Path | None = None,
    config: StateProjectionConfig | None = None,
) -> IssueProjection:
    resolved_config = config or (load_state_projection_config(base_dir) if base_dir else DEFAULT_STATE_PROJECTION_CONFIG)
    pr_state = pr_workflow_state_for_issue(
        issue_state=issue_state,
        has_pr_opened=has_pr_opened,
        evidence_status=evidence_status,
        release_status=release_status,
        release_merged=release_merged,
        config=resolved_config,
    )
    pr_label = str(resolved_config.pr_workflow_to_label.get(pr_state) or DEFAULT_PR_WORKFLOW_TO_LABEL["not_opened"])
    return IssueProjection(
        team_workflow=team_workflow_for_issue_state(issue_state, config=resolved_config),
        primary_label=primary_label_for_issue_state(issue_state, config=resolved_config),
        pr_workflow_state=pr_state,
        pr_workflow_label=pr_label,
        managed_labels_order=tuple(resolved_config.managed_issue_labels),
    )


def label_delta_for_projection(projection: IssueProjection) -> tuple[list[str], list[str]]:
    desired = set(projection.desired_labels)
    add_labels = [label for label in projection.managed_labels_order if label in desired]
    remove_labels = [label for label in projection.managed_labels_order if label not in desired]
    return add_labels, remove_labels
