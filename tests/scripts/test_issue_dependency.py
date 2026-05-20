from __future__ import annotations

from scripts.issue_dependency import dependency_issue_numbers, infer_dependency_lines, parse_issue_numbers


def test_parse_issue_numbers_extracts_issue_refs() -> None:
    text = "Depends on issue #11 and issue #12"
    assert parse_issue_numbers(text) == ["11", "12"]


def test_dependency_issue_numbers_parses_blocked_by_hash() -> None:
    deps = ["blocked by #11"]
    assert dependency_issue_numbers("12", deps) == ["11"]


def test_dependency_issue_numbers_ignores_self_reference() -> None:
    deps = ["Blocked by issue #10"]
    assert dependency_issue_numbers("10", deps) == []


def test_infer_dependency_lines_strips_headers_and_keeps_dependency_lines() -> None:
    body = "## Blocked by\n- Blocked by issue #9\n"
    assert infer_dependency_lines(body) == ["- Blocked by issue #9"]


def test_infer_dependency_lines_returns_none_sentinel_when_empty() -> None:
    assert infer_dependency_lines("No dependency hints here") == ["none"]
