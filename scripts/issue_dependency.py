"""Shared dependency parsing helpers for intake and selection."""

from __future__ import annotations

import re


def parse_issue_numbers(text: str) -> list[str]:
    return [match.group(1) for match in re.finditer(r"(?i)issue\s*#(\d+)", text)]


def dependency_issue_numbers(issue_number: str, dependencies: list[str]) -> list[str]:
    numbers: list[str] = []
    for dependency in dependencies:
        lowered = dependency.lower()
        blocked_match = re.search(r"blocked by(?:\s+issue)?\s*#(\d+)", lowered)
        if blocked_match:
            blocked_by = blocked_match.group(1)
            if blocked_by != issue_number and blocked_by not in numbers:
                numbers.append(blocked_by)
            continue
        if not any(token in lowered for token in ["released", "closed", "complete", "depends on", "requires"]):
            continue
        for found in parse_issue_numbers(dependency):
            if found != issue_number and found not in numbers:
                numbers.append(found)
        for found in re.findall(r"(?<!\w)#(\d+)", dependency):
            if found != issue_number and found not in numbers:
                numbers.append(found)
    return numbers


def infer_dependency_lines(body: str) -> list[str]:
    dependencies: list[str] = []
    for line in body.splitlines():
        stripped = line.strip()
        if re.search(r"(?i)depends on|blocked by|requires issue", stripped):
            if not re.match(r"^#{1,6}\s", stripped):
                dependencies.append(stripped)
    return dependencies or ["none"]
