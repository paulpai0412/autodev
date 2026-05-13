from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import cast

from pytest import MonkeyPatch

import scripts.subagent_startup_repro as repro
from scripts.subagent_startup_repro import (
    build_repro_prompt,
    extract_task_launch_details,
    resolve_child_prompt,
    run_matrix,
    run_once,
    session_outcome,
    summarize_background_task_observation,
    summarize_child_observation,
)


def test_build_repro_prompt_encodes_task_call_contract() -> None:
    prompt = build_repro_prompt(
        child_subagent_type="general",
        category="deep",
        load_skills=["karpathy-guidelines", "diagnose"],
        description="Repro child session",
        child_prompt="Reply READY.",
    )

    assert 'subagent_type: "general"' in prompt
    assert 'category: "deep"' in prompt
    assert 'load_skills: ["karpathy-guidelines", "diagnose"]' in prompt
    assert 'description: "Repro child session"' in prompt
    assert 'prompt: "Reply READY."' in prompt
    assert "Use the task tool exactly once" in prompt
    assert "Do not call any other tools." in prompt


def test_build_repro_prompt_supports_parent_keepalive_sleep() -> None:
    prompt = build_repro_prompt(
        child_subagent_type="general",
        category="deep",
        load_skills=[],
        description="Repro child session",
        child_prompt="Reply READY.",
        parent_keepalive_seconds=8.0,
    )

    assert 'command "sleep 8"' in prompt
    assert "After the bash sleep command finishes, stop." in prompt
    assert "stop immediately" not in prompt


def test_build_repro_prompt_supports_foreground_task_mode() -> None:
    prompt = build_repro_prompt(
        child_subagent_type="general",
        category="deep",
        load_skills=[],
        description="Repro child session",
        child_prompt="Reply READY.",
        run_in_background=False,
    )

    assert "run_in_background: false" in prompt
    assert "Wait for the task tool call to finish in the foreground, then stop." in prompt
    assert 'command "sleep 8"' not in prompt


def test_extract_task_launch_details_reads_task_ids_from_tool_output() -> None:
    details = extract_task_launch_details(
        [
            {
                "type": "tool",
                "tool": "task",
                "state": {
                    "output": "Background task launched.\n\n<task_metadata>\nsession_id: ses_child\ntask_id: ses_child\nbackground_task_id: bg_child\n</task_metadata>"
                },
            }
        ]
    )

    assert details["task_id"] == "ses_child"
    assert details["background_task_id"] == "bg_child"
    assert "Background task launched." in details["task_output"]


def test_extract_task_launch_details_reads_human_readable_background_task_id() -> None:
    details = extract_task_launch_details(
        [
            {
                "type": "tool",
                "tool": "task",
                "state": {
                    "output": "Background task launched.\n\nBackground Task ID: bg_child\nDescription: Repro child session"
                },
            }
        ]
    )

    assert details["background_task_id"] == "bg_child"


def test_summarize_background_task_observation_reports_failure_and_stale_poller_hit() -> None:
    observation = summarize_background_task_observation(
        [
            {"type": "text", "text": "Background task bg_child launched successfully."},
            {"type": "text", "text": "stale poller cancelled background task bg_child after it Aborted"},
        ],
        "bg_child",
    )

    assert observation["status"] == "failed"
    assert observation["matched_part_count"] == 2
    assert observation["stale_poller_hit"] == "hit"


def test_summarize_background_task_observation_handles_missing_background_task_id() -> None:
    observation = summarize_background_task_observation([], "")

    assert observation["status"] == "missing_background_task_id"
    assert observation["stale_poller_hit"] == "unknown"


def test_summarize_child_observation_reports_last_update_and_duration() -> None:
    observation = summarize_child_observation(
        {
            "session_id": "ses-child",
            "latest_assistant_status": "MessageAbortedError",
            "time_updated": 1_778_588_990_571,
            "duration_ms": 85,
        }
    )

    assert observation["session_id"] == "ses-child"
    assert observation["latest_assistant_status"] == "MessageAbortedError"
    assert observation["last_update_ms"] == 1_778_588_990_571
    assert observation["last_update_iso"]
    assert observation["duration_ms"] == 85


def test_session_outcome_classifies_aborted_and_startup_failures() -> None:
    assert session_outcome(None) == "missing"
    assert session_outcome(
        {
            "latest_assistant_status": "MessageAbortedError",
            "latest_assistant_error": {"name": "MessageAbortedError", "data": {"message": "Aborted"}},
            "message_count": 2,
            "part_count": 1,
        }
    ) == "aborted"
    assert session_outcome(
        {
            "latest_assistant_status": "no_assistant_message",
            "message_count": 0,
            "part_count": 0,
        }
    ) == "startup_failed_before_messages"
    assert session_outcome(
        {
            "latest_assistant_status": "unknown",
            "message_count": 1,
            "part_count": 1,
        }
    ) == "started"


def test_resolve_child_prompt_prefers_file_contents(tmp_path: Path) -> None:
    prompt_path = tmp_path / "child-prompt.txt"
    _ = prompt_path.write_text("Prompt from file", encoding="utf-8")

    assert resolve_child_prompt(child_prompt="inline", child_prompt_file=str(prompt_path)) == "Prompt from file"
    assert resolve_child_prompt(child_prompt="inline", child_prompt_file="") == "inline"


class _FakeProcess:
    def poll(self) -> None:
        return None


def test_run_once_inserts_explicit_root_agent_into_opencode_command(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(repro, "resolve_opencode_cli", lambda: "/fake/opencode")

    def fake_spawn_detached_opencode_run(command: list[str], *, workdir: Path) -> _FakeProcess:
        captured["command"] = command
        captured["workdir"] = workdir
        return _FakeProcess()

    monkeypatch.setattr(repro, "spawn_detached_opencode_run", fake_spawn_detached_opencode_run)
    monkeypatch.setattr(repro, "read_initial_session_id", lambda *args, **kwargs: ("ses-root", "", ""))
    monkeypatch.setattr(
        repro,
        "read_session_summary",
        lambda session_id: {
            "session_id": session_id,
            "latest_assistant_status": "stop",
            "message_count": 1,
            "part_count": 1,
        },
    )
    monkeypatch.setattr(repro, "_load_session_parts", lambda session_id: [])
    monkeypatch.setattr(repro, "poll_child_session", lambda *args, **kwargs: None)
    monkeypatch.setattr(repro.time, "sleep", lambda _seconds: None)

    result = run_once(
        workdir=tmp_path,
        title="root-title",
        root_agent="hephaestus",
        child_subagent_type="general",
        category="deep",
        skills=["karpathy-guidelines"],
        child_description="Repro child session",
        child_title_contains="Repro child session",
        child_prompt="Do work.",
        child_prompt_file="",
        run_in_background=True,
        parent_keepalive_seconds=0.0,
        root_timeout=15.0,
        db_timeout=5.0,
        child_timeout=30.0,
        poll_interval=0.5,
        settle_timeout=5.0,
    )

    assert captured["workdir"] == tmp_path
    command = cast(list[str], captured["command"])
    assert command[:4] == ["/fake/opencode", "run", "--format", "json"]
    assert command[4:8] == ["--agent", "hephaestus", "--title", "root-title"]
    assert 'subagent_type: "general"' in str(command[-1])
    assert result["root_agent"] == "hephaestus"
    assert result["child_subagent_type"] == "general"


def test_run_once_prompt_differs_between_immediate_stop_and_thirty_second_keepalive(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    captured_commands: list[list[str]] = []

    monkeypatch.setattr(repro, "resolve_opencode_cli", lambda: "/fake/opencode")

    def fake_spawn_detached_opencode_run(command: list[str], *, workdir: Path) -> _FakeProcess:
        _ = workdir
        captured_commands.append(command)
        return _FakeProcess()

    monkeypatch.setattr(repro, "spawn_detached_opencode_run", fake_spawn_detached_opencode_run)
    monkeypatch.setattr(repro, "read_initial_session_id", lambda *args, **kwargs: ("ses-root", "", ""))
    monkeypatch.setattr(
        repro,
        "read_session_summary",
        lambda session_id: {
            "session_id": session_id,
            "latest_assistant_status": "stop",
            "message_count": 1,
            "part_count": 1,
        },
    )
    monkeypatch.setattr(repro, "_load_session_parts", lambda session_id: [])
    monkeypatch.setattr(repro, "poll_child_session", lambda *args, **kwargs: None)
    monkeypatch.setattr(repro.time, "sleep", lambda _seconds: None)

    immediate_result = run_once(
        workdir=tmp_path,
        title="root-immediate",
        root_agent="",
        child_subagent_type="general",
        category="deep",
        skills=[],
        child_description="Repro child session",
        child_title_contains="Repro child session",
        child_prompt="Do work.",
        child_prompt_file="",
        run_in_background=True,
        parent_keepalive_seconds=0.0,
        root_timeout=15.0,
        db_timeout=5.0,
        child_timeout=30.0,
        poll_interval=0.5,
        settle_timeout=5.0,
    )
    keepalive_result = run_once(
        workdir=tmp_path,
        title="root-keepalive",
        root_agent="",
        child_subagent_type="general",
        category="deep",
        skills=[],
        child_description="Repro child session",
        child_title_contains="Repro child session",
        child_prompt="Do work.",
        child_prompt_file="",
        run_in_background=True,
        parent_keepalive_seconds=30.0,
        root_timeout=15.0,
        db_timeout=5.0,
        child_timeout=30.0,
        poll_interval=0.5,
        settle_timeout=5.0,
    )

    immediate_prompt = captured_commands[0][-1]
    keepalive_prompt = captured_commands[1][-1]

    assert immediate_result["parent_keepalive_seconds"] == 0.0
    assert keepalive_result["parent_keepalive_seconds"] == 30.0
    assert 'After the task tool returns, stop immediately.' in immediate_prompt
    assert 'command "sleep 30"' not in immediate_prompt
    assert 'command "sleep 30"' in keepalive_prompt
    assert 'After the bash sleep command finishes, stop.' in keepalive_prompt


def test_run_matrix_prefers_case_level_root_and_child_agent_overrides(monkeypatch, tmp_path: Path) -> None:
    matrix_path = tmp_path / "matrix.json"
    matrix_path.write_text(
        json.dumps(
            {
                "titlePrefix": "subagent-matrix",
                "cases": [
                    {
                        "name": "override-case",
                        "rootAgent": "hephaestus",
                        "childSubagentType": "explore",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    captured: dict[str, object] = {}

    def fake_run_once(**kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return {
            "title": kwargs["title"],
            "root_agent": kwargs["root_agent"],
            "child_subagent_type": kwargs["child_subagent_type"],
        }

    monkeypatch.setattr(repro, "run_once", fake_run_once)

    args = argparse.Namespace(
        workdir=str(tmp_path),
        root_agent="build",
        child_subagent_type="general",
        category="deep",
        skill=[],
        child_prompt="Do work.",
        child_prompt_file="",
        run_in_background="true",
        parent_keepalive_seconds=0.0,
        root_timeout=15.0,
        db_timeout=5.0,
        child_timeout=30.0,
        poll_interval=0.5,
        settle_timeout=5.0,
    )

    assert run_matrix(matrix_path, args) == 0
    assert captured["title"] == "subagent-matrix-override-case"
    assert captured["root_agent"] == "hephaestus"
    assert captured["child_subagent_type"] == "explore"
