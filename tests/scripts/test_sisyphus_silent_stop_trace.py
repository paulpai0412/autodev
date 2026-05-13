from __future__ import annotations

from scripts.sisyphus_silent_stop_trace import DEFAULT_CHILD_PROMPT, diagnose_trace


def test_diagnose_trace_reports_startup_failure_as_retryable_silent_stop() -> None:
    diagnosis = diagnose_trace(
        {
            "child_outcome": "startup_failed_before_messages",
            "child_summary": {"session_id": "ses-worker-42"},
            "root_task_launch": {"task_id": "ses-task-42", "background_task_id": "bg-task-42"},
        }
    )

    assert "never emitted assistant messages or tool parts" in diagnosis
    assert "retry" in diagnosis


def test_default_child_prompt_requires_real_repo_work() -> None:
    assert "Read package.json" in DEFAULT_CHILD_PROMPT
    assert "Read src/App.tsx" in DEFAULT_CHILD_PROMPT
    assert "Do not stop after a single-word reply" in DEFAULT_CHILD_PROMPT
    assert "Summary" in DEFAULT_CHILD_PROMPT
    assert "Verification" in DEFAULT_CHILD_PROMPT
