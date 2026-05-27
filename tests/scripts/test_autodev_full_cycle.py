from __future__ import annotations

from pathlib import Path

import scripts.autodev_full_cycle as full_cycle


def test_sleep_with_heartbeat_ticks_in_steps(monkeypatch) -> None:
    runner = full_cycle.FullCycleRunner()
    runner.heartbeat_seconds = 10
    sleep_calls: list[int] = []

    monkeypatch.setattr(full_cycle.time, "sleep", lambda seconds: sleep_calls.append(int(seconds)))

    runner.sleep_with_heartbeat(total_seconds=25, open_count=3)

    assert sleep_calls == [10, 10, 5]


def test_run_returns_130_on_keyboard_interrupt_during_sleep(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("REPO", "owner/repo")

    runner = full_cycle.FullCycleRunner()
    runner.interval_seconds = 30

    monkeypatch.setattr(runner, "require_tools", lambda: None)
    monkeypatch.setattr(runner, "print_autodev_yaml_settings", lambda: None)
    monkeypatch.setattr(runner, "print_startup_github_issue_list", lambda: None)
    monkeypatch.setattr(runner, "autodev_bootstrap_once", lambda: None)
    monkeypatch.setattr(runner, "autodev_intake", lambda: None)
    monkeypatch.setattr(runner, "autodev_start_one", lambda: None)
    monkeypatch.setattr(runner, "autodev_recovery", lambda: None)
    monkeypatch.setattr(runner, "autodev_reconcile", lambda: None)
    monkeypatch.setattr(runner, "autodev_release_verified", lambda: None)
    monkeypatch.setattr(runner, "print_db_snapshot", lambda: None)
    monkeypatch.setattr(runner, "print_github_snapshot", lambda: None)
    monkeypatch.setattr(runner, "open_issue_count", lambda: 1)

    def interrupting_sleep(_total_seconds: int, _open_count: int) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(runner, "sleep_with_heartbeat", interrupting_sleep)

    exit_code = runner.run()

    captured = capsys.readouterr()
    assert exit_code == 130
    assert "KeyboardInterrupt received (Ctrl+C); stopping full-cycle loop gracefully." in captured.out
