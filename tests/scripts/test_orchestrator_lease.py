from pathlib import Path

from scripts.control_plane_db import control_plane_db_path, ensure_control_plane_db
from scripts.orchestrator_lease import acquire_scheduler_lease, heartbeat_scheduler_lease, release_scheduler_lease


def test_acquire_scheduler_lease_blocks_second_live_scheduler(tmp_path: Path):
    ensure_control_plane_db(tmp_path)

    first = acquire_scheduler_lease(
        tmp_path,
        scheduler_id="scheduler-a",
        heartbeat_at="2026-05-11T10:00:00+08:00",
        ttl_seconds=60,
    )
    second = acquire_scheduler_lease(
        tmp_path,
        scheduler_id="scheduler-b",
        heartbeat_at="2026-05-11T10:00:30+08:00",
        ttl_seconds=60,
    )

    assert first is not None
    assert second is None


def test_acquire_scheduler_lease_replaces_expired_scheduler(tmp_path: Path):
    ensure_control_plane_db(tmp_path)
    first = acquire_scheduler_lease(
        tmp_path,
        scheduler_id="scheduler-a",
        heartbeat_at="2026-05-11T10:00:00+08:00",
        ttl_seconds=60,
    )

    second = acquire_scheduler_lease(
        tmp_path,
        scheduler_id="scheduler-b",
        heartbeat_at="2026-05-11T10:02:00+08:00",
        ttl_seconds=60,
    )

    assert first is not None
    assert second is not None
    assert second.scheduler_id == "scheduler-b"


def test_heartbeat_and_release_update_lease_state(tmp_path: Path):
    ensure_control_plane_db(tmp_path)
    lease = acquire_scheduler_lease(
        tmp_path,
        scheduler_id="scheduler-a",
        heartbeat_at="2026-05-11T10:00:00+08:00",
        ttl_seconds=60,
    )
    assert lease is not None

    heartbeat = heartbeat_scheduler_lease(
        tmp_path,
        scheduler_id="scheduler-a",
        lease_token=lease.lease_token,
        heartbeat_at="2026-05-11T10:00:30+08:00",
        ttl_seconds=60,
    )
    release_scheduler_lease(
        tmp_path,
        scheduler_id="scheduler-a",
        lease_token=lease.lease_token,
        released_at="2026-05-11T10:01:00+08:00",
    )

    assert heartbeat is not None
    connection = __import__("sqlite3").connect(control_plane_db_path(tmp_path))
    try:
        row = connection.execute(
            "SELECT state, heartbeat_at, expires_at FROM scheduler_leases WHERE scheduler_id = ?",
            ("scheduler-a",),
        ).fetchone()
    finally:
        connection.close()

    assert row is not None
    assert row[0] == "expired"
    assert row[1] == "2026-05-11T10:01:00+08:00"
    assert row[2] == "2026-05-11T10:01:00+08:00"
