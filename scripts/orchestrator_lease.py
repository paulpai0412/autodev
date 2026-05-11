#!/usr/bin/env python3
"""Scheduler lease helpers for the control plane."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from uuid import uuid4

import sqlite3

from scripts.control_plane_db import control_plane_db_path, ensure_control_plane_db


DEFAULT_LEASE_TTL_SECONDS = 60


@dataclass(frozen=True)
class SchedulerLease:
    scheduler_id: str
    lease_token: str
    heartbeat_at: str
    expires_at: str
    created_at: str


def _parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _lease_expiry(now: str, ttl_seconds: int) -> str:
    return (_parse_timestamp(now) + timedelta(seconds=ttl_seconds)).isoformat(timespec="seconds")


def acquire_scheduler_lease(
    base_dir: Path,
    *,
    scheduler_id: str,
    heartbeat_at: str,
    ttl_seconds: int = DEFAULT_LEASE_TTL_SECONDS,
) -> SchedulerLease | None:
    ensure_control_plane_db(base_dir)
    connection = sqlite3.connect(control_plane_db_path(base_dir))
    connection.row_factory = sqlite3.Row
    try:
        active = connection.execute(
            "SELECT * FROM scheduler_leases WHERE state = 'active' ORDER BY heartbeat_at DESC LIMIT 1"
        ).fetchone()
        if active is not None:
            active_scheduler_id = str(active["scheduler_id"])
            active_expires_at = str(active["expires_at"])
            if active_scheduler_id != scheduler_id and _parse_timestamp(active_expires_at) > _parse_timestamp(heartbeat_at):
                return None
            if active_scheduler_id != scheduler_id and _parse_timestamp(active_expires_at) <= _parse_timestamp(heartbeat_at):
                connection.execute(
                    "UPDATE scheduler_leases SET state = 'replaced', replaced_by_scheduler_id = ? WHERE scheduler_id = ?",
                    (scheduler_id, active_scheduler_id),
                )

        current = connection.execute(
            "SELECT * FROM scheduler_leases WHERE scheduler_id = ?",
            (scheduler_id,),
        ).fetchone()
        lease_token = uuid4().hex
        expires_at = _lease_expiry(heartbeat_at, ttl_seconds)
        if current is None:
            connection.execute(
                """
                INSERT INTO scheduler_leases (
                    scheduler_id,
                    lease_token,
                    heartbeat_at,
                    expires_at,
                    created_at,
                    replaced_by_scheduler_id,
                    state
                ) VALUES (?, ?, ?, ?, ?, '', 'active')
                """,
                (scheduler_id, lease_token, heartbeat_at, expires_at, heartbeat_at),
            )
            created_at = heartbeat_at
        else:
            created_at = str(current["created_at"])
            connection.execute(
                """
                UPDATE scheduler_leases
                SET lease_token = ?, heartbeat_at = ?, expires_at = ?, replaced_by_scheduler_id = '', state = 'active'
                WHERE scheduler_id = ?
                """,
                (lease_token, heartbeat_at, expires_at, scheduler_id),
            )
        connection.commit()
    finally:
        connection.close()

    return SchedulerLease(
        scheduler_id=scheduler_id,
        lease_token=lease_token,
        heartbeat_at=heartbeat_at,
        expires_at=expires_at,
        created_at=created_at,
    )


def heartbeat_scheduler_lease(
    base_dir: Path,
    *,
    scheduler_id: str,
    lease_token: str,
    heartbeat_at: str,
    ttl_seconds: int = DEFAULT_LEASE_TTL_SECONDS,
) -> SchedulerLease | None:
    ensure_control_plane_db(base_dir)
    expires_at = _lease_expiry(heartbeat_at, ttl_seconds)
    connection = sqlite3.connect(control_plane_db_path(base_dir))
    connection.row_factory = sqlite3.Row
    try:
        row = connection.execute(
            "SELECT * FROM scheduler_leases WHERE scheduler_id = ? AND lease_token = ? AND state = 'active'",
            (scheduler_id, lease_token),
        ).fetchone()
        if row is None:
            return None
        connection.execute(
            "UPDATE scheduler_leases SET heartbeat_at = ?, expires_at = ? WHERE scheduler_id = ?",
            (heartbeat_at, expires_at, scheduler_id),
        )
        connection.commit()
        created_at = str(row["created_at"])
    finally:
        connection.close()

    return SchedulerLease(
        scheduler_id=scheduler_id,
        lease_token=lease_token,
        heartbeat_at=heartbeat_at,
        expires_at=expires_at,
        created_at=created_at,
    )


def release_scheduler_lease(base_dir: Path, *, scheduler_id: str, lease_token: str, released_at: str) -> None:
    ensure_control_plane_db(base_dir)
    connection = sqlite3.connect(control_plane_db_path(base_dir))
    try:
        connection.execute(
            """
            UPDATE scheduler_leases
            SET state = 'expired', heartbeat_at = ?, expires_at = ?
            WHERE scheduler_id = ? AND lease_token = ?
            """,
            (released_at, released_at, scheduler_id, lease_token),
        )
        connection.commit()
    finally:
        connection.close()
