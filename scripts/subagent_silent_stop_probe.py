#!/usr/bin/env python3
"""Inspect and optionally replay stalled OpenCode subagent sessions."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from pathlib import Path
from typing import cast

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.opencode_session_trace import (
    find_latest_child_session_summary,
    read_session_summary,
    session_summary_abort_reason,
)
from scripts.opencode_host_adapter import (
    extract_session_id_from_run_output,
    find_session_id_in_db,
    read_initial_session_id,
    spawn_detached_opencode_run,
    stream_supports_fileno,
    wait_for_session_id_in_db,
)


def resolve_opencode_cli() -> str | None:
    cli = shutil.which("opencode")
    if cli:
        return cli
    for candidate in [Path.home() / ".opencode/bin/opencode", Path.home() / ".local/bin/opencode", Path.home() / "bin/opencode"]:
        if candidate.exists():
            return str(candidate)
    return shutil.which("opencode-desktop")


def launch_replay_from_session(
    source_session_id: str,
    *,
    workdir: Path,
    title: str | None,
    agent: str | None,
    model: str | None,
    variant: str | None,
    timeout_seconds: float,
) -> dict[str, object]:
    source = read_session_summary(source_session_id)
    if source is None:
        raise SystemExit(f"session not found: {source_session_id}")
    prompt = str(source.get("first_user_text") or "")
    if not prompt:
        raise SystemExit(f"session {source_session_id} has no user prompt text")
    cli = resolve_opencode_cli()
    if not cli:
        raise SystemExit("OpenCode CLI not found")
    replay_title = title or f"subagent-replay-{source_session_id[-8:]}-{int(time.time())}"
    command = [cli, "run", "--format", "json", "--title", replay_title]
    effective_agent = agent or str(source.get("agent") or "")
    if effective_agent:
        command.extend(["--agent", effective_agent])
    model_payload = cast(dict[str, object], source.get("model") or {}) if isinstance(source.get("model"), dict) else {}
    provider_id = str(model_payload.get("providerID") or "")
    model_id = str(model_payload.get("id") or "")
    if model:
        command.extend(["--model", model])
    elif provider_id and model_id:
        command.extend(["--model", f"{provider_id}/{model_id}"])
    effective_variant = variant or str(model_payload.get("variant") or "")
    if effective_variant:
        command.extend(["--variant", effective_variant])
    command.append(prompt)

    started_at_ms = int(time.time() * 1000)
    process = spawn_detached_opencode_run(command, workdir=workdir)
    stdout_session_id, stdout_text, stderr_text = read_initial_session_id(
        process,
        timeout_seconds=timeout_seconds,
        extract_session_id=extract_session_id_from_run_output,
        supports_fileno=stream_supports_fileno,
    )
    session_id = stdout_session_id or wait_for_session_id_in_db(
        title=replay_title,
        workdir=workdir,
        created_after_ms=started_at_ms,
        timeout_seconds=2.0,
        find_session_id=find_session_id_in_db,
    )
    time.sleep(1.0)
    replay_summary = read_session_summary(session_id) if session_id else None
    return {
        "source_session_id": source_session_id,
        "source_abort_reason": session_summary_abort_reason(source),
        "replay_title": replay_title,
        "command": command,
        "stdout_session_id": stdout_session_id,
        "stderr_text": stderr_text,
        "stdout_text": stdout_text,
        "replay_session_id": session_id,
        "replay_summary": replay_summary,
        "replay_abort_reason": session_summary_abort_reason(replay_summary),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session-id", action="append", default=[], help="Inspect an existing session id")
    parser.add_argument("--parent-session-id", help="Inspect the latest child session under this parent")
    parser.add_argument("--title-contains", default="worker", help="Child-session title filter")
    parser.add_argument("--agent", help="Child-session agent filter or replay agent override")
    parser.add_argument("--directory", default="", help="Optional directory filter for child lookup")
    parser.add_argument("--replay-session-id", help="Replay the original user prompt from an existing session")
    parser.add_argument("--workdir", default=".", help="Working directory for replay")
    parser.add_argument("--title", help="Explicit title for replay run")
    parser.add_argument("--model", help="Override model in provider/model form")
    parser.add_argument("--variant", help="Override model variant")
    parser.add_argument("--timeout", type=float, default=10.0, help="Replay stdout session-id timeout seconds")
    args = parser.parse_args()

    output: dict[str, object] = {"inspected_sessions": cast(list[object], []), "latest_child": None, "replay": None}
    for session_id in args.session_id:
        cast(list[object], output["inspected_sessions"]).append(read_session_summary(session_id))
    if args.parent_session_id:
        output["latest_child"] = find_latest_child_session_summary(
            args.parent_session_id,
            title_contains=args.title_contains,
            agent=args.agent or "",
            directory=args.directory,
        )
    if args.replay_session_id:
        output["replay"] = launch_replay_from_session(
            args.replay_session_id,
            workdir=Path(args.workdir).resolve(),
            title=args.title,
            agent=args.agent,
            model=args.model,
            variant=args.variant,
            timeout_seconds=args.timeout,
        )
    print(json.dumps(output, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
