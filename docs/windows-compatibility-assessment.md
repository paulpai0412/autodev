# Windows Compatibility Assessment (autodev)

Last updated: 2026-05-26

## Goal

Assess what must change so `autodev` can run reliably on native Windows (PowerShell/CMD),
not only Linux/macOS shells.

---

## Executive summary

Current codebase is **partially cross-platform at Python level**, but **not Windows-ready end-to-end**.

Main blockers are:

1. **Bash-only orchestration** (`autodev_full_cycle.sh`)
2. **Hardcoded executable names** (`python3`, `bash`) in runtime and generated command templates
3. **Unix-centric path conventions** (`~/.config/...`, `~/.local/...`, `/tmp` assumptions in scripts/tests)

`gh` and `git` are cross-platform tools, but current invocation strategy assumes Unix-style environment
and command availability patterns.

---

## Compatibility status by area

## 1) Runtime orchestration entrypoints

### Status
- **Not Windows-compatible** for full-cycle path.

### Evidence
- `autodev_full_cycle.sh` is Bash-only and uses:
  - shebang `#!/usr/bin/env bash`
  - `${BASH_SOURCE[0]}`
  - process substitution (`done < <(...)`)
  - `mktemp -u`, `mkfifo`

### Required change
- Replace `autodev_full_cycle.sh` with a Python runner (recommended), or add equivalent PowerShell implementation.

### Priority
- **P0**

---

## 2) Command dispatch and interpreter resolution

### Status
- **Partially compatible**; currently risky on Windows.

### Evidence
- `scripts/autodev_project.py` invokes `python3` directly in multiple flows (`start`, `reconcile`, `release`).
- `scripts/orchestrator_selection.py` builds intake command with literal `python3`.
- `scripts/autodev_host_packaging.py` templates embed `python3 ...` and `bash ...`.

### Required change
- Replace hardcoded `python3` with runtime-resolved interpreter (`sys.executable` or centralized resolver).
- In generated command templates, avoid hardcoded `bash`; provide platform-specific template variants.

### Priority
- **P0** for `bash`/`python3` hardcoding in operator entrypoints.

---

## 3) Host adapter and OpenCode CLI path discovery

### Status
- **Needs adaptation**.

### Evidence
- `scripts/opencode_host_adapter.py`, `session_readability_probe.py`,
  `subagent_silent_stop_probe.py` look up Linux/macOS-biased paths:
  - `~/.opencode/bin/opencode`
  - `~/.local/bin/opencode`
  - `~/bin/opencode`
- Commands dir capability defaults to `~/.config/opencode/commands`.

### Required change
- Add Windows path candidates and env-based overrides (e.g., `%APPDATA%`, `%LOCALAPPDATA%`, `.exe`).
- Keep `shutil.which()` first, but strengthen fallback strategy for native Windows installs.

### Priority
- **P1**

---

## 4) Git/GitHub operations

### Status
- **Mostly portable**, but operationally dependent on environment setup.

### Evidence
- Core flows call `git` and `gh` via `subprocess.run([...])`, not shell strings (good).
- However, successful operation still assumes CLI tools are installed and PATH-resolvable in runtime host.

### Required change
- Add explicit preflight checks/messages for Windows (installer guidance and PATH diagnostics).
- Optionally centralize command resolution and error reporting.

### Priority
- **P2**

---

## 5) Paths, temp files, and filesystem assumptions

### Status
- **Mixed**: many parts already use `pathlib`, but several scripts/tests still encode Unix expectations.

### Evidence
- Good: runtime code often uses `Path(...)`.
- Risky:
  - shell script uses `/tmp` semantics and FIFO behavior.
  - docs/templates and helper scripts assume Unix-style config dirs.

### Required change
- Move temp and IPC behavior into Python (`tempfile`, cross-platform primitives).
- Introduce helper utilities for config/data directories instead of hardcoded `~/.config` patterns.

### Priority
- **P1**

---

## 6) Test suite portability

### Status
- **Not Windows-neutral yet**.

### Evidence
- Tests include Unix-oriented command expectations (`python3`, bash-based wrappers, Unix-like paths).
- Some tests validate shell behavior that has no native Windows equivalent.

### Required change
- Parameterize command expectations by platform.
- Gate Bash-only tests with platform markers or migrate those behaviors to Python runners first.

### Priority
- **P1**

---

## Recommended implementation plan

## Phase A (P0, unblock native Windows)
1. Build Python full-cycle runner replacing `autodev_full_cycle.sh` behavior.
2. Centralize executable resolution (`python`, `gh`, `git`, host adapter CLI).
3. Update `autodev_project.py` and command templates to avoid hardcoded `python3`/`bash`.

## Phase B (P1, stabilize)
1. Add Windows-aware host adapter path discovery (`.exe`, AppData/LocalAppData candidates).
2. Normalize command-install target directory with platform-specific defaults.
3. Refactor tests to be platform-parameterized and skip only truly shell-specific cases.

## Phase C (P2, polish)
1. Improve doctor/preflight diagnostics for Windows setup.
2. Update docs and examples for PowerShell usage.

---

## Concrete file hotspots

- `autodev_full_cycle.sh` (Bash-only orchestration)
- `scripts/autodev_project.py` (hardcoded `python3`, Unix-biased commands-dir defaults)
- `scripts/orchestrator_selection.py` (`python3` in intake subprocess command)
- `scripts/autodev_host_packaging.py` (generated command templates include `python3` and `bash`)
- `scripts/opencode_host_adapter.py` (CLI binary fallback paths, commands_dir capability)
- `scripts/session_readability_probe.py` / `scripts/subagent_silent_stop_probe.py` (Unix fallback paths)
- `tests/scripts/test_autodev_project.py`, `tests/scripts/test_orchestrator_supervisor.py` (platform-specific assumptions)

---

## Bottom line

To run natively on Windows, the project needs a **cross-platform orchestration path** and
**platform-aware command/path resolution**. The largest blocker is the Bash full-cycle script;
once replaced with Python and command resolution is centralized, remaining work is mostly
adapter/path/test hardening.
