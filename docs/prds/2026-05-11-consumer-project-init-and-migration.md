# Consumer project init and migration

## Problem

Autodev is intended to run the same autonomous-development workflow across multiple projects, but consumer repositories such as `wferp` still contain local workflow commands, runner scripts, templates, and session-continuation plugins. This makes it hard to prove whether a test run is using the shared autodev implementation or a stale project-local copy.

## Goal

Provide autodev-owned project setup and migration commands so consumer repositories keep only project-specific configuration, domain context, and generated workflow artifacts.

## Non-goals

- Do not copy workflow policy, templates, commands, plugins, or runner scripts into consumer repositories.
- Do not delete historical issue/evidence/handoff artifacts during legacy workflow removal.
- Do not auto-commit migration changes.

## Required behavior

1. `autodev_project.py init` creates `.autodev.yaml`, required artifact directories, `.opencode/runtime/.gitkeep`, starter domain docs when missing, and an `AGENTS.md` managed block.
2. `init` is idempotent and supports `--dry-run`, `--check`, and `--force` without overwriting project-owned domain docs.
3. `install-commands` writes autodev-prefixed global OpenCode commands: `/autodev-start`, `/autodev-reconcile`, `/autodev-show-session`, and `/autodev-doctor`.
4. `doctor` reports missing project setup and legacy residue.
5. `migrate --dry-run` reports legacy local workflow files and preserved historical artifact directories.
6. `migrate --remove-legacy` removes only known legacy workflow code, local commands, plugins, templates, live checkpoints, and runtime JSON; it preserves historical artifacts.
7. `start` resolves issue packet, checkpoint, ledger, request, and workflow policy paths across the consumer project boundary.

## Success criteria

- Consumer projects do not need local `.opencode/commands`, workflow scripts, workflow templates, or workflow policy copies.
- Running the same init/install commands twice is a no-op on the second run.
- Legacy cleanup is explicit and reviewable before deletion.
- Command names make autodev ownership obvious.
