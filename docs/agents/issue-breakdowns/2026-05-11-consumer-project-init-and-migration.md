# Issue breakdown: consumer project init and migration

## Slice 1: project init

- Add `scripts/autodev_project.py init`.
- Generate `.autodev.yaml`, required directories, runtime `.gitkeep`, starter domain docs, and an `AGENTS.md` managed block.
- Acceptance: `tests/scripts/test_autodev_project.py` proves init writes expected files and `--dry-run` writes nothing.

## Slice 2: global command install

- Add `install-commands` to generate autodev-prefixed global OpenCode command docs.
- Commands must be project-agnostic and discover target project from current directory / `.autodev.yaml`.
- Acceptance: tests confirm `/autodev-*` command files are written and do not embed consumer project paths.

## Slice 3: doctor and migration report

- Add `doctor` to report missing config and legacy residue.
- Add `migrate --dry-run` to list removable legacy files and preserved historical artifact directories.
- Acceptance: tests confirm legacy local commands are reported and historical evidence is preserved.

## Slice 4: explicit legacy removal

- Add `migrate --remove-legacy` with a git safety check and a test-only `--skip-git-clean-check` escape hatch.
- Remove only known legacy workflow files; never remove issue packets, handoffs, worker results, evidence, or release results.
- Acceptance: tests confirm legacy files are deleted while historical artifacts remain.

## Slice 5: consumer project start bridge

- Add `start` bridge that passes consumer project artifact paths to the existing bootstrap runner.
- Acceptance: tests confirm issue packet, checkpoint, ledger, request, and autodev workflow policy paths are explicit.
