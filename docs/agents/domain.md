# Domain Docs

How mattpocock/skills engineering skills should consume this repo's domain documentation when exploring the codebase.

Source skill setup: <https://github.com/mattpocock/skills/tree/main/skills/engineering/setup-matt-pocock-skills>

## Layout

This repo uses a single-context domain-doc layout.

## Before exploring, read these

- **`CONTEXT.md`** at the repo root if it exists.
- **`docs/adr/`** for ADRs that touch the area about to be changed if it exists.
- **`AGENTS.md`** at the repo root for repo-specific generation and verification rules.

If any optional domain files don't exist, proceed silently. The producer skill (`grill-with-docs`) can create them lazily when terms or decisions get resolved.

## Use the glossary's vocabulary

When output names a domain concept, use the term as defined in `CONTEXT.md` if present. Don't drift to synonyms the glossary explicitly avoids.

If the concept needed isn't in the glossary yet, note it for `grill-with-docs` instead of inventing a new project vocabulary.

## Flag ADR conflicts

If output contradicts an existing ADR, surface it explicitly rather than silently overriding it.
