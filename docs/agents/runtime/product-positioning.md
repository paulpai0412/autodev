# Product positioning

## Status

- Status: active branch direction
- Branch: `db-only-control-plane`

## One-line positioning

Autodev is a durable orchestration and control-plane harness for coding-agent workflows.

## What it is

- a workflow harness
- a control plane for software delivery with AI agents
- a durable state and policy layer above coding-agent runtimes
- a system for issue routing, verification, release policy, recovery, and audit

## What it is not

- not another coding copilot
- not a replacement for Claude Code, Codex, or OpenCode
- not a prompt library pretending to be a product
- not just a command wrapper around agent sessions

## Product thesis

Coding-agent runtimes are becoming better at execution: they can edit code, spawn subagents, run commands, and resume sessions.

What they do not inherently provide is a durable software-delivery control plane with:

- issue state
- retry and recovery rules
- verifier and release policy
- audit history
- operator actions
- cross-run orchestration
- multi-issue scheduling

Autodev exists to provide that missing layer.

## Layered product view

### Host runtime layer

Examples:

- OpenCode
- Claude Code
- Codex

These products are execution surfaces.

### Autodev layer

Autodev owns:

- DB-backed issue state
- orchestration and scheduling
- verification policy
- PR ownership rules
- release policy
- monitoring and recovery
- auditability
- operator control

### Organization workflow layer

The adopting team owns:

- issue tracker policy
- approval policy
- release policy specifics
- repository conventions
- domain rules

## Why it still matters as host runtimes improve

Even if Claude Code or Codex become better at:

- subagents
- background execution
- session memory
- plugins
- MCP

Autodev still adds value if it remains the durable, host-agnostic orchestration layer.

The risk is not that host runtimes improve.
The risk is that autodev gets trapped in the same layer and becomes redundant.

## Design guardrails implied by this positioning

- keep the control plane outside host-specific session storage
- keep workflow state in SQLite, not in prompt/session memory
- keep plugins as integration surfaces, not as the core state machine
- keep skills thin and instructional
- keep the product focused on durable delivery orchestration rather than agent UX novelty

## Current branch implication

On branch `db-only-control-plane`, product positioning means:

- DB-only core
- host adapters for OpenCode, Claude Code, and Codex
- verifier-owned PR creation
- release decoupled from the per-issue development loop
- bounded multi-issue concurrency as a core feature rather than a future afterthought

## Acceptance criteria

This positioning is satisfied when:

1. the repo tells a clear control-plane story rather than an agent-shell story
2. the core remains useful even if the host runtime improves significantly
3. host portability is mostly adapter work, not schema redesign
4. the product surface is about orchestration, governance, and recovery rather than prompt tricks
