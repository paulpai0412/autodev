# Autodev Product Context

Autodev is a durable orchestration and control-plane harness for coding-agent software delivery workflows. Its product language should emphasize operator control, recovery, auditability, and delivery governance rather than chat-first agent interaction.

## Language

**Technical Operator**:
A technical user who supervises autodev delivery runs, investigates blocked issues, and performs recovery or release actions.
_Avoid_: PM, casual user, end user

**Engineering Lead**:
A technical decision-maker who needs portfolio-level delivery visibility, issue progress KPIs, and confidence that verification and release gates were enforced.
_Avoid_: Project manager, product owner

**Control Tower**:
The Web App surface that shows project setup, intake, issue execution, session progress, recovery actions, release status, and delivery KPIs from the control plane.
_Avoid_: Chat app, IDE, copilot UI

**Run Dashboard**:
The primary Control Tower landing view for active issue execution, capacity, blocked work, release queues, recent control-plane events, and operator intervention needs.
_Avoid_: Setup wizard as primary home, generic project board

**Project Registry**:
A lightweight Control Tower index of consumer repos and their local DB-only control planes.
_Avoid_: Central runtime database, cross-repo scheduler

**All Projects Overview**:
A read-only portfolio view across registered consumer repos that summarizes health, progress, and intervention needs without performing cross-repo lifecycle actions.
_Avoid_: Bulk reconcile, bulk release, global dispatcher

**Spec Pipeline**:
The Control Tower workflow that turns project intent into requirements, a PRD, GitHub issues, and then autodev runtime execution.
_Avoid_: Coming-soon placeholder, disconnected document editor

**One-stop Development Flow**:
The end-to-end product flow from project setup and requirement clarification through PRD, issue generation, intake, automated development, verification, release, recovery, and completion.
_Avoid_: Observe-only dashboard, monitoring-only console

**Interactive Flow Chat**:
The chat-like Control Tower surface that orchestrates skills, streams progress, asks blocking questions, and advances the One-stop Development Flow.
_Avoid_: Passive command log, disconnected document form

**Streaming Interaction Layer**:
The Web backend layer that projects skill progress, agent/session messages, question prompts, approvals, and runtime events to the browser as streaming UI updates.
_Avoid_: Treating existing CLI stdout as the product API, pretending the current runtime already has SSE

**Control Tower App DB**:
The Web App database that stores project registry entries, interactive flow runs, chat messages, questions, approvals, generated artifacts, and replayable streaming events.
_Avoid_: Storing Spec Pipeline chat state in consumer repo runtime tables

**Flow Run**:
A single Interactive Flow Chat execution bound to one selected consumer repo from requirement clarification through autodev completion.
_Avoid_: Global unbound planning session, cross-repo flow run

**Issue Plan**:
The reviewed output of issue generation that includes GitHub issue drafts plus execution order, dependencies, and readiness for autodev intake.
_Avoid_: Flat unordered issue list

**UI Prototype Gate**:
The Spec Pipeline approval gate that requires a human-approved Web UI prototype HTML before issue generation when the project includes user interface work.
_Avoid_: Generating UI implementation issues from text-only requirements

**Execution Lane**:
A simplified UI projection of an Issue Plan's dependency graph that shows the recommended runnable order, ready work, blocked work, and parallelizable lanes.
_Avoid_: Treating dependency order as a single serial list

**Run Policy**:
The operator-approved automation settings for a Flow Run, including development capacity, release capacity, release backfill mode, auto-release approval behavior, and related runtime environment variables.
_Avoid_: Hidden environment defaults, unreviewed automation switches

**PR Approval Queue**:
The Control Tower review surface for verified issues whose release is blocked by human merge approval policy.
_Avoid_: Forcing operators to discover pending approvals only in GitHub tabs

**GitHub Review Approval**:
A GitHub-native pull request review approval submitted through GitHub using an authorized reviewer identity.
_Avoid_: Treating internal release approval as equivalent to GitHub code review approval

**Local Operator GitHub Identity**:
The GitHub identity currently available to the local Control Tower backend through `gh auth` or an equivalent local token.
_Avoid_: Anonymous approval, hidden shared bot approval

**Local Control Tower**:
The v0 deployment mode where one technical operator runs the Control Tower on a local or trusted machine that can access consumer repo files, local SQLite control planes, `gh`, and host adapter CLIs.
_Avoid_: Multi-user shared server as the v0 assumption

**Project Readiness Gate**:
The setup check that must pass before a consumer repo can enter the One-stop Development Flow.
_Avoid_: Discovering missing GitHub or host authorization after runtime execution starts

**Chat Sidecar**:
The persistent secondary panel that streams skill progress, asks questions, collects approvals, and narrates the active Flow Run while the main workspace remains dashboard-first.
_Avoid_: Making chat the only primary workspace

**Monorepo MVP**:
The v0 implementation approach where Control Tower is developed inside the existing autodev repository on a feature branch before considering extraction into a separate repo or package.
_Avoid_: Starting v0 as a separate repository

## Relationships

- A **Technical Operator** uses the **Control Tower** to supervise active autodev runs and intervene when recovery is needed.
- An **Engineering Lead** uses the **Control Tower** to understand delivery progress, risk, and gate compliance across issues.
- The **Run Dashboard** is the default home for the **Control Tower**; project setup appears as an onboarding path or empty-state call to action.
- The **Project Registry** points to each consumer repo's own `.opencode/runtime/control-plane.sqlite3`; it does not replace per-repo runtime truth.
- The **All Projects Overview** may aggregate read-only KPIs across repos, but lifecycle actions must happen inside one selected consumer repo context.
- The **Spec Pipeline** is part of v0 and feeds the **One-stop Development Flow** before issue intake and runtime execution.
- A **One-stop Development Flow** produces or updates GitHub issues before the selected consumer repo enters autodev intake, run, verification, release, and completion.
- The **Interactive Flow Chat** is the primary v0 interaction model for the **Spec Pipeline** and can invoke `grill-with-docs`, `to-prd`, `to-issues`, and `autodev-flow`-style steps.
- The **Streaming Interaction Layer** is required for v0 when the browser must show SSE-style progress and ask-question prompts across skill execution and runtime orchestration.
- The **Control Tower App DB** owns **Interactive Flow Chat** and **Spec Pipeline** state, while each consumer repo's DB-only control plane remains the source of truth for autodev runtime issue state.
- Every **Flow Run** starts after selecting one consumer repo, so repo context, GitHub binding, domain docs, and runtime control-plane paths are known before requirements are clarified.
- When a **Flow Run** includes UI requirements, the **Spec Pipeline** must pass the **UI Prototype Gate** by producing and receiving human approval for a Web UI prototype HTML with `web-design-engineer` before `to-issues` creates the **Issue Plan**.
- The **Spec Pipeline** produces an **Issue Plan**, not merely a flat issue list; dependencies and execution order must be reviewed before publishing to GitHub.
- An **Issue Plan** uses DAG dependencies as its underlying model and exposes **Execution Lanes** so technical operators can understand both sequencing and parallelizable work.
- A **Run Policy** is configured before autodev execution and may enable automatic capacity backfill or release behavior after the operator approves the policy.
- When a **Run Policy** requires human release approval, verified PRs appear in the **PR Approval Queue** before release/merge proceeds.
- The **PR Approval Queue** must integrate with **GitHub Review Approval** when the operator chooses to approve a PR from the Control Tower.
- In v0, **GitHub Review Approval** may use the **Local Operator GitHub Identity**, but the UI must show the approving identity before submitting approval.
- v0 is a **Local Control Tower**, so local filesystem access, local `gh auth`, and local host adapter CLIs are valid setup assumptions.
- The **Project Readiness Gate** must check GitHub authorization during autodev project initialization before the repo can proceed into intake, issue publishing, PR approval, or release.
- Project initialization may finish as partial setup when readiness checks fail, but a **Flow Run** cannot start until the selected consumer repo passes the **Project Readiness Gate**.
- The Control Tower UI is dashboard-first with a **Chat Sidecar**, so operators can inspect state and act on structured surfaces while the active flow streams conversational progress.
- Control Tower v0 is a **Monorepo MVP** in the existing autodev repo, because it depends directly on current scripts, runtime docs, DB contracts, and autodev-flow behavior.
- The **Control Tower** sits above host runtimes such as OpenCode, Claude Code, and Codex; it does not replace them.

## Example dialogue

> **Dev:** "Should the dashboard feel like a PM planning board or an agent chat workspace?"
> **Domain expert:** "Neither as the primary frame — it should feel like a delivery control tower for technical operators and engineering leads."

## Flagged ambiguities

- "Web App user" was ambiguous between PM-style planning users and technical run operators — resolved: the primary persona is **Technical Operator / Engineering Lead**.
- "Home page" was ambiguous between setup wizard and execution dashboard — resolved: v0 home is the **Run Dashboard**, with project setup as a secondary path or empty-state CTA.
- "Multi-repo support" was ambiguous between repo switching and cross-repo orchestration — resolved: v0 supports consumer repo switching through a **Project Registry**, while **All Projects Overview** is read-only and does not run cross-repo actions.
- "Spec Pipeline" was ambiguous between future placeholder and v0 scope — resolved: v0 must include the **Spec Pipeline** as part of the **One-stop Development Flow**.
- "Spec Pipeline interaction" was ambiguous between web-triggered commands and an interactive chat surface — resolved: v0 needs an **Interactive Flow Chat** with SSE-style streaming and ask-question interactions.
- "Interactive flow state" was ambiguous between the existing consumer runtime DB and a Web App-owned store — resolved: use a separate **Control Tower App DB** for chat/spec/streaming state.
- "Flow Run scope" was ambiguous between global planning and repo-bound execution — resolved: every **Flow Run** must be bound to a selected consumer repo before it starts.
- "UI issue generation" was ambiguous between text-only issue generation and prototype-informed issue generation — resolved: UI work must pass a human-approved **UI Prototype Gate** before `to-issues` creates the **Issue Plan**.
- "Generated issues" was ambiguous between unordered issue drafts and an executable plan — resolved: `to-issues` must produce an **Issue Plan** with order and dependencies.
- "Issue order" was ambiguous between a serial list and a dependency graph — resolved: **Issue Plan** uses DAG dependencies with simplified **Execution Lane** UI.
- "Automation approval" was ambiguous between fully manual gates and fully automatic execution — resolved: the operator approves a **Run Policy**, after which configured automatic backfill/release behavior may run within that policy.
- "Human PR approval" was ambiguous between GitHub-only review and Control Tower-managed release governance — resolved: non-bypassed release flows need a **PR Approval Queue** in the Control Tower.
- "PR approval" was ambiguous between internal autodev release approval and GitHub-native review approval — resolved: v0 must support **GitHub Review Approval** integration from the **PR Approval Queue**.
- "GitHub approval identity" was ambiguous between OAuth, bot token, and local auth — resolved: v0 may use the **Local Operator GitHub Identity** from `gh auth`/local token, with the identity shown before approval.
- "Deployment mode" was ambiguous between local single-operator and shared team server — resolved: v0 is a **Local Control Tower**.
- "GitHub readiness" was ambiguous between lazy failure and setup-time validation — resolved: the **Project Readiness Gate** checks `gh` authorization during autodev init.
- "Partial setup" was ambiguous between blocking all initialization and allowing unusable flows — resolved: partial project setup is allowed, but **Flow Run** start is blocked until readiness passes.
- "Primary UI" was ambiguous between chat-first and dashboard-first — resolved: the Control Tower uses dashboard-first navigation with a persistent **Chat Sidecar**.
- "Implementation location" was ambiguous between a new repository and the existing autodev repo — resolved: v0 is a **Monorepo MVP** developed on an autodev feature branch.
