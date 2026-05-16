# Triage Labels

The skills speak in terms of five canonical triage roles. This file maps those roles to the actual label strings used in this repo's GitHub issue tracker.

Source skill setup: <https://github.com/mattpocock/skills/tree/main/skills/engineering/setup-matt-pocock-skills>

| Label in mattpocock/skills | Label in our tracker | Meaning                                  |
| -------------------------- | -------------------- | ---------------------------------------- |
| `needs-triage`             | `needs-triage`       | Maintainer needs to evaluate this issue  |
| `needs-info`               | `needs-info`         | Waiting on reporter for more information |
| `ready-for-agent`          | `ready-for-agent`    | Fully specified, ready for an AFK agent  |
| `ready-for-human`          | `ready-for-human`    | Requires human implementation            |
| `wontfix`                  | `wontfix`            | Will not be actioned                     |

When a skill mentions a role, use the corresponding label string from this table.

## Runtime coordination labels

| Supplemental label  | Meaning |
| ------------------- | ------- |
| `agent-dispatching` | Claimed by the scheduler and currently in DB-backed dispatch/bootstrap flow. |
| `agent-in-progress` | Claimed by an active autodev run; blocks duplicate starts for the same issue across sessions. |
| `quarantined`       | Issue is fenced for controlled recovery and must not be auto-started. |

These runtime coordination labels are additive. They help GitHub stay aligned with the SQLite control plane, but they do not replace the canonical DB state in `issues` and `issue_history`.
