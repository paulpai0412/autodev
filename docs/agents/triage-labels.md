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

## E2E test case labels

E2E-specific labels may add workflow detail, but they must not replace the
canonical triage labels above.

| Supplemental label | Meaning |
| ------------------ | ------- |
| `e2e-test-case`    | One executable E2E test case issue. |
| `blocked-by-spec`  | Expected behavior or acceptance criteria need PRD/spec repair. |
| `blocked-by-infra` | Shared harness, fixture, environment, or infrastructure failure blocks verification. |

## Refactor labels

Refactor-specific labels may add workflow detail, but they must not replace the
canonical triage labels above. `blocked-by-spec` is shared with E2E issues when
the blocker is a product or PRD/spec decision.

| Supplemental label   | Meaning |
| -------------------- | ------- |
| `refactor-candidate` | One behavior-preserving refactor candidate issue from the phase refactor audit. |
| `blocked-by-risk`    | Refactor candidate failed the risk gate; split smaller or route to human. |
