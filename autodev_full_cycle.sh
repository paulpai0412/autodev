#!/usr/bin/env bash

set -u

# End-to-end autodev loop for this project:
# init -> intake -> start -> reconcile -> release -> recovery -> monitor(gh)
# Runs until no open GitHub issues remain (or max cycles reached).

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AUTODEV_HOME="${AUTODEV_HOME:-$SCRIPT_DIR}"
PROJECT_ROOT="${PROJECT_ROOT:-}"
REPO="${REPO:-}"
INTERVAL_SECONDS="${INTERVAL_SECONDS:-180}"
MAX_CYCLES="${MAX_CYCLES:-0}" # 0 = infinite
AUTO_APPROVE_RELEASE="${AUTO_APPROVE_RELEASE:-1}"
AUTO_LABEL_READY="${AUTO_LABEL_READY:-0}" # 1 => add ready-for-agent to all open issues
HEARTBEAT_SECONDS="${HEARTBEAT_SECONDS:-10}"

# Force non-interactive output in shell dashboards (avoid pager freeze on gh lists).
export GH_PAGER="${GH_PAGER:-cat}"
export PAGER="${PAGER:-cat}"
export GIT_PAGER="${GIT_PAGER:-cat}"

AUTODEV_PROJECT_PY="$AUTODEV_HOME/scripts/autodev_project.py"
INTAKE_PY="$AUTODEV_HOME/scripts/issue_packet_intake.py"
SUPERVISOR_PY="$AUTODEV_HOME/scripts/orchestrator_supervisor.py"
DB_PATH=""
STATE_DIR=""

RESUME_MAX_ATTEMPTS="${RESUME_MAX_ATTEMPTS:-2}"
REDISPATCH_MAX_ATTEMPTS="${REDISPATCH_MAX_ATTEMPTS:-2}"
AUTO_FAIL_QUARANTINED="${AUTO_FAIL_QUARANTINED:-1}"

resolve_consumer_project_root() {
  local start
  if [ -n "${PROJECT_ROOT:-}" ]; then
    start="$PROJECT_ROOT"
  else
    start="$PWD"
  fi

  if [ ! -d "$start" ]; then
    log "ERROR: project root candidate does not exist: $start"
    exit 1
  fi

  local current
  current="$(cd "$start" && pwd)"

  while true; do
    if [ -f "$current/.autodev.yaml" ]; then
      printf '%s' "$current"
      return 0
    fi
    if [ "$current" = "/" ]; then
      break
    fi
    current="$(dirname "$current")"
  done

  printf '%s' "$(cd "$start" && pwd)"
  return 0
}

read_repo_from_env_file() {
  local root="$1"
  local env_file="$root/.env"
  if [ ! -f "$env_file" ]; then
    printf ''
    return 0
  fi

  local value
  value=$(grep -E '^AUTODEV_GITHUB_REPO=' "$env_file" 2>/dev/null | tail -n 1 | cut -d= -f2- | tr -d '"' | tr -d "'" | xargs)
  printf '%s' "$value"
}

read_repo_from_autodev_yaml() {
  local root="$1"
  local config="$root/.autodev.yaml"
  if [ ! -f "$config" ]; then
    printf ''
    return 0
  fi

  local value
  value=$(python3 - "$config" <<'PY'
import re
import sys

path = sys.argv[1]
try:
    text = open(path, "r", encoding="utf-8").read().splitlines()
except OSError:
    print("")
    raise SystemExit(0)

in_project = False
for raw in text:
    line = raw.rstrip("\n")
    stripped = line.strip()
    if not stripped:
        continue
    indent = len(line) - len(line.lstrip(" "))
    if indent == 0 and stripped == "project:":
        in_project = True
        continue
    if in_project and indent == 0:
        break
    if in_project and indent >= 2 and stripped.startswith("github_repo:"):
        value = stripped.split(":", 1)[1].strip().strip('"').strip("'")
        print(value)
        raise SystemExit(0)

print("")
PY
)
  printf '%s' "$value"
}

read_repo_from_git_remote() {
  local root="$1"
  local remote
  remote=$(git -C "$root" remote get-url origin 2>/dev/null || true)
  if [ -z "$remote" ]; then
    printf ''
    return 0
  fi

  local value
  value=$(python3 - "$remote" <<'PY'
import re
import sys

url = (sys.argv[1] or "").strip()
match = re.search(r"github\.com[:/]([^/]+)/([^/]+?)(?:\.git)?$", url)
if match:
    print(f"{match.group(1)}/{match.group(2)}")
else:
    print("")
PY
)
  printf '%s' "$value"
}

resolve_repo() {
  local root="$1"
  if [ -n "${REPO:-}" ]; then
    printf '%s' "$REPO"
    return 0
  fi

  local from_env
  from_env=$(read_repo_from_env_file "$root")
  if [ -n "$from_env" ]; then
    printf '%s' "$from_env"
    return 0
  fi

  local from_config
  from_config=$(read_repo_from_autodev_yaml "$root")
  if [ -n "$from_config" ]; then
    printf '%s' "$from_config"
    return 0
  fi

  local from_git
  from_git=$(read_repo_from_git_remote "$root")
  if [ -n "$from_git" ]; then
    printf '%s' "$from_git"
    return 0
  fi

  printf ''
}

initialize_runtime_context() {
  PROJECT_ROOT="$(resolve_consumer_project_root)"
  REPO="$(resolve_repo "$PROJECT_ROOT")"
  DB_PATH="$PROJECT_ROOT/.opencode/runtime/control-plane.sqlite3"
  STATE_DIR="$PROJECT_ROOT/.opencode/runtime/full-cycle-state"

  if [ -z "$REPO" ]; then
    log "ERROR: unable to resolve GitHub repo. Set REPO or AUTODEV_GITHUB_REPO, or configure project.github_repo in .autodev.yaml"
    exit 1
  fi
}

timestamp() {
  date +"%Y-%m-%dT%H:%M:%S%z"
}

dashboard_touch() {
  if [ ! -t 1 ]; then
    return 0
  fi

  local stamp label cols row col
  stamp=$(date +"%Y-%m-%d %H:%M:%S")
  label="Last update: ${stamp}"

  cols=$(tput cols 2>/dev/null || printf '0')
  if [ "$cols" -le 0 ]; then
    return 0
  fi

  row=1
  col=$((cols - ${#label} + 1))
  if [ "$col" -lt 1 ]; then
    col=1
  fi

  # Save cursor, draw top-right timestamp, restore cursor.
  printf '\033[s\033[%d;%dH\033[1;36m%s\033[0m\033[u' "$row" "$col" "$label"
}

log() {
  dashboard_touch
  printf '[%s] %s\n' "$(timestamp)" "$*"
}

run_cmd() {
  log "RUN: $*"
  local cmd_pid=""
  local monitor_pid=""

  "$@" &
  cmd_pid=$!

  if [ -t 1 ]; then
    (
      while kill -0 "$cmd_pid" 2>/dev/null; do
        dashboard_touch
        sleep 1
      done
    ) &
    monitor_pid=$!
  fi

  wait "$cmd_pid"
  local rc=$?

  if [ -n "$monitor_pid" ]; then
    kill "$monitor_pid" 2>/dev/null || true
    wait "$monitor_pid" 2>/dev/null || true
  fi

  if [ $rc -ne 0 ]; then
    log "WARN: command failed (exit=$rc): $*"
  fi
  return $rc
}

require_tools() {
  local missing=0
  for tool in gh python3; do
    if ! command -v "$tool" >/dev/null 2>&1; then
      log "ERROR: required tool not found: $tool"
      missing=1
    fi
  done
  if [ ! -f "$AUTODEV_PROJECT_PY" ] || [ ! -f "$INTAKE_PY" ] || [ ! -f "$SUPERVISOR_PY" ]; then
    log "ERROR: autodev scripts not found under AUTODEV_HOME=$AUTODEV_HOME"
    missing=1
  fi
  if [ $missing -ne 0 ]; then
    exit 1
  fi
}

ensure_state_dir() {
  mkdir -p "$STATE_DIR"
}

state_file_for_issue() {
  local issue="$1"
  printf '%s' "$STATE_DIR/issue-${issue}.state"
}

read_state_value() {
  local issue="$1"
  local key="$2"
  local file
  file=$(state_file_for_issue "$issue")
  if [ ! -f "$file" ]; then
    printf '0'
    return 0
  fi
  local value
  value=$(python3 - "$file" "$key" <<'PY'
import json, sys
path, key = sys.argv[1], sys.argv[2]
try:
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
except Exception:
    data = {}
v = data.get(key, 0)
try:
    print(int(v))
except Exception:
    print(0)
PY
)
  printf '%s' "$value"
}

write_state_values() {
  local issue="$1"
  local resume_count="$2"
  local redispatch_count="$3"
  local file
  file=$(state_file_for_issue "$issue")
  python3 - "$file" "$resume_count" "$redispatch_count" <<'PY'
import json, sys
path = sys.argv[1]
resume_count = int(sys.argv[2])
redispatch_count = int(sys.argv[3])
data = {
    "resume_fail_count": resume_count,
    "redispatch_fail_count": redispatch_count,
}
with open(path, 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False)
PY
}

reset_issue_state() {
  local issue="$1"
  local file
  file=$(state_file_for_issue "$issue")
  rm -f "$file"
}

is_issue_still_quarantined() {
  local issue="$1"
  local state
  state=$(python3 - "$DB_PATH" "$issue" <<'PY'
import sqlite3, sys
db, issue = sys.argv[1], sys.argv[2]
con = sqlite3.connect(db)
row = con.execute("select state from issues where issue_number=?", (issue,)).fetchone()
print((row[0] if row else ""))
con.close()
PY
)
  [ "$state" = "quarantined" ]
}

open_issue_numbers() {
  gh issue list --repo "$REPO" --state open --limit 200 --json number --jq '.[].number'
}

open_issue_count() {
  local count
  count=$(gh issue list --repo "$REPO" --state open --limit 200 --json number --jq 'length')
  printf '%s' "$count"
}

first_ready_issue_number() {
  gh issue list --repo "$REPO" --state open --label ready-for-agent --limit 200 --json number --jq '.[0].number // empty'
}

print_github_snapshot() {
  log "GitHub snapshot (open issues):"
  gh issue list --repo "$REPO" --state open --limit 200 || true

  log "GitHub snapshot (open PRs):"
  gh pr list --repo "$REPO" --state open || true
}

print_db_snapshot() {
  if [ ! -f "$DB_PATH" ]; then
    log "DB snapshot skipped: $DB_PATH does not exist yet"
    return 0
  fi

  log "Control-plane snapshot (issues table):"
  python3 - "$DB_PATH" <<'PY'
import sqlite3, json, sys
db = sys.argv[1]
con = sqlite3.connect(db)
con.row_factory = sqlite3.Row
rows = con.execute(
    """
    select issue_number, state, current_role, current_stage, current_status, current_session_id, updated_at
    from issues
    order by cast(issue_number as integer)
    """
).fetchall()
print(json.dumps([dict(r) for r in rows], ensure_ascii=False, indent=2))
con.close()
PY
}

autodev_init() {
  run_cmd python3 "$AUTODEV_PROJECT_PY" init --project-root "$PROJECT_ROOT" --json
  run_cmd python3 "$AUTODEV_PROJECT_PY" doctor --project-root "$PROJECT_ROOT" --json
}

autodev_intake() {
  if [ "$AUTO_LABEL_READY" = "1" ]; then
    log "AUTO_LABEL_READY=1 => add ready-for-agent to all open issues"
    while IFS= read -r n; do
      [ -z "$n" ] && continue
      run_cmd gh issue edit "$n" --repo "$REPO" --add-label ready-for-agent
    done < <(open_issue_numbers)
  fi

  run_cmd python3 "$INTAKE_PY" --project-root "$PROJECT_ROOT" --repo "$REPO"
}

autodev_start_one() {
  local issue
  issue=$(first_ready_issue_number)
  if [ -n "$issue" ]; then
    run_cmd python3 "$AUTODEV_PROJECT_PY" start --project-root "$PROJECT_ROOT" --issue-number "$issue"
  else
    log "No ready-for-agent issue found for explicit start step"
  fi
}

autodev_recovery() {
  if [ ! -f "$DB_PATH" ]; then
    return 0
  fi

  # failed -> retry-failed
  while IFS= read -r issue; do
    [ -z "$issue" ] && continue
    run_cmd python3 "$SUPERVISOR_PY" retry-failed --base-dir "$PROJECT_ROOT" --issue-number "$issue" --reason "auto-recovery loop: retry failed issue"
  done < <(python3 - "$DB_PATH" <<'PY'
import sqlite3, sys
db = sys.argv[1]
con = sqlite3.connect(db)
rows = con.execute("select issue_number from issues where state='failed'").fetchall()
for (n,) in rows:
    print(n)
con.close()
PY
)

  # ready with stale fence -> clear-ready-session-fence
  while IFS= read -r issue; do
    [ -z "$issue" ] && continue
    run_cmd python3 "$SUPERVISOR_PY" clear-ready-session-fence --base-dir "$PROJECT_ROOT" --issue-number "$issue" --reason "auto-recovery loop: clear stale ready fence"
  done < <(python3 - "$DB_PATH" <<'PY'
import sqlite3, sys
db = sys.argv[1]
con = sqlite3.connect(db)
rows = con.execute(
    "select issue_number from issues where state='ready' and ifnull(current_session_id,'')<>''"
).fetchall()
for (n,) in rows:
    print(n)
con.close()
PY
)

  # quarantined -> resume-quarantined -> redispatch-quarantined -> fail-quarantined (optional)
  while IFS= read -r issue; do
    [ -z "$issue" ] && continue
    local resume_fail_count redispatch_fail_count
    resume_fail_count=$(read_state_value "$issue" "resume_fail_count")
    redispatch_fail_count=$(read_state_value "$issue" "redispatch_fail_count")

    if [ "$resume_fail_count" -lt "$RESUME_MAX_ATTEMPTS" ]; then
      if run_cmd python3 "$SUPERVISOR_PY" resume-quarantined --base-dir "$PROJECT_ROOT" --issue-number "$issue" --reason "auto-recovery loop: resume quarantined issue"; then
        if is_issue_still_quarantined "$issue"; then
          resume_fail_count=$((resume_fail_count + 1))
          log "WARN: issue #$issue still quarantined after resume attempt (count=$resume_fail_count/$RESUME_MAX_ATTEMPTS)"
        else
          log "INFO: issue #$issue resumed from quarantined"
          reset_issue_state "$issue"
          continue
        fi
      else
        resume_fail_count=$((resume_fail_count + 1))
      fi
      write_state_values "$issue" "$resume_fail_count" "$redispatch_fail_count"
      continue
    fi

    if [ "$redispatch_fail_count" -lt "$REDISPATCH_MAX_ATTEMPTS" ]; then
      if run_cmd python3 "$SUPERVISOR_PY" redispatch-quarantined --base-dir "$PROJECT_ROOT" --issue-number "$issue" --reason "auto-recovery loop: redispatch quarantined issue" --source-session-id "full-cycle-auto-recovery"; then
        if is_issue_still_quarantined "$issue"; then
          redispatch_fail_count=$((redispatch_fail_count + 1))
          log "WARN: issue #$issue still quarantined after redispatch (count=$redispatch_fail_count/$REDISPATCH_MAX_ATTEMPTS)"
        else
          log "INFO: issue #$issue redispatched from quarantined"
          reset_issue_state "$issue"
          continue
        fi
      else
        redispatch_fail_count=$((redispatch_fail_count + 1))
      fi
      write_state_values "$issue" "$resume_fail_count" "$redispatch_fail_count"
      continue
    fi

    if [ "$AUTO_FAIL_QUARANTINED" = "1" ]; then
      run_cmd python3 "$SUPERVISOR_PY" fail-quarantined --base-dir "$PROJECT_ROOT" --issue-number "$issue" --reason "auto-recovery loop: exceeded resume/redispatch limits"
      reset_issue_state "$issue"
      log "WARN: issue #$issue marked failed after exceeding quarantine recovery limits"
    else
      log "WARN: issue #$issue exceeded recovery limits but AUTO_FAIL_QUARANTINED=0, leaving quarantined"
      write_state_values "$issue" "$resume_fail_count" "$redispatch_fail_count"
    fi
  done < <(python3 - "$DB_PATH" <<'PY'
import sqlite3, sys
db = sys.argv[1]
con = sqlite3.connect(db)
rows = con.execute("select issue_number from issues where state='quarantined'").fetchall()
for (n,) in rows:
    print(n)
con.close()
PY
)
}

autodev_reconcile() {
  run_cmd python3 "$AUTODEV_PROJECT_PY" reconcile --project-root "$PROJECT_ROOT"
}

autodev_release_verified() {
  if [ ! -f "$DB_PATH" ]; then
    return 0
  fi

  while IFS= read -r issue; do
    [ -z "$issue" ] && continue
    if [ "$AUTO_APPROVE_RELEASE" = "1" ]; then
      run_cmd python3 "$AUTODEV_PROJECT_PY" release --project-root "$PROJECT_ROOT" --issue-number "$issue" --auto-approve
    else
      run_cmd python3 "$AUTODEV_PROJECT_PY" release --project-root "$PROJECT_ROOT" --issue-number "$issue"
    fi
  done < <(python3 - "$DB_PATH" <<'PY'
import sqlite3, sys
db = sys.argv[1]
con = sqlite3.connect(db)
rows = con.execute("select issue_number from issues where state='verified'").fetchall()
for (n,) in rows:
    print(n)
con.close()
PY
)
}

autodev_release_pending_fallback() {
  # Optional fallback path: if an issue is release_pending and has open PR, run release-finalize.
  # This helps when release root session is degraded/stuck but merge state is already clean.
  if [ ! -f "$DB_PATH" ]; then
    return 0
  fi

  while IFS=$'\t' read -r issue branch worktree pr; do
    [ -z "$issue" ] && continue
    [ -z "$pr" ] && continue

    local pr_state
    pr_state=$(gh pr view "$pr" --repo "$REPO" --json state --jq '.state' 2>/dev/null || true)
    if [ "$pr_state" = "MERGED" ] || [ "$pr_state" = "CLOSED" ]; then
      continue
    fi

    run_cmd python3 "$SUPERVISOR_PY" release-finalize \
      --base-dir "$PROJECT_ROOT" \
      --issue-number "$issue" \
      --pr-number "$pr" \
      --repo "$REPO" \
      --issue-branch "$branch" \
      --worktree-path "$worktree" \
      --merge-method squash
  done < <(python3 - "$DB_PATH" <<'PY'
import sqlite3, json, sys
db = sys.argv[1]
con = sqlite3.connect(db)
con.row_factory = sqlite3.Row
rows = con.execute("""
  select issue_number, branch, worktree_path, artifact_status_json
  from issues
  where state='release_pending'
""").fetchall()
for r in rows:
    pr = ""
    try:
        payload = json.loads(r[3] or "{}")
        p = payload.get("evidence_packet")
        if isinstance(p, dict):
            pr = str(p.get("pr_number") or "")
    except Exception:
        pr = ""
    if not pr:
        continue
    print(f"{r['issue_number']}\t{r['branch']}\t{r['worktree_path']}\t{pr}")
con.close()
PY
)
}

sleep_with_heartbeat() {
  local total="$1"
  local open_count="$2"

  if [ "$total" -le 0 ]; then
    return 0
  fi

  local tick="$HEARTBEAT_SECONDS"
  if [ "$tick" -le 0 ]; then
    tick=10
  fi

  local remaining="$total"
  while [ "$remaining" -gt 0 ]; do
    local step="$tick"
    if [ "$remaining" -lt "$step" ]; then
      step="$remaining"
    fi

    log "Heartbeat: next cycle in ${remaining}s (open issues: ${open_count})"
    sleep "$step"
    remaining=$((remaining - step))
  done
}

main() {
  initialize_runtime_context
  require_tools
  ensure_state_dir

  log "Start autodev full cycle"
  log "PROJECT_ROOT=$PROJECT_ROOT"
  log "AUTODEV_HOME=$AUTODEV_HOME"
  log "REPO=$REPO"
  log "INTERVAL_SECONDS=$INTERVAL_SECONDS"
  log "MAX_CYCLES=$MAX_CYCLES"
  log "HEARTBEAT_SECONDS=$HEARTBEAT_SECONDS"
  log "RESUME_MAX_ATTEMPTS=$RESUME_MAX_ATTEMPTS"
  log "REDISPATCH_MAX_ATTEMPTS=$REDISPATCH_MAX_ATTEMPTS"
  log "AUTO_FAIL_QUARANTINED=$AUTO_FAIL_QUARANTINED"

  autodev_init

  local cycle=0
  while true; do
    cycle=$((cycle + 1))
    log "===== CYCLE $cycle ====="

    autodev_intake
    autodev_start_one
    autodev_recovery
    autodev_reconcile
    autodev_release_verified
    autodev_release_pending_fallback

    print_db_snapshot
    print_github_snapshot

    local open_count
    open_count=$(open_issue_count)
    log "Open issue count: $open_count"

    if [ "$open_count" = "0" ]; then
      log "All GitHub issues are closed. Done."
      break
    fi

    if [ "$MAX_CYCLES" != "0" ] && [ "$cycle" -ge "$MAX_CYCLES" ]; then
      log "Reached MAX_CYCLES=$MAX_CYCLES. Stop loop."
      break
    fi

    log "Sleep ${INTERVAL_SECONDS}s before next cycle"
    sleep_with_heartbeat "$INTERVAL_SECONDS" "$open_count"
  done
}

main "$@"
