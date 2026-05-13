# autodev 系統說明書（繁體中文）

> 本文件以目前 repo 版本為準，對應此 workspace 內 `scripts/`、`docs/agents/` 與 `tests/scripts/` 的實作與命令 surface。

---

## 1. 系統定位

`autodev` 是一套把 **GitHub issue → 自動派工 → 實作 → 驗證 → 發佈 / 收尾 → 恢復** 串成可恢復工作流的自治開發系統。

它不是單一 agent prompt，也不是單一排程腳本，而是一個由下列部分共同組成的執行系統：

- `main_orchestrator` / `issue_worker` / `pr_verifier` / `release_worker` 角色分工
- SQLite control plane
- runtime JSON artifacts
- GitHub issue / label 協調面
- operator recovery / quarantine / retry 命令

目前版本的核心設計原則是：

- **SQLite 是唯一 canonical control plane**
- GitHub 是協調面，不是主真相來源
- runtime JSON / YAML artifacts 保留，但只作為 projection、evidence 或 dispatch handoff
- `main_orchestrator` 負責協調，不直接做 issue 實作
- issue 執行是 **serial flow**：一次只處理一個 ready issue、一路走完同一條 orchestrator path

---

## 2. 整體系統架構

### 2.1 角色分工

#### `main_orchestrator`

`main_orchestrator` 是 orchestration-only 角色，負責：

- 選擇可執行 issue
- 啟動或恢復 root session
- 委派 `issue_worker`、`pr_verifier`、`release_worker`
- 驅動 `reconcile` / recovery / quarantine 路徑
- 維持整體流程契約一致性

它**不直接實作 issue scope**，也**不自己做最終驗收**。

#### `issue_worker`

負責實際實作 issue 內容，例如：

- 修改程式碼
- 補測試
- 產出 worker result artifact

#### `pr_verifier`

負責 verifier-owned 驗證與 evidence 建立，例如：

- 驗證 acceptance criteria 是否成立
- 檢查 worker 產物是否達到要求
- 產出 evidence packet

#### `release_worker`

在 verifier 通過後負責最後一段收尾流程，例如：

- release / merge 前後的收尾邏輯
- workspace hygiene
- issue completion 前的最後 transition

#### `operator`

人類 operator 不負責日常 issue 實作，但負責：

- 初始安裝與 consumer project bootstrap
- 執行 `inspect` / `quarantine` / `resume-quarantined`
- 重試 GitHub sync
- 處理環境異常（例如 `gh auth`、網路、`opencode` CLI）

---

### 2.2 控制平面與 runtime artifacts

#### SQLite control plane（主真相）

主要資料庫位於：

- `.opencode/runtime/control-plane.sqlite3`

目前版本中，它是整個系統的 **canonical truth**，用來保存：

- issue lifecycle state
- issue history / audit trail
- root event / session request / session result
- GitHub sync attempt
- issue ranking / selection 結果
- runtime snapshot / artifact refs / issue packet projection

#### runtime JSON artifacts（接縫與 handoff）

系統仍保留以下 runtime 檔案：

- `.opencode/runtime/orchestrator-ledger.json`
- `.opencode/runtime/new-session-request.json`
- `.opencode/runtime/new-session-result.json`

它們的用途是：

- 保存目前 orchestrator 上下文與 runtime snapshot
- 作為新 root session dispatch 的 request / result handoff
- 維持既有流程接縫，而不是取代 SQLite

#### issue packet / worker result / evidence / release result

主要 artifact 目錄：

- `docs/agents/issue-packets/`
- `docs/agents/handoffs/`
- `docs/agents/worker-results/`
- `docs/agents/evidence/`
- `docs/agents/release-results/`
- `docs/agents/runtime/`

這些 artifact 採 **compact / index-only** 原則：

- 可以保存摘要與 reference
- 不應直接把 raw log、完整 trace、SQL log、截圖或 transcript 貼進 repo

---

### 2.3 目前版本的模組分層

#### 入口模組（entrypoints）

| 模組 | 作用 |
|---|---|
| `scripts/autodev_project.py` | consumer project 初始化、安裝全域命令、doctor、`start` / `reconcile` / `show-session` wrapper |
| `scripts/orchestrator_bootstrap_runner.py` | 針對指定 issue 進行 bootstrap、checkpoint/ledger/request 建立與可選 dispatch |
| `scripts/orchestrator_supervisor.py` | supervisor CLI surface、runtime reconcile 總控、operator commands |
| `scripts/orchestrator_compact_payload.py` | checkpoint / compact payload 解析與生成 |
| `scripts/issue_packet_intake.py` | 從 GitHub ready-for-agent issue materialize 成本地 issue packets |

#### supervisor 拆分後的 helper 模組

| 模組 | 作用 |
|---|---|
| `scripts/orchestrator_artifacts.py` | issue packet / worker result / evidence packet / release result parsing |
| `scripts/orchestrator_sessions.py` | detached `opencode run`、session ID 解析與 DB lookup |
| `scripts/orchestrator_lifecycle.py` | issue claim / lock / lifecycle transition / GitHub label sync / quarantine |
| `scripts/orchestrator_requests.py` | prompt 與 session request builder |
| `scripts/orchestrator_selection.py` | issue packet sync、selection、intake 協調 |
| `scripts/orchestrator_reconcile.py` | transition / recovery helpers、role-specific branch handlers、main-orchestrator branch handlers |

#### 測試模組

- `tests/scripts/`：針對每個核心腳本的 regression tests

---

### 2.4 高層執行流程

1. GitHub 上標記為 `ready-for-agent` 的 issue 被 materialize 成本地 `issue-<n>.yaml`
2. bootstrap runner 針對指定 issue 建立 checkpoint、ledger 與新 root session request
3. `main_orchestrator` 被 dispatch
4. `main_orchestrator` 依序委派：
   - `issue_worker`
   - `pr_verifier`
   - `release_worker`
5. 每次有新 artifact 落地後，supervisor 執行 `reconcile`
6. `reconcile` 會同步 SQLite control plane，判斷下一步是：
   - 繼續派工
   - recovery
   - quarantine
   - 完成 / 失敗

---

## 3. 核心功能總覽

### 3.1 Consumer project bootstrap

`autodev_project.py init` 會完成：

- 建立 `.autodev.yaml`
- 建立 `docs/agents/...` 與 `.opencode/runtime/`
- 建立 `.opencode/runtime/control-plane.sqlite3`
- 建立 / 更新 consumer project 的 `AGENTS.md` managed block
- 初始化 git repository（必要時）
- 設定 `origin`
- 自動建立 GitHub repository（必要時）
- 自動補齊 autodev workflow labels

### 3.2 全域 OpenCode 指令安裝

`autodev_project.py install-commands` 會安裝以下全域命令：

- `/autodev-start <issue-number>`
- `/autodev-reconcile`
- `/autodev-show-session`
- `/autodev-doctor`

### 3.3 自動 issue packet intake

`issue_packet_intake.py` 會：

- 從 GitHub 讀取 `ready-for-agent` issue
- 產出 `docs/agents/issue-packets/issue-<n>.yaml`
- 推導 branch name、parent reference、dependencies、acceptance criteria

### 3.4 嚴格 issue state machine

目前 canonical states：

- `ready`
- `claimed`
- `dispatching`
- `running`
- `verifying`
- `completed`
- `failed`
- `quarantined`

這些狀態保證 duplicate-start prevention、recovery 與 audit 都有明確規則。

### 3.5 Duplicate-start 防護

duplicate-start 的 canonical 防護來自 SQLite `issues.state`，不是 lease table。

另外，`.opencode/runtime/issue-locks/issue-<n>.json` 只保留為：

- operator safety artifact
- duplicate-start 訊息 projection
- 已知 live session 的 resume hint 輔助

### 3.6 GitHub coordination labels

目前主要 labels：

- `ready-for-agent`
- `agent-dispatching`
- `agent-in-progress`
- `quarantined`

GitHub 仍是 operator 協調面，但不是 canonical truth。

### 3.7 Recovery / quarantine / retry

當出現 heartbeat timeout、runtime inconsistency 或 label sync failure 時，系統支援：

- `inspect`
- `quarantine`
- `resume-quarantined`
- `fail-quarantined`
- `retry-github-sync`

### 3.8 稽核與可追蹤性

每次關鍵 control-plane 動作都會留下可追蹤痕跡：

- `issues` current state
- `issue_history` append-only audit
- GitHub sync attempt 記錄
- runtime artifact refs

---

## 4. 安裝與環境需求

### 4.1 基本需求

建議在 Linux / macOS 的 shell 環境中使用，至少需要：

- `python3`
- `git`
- `gh`（GitHub CLI）
- `opencode` 或 `opencode-desktop`
- `pytest`

### 4.2 最小驗證安裝

在 repo 根目錄執行：

```bash
python3 -m pip install pytest
```

如果你要實際對 GitHub issue 執行 workflow，還需要：

- `gh auth status` 正常
- `opencode` CLI 在 PATH 中可用

### 4.3 初始化 consumer project

```bash
PYTHONPATH=. python3 scripts/autodev_project.py init --project-root <project> --github-repo <owner/repo>
```

例如：

```bash
PYTHONPATH=. python3 scripts/autodev_project.py init --project-root /path/to/project --github-repo myorg/myrepo
```

常用選項：

- `--dry-run`：只顯示會做什麼，不真的修改
- `--check`：檢查是否需要變更；有差異時用非零 exit code
- `--force`：必要時更新既有 remote / managed 內容
- `--json`：以 JSON 方式輸出結果

### 4.4 安裝全域 OpenCode 命令

```bash
PYTHONPATH=. python3 scripts/autodev_project.py install-commands
```

常用選項：

- `--commands-dir <path>`：指定安裝目錄
- `--dry-run`
- `--force`
- `--json`

### 4.5 專案健康檢查

```bash
PYTHONPATH=. python3 scripts/autodev_project.py doctor --project-root <project>
```

doctor 會檢查至少以下項目：

- `.autodev.yaml`
- `.opencode/runtime/control-plane.sqlite3`
- `AGENTS.md` managed markers

若 consumer project 已正確初始化，doctor 通常會輸出：

```text
autodev project: no changes needed
```

---

## 5. 使用方式（建議流程）

### 5.1 同步 GitHub issue packets

預設 tracker repo 是 `paulpai0412/wferp`。若要指定其他 repo：

```bash
AUTODEV_GITHUB_REPO=<owner/repo> PYTHONPATH=. python3 scripts/issue_packet_intake.py
```

若要用 fixture JSON 做本地測試：

```bash
PYTHONPATH=. python3 scripts/issue_packet_intake.py --issues-json /path/to/issues.json --output-dir /tmp/issue-packets
```

### 5.2 啟動指定 issue（高階 wrapper，建議用）

```bash
PYTHONPATH=. python3 scripts/autodev_project.py start --project-root <project> --issue-number <n>
```

這個 wrapper 會在目標 consumer project 內：

- 確保 checkpoint 檔存在
- 呼叫 `scripts/orchestrator_bootstrap_runner.py`
- 寫入 checkpoint / ledger / request
- 直接 dispatch root session

如果你已安裝全域命令，也可直接在 consumer project 內使用：

```text
/autodev-start <issue-number>
```

### 5.3 持續 reconcile（高階 wrapper，建議用）

```bash
PYTHONPATH=. python3 scripts/autodev_project.py reconcile --project-root <project>
```

這個 wrapper 會實際呼叫：

```bash
python3 -m scripts.orchestrator_supervisor reconcile \
  --ledger .opencode/runtime/orchestrator-ledger.json \
  --request .opencode/runtime/new-session-request.json \
  --session-result .opencode/runtime/new-session-result.json \
  --write-request \
  --dispatch-now \
  --source-session-id autodev-reconcile
```

也就是說，它不只做 decision，還會：

- 寫入下一個 request
- 直接 dispatch 下一個 session（若需要）

若已安裝全域命令，也可使用：

```text
/autodev-reconcile
```

### 5.4 查看目前 root session

```bash
PYTHONPATH=. python3 scripts/autodev_project.py show-session --project-root <project>
```

這會直接輸出 `.opencode/runtime/new-session-result.json` 的內容。

若已安裝全域命令，也可使用：

```text
/autodev-show-session
```

當 session result 內有 `rootSessionID` 時，通常可以用：

```bash
opencode --session <rootSessionID>
```

來進入對應 session。

### 5.5 專案 readiness 檢查

```bash
PYTHONPATH=. python3 scripts/autodev_project.py doctor --project-root <project>
```

若已安裝全域命令，也可使用：

```text
/autodev-doctor
```

---

## 6. 低階 / 進階操作說明

### 6.1 直接使用 bootstrap runner

```bash
PYTHONPATH=. python3 scripts/orchestrator_bootstrap_runner.py --issue-number <n> --dispatch-now --source-session-id auto-dev
```

這是 lower-level 啟動方式。它會：

- 定位 issue packet
- claim issue execution
- 更新 checkpoint
- 建立 ledger
- 建立 new session request
- 視需要直接 dispatch root session

### 6.2 直接使用 supervisor reconcile

只做 reconcile decision：

```bash
PYTHONPATH=. python3 scripts/orchestrator_supervisor.py reconcile --ledger .opencode/runtime/orchestrator-ledger.json
```

若要像高階 wrapper 一樣繼續往下寫 request 並 dispatch：

```bash
PYTHONPATH=. python3 scripts/orchestrator_supervisor.py reconcile \
  --ledger .opencode/runtime/orchestrator-ledger.json \
  --request .opencode/runtime/new-session-request.json \
  --session-result .opencode/runtime/new-session-result.json \
  --write-request \
  --dispatch-now \
  --source-session-id supervisor-reconcile
```

### 6.3 直接 dispatch 已存在的 request

```bash
PYTHONPATH=. python3 scripts/orchestrator_supervisor.py dispatch \
  --request .opencode/runtime/new-session-request.json \
  --session-result .opencode/runtime/new-session-result.json \
  --ledger .opencode/runtime/orchestrator-ledger.json \
  --source-session-id manual_dispatch
```

### 6.4 inspect：檢查 control-plane 狀態

```bash
PYTHONPATH=. python3 scripts/orchestrator_supervisor.py inspect --ledger .opencode/runtime/orchestrator-ledger.json
```

輸出內容包含：

- `issue`
- `latestDecision`
- `latestGitHubSyncAttempt`

### 6.5 quarantine：人工隔離 issue

```bash
PYTHONPATH=. python3 scripts/orchestrator_supervisor.py quarantine --ledger .opencode/runtime/orchestrator-ledger.json --reason <why>
```

### 6.6 resume-quarantined：恢復隔離 issue

```bash
PYTHONPATH=. python3 scripts/orchestrator_supervisor.py resume-quarantined --ledger .opencode/runtime/orchestrator-ledger.json --reason <why>
```

### 6.7 fail-quarantined：將隔離 issue 判定為失敗

```bash
PYTHONPATH=. python3 scripts/orchestrator_supervisor.py fail-quarantined --ledger .opencode/runtime/orchestrator-ledger.json --reason <why>
```

### 6.8 retry-github-sync：重試 GitHub label 同步

```bash
PYTHONPATH=. python3 scripts/orchestrator_supervisor.py retry-github-sync --ledger .opencode/runtime/orchestrator-ledger.json --command-id <id>
```

這個指令只會重播**已記錄且最新的失敗 GitHub sync attempt**，不是任意重放所有歷史操作。

---

## 7. 資料模型與 runtime 檔案

### 7.1 重要 runtime 檔案

- `.opencode/runtime/control-plane.sqlite3`
- `.opencode/runtime/orchestrator-ledger.json`
- `.opencode/runtime/new-session-request.json`
- `.opencode/runtime/new-session-result.json`
- `.opencode/runtime/issue-locks/issue-<n>.json`
- `docs/agents/runtime/context-checkpoint.yaml`

### 7.2 重要 artifact 類型

- issue packet：`docs/agents/issue-packets/issue-<n>.yaml`
- handoff：`docs/agents/handoffs/issue-<n>.yaml`
- worker result：`docs/agents/worker-results/...`
- evidence packet：`docs/agents/evidence/...`
- release result：`docs/agents/release-results/...`

### 7.3 SQLite 主要資料表

#### `issues`

保存每個 issue 的 canonical current state，例如：

- `state`
- `rank_score`
- `current_role`
- `current_stage`
- `current_root_session_id`
- `current_verifier_session_id`
- `attempts_json`
- `limits_json`
- `last_failure_json`
- `artifact_refs_json`
- `issue_packet_json`

#### `issue_history`

append-only audit table，用來保存：

- state transition
- root event
- session request / session result
- GitHub sync attempt
- admin decision

---

## 8. Issue state machine 與 label 規則

### 8.1 Canonical issue states

- `ready`
- `claimed`
- `dispatching`
- `running`
- `verifying`
- `completed`
- `failed`
- `quarantined`

### 8.2 常見 transition

- `ready -> claimed`
- `claimed -> dispatching`
- `dispatching -> running`
- `dispatching -> ready`
- `running -> verifying`
- `running -> quarantined`
- `verifying -> completed`
- `verifying -> failed`
- `quarantined -> running`
- `quarantined -> failed`

### 8.3 GitHub labels

- `ready-for-agent`
- `agent-dispatching`
- `agent-in-progress`
- `quarantined`

規則重點：

- GitHub label 是 coordination surface
- SQLite state 才是 canonical truth
- post-root-start 的 GitHub sync 失敗，不應把 live root session 靜默回滾成 `ready`

---

## 9. 快速開始（建議順序）

### 步驟 1：初始化 consumer project

```bash
PYTHONPATH=. python3 scripts/autodev_project.py init --project-root <project> --github-repo <owner/repo>
```

### 步驟 2：檢查環境

```bash
PYTHONPATH=. python3 scripts/autodev_project.py doctor --project-root <project>
```

### 步驟 3：同步 GitHub issues 成 issue packets

```bash
AUTODEV_GITHUB_REPO=<owner/repo> PYTHONPATH=. python3 scripts/issue_packet_intake.py
```

### 步驟 4：啟動指定 issue

```bash
PYTHONPATH=. python3 scripts/autodev_project.py start --project-root <project> --issue-number <n>
```

### 步驟 5：持續 reconcile

```bash
PYTHONPATH=. python3 scripts/autodev_project.py reconcile --project-root <project>
```

### 步驟 6：必要時查看 session / inspect / quarantine

```bash
PYTHONPATH=. python3 scripts/autodev_project.py show-session --project-root <project>
PYTHONPATH=. python3 scripts/orchestrator_supervisor.py inspect --ledger .opencode/runtime/orchestrator-ledger.json
```

---

## 10. 測試與驗證

### 10.1 全量 regression

```bash
pytest tests/scripts -q
```

### 10.2 單一腳本 focused regression

```bash
pytest tests/scripts/test_<script_name>.py -q
```

### 10.3 建議優先檢查的面向

- bootstrap runner 是否能正確寫出 checkpoint / ledger / request
- reconcile 是否能推進 state machine
- quarantine / resume / fail-quarantined 是否保持 canonical state 一致
- GitHub label sync failure 是否可 retry 且可稽核

---

## 11. 使用與維運注意事項

### 11.1 不要讓 `main_orchestrator` 直接做 issue 實作

`main_orchestrator` 只做 orchestration 與 contract routing。

### 11.2 同一 issue 不要啟動兩次

目前設計是 issue-scoped serial flow。若 issue 已在：

- `claimed`
- `dispatching`
- `running`
- `verifying`
- `quarantined`

就不應重複啟動。

### 11.3 GitHub 不是主真相來源

若 GitHub label 和 SQLite DB 看起來不一致，請以 SQLite control plane 為準，再視情況執行 `inspect` / `retry-github-sync`。

### 11.4 保持 artifacts 精簡

不要把 raw logs、browser trace、完整 transcript 直接貼進 repo docs 或 issue comments。

### 11.5 先修復環境，再做 recovery

若 `gh auth`、網路、`opencode` CLI、PATH、或 Python 執行環境本身有問題，請先修復環境，再執行 recovery / retry 操作。

### 11.6 Consumer project 不要複製 workflow 實作

consumer project 應保留：

- `.autodev.yaml`
- `docs/agents/...`
- `.opencode/runtime/...`

但不應複製 shared workflow scripts / templates / command docs 到專案內。

---

## 12. 建議先讀哪些文件

若要從使用一路讀到設計細節，建議依序閱讀：

1. `README.md`
2. `AGENTS.md`
3. `docs/agents/autonomous-development-workflow.yaml`
4. `docs/agents/runtime/orchestrator-control-plane-spec.md`
5. `docs/agents/runtime/nonstop-supervisor-loop.md`
6. `docs/agents/issue-tracker.md`
7. `scripts/autodev_project.py`
8. `scripts/orchestrator_bootstrap_runner.py`
9. `scripts/orchestrator_supervisor.py`

這樣可以先理解「怎麼用」，再往下看到「系統為什麼這樣設計」。
