# autodev 系統說明書（繁體中文）

> 本文件以目前 repo 版本為準，對應此 workspace 內 `scripts/`、`docs/agents/` 與 `tests/scripts/` 的實作與命令 surface。

---

## 1. 系統定位

`autodev` 是一套把 **GitHub issue → 自動派工 → 實作 → 驗證 → 發佈 / 收尾 → 恢復** 串成可恢復工作流的自治開發 harness。

它不是單一 agent prompt，也不是單一排程腳本，而是一個由下列部分共同組成的執行系統：

- `main_orchestrator` / `issue_worker` / `pr_verifier` / `release_worker` 角色分工
- SQLite control plane（`issues` / `issue_history`）
- host adapter 與 operator 命令 surface
- GitHub issue / label 協調面
- operator recovery / quarantine / retry 命令

目前版本的核心設計原則是：

- **SQLite 是唯一 canonical control plane**
- GitHub 是協調面，不是主真相來源
- runtime control 只允許存在於 SQLite `issues` / `issue_history`
- 舊有 JSON / YAML runtime artifact 若仍存在，只能是歷史投影或 compatibility surface，不能成為 workflow progress 的必要條件
- `main_orchestrator` 負責協調，不直接做 issue 實作
- `issues.current_session_id` 是唯一 current session pointer
- OpenCode 只是目前 shipped 的 default host adapter，不是 control-plane schema 的來源
- 執行模型是 **bounded issue-scoped concurrency**：多個 issue 可並行，但同一 issue 永遠只能有一條 active root orchestrator / development path

---

## 2. 整體系統架構

### 2.1 角色分工

#### `main_orchestrator`

`main_orchestrator` 是 orchestration-only 角色，負責：

- 選擇可執行 issue
- 啟動或恢復 root session
- 委派 development loop 內的 `issue_worker`、`pr_verifier`
- 驅動 `reconcile` / recovery / quarantine 路徑
- 維持整體流程契約一致性

它**不直接實作 issue scope**，也**不自己做最終驗收**。

#### `issue_worker`

負責實際實作 issue 內容，例如：

- 修改程式碼
- 補測試
- 產出 `worker_result` DB fact

#### `pr_verifier`

負責 verifier-owned 驗證與 evidence 建立，例如：

- 驗證 acceptance criteria 是否成立
- 檢查 worker 產物是否達到要求
- 產出 verifier-owned `evidence_packet` fact，並在驗收通過後擁有 formal PR creation

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

### 2.2 控制平面與歷史投影

#### SQLite control plane（主真相）

主要資料庫位於：

- `.opencode/runtime/control-plane.sqlite3`

目前版本中，它是整個系統的 **canonical truth**，用來保存：

- issue lifecycle state
- issue history / audit trail
- dispatch request / dispatch result / root event
- GitHub sync attempt
- issue ranking / selection 結果
- runtime context、artifact refs、issue packet 內容與歷史 payload
- verifier-owned PR facts 與 release 決策

#### 歷史 artifact 與 payload 投影（非 runtime 真相）

在 `db-only-control-plane` branch，唯一有效的控制平面是：

- `.opencode/runtime/control-plane.sqlite3`
- `issues`
- `issue_history`

`issue_history` 會保存過去分散在 packet / handoff / worker result / evidence / release result 中的 compact payload 與 audit fact。若 repo 內仍殘留對應檔案或模板，應只視為歷史投影或 compatibility surface。

#### issue packet / handoff / result 類歷史投影

若下列 artifact 目錄仍存在，應只視為歷史投影或 compatibility surface，不可作為 runtime progress 的必要條件：

- `docs/agents/issue-packets/`
- `docs/agents/handoffs/`
- `docs/agents/worker-results/`
- `docs/agents/evidence/`
- `docs/agents/release-results/`

這些 artifact 採 **compact / index-only** 原則：

- 可以保存摘要與 reference
- 不應直接把 raw log、完整 trace、SQL log、截圖或 transcript 貼進 repo

`docs/agents/runtime/` 是分支契約文件所在，不屬於 runtime state；真正的 runtime state 仍只在 SQLite。

---

### 2.3 目前版本的模組分層

#### 入口模組（entrypoints）

| 模組 | 作用 |
|---|---|
| `scripts/autodev_project.py` | consumer project 初始化、安裝全域命令、doctor、`start` / `reconcile` / `show-session` wrapper |
| `scripts/orchestrator_bootstrap_runner.py` | 針對指定 issue 進行 DB-backed bootstrap 與 root session dispatch |
| `scripts/orchestrator_supervisor.py` | supervisor CLI surface、runtime reconcile 總控、operator commands |
| `scripts/issue_packet_intake.py` | 從 GitHub ready-for-agent issue 同步 SQLite-backed intake inputs |

#### supervisor 拆分後的 helper 模組

| 模組 | 作用 |
|---|---|
| `scripts/orchestrator_artifacts.py` | compact parsing / compatibility helper；不是 runtime source of truth |
| `scripts/orchestrator_sessions.py` | host-neutral session facade，提供 default host adapter 與共用型別 |
| `scripts/opencode_host_adapter.py` | 目前 shipped 的 OpenCode adapter 實作 |
| `scripts/orchestrator_lifecycle.py` | issue claim / lifecycle transition / GitHub label sync / quarantine |
| `scripts/orchestrator_requests.py` | prompt 與 session request builder |
| `scripts/orchestrator_selection.py` | issue packet sync、selection、intake 協調 |
| `scripts/orchestrator_reconcile.py` | transition / recovery helpers、role-specific branch handlers、main-orchestrator branch handlers |

#### 測試模組

- `tests/scripts/`：針對每個核心腳本的 regression tests

---

### 2.4 高層執行流程

1. GitHub 上標記為 `ready-for-agent` 的 issue 被 intake 到 SQLite control plane
2. workspace reconcile 依 development capacity 從 `ready` issues 中做 deterministic selection，並以 `ready -> claimed` 作為 DB fence
3. bootstrap runner 或 supervisor 為被選中的 issue 啟動 root session
4. fresh `main_orchestrator` 進入 issue flow 後，由 DB-backed reconcile 推進 bootstrap → `issue_worker_execution`
5. `main_orchestrator` 在同一個 root session 內依序委派 development loop 子任務：
    - `issue_worker`
    - `pr_verifier`
    - 以上子任務應以 `task(..., run_in_background=false)` 前景執行，讓同一個 root orchestrator session 逐一等待完成後再繼續
    - child role 的 outcome 必須用 `scripts/orchestrator_supervisor.py submit-artifact` 之類的 DB-backed submission 寫回 SQLite；repo-local artifact file 不是必要 runtime gate
    - `issue_worker` 成功只代表 implementation-ready；formal PR creation 與 acceptance gate 由 verifier path 擁有
6. `pr_verifier` 通過後 issue 停在 `verified`；PR merge / release 由獨立 `/autodev-release [issue-number]` 命令 claim 成 `release_pending` 後啟動 `release_worker`
7. 每次有新的 DB-backed fact / artifact submission 後，supervisor 執行 `reconcile`
8. `reconcile` 會同步 SQLite control plane，判斷下一步是：
   - 繼續派工
   - recovery
   - quarantine
   - 進入 `verified` / `release_pending`
   - 完成 / 失敗
9. 已 `verified` 或 `release_pending` 的 issue 不應阻塞其他 `ready` issue 進入 development loop

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

### 3.2 全域 autodev host 命令安裝（目前預設 OpenCode adapter）

`autodev_project.py install-commands` 會安裝以下全域命令：

- `/autodev-start <issue-number>`
- `/autodev-reconcile`
- `/autodev-release [issue-number]`
- `/autodev-show-session`
- `/autodev-doctor`

### 3.3 自動 issue packet intake

`issue_packet_intake.py` 會：

- 從 GitHub 讀取 `ready-for-agent` issue
- 同步 issue 資訊到 SQLite-backed intake surface
- 推導 branch name、parent reference、dependencies、acceptance criteria

### 3.4 嚴格 issue state machine

目前 canonical states：

- `ready`
- `claimed`
- `dispatching`
- `running`
- `verifying`
- `verified`
- `release_pending`
- `completed`
- `failed`
- `quarantined`

這些狀態保證 duplicate-start prevention、recovery 與 audit 都有明確規則。

### 3.5 Duplicate-start 防護

duplicate-start 的 canonical 防護來自 SQLite `issues.state`，不是 lease table。

此 branch 的 duplicate-start canonical 防護完全來自 SQLite state；issue-lock projection 檔不再是 active runtime contract。

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
- runtime artifact refs / PR facts / release decisions

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

### 4.4 安裝全域 autodev host 命令（目前預設 OpenCode adapter）

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

### 5.1 同步 GitHub ready issues 到 SQLite intake

預設 tracker repo 是 `paulpai0412/wferp`。請把 intake 指到 consumer project，這樣 ready issue 會直接同步進該專案的 SQLite-backed intake flow。若要指定其他 repo：

```bash
AUTODEV_GITHUB_REPO=<owner/repo> PYTHONPATH=. python3 scripts/issue_packet_intake.py --project-root <project>
```

若要用 fixture JSON 做本地測試：

```bash
PYTHONPATH=. python3 scripts/issue_packet_intake.py --issues-json /path/to/issues.json --output-dir /tmp/issue-packets
```

其中 `--output-dir` 已是 deprecated compatibility flag；DB-backed intake 會忽略它，不再輸出 issue packet 檔案。

### 5.2 啟動指定 issue（高階 wrapper，建議用）

```bash
PYTHONPATH=. python3 scripts/autodev_project.py start --project-root <project> --issue-number <n>
```

這個 wrapper 會在目標 consumer project 內：

- 呼叫 `scripts/orchestrator_bootstrap_runner.py`
- 同步 DB-backed dispatch context
- 直接 dispatch root session

如果你已安裝全域命令，也可直接在 consumer project 內使用：

```text
/autodev-start <issue-number>
```

### 5.3 持續 reconcile（高階 wrapper，建議用）

```bash
PYTHONPATH=. python3 scripts/autodev_project.py reconcile --project-root <project>
```

這個 wrapper 會以 DB-backed issue state 執行 workspace reconcile，先處理所有 active / fenced issues，再視 available development capacity 啟動新的 ready issue；整個流程不依賴任何本地 ledger / request / session-result artifact。

若你需要直接使用低階命令，對應的是：

```bash
PYTHONPATH=. python3 scripts/orchestrator_supervisor.py reconcile-workspace --base-dir <project>
```

若已安裝全域命令，也可使用：

```text
/autodev-reconcile
```

若要讓 workspace 持續自動補位，可使用 watch wrapper：

```bash
PYTHONPATH=. python3 scripts/autodev_project.py reconcile-watch --project-root <project> --interval-seconds 30
```

`reconcile-watch` 不改變 supervisor 的核心排程邏輯；它只是定期重跑 DB-backed `reconcile-workspace`。測試或短期執行時可加上 `--iterations <n>` 限制輪數，若希望任何一輪失敗就停止，可加上 `--stop-on-error`。

### 5.4 查看目前 root session

```bash
PYTHONPATH=. python3 scripts/autodev_project.py show-session --project-root <project>
```

這會從 SQLite control plane 輸出目前 issue 的 session / resume 資訊。

若已安裝全域命令，也可使用：

```text
/autodev-show-session
```

當目前 host adapter 是 OpenCode，且 session payload 內有 `rootSessionID` 時，通常可以用：

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
PYTHONPATH=. python3 scripts/orchestrator_bootstrap_runner.py --base-dir <project> --issue-number <n> --source-session-id auto-dev
```

這是 lower-level 啟動方式。它會：

- 從 SQLite control plane 取得 issue context
- claim issue execution
- 建立 DB-backed dispatch context
- 直接 dispatch root session

### 6.2 直接使用 supervisor reconcile

```bash
PYTHONPATH=. python3 scripts/orchestrator_supervisor.py reconcile --base-dir <project> --issue-number <n>
```

這是單一 issue 的低階 reconcile。若要讓 supervisor 在整個 workspace 內先處理 active issues、再補滿 development capacity，請使用：

```bash
PYTHONPATH=. python3 scripts/orchestrator_supervisor.py reconcile-workspace --base-dir <project>
```

### 6.3 inspect：檢查 control-plane 狀態

```bash
PYTHONPATH=. python3 scripts/orchestrator_supervisor.py inspect --base-dir <project> --issue-number <n>
```

輸出內容包含：

- `issue`
- `latestDecision`
- `latestGitHubSyncAttempt`

### 6.4 quarantine：人工隔離 issue

```bash
PYTHONPATH=. python3 scripts/orchestrator_supervisor.py quarantine --base-dir <project> --issue-number <n> --reason <why>
```

### 6.5 resume-quarantined：恢復隔離 issue

```bash
PYTHONPATH=. python3 scripts/orchestrator_supervisor.py resume-quarantined --base-dir <project> --issue-number <n> --reason <why>
```

### 6.6 fail-quarantined：將隔離 issue 判定為失敗

```bash
PYTHONPATH=. python3 scripts/orchestrator_supervisor.py fail-quarantined --base-dir <project> --issue-number <n> --reason <why>
```

### 6.7 retry-github-sync：重試 GitHub label 同步

```bash
PYTHONPATH=. python3 scripts/orchestrator_supervisor.py retry-github-sync --base-dir <project> --issue-number <n> --command-id <id>
```

這個指令只會重播**已記錄且最新的失敗 GitHub sync attempt**，不是任意重放所有歷史操作。

### 6.8 retry-failed：重試可重試的 failed issue

```bash
PYTHONPATH=. python3 scripts/orchestrator_supervisor.py retry-failed --base-dir <project> --issue-number <n> --reason <why>
```

這個指令會把符合條件的 failed issue 移回可再調度狀態，並把操作記錄進 SQLite audit trail。

---

## 7. 資料模型與歷史 artifact

### 7.1 重要 runtime 儲存面

- `.opencode/runtime/control-plane.sqlite3`
- `issues`
- `issue_history`

### 7.2 歷史 artifact 類型

- 若仍存在的 issue packet / handoff / worker result / evidence packet / release result 檔案，應只視為歷史投影或 compatibility artifact，而非 canonical runtime input
- runtime progress 必須只依賴 SQLite `issues` / `issue_history`

### 7.3 SQLite 主要資料表

#### `issues`

保存每個 issue 的 canonical current state，例如：

- `state`
- `rank_score`
- `lane`
- `current_role`
- `current_stage`
- `current_session_id`
- `attempts_json`
- `limits_json`
- `last_failure_json`
- `runtime_context_json`
- `issue_packet_json`

#### `issue_history`

append-only audit table，用來保存：

- state transition
- root event
- dispatch / artifact / release / admin / github sync facts
- GitHub sync attempt
- admin decision
- `pr_opened` 與其他 verifier-owned acceptance / release facts

---

## 8. Issue state machine 與 label 規則

### 8.1 Canonical issue states

- `ready`
- `claimed`
- `dispatching`
- `running`
- `verifying`
- `verified`
- `release_pending`
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
- `verifying -> verified`
- `verifying -> failed`
- `verifying -> quarantined`
- `verified -> release_pending`
- `verified -> completed`
- `release_pending -> completed`
- `release_pending -> failed`
- `quarantined -> running`
- `quarantined -> claimed`
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

### 步驟 3：同步 GitHub issues 進 SQLite intake

```bash
AUTODEV_GITHUB_REPO=<owner/repo> PYTHONPATH=. python3 scripts/issue_packet_intake.py --project-root <project>
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
PYTHONPATH=. python3 scripts/orchestrator_supervisor.py inspect --base-dir <project> --issue-number <n>
```

---

## 10. 測試與驗證

### 10.1 全量 regression

```bash
python3 -m pytest tests/scripts -q
```

### 10.2 單一腳本 focused regression

```bash
python3 -m pytest tests/scripts/test_<script_name>.py -q
```

### 10.3 建議優先檢查的面向

- bootstrap runner 是否能正確建立 DB-backed dispatch context 並啟動 root session
- reconcile 是否能推進 state machine
- quarantine / resume / fail-quarantined 是否保持 canonical state 一致
- GitHub label sync failure 是否可 retry 且可稽核

---

## 11. 使用與維運注意事項

### 11.1 不要讓 `main_orchestrator` 直接做 issue 實作

`main_orchestrator` 只做 orchestration 與 contract routing。

### 11.2 同一 issue 不要啟動兩次；不同 issue 可並行

目前設計是 bounded issue-scoped concurrency。也就是說：

- 不同 issue 可以在同一 workspace 內並行
- 同一 issue 若已在下列任一狀態，就不應重複啟動：

- `claimed`
- `dispatching`
- `running`
- `verifying`

`quarantined` issue 仍會被 fenced 避免 duplicate-start，但不應無限期佔用 development slot。

SQLite `issues.state` 與 `issues.current_session_id` 是 duplicate-start fence 的 canonical 來源，不是 file lock。

### 11.3 GitHub 不是主真相來源

若 GitHub label 和 SQLite DB 看起來不一致，請以 SQLite control plane 為準，再視情況執行 `inspect` / `retry-github-sync`。

### 11.4 保持歷史投影與 evidence 精簡

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
3. `docs/agents/runtime/db-only-control-plane-spec.md`
4. `docs/agents/runtime/db-only-control-plane-implementation-plan.md`
5. `docs/agents/runtime/host-adapter-strategy.md`
6. `docs/agents/runtime/product-positioning.md`
7. `docs/agents/runtime/multi-issue-concurrency.md`
8. `docs/agents/autonomous-development-workflow.yaml`
9. `docs/agents/issue-tracker.md`
10. `scripts/autodev_project.py`
11. `scripts/orchestrator_bootstrap_runner.py`
12. `scripts/orchestrator_supervisor.py`

這樣可以先理解「怎麼用」，再往下看到這個 branch 的 DB-only runtime 為什麼這樣設計。
