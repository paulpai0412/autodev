# autodev 使用說明書（繁體中文）

## 1. 系統定位

`autodev` 是一套把 **GitHub issue → 自動派工 → 實作 → 驗證 → 發佈/收尾** 串成可恢復工作流的自治開發系統。

它不是單一 agent 腳本，而是由 orchestrator、worker、verifier、release worker、runtime artifacts、以及 SQLite control plane 共同組成的執行架構。

目前這個 repo 的實作基線是：

- 依 `docs/agents/runtime/orchestrator-control-plane-spec.md` 完成 control-plane 落地
- `.opencode/runtime/control-plane.sqlite3` 是 canonical control plane
- JSON runtime artifacts 仍保留，作為既有流程接縫
- operator recovery / quarantine / GitHub sync retry 已可用

---

## 2. 系統架構

### 2.1 角色分工

#### `main_orchestrator`

負責協調，不直接實作 issue。本角色的責任是：

- 選擇可執行的 issue
- 建立與消費 runtime request/result artifacts
- 驅動 `issue_worker`、`pr_verifier`、`release_worker`
- 透過 supervisor 推進整體流程與恢復路徑

#### `issue_worker`

負責實際實作 issue 內容，例如改程式、補測試、產出 worker result。

#### `pr_verifier`

負責驗證 worker 產物是否滿足 acceptance criteria，並產出 verifier-owned evidence。它不是 root orchestrator，也不應被 root 直接取代。

#### `release_worker`

在 verifier 通過後負責合併、收尾、workspace hygiene 與 issue 關閉前的最後一段流程。

---

### 2.2 控制平面與 runtime artifacts

#### SQLite control plane

主要控制平面資料庫位於：

- `.opencode/runtime/control-plane.sqlite3`

它保存：

- scheduler lease
- issue lifecycle state
- issue events
- decision log
- GitHub sync attempts
- issue ranking / selection 結果

這是系統的 **canonical truth**。

#### JSON runtime artifacts

流程仍保留以下 artifacts：

- `.opencode/runtime/orchestrator-ledger.json`
- `.opencode/runtime/new-session-request.json`
- `.opencode/runtime/new-session-result.json`

這些檔案的角色不是取代 DB，而是保留 orchestrator 與新 session dispatch 的既有接縫。

#### Issue packets

本地 issue 封包位於：

- `docs/agents/issue-packets/issue-<n>.yaml`

這是 worker 實作時的重要輸入來源。若本地沒有 packet，supervisor/bootstrapping 會在允許的情況下嘗試從 GitHub intake 一次。

---

### 2.3 高層流程

1. GitHub 上標記為 `ready-for-agent` 的 issue 先被 materialize 成本地 issue packet。
2. bootstrap runner 針對指定 issue 啟動新的 `main_orchestrator` root session。
3. `main_orchestrator` 在同一條 orchestrator 路徑中，依序委派 `issue_worker`、`pr_verifier`、`release_worker`。
4. 每次子任務產出 compact artifact 後，supervisor 透過 `reconcile` 讀取 artifacts、同步 control plane、決定下一步。
5. 若 root session heartbeat 過期、流程不一致、或 GitHub sync 出錯，系統走 quarantine / recovery 路徑，而不是直接失控。

---

## 3. 主要功能

### 3.1 自動 issue 選擇與派工

系統根據 canonical DB 中的 ranking、labels、runtime state 與本地 packet 可用性選擇 issue。失格 issue 會被寫成 `rank_score = -1`，避免被錯誤派工。

### 3.2 嚴格 issue state machine

issue lifecycle 採用明確 canonical states：

- `ready`
- `claimed`
- `dispatching`
- `running`
- `verifying`
- `completed`
- `failed`
- `quarantined`

重要 transition 範例：

- `ready -> claimed`
- `claimed -> dispatching`
- `dispatching -> running`
- `running -> verifying`
- `running -> quarantined`
- `verifying -> completed`
- `verifying -> failed`
- `quarantined -> running`
- `quarantined -> failed`

這保證 supervisor 與 operator 動作都不會繞過合法狀態轉移。

### 3.3 Scheduler lease 與單一控制權

control plane 只允許單一 active scheduler 持有 lease。當 lease TTL 過期，新的 scheduler 才能接手，避免重複 dispatch。

### 3.4 GitHub label 同步

GitHub 仍是 operator 可視化與人工防呆的重要協調面，但不是主真相來源。主要 coordination labels 為：

- `ready-for-agent`
- `agent-dispatching`
- `agent-in-progress`
- `quarantined`

dispatch-critical 狀態與 GitHub label 同步是強耦合的：若 DB transition 成功但關鍵 GitHub label 更新失敗，系統必須回滾該 transition。

### 3.5 Recovery / quarantine

當遇到 heartbeat timeout、runtime inconsistency、或其他不可安全分類的問題時，issue 會轉入 `quarantined`，待 operator 決定恢復或結案。

### 3.6 稽核與可追蹤性

每次 scheduler 決策都會有 command/decision ID，並關聯到：

- DB state transition
- GitHub sync attempt
- decision log

因此重試與恢復可以做到可追蹤、可去重、可稽核。

---

## 4. 安裝與初始化

### 4.1 最小驗證安裝

```bash
python3 -m pip install pytest
```

如果你要跑更完整的 repo setup，請以 repo 內各 runbook 所要求的依賴為準。

### 4.2 初始化 consumer project

把某個專案接入 autodev：

```bash
PYTHONPATH=. python3 scripts/autodev_project.py init --project-root <project> --github-repo <owner/repo>
```

例如：

```bash
PYTHONPATH=. python3 scripts/autodev_project.py init --project-root /path/to/project --github-repo myorg/myrepo
```

初始化後，consumer project 會具備：

- `.autodev.yaml`
- `docs/agents/...` 基本目錄
- `.opencode/runtime/`
- `.opencode/runtime/control-plane.sqlite3`

目前 `init` 也會一併完成 repository bootstrap：

- 若目標目錄尚未是 git repo，會自動以 `main` 初始化本地 git repository
- 自動設定 `origin` 為 `https://github.com/<owner/repo>.git`
- 若指定的 GitHub repository 尚不存在，會自動建立該 repo
- 自動補齊 autodev 核心 workflow labels，例如 `needs-triage`、`ready-for-agent`、`agent-dispatching`、`agent-in-progress`、`quarantined`

這代表 `init` 現在不只建立 consumer project 契約，也會把專案接到可用的 git / GitHub tracker 基礎設施。

### 4.3 安裝全域 OpenCode 指令

```bash
PYTHONPATH=. python3 scripts/autodev_project.py install-commands
```

安裝後可從 consumer project 使用：

- `/autodev-start <issue-number>`
- `/autodev-reconcile`
- `/autodev-show-session`
- `/autodev-doctor`

### 4.4 專案健康檢查

```bash
PYTHONPATH=. python3 scripts/autodev_project.py doctor --project-root <project>
```

doctor 會檢查：

- `.autodev.yaml`
- runtime 目錄
- control-plane DB
- legacy local workflow residue
- 命令安裝情況

如果 `init` 已成功完成，doctor 應該能在乾淨 consumer project 上回報 `autodev project: no changes needed`。

### 4.5 舊版本地 workflow 遷移

先看 dry-run：

```bash
PYTHONPATH=. python3 scripts/autodev_project.py migrate --project-root <project> --dry-run
```

確認後再移除舊檔：

```bash
PYTHONPATH=. python3 scripts/autodev_project.py migrate --project-root <project> --remove-legacy
```

---

## 5. 日常使用流程

### 5.1 從 GitHub materialize issue packets

預設 tracker repo 是 `paulpai0412/wferp`。若要指定其他 repo：

```bash
AUTODEV_GITHUB_REPO=<owner/repo> PYTHONPATH=. python3 scripts/issue_packet_intake.py
```

先決條件：

- 已安裝 `gh`
- `gh auth status --repo <owner/repo>` 可成功
- 執行主機能連 GitHub

### 5.2 啟動指定 issue

```bash
PYTHONPATH=. python3 scripts/orchestrator_bootstrap_runner.py --issue-number <n> --dispatch-now --source-session-id auto-dev
```

例如：

```bash
PYTHONPATH=. python3 scripts/orchestrator_bootstrap_runner.py --issue-number 32 --dispatch-now --source-session-id auto-dev
```

這會：

- 建立或更新 checkpoint / ledger
- claim issue execution
- 建立新的 root orchestrator session request
- dispatch `main_orchestrator`

### 5.3 持續 reconcile runtime

當 worker / verifier / release 有新 artifact 後，執行：

```bash
PYTHONPATH=. python3 scripts/orchestrator_supervisor.py reconcile --ledger .opencode/runtime/orchestrator-ledger.json --source-session-id supervisor-reconcile
```

supervisor 會：

- 讀取 ledger 與 session result
- 同步到 SQLite control plane
- 寫入 root events
- 推進 issue state machine
- 決定下一個 subagent 或 recovery 動作

### 5.4 查看目前 root session

如果已安裝全域命令，可從 consumer project 使用：

- `/autodev-show-session`

若使用 Python entrypoint，則可透過 `autodev_project.py show-session` 讀取目前 root session 資訊。

---

## 6. Operator 操作手冊

### 6.1 inspect：檢查 control-plane 狀態

```bash
PYTHONPATH=. python3 scripts/orchestrator_supervisor.py inspect --ledger .opencode/runtime/orchestrator-ledger.json
```

可用來查看：

- active scheduler lease
- canonical issue state
- latest decision
- latest GitHub sync attempt

### 6.2 quarantine：人工隔離執行中 issue

```bash
PYTHONPATH=. python3 scripts/orchestrator_supervisor.py quarantine --ledger .opencode/runtime/orchestrator-ledger.json --reason <why>
```

適合用在：

- root session 卡住
- heartbeat 逾時後要保守處理
- issue 進入不可信狀態

### 6.3 resume-quarantined：恢復隔離 issue

```bash
PYTHONPATH=. python3 scripts/orchestrator_supervisor.py resume-quarantined --ledger .opencode/runtime/orchestrator-ledger.json --reason <why>
```

此動作會執行 fenced resume，把 issue 從 `quarantined` 恢復到 `running`。

### 6.4 fail-quarantined：將隔離 issue 判定失敗

```bash
PYTHONPATH=. python3 scripts/orchestrator_supervisor.py fail-quarantined --ledger .opencode/runtime/orchestrator-ledger.json --reason <why>
```

適合用在 recovery policy 已耗盡、或已確認該次執行不能安全恢復時。

### 6.5 retry-github-sync：重試 GitHub label 同步

```bash
PYTHONPATH=. python3 scripts/orchestrator_supervisor.py retry-github-sync --ledger .opencode/runtime/orchestrator-ledger.json --command-id <id>
```

這個指令只會重播 DB 中已記錄、最新且失敗的 GitHub sync attempt，不是任意重放所有歷史動作。

---

## 7. Runtime artifact 與資料表說明

### 7.1 重要檔案

- `.opencode/runtime/control-plane.sqlite3`：canonical control plane
- `.opencode/runtime/orchestrator-ledger.json`：目前流程與 issue 上下文總帳
- `.opencode/runtime/new-session-request.json`：新 root session 的 dispatch request
- `.opencode/runtime/new-session-result.json`：新 root session 建立結果
- `docs/agents/issue-packets/issue-<n>.yaml`：issue 執行封包

### 7.2 重要資料表

- `scheduler_leases`
- `issues`
- `issue_events`
- `decision_log`
- `github_sync_attempts`

可簡單理解為：

- `scheduler_leases`：誰現在持有控制權
- `issues`：每個 issue 的 canonical lifecycle state
- `issue_events`：root session 寫入的事實事件
- `decision_log`：scheduler/operator 做過的決策
- `github_sync_attempts`：GitHub label 同步成功或失敗紀錄

---

## 8. 快速開始

第一次使用，建議照以下順序：

### 步驟 1：初始化專案

```bash
PYTHONPATH=. python3 scripts/autodev_project.py init --project-root <project> --github-repo <owner/repo>
```

### 步驟 2：執行 doctor

```bash
PYTHONPATH=. python3 scripts/autodev_project.py doctor --project-root <project>
```

### 步驟 3：同步 GitHub issue packets

```bash
AUTODEV_GITHUB_REPO=<owner/repo> PYTHONPATH=. python3 scripts/issue_packet_intake.py
```

### 步驟 4：啟動 issue

```bash
PYTHONPATH=. python3 scripts/orchestrator_bootstrap_runner.py --issue-number <n> --dispatch-now --source-session-id auto-dev
```

### 步驟 5：持續 reconcile

```bash
PYTHONPATH=. python3 scripts/orchestrator_supervisor.py reconcile --ledger .opencode/runtime/orchestrator-ledger.json --source-session-id supervisor-reconcile
```

### 步驟 6：異常時 inspect / quarantine / resume

```bash
PYTHONPATH=. python3 scripts/orchestrator_supervisor.py inspect --ledger .opencode/runtime/orchestrator-ledger.json
```

---

## 9. 使用與維運注意事項

### 9.1 不要讓 `main_orchestrator` 直接做 issue 實作

`main_orchestrator` 是 orchestration-only。它負責路由與契約驗證，不直接取代 worker / verifier。

### 9.2 同一 issue 不應被啟動兩次

autodev 的 issue 執行是 issue-scoped serial flow。同一 issue 不應在 `agent-in-progress` 或相關中間態期間再次啟動。

### 9.3 GitHub 是協調面，不是主真相來源

請以 SQLite control plane 為主。GitHub labels 是協調與可視化工具，不應被視為唯一真相。

### 9.4 保持 artifacts 精簡

repo 中各種 handoff / worker result / evidence packet 都採 compact artifact 規則。不要把完整測試 log、trace、截圖、SQL log 直接貼進 repo docs 或 issue comments。

### 9.5 先修復環境，再做 recovery

若 `gh auth`、網路、或 command install 本身有問題，先修復環境，再執行 `retry-github-sync`、`resume-quarantined` 等 operator 動作，否則容易重複失敗。

---

## 10. 建議先讀文件

若要更深入理解這套系統，建議依序閱讀：

1. `README.md`
2. `AGENTS.md`
3. `docs/agents/autonomous-development-workflow.yaml`
4. `docs/agents/runtime/orchestrator-control-plane-spec.md`
5. `docs/agents/runtime/nonstop-supervisor-loop.md`
6. `docs/agents/issue-tracker.md`
7. `scripts/autodev_project.py`
8. `scripts/orchestrator_bootstrap_runner.py`
9. `scripts/orchestrator_supervisor.py`

這樣可以從「如何使用」一路看到「為什麼這樣設計」。
