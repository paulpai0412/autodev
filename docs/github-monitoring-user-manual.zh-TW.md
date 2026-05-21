# GitHub 專案進度監控使用手冊（繁體中文）

> 適用對象：想用 GitHub 原生 UI（Issue / Comment / Label / Projects V2 / PR / Release）監控 autodev 專案進度的 PM、Tech Lead、Operator。

> 核心原則：**SQLite 是真相源、GitHub 是投影面**。若兩者不一致，請以 SQLite control plane 為準，再執行重試或 reconcile。

---

## 1. 你會看到什麼（監控全景）

在這套流程裡，GitHub 用來呈現進度，不直接當控制平面。你在 GitHub 會看到三個主要監控面：

1. **Issue body snapshot**（固定區塊）
   - 用 `<!-- autodev:projection:start --> ... <!-- autodev:projection:end -->` 包起來
   - 顯示目前 state、role/stage/status、dependencies、最新 DB ref

2. **Sticky status comment**（固定單一留言）
   - 用 `<!-- autodev:status-comment -->` 識別
   - 會更新同一則留言，不應每次新增新留言洗版

3. **Projects V2 欄位**
   - 最小必要欄位：workflow state、stage、最後同步時間（與你的欄位設計對應）

搭配既有 GitHub Labels：

- `ready-for-agent`
- `agent-dispatching`
- `agent-in-progress`
- `quarantined`

---

## 2. 先決條件（第一次設定）

### 2.1 GitHub CLI 與權限

```bash
gh auth status --repo <owner/repo>
```

必須可通過，否則 intake / 同步 / 關單都會失敗。

### 2.2 專案初始化

```bash
PYTHONPATH=. python3 scripts/autodev_project.py init --project-root <project> --github-repo <owner/repo>
PYTHONPATH=. python3 scripts/autodev_project.py doctor --project-root <project>
```

### 2.3（選用）Projects V2 同步設定

目前最小同步需要你提供：

- `github_project_id`
- `github_project_field_ids`（至少 state / stage 的 field id）

可以放在 issue runtime context（程式會讀），或設定環境變數：

```bash
export AUTODEV_GITHUB_PROJECT_ID=<project_node_id>
```

---

## 3. 日常監控 SOP（建議每天照這個跑）

### Step A：同步 GitHub ready issues 進 SQLite

```bash
AUTODEV_GITHUB_REPO=<owner/repo> PYTHONPATH=. python3 scripts/issue_packet_intake.py --project-root <project>
```

### Step B：執行 workspace reconcile（推進狀態）

```bash
PYTHONPATH=. python3 scripts/autodev_project.py reconcile --project-root <project>
```

### Step C：觀察 GitHub 監控面

對每個目標 issue 檢查：

1. body snapshot 是否更新（marker 區塊）
2. sticky status comment 是否是同一則被更新
3. labels 是否符合目前 state
4. Projects V2 欄位是否反映 state/stage

### Step D：若有異常，先看 SQLite 再修復

```bash
PYTHONPATH=. python3 scripts/orchestrator_supervisor.py inspect --base-dir <project> --issue-number <n>
```

重點看：

- `issue`
- `latestGitHubSyncAttempt`
- `latestDecision`

---

## 4. 監控解讀表（GitHub 看到什麼代表什麼）

| GitHub 現象 | 代表含義 | 下一步 |
|---|---|---|
| `agent-dispatching` label | root session dispatch 中 | 等待進入 `agent-in-progress` |
| `agent-in-progress` label | issue 已進入 active 開發路徑 | 看 sticky comment 與 body snapshot |
| `quarantined` label | issue 被隔離，需人工介入 | inspect + resume/fail |
| body snapshot 有最新 DB ref | 投影更新成功 | 可追蹤下一個 gate |
| sticky comment 不更新但 state 在變 | comment sync 可能失敗 | 看 `latestGitHubSyncAttempt` |
| Projects 欄位未更新 | 可能缺 project 綁定或欄位 id 設定 | 檢查 project id/field id |

---

## 5. 常用操作指令（監控與修復）

### 5.1 看目前 session（快速定位誰在跑）

```bash
PYTHONPATH=. python3 scripts/autodev_project.py show-session --project-root <project>
```

### 5.2 Issue 細查（最重要）

```bash
PYTHONPATH=. python3 scripts/orchestrator_supervisor.py inspect --base-dir <project> --issue-number <n>
```

### 5.3 重試 GitHub 同步（針對已失敗且最新的一筆）

```bash
PYTHONPATH=. python3 scripts/orchestrator_supervisor.py retry-github-sync --base-dir <project> --command-id <failed_command_id>
```

### 5.4 隔離與恢復

```bash
PYTHONPATH=. python3 scripts/orchestrator_supervisor.py quarantine --base-dir <project> --issue-number <n> --reason "<why>"
PYTHONPATH=. python3 scripts/orchestrator_supervisor.py resume-quarantined --base-dir <project> --issue-number <n> --reason "<why>"
PYTHONPATH=. python3 scripts/orchestrator_supervisor.py fail-quarantined --base-dir <project> --issue-number <n> --reason "<why>"
```

---

## 6. 建議的 GitHub Projects V2 欄位設計（最小可用）

建議先只做這幾欄，避免過度複雜：

1. `Workflow State`（single-select）
   - ready / claimed / dispatching / running / verifying / verified / release_pending / completed / failed / quarantined

2. `Current Stage`（text 或 single-select）
   - 例如 `issue_worker_execution`、`pr_verifier_execution`

3. `Last Synced At`（date 或 text）

4. `Owner Session`（text，選用）
   - 放簡短 session hint 即可，不要貼完整 transcript

---

## 7. 事件節奏建議（避免洗版）

### 建議更新節奏

- **Issue body**：只在狀態轉換與重大 gate 變化更新
- **Sticky comment**：只更新同一則 comment，不要每次 append
- **Project 欄位**：欄位值真的變了才更新

### 不建議

- 每次 reconcile tick 都發 comment
- 把 raw logs / 大量測試輸出貼進 comment
- 把 GitHub 當唯一真相源

---

## 8. 故障排除（Troubleshooting）

### 問題 A：GitHub 有更新延遲，SQLite 已前進

處理順序：

1. `inspect` 看 `latestGitHubSyncAttempt`
2. 若最新為 failed，執行 `retry-github-sync`
3. 再跑一次 `reconcile`

### 問題 B：Issue body 沒有 projection block

可能原因：

- issue 為 local-seeded（會跳過 GitHub 寫入）
- repo 未設定
- `gh auth` / API 呼叫失敗

先用 `inspect` 看 `latestGitHubSyncAttempt.last_error`。

### 問題 C：Projects 欄位沒動

先檢查：

- `github_project_id` 是否正確
- field ids 是否存在且對應型別正確
- 該 issue 是否已加入該 project item

---

## 9. 維運守則（很重要）

1. **先看 SQLite、再看 GitHub**
2. `main_orchestrator` 只做協調，不直接做 issue 實作
3. 同一 issue 不可重複啟動
4. GitHub comment 僅保留 compact 摘要與 DB ref
5. 把失敗重試做成可稽核命令，不做手動無痕操作

---

## 10. 一頁式值班清單（On-call Checklist）

每次值班只要確認：

- [ ] `reconcile` 最近一輪正常
- [ ] 進行中 issue 的 labels 與 state 對齊
- [ ] 每個進行中 issue 都有最新 body snapshot
- [ ] sticky comment 沒有洗版（同一則更新）
- [ ] `latestGitHubSyncAttempt` 無長時間 failed
- [ ] quarantined issue 有 owner 與 next action

如果以上都成立，代表 GitHub 監控面是健康可用的。
