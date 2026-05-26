# Codex Host Adapter 實作指南（DB-only control plane）

## 文件目的

這份文件回答：「若要在目前 `autodev` 分支（`db-only-control-plane`）建立 Codex host adapter，要改哪些地方？」

重點是 **新增一個 `codex` adapter**，而不是改動 DB control-plane schema。`issues` / `issue_history` 與 supervisor 狀態機維持不變，host 差異都收斂到 adapter seam。

---

## 一句話結論

要做的核心改動有 5 類：

1. 新增 `scripts/codex_host_adapter.py`（對齊 `HostAdapter` protocol）
2. 在 `scripts/orchestrator_sessions.py` 註冊 `codex` factory
3. 讓 `scripts/autodev_project.py` 與 `scripts/autodev_host_packaging.py` 支援 Codex 命令包裝與 doctor 檢查
4. 擴充 tests（adapter 單元測試 + supervisor/session/packaging 整合測試）
5. 更新文件（README、runtime architecture、host-adapter strategy、user manual）

---

## 1) 不該改的邊界（先定義 guardrails）

以下是 branch 契約，Codex adapter 實作時不可破壞：

- runtime 真相仍是 SQLite `issues` / `issue_history`
- `issues.current_session_id` 仍是唯一 current session pointer
- 不可引入 file-backed runtime 依賴
- host-specific 行為（啟動、resume、命令 UX）必須留在 adapter 層

對應文件：

- `docs/agents/runtime/db-only-control-plane-spec.md`
- `docs/agents/runtime/host-adapter-strategy.md`

---

## 2) 必改程式檔與改動內容

### A. Adapter contract 與註冊層

#### `scripts/host_adapter.py`（通常不需改 schema）

已存在的契約：

- `SessionStartContext`
- `SessionStartResult`
- `SessionOutcome`
- `HostAdapter` protocol

Codex adapter 必須完整實作：

- `start_root_session(context)`
- `start_child_role(role, context)`
- `read_session_outcome(runtime_session_id)`
- `resume_link(runtime_session_id)`
- `operator_entrypoints()`
- `capabilities()`

> 建議：除非 Codex 真的需要額外標準欄位，先不要擴 `SessionStartResult` / `SessionOutcome`，避免影響 supervisor 與既有測試面。

#### `scripts/orchestrator_sessions.py`

要改：

- 在 `_register_builtin_adapters()` 增加 `codex` factory（例如 `_build_codex_adapter`）
- 保持 `AUTODEV_HOST_ADAPTER` selector 流程不變

目標：`resolve_host_adapter("codex")` 與 `AUTODEV_HOST_ADAPTER=codex` 都能正常拿到 adapter instance。

---

### B. 新增 Codex adapter 實作

#### 新檔：`scripts/codex_host_adapter.py`

建議最小可行版本（MVP）行為：

1. **CLI 探測**
   - 提供 `resolve_codex_cli()`（比照 `resolve_opencode_cli()`）
   - 錯誤訊息風格對齊 OpenCode adapter（讓 supervisor 可直接回報）

2. **root session 啟動**
   - 以 `codex exec` 啟動非互動任務
   - 解析 JSON 事件流中的 `thread.started.thread_id` 作為 `session_id`
   - 填入 `SessionStartResult(status="success", session_id=...)`

3. **child role 啟動**
   - 先用同一條啟動管線（可先採「同 root 邏輯 + role metadata」）
   - 若 Codex 尚無穩定 external child-agent automation API，不要假裝有 OpenCode-style child session；把可觀測結果放入 metadata 並保持欄位一致

4. **session outcome 讀取**
   - 讀取/投影執行結果到 `SessionOutcome`
   - `status`、`error_kind`、`error`、`resume_hint` 必須可供 reconcile/supervisor 使用

5. **resume link**
   - `resume_link(session_id)` 回傳可操作命令（例如 `codex exec resume <thread_id>`）

6. **operator entrypoints / capabilities**
   - `operator_entrypoints()` 回傳 host command 檔名對應
   - `capabilities()` 至少包含 `host`, `commands_dir`, `background_sessions`, `subagents`, `plugin_commands`

---

### C. Supervisor / reconcile 相容面（通常只驗證，不大改）

#### `scripts/orchestrator_supervisor.py`

這邊是 adapter 消費端，重點確認新 adapter 回傳資料有對齊：

- `dispatch_session_request(...)` 透過 `_default_host_adapter()` 呼叫
- `session_result_field(...)` 會從 dataclass 欄位或 metadata fallback 取值
- `_record_session_result_history(...)` 會把結果寫進 `issue_history`

你要確保 Codex adapter 的 `SessionStartResult` 能讓下列鍵有合理值：

- `rootSessionID`
- `cliOpenCommand`
- `recommendedAction`
- `sessionReadabilityStatus`
- `executionMode`
- `childRole` / `childSessionID` / `childSessionStatus`
- `tuiResumeCommand`, `stopContinuationStatus`, `stopContinuationAttempts`

#### `scripts/orchestrator_reconcile.py`

重點在 `read_session_outcome(...)` 兼容：

- `reconcile_pr_verifier(...)`
- `reconcile_release_worker(...)`

這兩段只要求 outcome 有可判斷終止/進行中的 `status` 與錯誤資訊，不需要 host-specific 物件。

---

### D. 包裝層與 operator command

#### `scripts/autodev_host_packaging.py`

確認/調整：

- `host_packaging_config_from_adapter(...)` 可正確吃到 Codex adapter 的 `commands_dir` 與 `entrypoints`
- `command_templates(...)` 是否仍可共用（若 Codex command 名稱不同可在 `operator_entrypoints()` 映射）

#### `scripts/autodev_project.py`

必改點：

- `DEFAULT_ENV_VARS["AUTODEV_HOST_ADAPTER"]` 視產品策略決定是否維持 `opencode` 預設（通常維持）
- Windows preflight：目前只檢查 OpenCode CLI，需擴充為 host-aware（`opencode` 檢查 opencode、`codex` 檢查 codex）
- `install-commands` 路徑與命令生成需在 `AUTODEV_HOST_ADAPTER=codex` 時正常

---

## 3) 測試改動清單（高優先）

### 必新增

1. `tests/scripts/test_codex_host_adapter.py`
   - CLI resolve
   - session id 解析（`thread_id`）
   - start_root_session success/error
   - start_child_role 行為
   - read_session_outcome mapping
   - resume_link 格式
   - capabilities / operator_entrypoints

### 必調整

2. `tests/scripts/test_orchestrator_sessions.py`
   - 增加 built-in `codex` 註冊/resolve 驗證
   - `AUTODEV_HOST_ADAPTER=codex` 路徑

3. `tests/scripts/test_orchestrator_supervisor.py`
   - 既有 FakeHostAdapter 測試可保留
   - 增加 Codex 風格 session/resume 文案斷言（避免硬綁 `opencode --session`）

4. `tests/scripts/test_autodev_project.py`
   - host-aware doctor/preflight
   - `install-commands` 在 codex adapter 下 commands_dir / entrypoints 正確

5. `tests/scripts/test_orchestrator_requests.py`（若 prompt 文案含 host 專屬指令）
   - 把 OpenCode 專屬文案限制在 adapter 或 host template 層
   - 核心 prompt 保持 host-neutral

### 建議審視（可分批）

- `tests/scripts/test_opencode_session_trace.py`
- `tests/scripts/test_probe_scripts.py`
- `tests/scripts/test_subagent_startup_repro.py`

這些多為 OpenCode 專屬測試，可維持，但要避免把 OpenCode 行為誤當核心 contract。

---

## 4) 文件改動清單

至少更新以下文件：

1. `README.md`
   - 補充 `AUTODEV_HOST_ADAPTER=codex` 用法
   - 補充 Codex CLI 需求與驗證命令

2. `docs/agents/runtime/current-architecture.md`
   - 在 host adapter seam 註明 Codex adapter 狀態（planned / shipped）

3. `docs/agents/runtime/host-adapter-strategy.md`
   - 補上 Codex adapter 的實作現況與限制（例如 child-agent automation 能力差異）

4. `docs/autodev-user-manual.zh-TW.md`
   - 補充 Codex 操作與 resume 指令

---

## 5) 建議實作順序（可直接照做）

1. 新增 `scripts/codex_host_adapter.py` + 單元測試
2. 註冊 `codex` 到 `orchestrator_sessions.py` + session registry 測試
3. 調整 `autodev_project.py` host-aware doctor/preflight
4. 跑 supervisor/reconcile 相關回歸，修正文案耦合
5. 更新 README 與 runtime docs

---

## 6) 驗證命令（完成後必跑）

先跑聚焦測試：

```bash
python3 -m pytest tests/scripts/test_codex_host_adapter.py -q
python3 -m pytest tests/scripts/test_orchestrator_sessions.py -q
python3 -m pytest tests/scripts/test_autodev_project.py -q
python3 -m pytest tests/scripts/test_orchestrator_supervisor.py -q
```

再跑整體 scripts 回歸：

```bash
python3 -m pytest tests/scripts -q
```

---

## 7) 風險與對策

### 風險 1：把 OpenCode 專屬欄位當成核心必填

- 對策：所有 host 差異透過 `session_result_field(...)` + metadata fallback

### 風險 2：Codex 子代理能力與 OpenCode 不同，導致 release_root_execution 行為不一致

- 對策：先保證 `start_child_role` 在契約層「可回傳可追蹤 session 結果」，不要硬套 OpenCode trace 形狀

### 風險 3：doctor/install-commands 仍隱性依賴 OpenCode

- 對策：把 CLI 探測與 commands_dir 完全 adapter 化；`autodev_project.py` 僅讀 adapter 能力

### 風險 4：文件與實作不一致

- 對策：同 PR 內同步更新 README + runtime architecture + user manual

---

## 8) 外部參考（Codex 能力映射）

- Codex non-interactive / CI：`codex exec`、`--json`、resume
  - https://developers.openai.com/codex/noninteractive
- Codex CLI reference
  - https://developers.openai.com/codex/cli/reference
- Codex slash commands（偏互動，非穩定 automation API）
  - https://developers.openai.com/codex/cli/slash-commands
- openai/codex SDK: exec + event stream
  - https://github.com/openai/codex/blob/3936ed221d90278a64d70a423fd7b456799f112b/sdk/typescript/src/exec.ts
  - https://github.com/openai/codex/blob/3936ed221d90278a64d70a423fd7b456799f112b/sdk/typescript/src/events.ts
  - https://github.com/openai/codex/blob/3936ed221d90278a64d70a423fd7b456799f112b/sdk/typescript/src/thread.ts

> 註：若實際採用的 Codex 版本 CLI 旗標與上述引用不同，請以「你部署環境中的 `codex --help`」為最終真相，並同步更新 adapter 與測試。
