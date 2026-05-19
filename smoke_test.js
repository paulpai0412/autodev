const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

function loadControlTowerModule() {
  const indexPath = path.join(__dirname, "index.html");
  const html = fs.readFileSync(indexPath, "utf8");
  const match = html.match(
    /<script id="control-tower-core"[^>]*>([\s\S]*?)<\/script>/i,
  );

  if (!match) {
    throw new Error("index.html is missing <script id=\"control-tower-core\">.");
  }

  const sandbox = {
    module: { exports: {} },
    exports: {},
    globalThis: {},
    Date,
    Math,
    JSON,
  };

  vm.createContext(sandbox);
  vm.runInContext(match[1], sandbox, { filename: "index.html:inline-script" });

  return sandbox.module.exports;
}

function createMemoryStorage() {
  const map = new Map();

  return {
    getItem(key) {
      return map.has(key) ? map.get(key) : null;
    },
    setItem(key, value) {
      map.set(key, String(value));
    },
    removeItem(key) {
      map.delete(key);
    },
    clear() {
      map.clear();
    },
  };
}

function runTests() {
  const core = loadControlTowerModule();
  const { createControlTowerApp } = core;
  const tests = [];

  function test(name, fn) {
    tests.push({ name, fn });
  }

  test("AC3: UI-scoped flow run is blocked from issue-plan without approved prototype", () => {
    const app = createControlTowerApp({
      storage: createMemoryStorage(),
      env: {
        githubToken: "ghp_demo",
        runtimePathExists: true,
        runtimeConfigExists: true,
      },
    });

    app.registerProject({ repoId: "acme/alpha", displayName: "Alpha" });
    app.switchActiveProject("acme/alpha");

    const blockedRun = app.startFlowRun({
      ui_scope: true,
      prototype_approved: false,
      prototype_artifact_ref: null,
    });

    assert.equal(blockedRun.stage, "spec_pipeline");
    assert.equal(blockedRun.prototype_gate_decision, "blocked");
    assert.equal(blockedRun.ui_scope, true);

    assert.throws(
      () => {
        app.advanceFlowRunStage(blockedRun.flowRunId, "issue-plan");
      },
      /Prototype gate blocked/i,
    );

    const storedBlockedRun = app.getFlowRun(blockedRun.flowRunId);
    assert.equal(storedBlockedRun.stage, "spec_pipeline");
    assert.equal(storedBlockedRun.prototype_gate_decision, "blocked");
  });

  test("AC4: prototype gate decision + approver metadata are recorded in flow run state", () => {
    const app = createControlTowerApp({
      storage: createMemoryStorage(),
      env: {
        githubToken: "ghp_demo",
        runtimePathExists: true,
        runtimeConfigExists: true,
      },
    });

    app.registerProject({ repoId: "acme/alpha", displayName: "Alpha" });
    app.switchActiveProject("acme/alpha");

    const flowRun = app.startFlowRun({
      ui_scope: true,
      prototype_approved: true,
      prototype_artifact_ref: "docs/control-tower-ui-prototype-claude.html",
      prototype_approver: "paulpai0412",
      prototype_approved_at: "2026-05-19T12:34:56.000Z",
    });

    const moved = app.advanceFlowRunStage(flowRun.flowRunId, "issue-plan");
    assert.equal(moved.stage, "issue-plan");
    assert.equal(moved.prototype_gate_decision, "approved");
    assert.equal(moved.prototype_approver, "paulpai0412");
    assert.equal(
      moved.prototype_artifact_ref,
      "docs/control-tower-ui-prototype-claude.html",
    );
    assert.equal(moved.prototype_approved_at, "2026-05-19T12:34:56.000Z");

    const stored = app.getFlowRun(flowRun.flowRunId);
    assert.equal(stored.stage, "issue-plan");
    assert.equal(stored.prototype_gate_decision, "approved");
    assert.equal(stored.prototype_approver, "paulpai0412");
  });

  test("AC5: emitted issue acceptance criteria include prototype refs for UI slices", () => {
    const app = createControlTowerApp({
      storage: createMemoryStorage(),
      env: {
        githubToken: "ghp_demo",
        runtimePathExists: true,
        runtimeConfigExists: true,
      },
    });

    app.registerProject({ repoId: "acme/alpha", displayName: "Alpha" });
    app.switchActiveProject("acme/alpha");

    const flowRun = app.startFlowRun({
      ui_scope: true,
      prototype_approved: true,
      prototype_artifact_ref: "docs/control-tower-ui-prototype-claude.html",
      prototype_approver: "operator-1",
      prototype_approved_at: "2026-05-19T09:00:00.000Z",
    });
    app.advanceFlowRunStage(flowRun.flowRunId, "issue-plan");

    const acceptanceCriteria = app.emitIssuePlanAcceptanceCriteria(flowRun.flowRunId, [
      { id: "AC2", text: "Final UI prototype baseline exists" },
      { id: "AC3", text: "UI-scoped flow runs are gated before issue-plan" },
    ]);

    assert.equal(acceptanceCriteria.length, 2);
    acceptanceCriteria.forEach((criterion) => {
      assert.equal(
        criterion.prototype_ref,
        "docs/control-tower-ui-prototype-claude.html",
      );
      assert.equal(criterion.prototype_gate_decision, "approved");
      assert.equal(criterion.prototype_approver, "operator-1");
      assert.equal(criterion.prototype_approved_at, "2026-05-19T09:00:00.000Z");
    });
  });

  test("existing behavior: all-projects view remains read-only", () => {
    const app = createControlTowerApp({
      storage: createMemoryStorage(),
      env: {
        githubToken: "ghp_demo",
        runtimePathExists: true,
        runtimeConfigExists: true,
      },
    });

    app.registerProject({ repoId: "acme/alpha", displayName: "Alpha" });
    app.registerProject({ repoId: "acme/beta", displayName: "Beta" });
    const allProjects = app.getAllProjectsView();

    assert.equal(allProjects.readOnly, true);
    assert.equal(Array.isArray(allProjects.projects), true);
    assert.equal(allProjects.projects.length, 2);
    allProjects.projects.forEach((project) => {
      assert.equal(Array.isArray(project.actions), true);
      assert.equal(project.actions.length, 0);
    });

    assert.throws(
      () => {
        app.triggerAllProjectsLifecycleAction("reconcile-all");
      },
      /read-only/i,
    );
  });

  let passed = 0;
  for (const { name, fn } of tests) {
    try {
      fn();
      passed += 1;
      process.stdout.write(`PASS ${name}\n`);
    } catch (error) {
      process.stderr.write(`FAIL ${name}\n`);
      throw error;
    }
  }

  process.stdout.write(`\n${passed}/${tests.length} smoke tests passed\n`);
}

runTests();
