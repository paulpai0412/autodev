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

  test("AC3: switching project context does not trigger cross-repo lifecycle actions", () => {
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

    app.switchActiveProject("acme/alpha");
    app.switchActiveProject("acme/beta");

    assert.equal(app.getActiveProject().repoId, "acme/beta");
    assert.equal(app.getLifecycleEvents().length, 0);
  });

  test("AC4: readiness gate blocks flow run start when checks fail", () => {
    const app = createControlTowerApp({
      storage: createMemoryStorage(),
      env: {
        githubToken: "",
        runtimePathExists: false,
        runtimeConfigExists: false,
      },
    });

    app.registerProject({ repoId: "acme/alpha", displayName: "Alpha" });
    app.switchActiveProject("acme/alpha");

    const gate = app.evaluateReadiness();
    assert.equal(gate.canStartFlowRun, false);
    assert.equal(gate.checks.githubAuth.ok, false);
    assert.equal(gate.checks.runtimePath.ok, false);
    assert.equal(gate.checks.runtimeConfig.ok, false);

    assert.throws(
      () => {
        app.startFlowRun();
      },
      /Readiness gate failed/i,
    );
  });

  test("AC5: all-projects view is read-only and exposes no cross-repo lifecycle actions", () => {
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
