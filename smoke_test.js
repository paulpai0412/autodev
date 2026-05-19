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
  const { createControlTowerApp, APP_DB_KEY } = core;
  const tests = [];

  function test(name, fn) {
    tests.push({ name, fn });
  }

  test("AC3: rejects flow run creation without selected consumer repo", () => {
    const app = createControlTowerApp({ storage: createMemoryStorage() });

    assert.throws(
      () => {
        app.createFlowRun({ stage: "spec_pipeline" });
      },
      /selected consumer repo/i,
    );
  });

  test("AC4: app DB persists flow run identity stage and timestamps", () => {
    const storage = createMemoryStorage();
    const app = createControlTowerApp({ storage });

    app.selectConsumerRepo({ repoId: "acme/app", displayName: "Acme App" });
    const flowRun = app.createFlowRun({ stage: "spec_pipeline" });

    assert.equal(flowRun.repoId, "acme/app");
    assert.equal(flowRun.stage, "spec_pipeline");
    assert.ok(flowRun.flowRunId);
    assert.ok(flowRun.createdAt);
    assert.ok(flowRun.updatedAt);

    const appReloaded = createControlTowerApp({ storage });
    const snapshot = appReloaded.readAppDb();

    assert.equal(snapshot.flowRuns.length, 1);
    assert.equal(snapshot.flowRuns[0].flowRunId, flowRun.flowRunId);
    assert.equal(snapshot.flowRuns[0].stage, "spec_pipeline");

    const raw = JSON.parse(storage.getItem(APP_DB_KEY));
    assert.equal(raw.flowRuns[0].flowRunId, flowRun.flowRunId);
  });

  test("AC5: chat/spec writes never touch consumer issues or issue_history", () => {
    const writes = [];
    const consumerRuntimeDb = {
      write(tableName) {
        writes.push(tableName);
      },
    };

    const app = createControlTowerApp({
      storage: createMemoryStorage(),
      consumerRuntimeDb,
    });

    app.selectConsumerRepo({ repoId: "acme/app", displayName: "Acme App" });
    const run = app.createFlowRun({ stage: "spec_pipeline" });
    app.saveChatSpecState(run.flowRunId, { prompt: "build dashboard" });
    app.setFlowRunStage(run.flowRunId, "planning");

    assert.deepEqual(writes, []);
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
