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

  test("AC3: flow run exposes explicit clarify -> prd -> issue-plan transitions", () => {
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

    const flowRun = app.startFlowRun();
    const stageNames = app
      .getSpecPipelineStatus(flowRun.flowRunId)
      .stages.map((stage) => stage.stage);

    assert.equal(stageNames.join("->"), "clarify->prd->issue-plan");
    assert.equal(app.getSpecPipelineStatus(flowRun.flowRunId).currentStage, "clarify");

    app.advanceSpecPipeline(flowRun.flowRunId);
    assert.equal(app.getSpecPipelineStatus(flowRun.flowRunId).currentStage, "prd");

    app.advanceSpecPipeline(flowRun.flowRunId);
    assert.equal(app.getSpecPipelineStatus(flowRun.flowRunId).currentStage, "issue-plan");
  });

  test("AC4: pipeline emits structured stage status and blocking prompts", () => {
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
    const flowRun = app.startFlowRun();

    const blocking = app.addBlockingQuestion(flowRun.flowRunId, {
      stage: "clarify",
      prompt: "Who is the primary operator persona?",
    });

    const blockedStatus = app.getSpecPipelineStatus(flowRun.flowRunId);
    const clarify = blockedStatus.stages.find((stage) => stage.stage === "clarify");

    assert.equal(clarify.status, "blocked");
    assert.equal(Array.isArray(clarify.blockingQuestions), true);
    assert.equal(clarify.blockingQuestions.length, 1);
    assert.equal(clarify.blockingQuestions[0].questionId, blocking.questionId);
    assert.equal(blockedStatus.blockingQuestions.length, 1);

    assert.throws(
      () => {
        app.advanceSpecPipeline(flowRun.flowRunId);
      },
      /blocking question/i,
    );

    app.resolveBlockingQuestion(flowRun.flowRunId, blocking.questionId, "Tech lead");
    const unblockedStatus = app.getSpecPipelineStatus(flowRun.flowRunId);
    const clarifyAfterResolve = unblockedStatus.stages.find(
      (stage) => stage.stage === "clarify",
    );

    assert.equal(clarifyAfterResolve.status, "in_progress");
    assert.equal(clarifyAfterResolve.blockingQuestions[0].status, "resolved");
  });

  test("AC5: stage progression is rejected when flow run is not readiness-passed", () => {
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
    const flowRun = app.createFlowRunFromOrchestration({
      repoId: "acme/alpha",
      readinessPassed: false,
    });

    assert.throws(
      () => {
        app.advanceSpecPipeline(flowRun.flowRunId);
      },
      /readiness-passed/i,
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
