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

  function toPlain(value) {
    return JSON.parse(JSON.stringify(value));
  }

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

  test("AC3: flow event channel emits ordered events with stable event IDs", () => {
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

    const socket = app.openFlowRunSocket(flowRun.flowRunId);
    const event1 = app.appendFlowRunEvent({
      flowRunId: flowRun.flowRunId,
      kind: "skill.progress",
      skillProgress: {
        skillName: "to-prd",
        completedSteps: 1,
        totalSteps: 3,
        status: "running",
      },
    });
    const event2 = app.appendFlowRunEvent({
      flowRunId: flowRun.flowRunId,
      kind: "gate.outcome",
      gateOutcome: {
        gateId: "spec_gate",
        status: "pass",
        detail: "problem_statement_is_clear",
      },
    });

    const live = socket.drain();
    assert.equal(live.length, 2);
    assert.equal(live[0].sequence, 1);
    assert.equal(live[1].sequence, 2);
    assert.equal(live[0].eventId, event1.eventId);
    assert.equal(live[1].eventId, event2.eventId);

    const persisted = app.listFlowRunEvents(flowRun.flowRunId);
    assert.deepEqual(
      persisted.map((item) => item.eventId),
      [event1.eventId, event2.eventId],
    );
  });

  test("AC4: reconnect replay returns missed events from persisted event store", () => {
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

    const event1 = app.appendFlowRunEvent({
      flowRunId: flowRun.flowRunId,
      kind: "skill.progress",
      skillProgress: {
        skillName: "grill-with-docs",
        completedSteps: 1,
        totalSteps: 2,
        status: "running",
      },
    });
    const event2 = app.appendFlowRunEvent({
      flowRunId: flowRun.flowRunId,
      kind: "question.prompt",
      questionPrompt: {
        promptId: "q-1",
        message: "Approve issue plan?",
        choices: ["approve", "revise"],
      },
    });
    const event3 = app.appendFlowRunEvent({
      flowRunId: flowRun.flowRunId,
      kind: "gate.outcome",
      gateOutcome: {
        gateId: "ui_prototype_gate",
        status: "pass",
        detail: "approved prototype reference recorded",
      },
    });

    const replay = app.replayFlowRunEvents(flowRun.flowRunId, event1.eventId);
    assert.deepEqual(
      replay.map((item) => item.eventId),
      [event2.eventId, event3.eventId],
    );

    const reconnectSocket = app.openFlowRunSocket(flowRun.flowRunId, {
      lastEventId: event2.eventId,
    });
    const missedAtConnect = reconnectSocket.drain();
    assert.equal(missedAtConnect.length, 1);
    assert.equal(missedAtConnect[0].eventId, event3.eventId);
  });

  test("AC5: UI-facing event contract carries progress prompts and gate outcomes", () => {
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

    const saved = app.appendFlowRunEvent({
      flowRunId: flowRun.flowRunId,
      kind: "flow.snapshot",
      skillProgress: {
        skillName: "to-issues",
        completedSteps: 2,
        totalSteps: 4,
        status: "running",
      },
      questionPrompt: {
        promptId: "q-2",
        message: "Approve DAG issue plan?",
        choices: ["approve", "request-revision"],
      },
      gateOutcome: {
        gateId: "traceability_gate",
        status: "pass",
        detail: "every_acceptance_criterion_maps_to_evidence",
      },
    });

    assert.equal(saved.ui.skillProgress.skillName, "to-issues");
    assert.equal(saved.ui.questionPrompt.promptId, "q-2");
    assert.equal(saved.ui.gateOutcome.gateId, "traceability_gate");
    assert.equal(saved.ui.gateOutcome.status, "pass");
    assert.equal(saved.kind, "flow.snapshot");
  });

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

  test("AC3: issue plan output includes explicit edges and runnable-order projection", () => {
    const app = createControlTowerApp({ storage: createMemoryStorage() });

    const plan = app.generateIssuePlan([
      { id: "I-001", title: "Foundation", deps: [], state: "done" },
      { id: "I-002", title: "Registry", deps: ["I-001"], state: "todo" },
      { id: "I-003", title: "Stream", deps: ["I-001"], state: "todo" },
      { id: "I-004", title: "Dashboard", deps: ["I-002", "I-003"], state: "todo" },
    ]);

    assert.equal(plan.valid, true);
    assert.equal(Array.isArray(plan.edges), true);
    assert.deepEqual(
      toPlain(plan.edges.map((edge) => [edge.from, edge.to])),
      [
        ["I-001", "I-002"],
        ["I-001", "I-003"],
        ["I-002", "I-004"],
        ["I-003", "I-004"],
      ],
    );
    assert.deepEqual(toPlain(plan.runnableOrder), ["I-001", "I-002", "I-003", "I-004"]);
  });

  test("AC4: execution lane projection distinguishes ready, blocked, and parallel lanes", () => {
    const app = createControlTowerApp({ storage: createMemoryStorage() });

    const plan = app.generateIssuePlan([
      { id: "I-001", title: "Foundation", deps: [], state: "done" },
      { id: "I-002", title: "Registry", deps: ["I-001"], state: "todo" },
      { id: "I-003", title: "Spec Chat", deps: ["I-001"], state: "todo" },
      { id: "I-004", title: "Dashboard", deps: ["I-002", "I-003"], state: "todo" },
    ]);

    assert.equal(plan.valid, true);
    assert.deepEqual(toPlain(plan.executionLanes.ready.map((item) => item.id)), ["I-002", "I-003"]);
    assert.deepEqual(toPlain(plan.executionLanes.blocked.map((item) => item.id)), ["I-004"]);
    assert.deepEqual(
      toPlain(plan.executionLanes.parallel.map((lane) => lane.issueIds)),
      [["I-002", "I-003"]],
    );
  });

  test("AC5: cycle validation rejects publish and returns split guidance", () => {
    const app = createControlTowerApp({ storage: createMemoryStorage() });

    const validation = app.validateIssuePlanBeforePublish([
      { id: "I-101", title: "A", deps: ["I-103"], state: "todo" },
      { id: "I-102", title: "B", deps: ["I-101"], state: "todo" },
      { id: "I-103", title: "C", deps: ["I-102"], state: "todo" },
    ]);

    assert.equal(validation.canPublish, false);
    assert.equal(validation.reason, "cyclic_dependencies");
    assert.equal(Array.isArray(validation.cycles), true);
    assert.equal(validation.cycles.length > 0, true);
    assert.equal(typeof validation.splitGuidance, "string");
    assert.match(validation.splitGuidance, /split|break|cycle/i);
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
