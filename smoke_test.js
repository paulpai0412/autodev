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

  test("AC3: approval queue lists release-blocked PRs with approval state in one view", () => {
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

    const queue = app.listApprovalQueue(flowRun.flowRunId);
    assert.equal(Array.isArray(queue), true);
    assert.equal(queue.length >= 2, true);
    assert.equal(queue[0].releaseBlocked, true);
    assert.equal(typeof queue[0].approvalState, "string");
    assert.equal(typeof queue[0].releasePathState, "string");
  });

  test("AC4: approving GitHub identity is explicit before approval submit", () => {
    const app = createControlTowerApp({
      storage: createMemoryStorage(),
      env: {
        githubToken: "ghp_demo",
        runtimePathExists: true,
        runtimeConfigExists: true,
        githubLogin: "paulpai0412",
        githubAvatarUrl: "https://avatars.githubusercontent.com/u/1?v=4",
      },
    });

    app.registerProject({ repoId: "acme/alpha", displayName: "Alpha" });
    app.switchActiveProject("acme/alpha");
    const flowRun = app.startFlowRun();

    const identity = app.getApprovingIdentity(flowRun.flowRunId);
    assert.equal(identity.login, "paulpai0412");
    assert.equal(
      identity.avatarUrl,
      "https://avatars.githubusercontent.com/u/1?v=4",
    );
  });

  test("AC5: approval action records GitHub review result and unblocks release path", () => {
    const app = createControlTowerApp({
      storage: createMemoryStorage(),
      env: {
        githubToken: "ghp_demo",
        runtimePathExists: true,
        runtimeConfigExists: true,
        githubLogin: "paulpai0412",
        approvePullRequest(payload) {
          return {
            reviewId: "rvw-1",
            accepted: true,
            prNumber: payload.prNumber,
          };
        },
      },
    });

    app.registerProject({ repoId: "acme/alpha", displayName: "Alpha" });
    app.switchActiveProject("acme/alpha");
    const flowRun = app.startFlowRun();

    const queue = app.listApprovalQueue(flowRun.flowRunId);
    const target = queue.find((item) => item.requiredChecksPassed && item.policyReady);
    assert.ok(target, "expected at least one approvable queue item");

    const result = app.approveReleaseBlockedPullRequest(flowRun.flowRunId, {
      prNumber: target.prNumber,
    });
    assert.equal(result.approvalState, "approved");
    assert.equal(result.reviewResult.state, "APPROVED");
    assert.equal(result.reviewResult.reviewer, "paulpai0412");
    assert.equal(result.releasePathState, "ready");
    assert.equal(result.releaseBlocked, false);

    const stored = app.listApprovalQueue(flowRun.flowRunId).find((item) => item.prNumber === target.prNumber);
    assert.equal(stored.releasePathState, "ready");
    assert.equal(stored.releaseBlocked, false);
    assert.equal(stored.reviewResult.state, "APPROVED");
  });

  test("AC5: GitHub-native approval command is gh pr review --approve", () => {
    const app = createControlTowerApp({ storage: createMemoryStorage() });

    const command = app.buildGitHubApproveCommand({
      repoId: "acme/alpha",
      prNumber: 88,
      body: "Ship it",
    });

    assert.deepEqual(Array.from(command), [
      "gh",
      "pr",
      "review",
      "88",
      "--repo",
      "acme/alpha",
      "--approve",
      "--body",
      "Ship it",
    ]);
  });

  test("AC5: approval action can submit through gh command adapter and record command", () => {
    const recorded = [];
    const app = createControlTowerApp({
      storage: createMemoryStorage(),
      env: {
        githubToken: "ghp_demo",
        runtimePathExists: true,
        runtimeConfigExists: true,
        githubLogin: "paulpai0412",
        runCommand(payload) {
          recorded.push(payload.command);
          return { ok: true };
        },
      },
    });

    app.registerProject({ repoId: "acme/alpha", displayName: "Alpha" });
    app.switchActiveProject("acme/alpha");
    const flowRun = app.startFlowRun();
    const target = app
      .listApprovalQueue(flowRun.flowRunId)
      .find((item) => item.requiredChecksPassed && item.policyReady);

    const result = app.approveReleaseBlockedPullRequest(flowRun.flowRunId, {
      prNumber: target.prNumber,
      body: "Approved by operator",
    });

    assert.equal(recorded.length, 1);
    assert.deepEqual(Array.from(recorded[0]), [
      "gh",
      "pr",
      "review",
      String(target.prNumber),
      "--repo",
      "acme/alpha",
      "--approve",
      "--body",
      "Approved by operator",
    ]);
    assert.deepEqual(Array.from(result.reviewCommand), Array.from(recorded[0]));
    assert.equal(result.releasePathState, "ready");
    assert.equal(result.releaseBlocked, false);
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

  test("AC3: publisher creates issues blocker-first and records created issue numbers", () => {
    const app = createControlTowerApp({ storage: createMemoryStorage() });
    const createdPayloads = [];
    let nextIssueNumber = 300;

    const published = app.publishIssuesInDependencyOrder(
      [
        {
          id: "I-201",
          title: "Foundation",
          deps: [],
          acceptanceCriteria: ["schema is created"],
        },
        {
          id: "I-202",
          title: "Registry",
          deps: ["I-201"],
          acceptanceCriteria: ["registry reads schema"],
        },
        {
          id: "I-203",
          title: "Dashboard",
          deps: ["I-202"],
          acceptanceCriteria: ["dashboard renders registry data"],
        },
      ],
      {
        createIssue(payload) {
          createdPayloads.push(payload);
          nextIssueNumber += 1;
          return { number: nextIssueNumber };
        },
      },
    );

    assert.deepEqual(
      toPlain(createdPayloads.map((payload) => payload.id)),
      ["I-201", "I-202", "I-203"],
    );
    assert.deepEqual(toPlain(published.order), ["I-201", "I-202", "I-203"]);
    assert.equal(published.issueNumbers["I-201"], 301);
    assert.equal(published.issueNumbers["I-202"], 302);
    assert.equal(published.issueNumbers["I-203"], 303);
  });

  test("AC4: published issue body includes parseable Blocked by phrases", () => {
    const app = createControlTowerApp({ storage: createMemoryStorage() });
    const createdPayloads = [];
    let nextIssueNumber = 400;

    app.publishIssuesInDependencyOrder(
      [
        {
          id: "I-301",
          title: "Foundation",
          deps: [],
          acceptanceCriteria: ["foundation exists"],
        },
        {
          id: "I-302",
          title: "Runtime Reader",
          deps: ["I-301"],
          acceptanceCriteria: ["reader can read runtime DB"],
        },
      ],
      {
        createIssue(payload) {
          createdPayloads.push(payload);
          nextIssueNumber += 1;
          return { number: nextIssueNumber };
        },
      },
    );

    assert.match(createdPayloads[1].body, /## Blocked by/);
    assert.match(createdPayloads[1].body, /Blocked by #401/);
  });

  test("AC5: published issues include ready-for-agent label and acceptance checklist", () => {
    const app = createControlTowerApp({ storage: createMemoryStorage() });
    const createdPayloads = [];

    app.publishIssuesInDependencyOrder(
      [
        {
          id: "I-401",
          title: "Issue publisher",
          deps: [],
          acceptanceCriteria: [
            "publisher creates issues in dependency order",
            "publisher captures created issue numbers",
          ],
          labels: ["autodev"],
        },
      ],
      {
        createIssue(payload) {
          createdPayloads.push(payload);
          return { number: 501 };
        },
      },
    );

    assert.equal(createdPayloads.length, 1);
    assert.equal(createdPayloads[0].labels.includes("ready-for-agent"), true);
    assert.equal(createdPayloads[0].labels.includes("autodev"), true);
    assert.match(createdPayloads[0].body, /## Acceptance Checklist/);
    assert.match(
      createdPayloads[0].body,
      /- \[ \] publisher creates issues in dependency order/,
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
