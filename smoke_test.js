const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const htmlPath = path.join(__dirname, "index.html");
const html = fs.readFileSync(htmlPath, "utf8");

function mustInclude(regex, message) {
  assert.match(html, regex, message);
}

function run() {
  mustInclude(/<!DOCTYPE html>/i, "HTML should be parseable and include doctype");

  mustInclude(/id=["']screen-run["']/i, "Missing Run Dashboard section");
  mustInclude(/id=["']screen-issue-detail["']/i, "Missing Issue Detail section");
  mustInclude(/id=["']screen-recovery["']/i, "Missing Recovery Center section");

  mustInclude(/id=["']kpi-total-issues["']/i, "Missing KPI total issues element");
  mustInclude(/id=["']kpi-running-verified["']/i, "Missing KPI running\/verified element");
  mustInclude(/id=["']kpi-blocked-count["']/i, "Missing KPI blocked count element");

  mustInclude(/id=["']issue-detail-progression["']/i, "Missing issue detail progression container");
  mustInclude(/main_orchestrator/i, "Issue detail should include main_orchestrator role progression");
  mustInclude(/issue_worker/i, "Issue detail should include issue_worker role progression");
  mustInclude(/pr_verifier/i, "Issue detail should include pr_verifier role progression");
  mustInclude(/release_worker/i, "Issue detail should include release_worker role progression");

  mustInclude(/data-recovery-action=["']quarantine["']/i, "Missing quarantine action button");
  mustInclude(/data-recovery-action=["']resume["']/i, "Missing resume action button");
  mustInclude(/data-recovery-action=["']fail["']/i, "Missing fail action button");
  mustInclude(/data-recovery-action=["']retry["']/i, "Missing retry action button");

  process.stdout.write("smoke_test.js: all checks passed\n");
}

run();
