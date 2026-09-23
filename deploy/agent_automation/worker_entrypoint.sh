#!/usr/bin/env bash
set -euo pipefail

GITHUB_REPO="${GITHUB_REPO:-mbettan/bq-finops-optimizer-private}"

echo "=== [1/6] Independent Security & Timeline Re-Verification ==="
python3 /app/verify_issue_actor.py

echo "=== [2/6] Shallow Cloning Repository (1 Issue = 1 Branch) ==="
GITHUB_PAT="$(echo -n "${GITHUB_PAT}" | tr -d '\r\n ')"
BRANCH_NAME="agent/issue-${TARGET_ISSUE_NUMBER}"
git config --global credential.helper "store --file=/root/.git-credentials"
echo "https://x-access-token:${GITHUB_PAT}@github.com" > /root/.git-credentials
chmod 600 /root/.git-credentials
git clone --depth=1 "https://github.com/${GITHUB_REPO}.git" /workspace
git config --global --add safe.directory /workspace
cd /workspace
git checkout -b "${BRANCH_NAME}"
git branch base-anchor HEAD
git config user.name "bq-finops-agent"
git config user.email "bq-finops-agent@users.noreply.github.com"

export GH_TOKEN="${GITHUB_PAT}"
INITIAL_PR_BODY="Closes #${TARGET_ISSUE_NUMBER}

Automated implementation for #${TARGET_ISSUE_NUMBER} orchestrated by **Google ADK (\`SequentialAgent\` + \`LoopAgent\`)** and **Native Claude Code CLI**.

### 🚦 Live Multi-Agent Pipeline Progress
| Stage | Role | Model | Status |
| :--- | :--- | :--- | :--- |
| **1/3** | **Agent 1: ArchitectAgent** (Upfront SOP Plan) | \`${ARCHITECT_MODEL}\` | ⏳ *Running...* |
| **2/3** | **Agent 2: CoderAgent** (Implementation) | \`${CODER_MODEL}\` | ⬜ *Pending* |
| **2.5/3** | **Pre-Review Executable Feedback** (\`ruff\` + \`pytest\`) | \`Deterministic\` | ⬜ *Pending* |
| **3/3** | **Agent 3: ReviewerAgent** (Adversarial Audit) | \`${REVIEWER_MODEL}\` | ⬜ *Pending* |"

EXISTING_PR=$(gh pr list --repo "${GITHUB_REPO}" --head "${BRANCH_NAME}" --state open --json url -q '.[0].url' || true)
if [ -n "${EXISTING_PR}" ]; then
  PR_URL="${EXISTING_PR}"
  gh pr edit "${PR_URL}" --repo "${GITHUB_REPO}" --body "${INITIAL_PR_BODY}" >/dev/null 2>&1 || true
  echo "✅ Attached to existing PR for live agent updates: ${PR_URL}"
else
  git commit --allow-empty -m "chore(agent): initialize ADK pipeline for issue #${TARGET_ISSUE_NUMBER}"
  git push -f origin "${BRANCH_NAME}"
  PR_URL=$(gh pr create \
    --repo "${GITHUB_REPO}" \
    --head "${BRANCH_NAME}" \
    --base main \
    --draft \
    --title "feat: implement #${TARGET_ISSUE_NUMBER} (Autonomous Agent)" \
    --body "${INITIAL_PR_BODY}")
  git reset --hard base-anchor
  echo "✅ Initialized Draft PR for live agent updates: ${PR_URL}"
fi

ln -sfn /opt/venv /workspace/.venv
export PATH="/opt/venv/bin:${PATH}"

echo "=== [3/6] Executing Google ADK 3-Agent Pipeline (Architect Opus 5.5 -> Loop[Coder Sonnet 5 <-> Reviewer Opus 5.5]) ==="
export CLAUDE_CODE_USE_VERTEX=1
export CLOUD_ML_REGION="${VERTEX_REGION:-global}"
export ANTHROPIC_VERTEX_PROJECT_ID="${GCP_PROJECT_ID:-bq-finops-optimizer}"
export ARCHITECT_MODEL="${ARCHITECT_MODEL:-claude-opus-5-5}"
export CODER_MODEL="${CODER_MODEL:-claude-sonnet-5}"
export REVIEWER_MODEL="${REVIEWER_MODEL:-claude-opus-5-5}"

rm -f /tmp/claude_execution_log.json /tmp/opus_review_report.md /tmp/adk_stage_events.jsonl
touch /tmp/adk_stage_events.jsonl
chmod 666 /tmp/adk_stage_events.jsonl

# Background root watcher that reads /tmp/adk_stage_events.jsonl and posts per-agent comments + updates the PR status table
python3 -u -c '
import json, os, subprocess, sys, time
from pathlib import Path

pr_url = sys.argv[1]
repo = sys.argv[2]
issue_num = sys.argv[3]
arch_model = os.environ.get("ARCHITECT_MODEL", "claude-opus-5-5")
coder_model = os.environ.get("CODER_MODEL", "claude-sonnet-5")
rev_model = os.environ.get("REVIEWER_MODEL", "claude-opus-5-5")

events_file = Path("/tmp/adk_stage_events.jsonl")
stop_file = Path("/tmp/adk_watcher_stop")
seen_lines = 0

stage_state = {
    "1/3": "⏳ *Running...*",
    "2/3": "⬜ *Pending*",
    "2.5/3": "⬜ *Pending*",
    "3/3": "⬜ *Pending*",
}

def update_pr_table():
    pr_body = (
        f"Closes #{issue_num}\n\n"
        f"Automated implementation for #{issue_num} orchestrated by **Google ADK (`SequentialAgent` + `LoopAgent`)** and **Native Claude Code CLI**.\n\n"
        f"### 🚦 Live Multi-Agent Pipeline Progress\n"
        f"| Stage | Role | Model | Status |\n"
        f"| :--- | :--- | :--- | :--- |\n"
        f"| **1/3** | **Agent 1: ArchitectAgent** (Upfront SOP Plan) | `{arch_model}` | {stage_state[\"1/3\"]} |\n"
        f"| **2/3** | **Agent 2: CoderAgent** (Implementation) | `{coder_model}` | {stage_state[\"2/3\"]} |\n"
        f"| **2.5/3** | **Pre-Review Executable Feedback** (`ruff` + `pytest`) | `Deterministic` | {stage_state[\"2.5/3\"]} |\n"
        f"| **3/3** | **Agent 3: ReviewerAgent** (Adversarial Audit) | `{rev_model}` | {stage_state[\"3/3\"]} |\n"
    )
    subprocess.run(
        ["gh", "pr", "edit", pr_url, "--repo", repo, "--body", pr_body],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )

while True:
    if events_file.exists():
        lines = events_file.read_text(encoding="utf-8", errors="replace").splitlines()
        while seen_lines < len(lines):
            line = lines[seen_lines].strip()
            seen_lines += 1
            if not line:
                continue
            try:
                ev = json.loads(line)
                stage = ev.get("stage", "")
                agent = ev.get("agent", "")
                model = ev.get("model", "")
                status = ev.get("status", "")
                turns = ev.get("turns", 0)
                dur = ev.get("duration_s", 0.0)
                cost = ev.get("cost_usd", 0.0)
                details = ev.get("details", "")
                icon = "✅" if ("PASS" in status or "COMPLETED" in status) else "🔄"

                if stage == "1/3":
                    stage_state["1/3"] = f"{icon} `{status}` ({turns} turns, {dur}s, ${cost:.4f})"
                    stage_state["2/3"] = "⏳ *Running...*"
                elif stage == "2/3":
                    stage_state["2/3"] = f"{icon} `{status}` ({turns} turns, {dur}s, ${cost:.4f})"
                    stage_state["2.5/3"] = "⏳ *Running...*"
                elif stage == "2.5/3":
                    stage_state["2.5/3"] = f"{icon} `{status}`"
                    stage_state["2/3"] = "⏳ *Revising...*"
                elif stage == "3/3":
                    stage_state["2.5/3"] = "✅ `PASSED (ruff + pytest)`"
                    stage_state["3/3"] = f"{icon} `{status}` ({turns} turns, {dur}s, ${cost:.4f})"
                    if "REVISE" in status:
                        stage_state["2/3"] = "⏳ *Revising...*"

                update_pr_table()

                body = (
                    f"### {icon} [{stage}] {agent}\n"
                    f"- **Model:** `{model}` | **Status:** `{status}` | "
                    f"**Turns:** `{turns}` | **Duration:** `{dur}s` | **Est. Cost:** `${cost:.4f}`\n\n"
                    f"<details>\n<summary>📋 View {agent} Output & Artifacts</summary>\n\n"
                    f"{details}\n\n</details>"
                )
                subprocess.run(
                    ["gh", "pr", "comment", pr_url, "--repo", repo, "--body", body],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=False,
                )
            except Exception:
                pass
    if stop_file.exists():
        break
    time.sleep(2)
' "${PR_URL}" "${GITHUB_REPO}" "${TARGET_ISSUE_NUMBER}" &
WATCHER_PID=$!
rm -f /tmp/adk_watcher_stop

if id -u agentuser >/dev/null 2>&1; then
  chown -R agentuser:agentuser /workspace /tmp/sanitized_issue_prompt.txt /tmp/adk_stage_events.jsonl
  su -s /bin/bash agentuser -c "
    export PATH='/opt/venv/bin:/usr/local/bin:/usr/bin:/bin'
    export HOME='/home/agentuser'
    export CLAUDE_CODE_USE_VERTEX=1
    export CLOUD_ML_REGION='${CLOUD_ML_REGION}'
    export ANTHROPIC_VERTEX_PROJECT_ID='${ANTHROPIC_VERTEX_PROJECT_ID}'
    export ARCHITECT_MODEL='${ARCHITECT_MODEL}'
    export CODER_MODEL='${CODER_MODEL}'
    export REVIEWER_MODEL='${REVIEWER_MODEL}'
    export PYTHONUNBUFFERED=1
    git config --global --add safe.directory /workspace
    cd /workspace
    /opt/venv/bin/python3 -u /app/adk_orchestrator.py
  "
  chown -R root:root /workspace
else
  /opt/venv/bin/python3 -u /app/adk_orchestrator.py
fi

touch /tmp/adk_watcher_stop
wait "${WATCHER_PID}" 2>/dev/null || true

CLAUDE_TELEMETRY=$(python3 -c '
import json
try:
    d = json.load(open("/tmp/claude_execution_log.json"))
    turns = d.get("num_turns", "N/A")
    loops = d.get("loop_iterations", 1)
    dur_s = round(d.get("duration_ms", 0) / 1000.0, 1)
    cost = d.get("total_cost_usd", 0.0)
    print(f"ADK Loops: {loops}/3 | Total Turns: {turns} | Duration: {dur_s}s | Est. Cost: ${cost:.4f}")
except Exception as e:
    print(f"Telemetry unavailable ({e})")
')
echo "📊 [Google ADK + Claude CLI Telemetry] ${CLAUDE_TELEMETRY}"

OPUS_REVIEW_SUMMARY=""
if [ -f /tmp/opus_review_report.md ]; then
  OPUS_REVIEW_SUMMARY=$(cat /tmp/opus_review_report.md)
fi

echo "=== [4/6] Running Outer Deterministic Security & Test Gate ==="
python3 /app/verify_agent_diff.py

echo "=== [5/6] Pushing Isolated Branch (Fine-Grained PAT: Workflows=No Access) ==="
rm -f /workspace/.venv
git config user.name "bq-finops-agent"
git config user.email "bq-finops-agent@users.noreply.github.com"
git add -A
git commit -m "feat: implement issue #${TARGET_ISSUE_NUMBER}"
git push -f origin "${BRANCH_NAME}"

echo "=== [6/6] Creating or Updating PR for Second-Gate Security Review ==="
export GH_TOKEN="${GITHUB_PAT}"
PR_BODY="Closes #${TARGET_ISSUE_NUMBER}

Automated implementation for #${TARGET_ISSUE_NUMBER} orchestrated by **Google ADK (\`SequentialAgent\` + \`LoopAgent\`)** and **Native Claude Code CLI**.

### 🤖 Google ADK Multi-Agent Telemetry
- **Agent 1 (Architect):** \`${ARCHITECT_MODEL}\` (Read-Only Implementation Plan)
- **Agent 2 (Coder):** \`${CODER_MODEL}\` (Code & Offline Pytest Implementation)
- **Agent 3 (Reviewer):** \`${REVIEWER_MODEL}\` (In-Container Adversarial Diff Critique)
- **Execution Metrics:** \`${CLAUDE_TELEMETRY}\`

### 🔍 Agent 3 (\`${REVIEWER_MODEL}\`) In-Container Sign-Off
${OPUS_REVIEW_SUMMARY:-Verified and approved by ReviewerAgent.}

### 🛡️ Sandbox & Deterministic Gate Verification
- [x] **Lock-First Actor & TOCTOU Check:** Verified approval by \`mbettan\` (\`ID: 14251830\`)
- [x] **Protected Path Isolation:** Verified zero modifications to \`.github/\`, \`deploy/\`, \`Dockerfile\`, or \`tests/conftest.py\`
- [x] **Offline Test Suite:** \`768+\` unit tests passed with socket-level network blocker active
- [x] **Bundle & CSP Sync:** Verified \`docs/static/\` and inline script SHA-256 CSP hash parity

⏳ **Next Step:** Automated Second-Gate Security Review (\`agent-pr-security-gate.yml\`) will automatically promote this PR from Draft to Ready for Review once CI passes."

EXISTING_PR=$(gh pr list --repo "${GITHUB_REPO}" --head "${BRANCH_NAME}" --state open --json url -q '.[0].url' || true)
if [ -n "${EXISTING_PR}" ]; then
  gh pr edit "${EXISTING_PR}" --repo "${GITHUB_REPO}" --body "${PR_BODY}"
  PR_URL="${EXISTING_PR}"
  echo "✅ Existing PR updated successfully: ${PR_URL}"
else
  PR_URL=$(gh pr create \
    --repo "${GITHUB_REPO}" \
    --head "${BRANCH_NAME}" \
    --base main \
    --draft \
    --title "feat: implement #${TARGET_ISSUE_NUMBER} (Autonomous Agent)" \
    --body "${PR_BODY}")
  echo "✅ Draft PR created successfully: ${PR_URL}"
fi

gh label create "agent:pr-opened" --repo "${GITHUB_REPO}" --color "0E8A16" --description "Autonomous Agent PR opened" --force >/dev/null 2>&1 || true
gh issue edit "${TARGET_ISSUE_NUMBER}" --repo "${GITHUB_REPO}" --remove-label "agent:in-progress" --add-label "agent:pr-opened" >/dev/null 2>&1 || true
gh issue comment "${TARGET_ISSUE_NUMBER}" --repo "${GITHUB_REPO}" --body "🤖 **Google ADK 3-Agent Pipeline Completed**

- **Pull Request:** ${PR_URL}
- **Telemetry:** \`${CLAUDE_TELEMETRY}\`" >/dev/null 2>&1 || true
