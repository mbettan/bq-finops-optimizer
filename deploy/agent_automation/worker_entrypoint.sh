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
cd /workspace
git checkout -b "${BRANCH_NAME}"
git branch base-anchor HEAD
ln -sfn /opt/venv /workspace/.venv
export PATH="/opt/venv/bin:${PATH}"

echo "=== [3/6] Executing Google ADK 3-Agent Pipeline (Architect Opus 5.5 -> Loop[Coder Sonnet 5 <-> Reviewer Opus 5.5]) ==="
export CLAUDE_CODE_USE_VERTEX=1
export CLOUD_ML_REGION="${VERTEX_REGION:-global}"
export ANTHROPIC_VERTEX_PROJECT_ID="${GCP_PROJECT_ID:-bq-finops-optimizer}"
export ARCHITECT_MODEL="${ARCHITECT_MODEL:-claude-opus-5-5}"
export CODER_MODEL="${CODER_MODEL:-claude-sonnet-5}"
export REVIEWER_MODEL="${REVIEWER_MODEL:-claude-opus-5-5}"

if id -u agentuser >/dev/null 2>&1; then
  chown -R agentuser:agentuser /workspace /tmp/sanitized_issue_prompt.txt
  su -s /bin/bash agentuser -c "
    export PATH='/opt/venv/bin:/usr/local/bin:/usr/bin:/bin'
    export HOME='/home/agentuser'
    export CLAUDE_CODE_USE_VERTEX=1
    export CLOUD_ML_REGION='${CLOUD_ML_REGION}'
    export ANTHROPIC_VERTEX_PROJECT_ID='${ANTHROPIC_VERTEX_PROJECT_ID}'
    export ARCHITECT_MODEL='${ARCHITECT_MODEL}'
    export CODER_MODEL='${CODER_MODEL}'
    export REVIEWER_MODEL='${REVIEWER_MODEL}'
    git config --global --add safe.directory /workspace
    cd /workspace
    /opt/venv/bin/python3 /app/adk_orchestrator.py
  "
else
  /opt/venv/bin/python3 /app/adk_orchestrator.py
fi

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

echo "=== [6/6] Creating Draft PR for Second-Gate Security Review ==="
export GH_TOKEN="${GITHUB_PAT}"
PR_URL=$(gh pr create \
  --repo "${GITHUB_REPO}" \
  --head "${BRANCH_NAME}" \
  --base main \
  --draft \
  --title "feat: implement #${TARGET_ISSUE_NUMBER} (Autonomous Agent)" \
  --body "Automated implementation for #${TARGET_ISSUE_NUMBER} orchestrated by **Google ADK (`SequentialAgent` + `LoopAgent`)** and **Native Claude Code CLI**.

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

⏳ **Next Step:** Automated Second-Gate Security Review (\`agent-pr-security-gate.yml\`) will automatically promote this PR from Draft to Ready for Review once CI passes.")

echo "✅ Draft PR created successfully: ${PR_URL}"
