#!/usr/bin/env bash
set -euo pipefail

GITHUB_REPO="${GITHUB_REPO:-mbettan/bq-finops-optimizer-private}"
export CLAUDE_CODE_USE_VERTEX=1
export CLOUD_ML_REGION="${VERTEX_REGION:-global}"
export ANTHROPIC_VERTEX_PROJECT_ID="${GCP_PROJECT_ID:-bq-finops-optimizer}"
export ARCHITECT_MODEL="${ARCHITECT_MODEL:-claude-opus-5-5}"
export CODER_MODEL="${CODER_MODEL:-claude-sonnet-5}"
export REVIEWER_MODEL="${REVIEWER_MODEL:-claude-opus-5-5}"
# Declared here (not just in the su block) so `set -u` cannot abort the worker when an operator
# has not set them. GOLDFISH_ENABLED is the kill switch for the GAS intake gate.
export GOLDFISH_MODEL="${GOLDFISH_MODEL:-${CODER_MODEL}}"
export GOLDFISH_ENABLED="${GOLDFISH_ENABLED:-true}"

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
  INITIAL_PR_SHA=$(gh pr view "${PR_URL}" --repo "${GITHUB_REPO}" --json headRefOid -q '.headRefOid' || git rev-parse HEAD)
  gh pr edit "${PR_URL}" --repo "${GITHUB_REPO}" --body "${INITIAL_PR_BODY}" >/dev/null 2>&1 || true
  echo "✅ Attached to existing PR for live agent updates: ${PR_URL} (SHA: ${INITIAL_PR_SHA})"
else
  git commit --allow-empty -m "chore(agent): initialize ADK pipeline for issue #${TARGET_ISSUE_NUMBER}"
  INITIAL_PR_SHA=$(git rev-parse HEAD)
  git push -f origin "${BRANCH_NAME}"
  PR_URL=$(gh pr create \
    --repo "${GITHUB_REPO}" \
    --head "${BRANCH_NAME}" \
    --base main \
    --draft \
    --title "feat: implement #${TARGET_ISSUE_NUMBER} (Autonomous Agent)" \
    --body "${INITIAL_PR_BODY}")
  git reset --hard base-anchor
  echo "✅ Initialized Draft PR for live agent updates: ${PR_URL} (SHA: ${INITIAL_PR_SHA})"
fi
export INITIAL_PR_SHA

ln -sfn /opt/venv /workspace/.venv
mkdir -p /workspace/.git/info
printf "\n.venv\n.claude/\n.tmp*\n.coverage*\n" >> /workspace/.git/info/exclude
export PATH="/opt/venv/bin:${PATH}"

echo "=== [3/6] Executing Google ADK 3-Agent Pipeline (Architect Opus 5.5 -> Loop[Coder Sonnet 5 <-> Reviewer Opus 5.5]) ==="
export CLAUDE_CODE_USE_VERTEX=1
export CLOUD_ML_REGION="${VERTEX_REGION:-global}"
export ANTHROPIC_VERTEX_PROJECT_ID="${GCP_PROJECT_ID:-bq-finops-optimizer}"
export ARCHITECT_MODEL="${ARCHITECT_MODEL:-claude-opus-5-5}"
export CODER_MODEL="${CODER_MODEL:-claude-sonnet-5}"
export REVIEWER_MODEL="${REVIEWER_MODEL:-claude-opus-5-5}"

rm -f /tmp/claude_execution_log.json /tmp/opus_review_report.md /tmp/adk_stage_events.jsonl /tmp/adk_pr_telemetry_state.json /tmp/adk_watcher_stop
touch /tmp/adk_stage_events.jsonl
chmod 666 /tmp/adk_stage_events.jsonl

# Launch privileged root PR telemetry watcher (Sticky Comment + Milestone Artifact Comments + Commit Statuses)
/opt/venv/bin/python3 -u /app/pr_live_telemetry.py \
  --mode watch \
  --repo "${GITHUB_REPO}" \
  --pr-url "${PR_URL}" \
  --issue "${TARGET_ISSUE_NUMBER}" &
WATCHER_PID=$!

PIPELINE_SUCCEEDED=0
handle_worker_exit() {
  local exit_code=$?
  if [ "${PIPELINE_SUCCEEDED}" -eq 1 ] || [ "${exit_code}" -eq 0 ]; then
    return 0
  fi
  touch /tmp/adk_watcher_stop 2>/dev/null || true
  if [ -n "${WATCHER_PID:-}" ]; then
    wait "${WATCHER_PID}" 2>/dev/null || true
  fi
  export GH_TOKEN="${GITHUB_PAT}"
  FAILURE_META=$(python3 -c "
import json
try:
    d = json.load(open('/tmp/claude_execution_log.json'))
    fc = d.get('failure_class') or 'infrastructure'
    fl = d.get('failure_label') or 'failure:infrastructure'
    fg = d.get('failure_glossary') or 'Infrastructure Failure: Worker container exited before completion.'
except Exception:
    fc, fl, fg = 'infrastructure', 'failure:infrastructure', f'Infrastructure Failure (exit ${exit_code}): Worker container exited before completion.'
print(f'{fc}|{fl}|{fg}')
")
  FAILURE_CLASS=$(echo "${FAILURE_META}" | cut -d'|' -f1)
  FAILURE_LABEL=$(echo "${FAILURE_META}" | cut -d'|' -f2)
  FAILURE_GLOSSARY=$(echo "${FAILURE_META}" | cut -d'|' -f3-)

  # GAS `failure_classification.py`: a `spec_gap` is a request for the human author to sharpen the
  # issue, NOT an agent crash. It gets an amber `agent:needs-clarification` label and skips the red
  # `agent:failed-needs-human` state so the triage queue stays honest about what actually broke.
  if [ "${FAILURE_CLASS}" = "spec_gap" ]; then
    ESCALATION_LABELS="${FAILURE_LABEL}"
    LABEL_COLOR="FBCA04"
    LABEL_DESC="GAS Goldfish: issue spec is underspecified; author action required"
    STATUS_CONTEXT="GAS Intake / 0. Goldfish Spec-Completeness"
    echo "🐠 [GAS State Escalation] Goldfish refused issue #${TARGET_ISSUE_NUMBER} as underspecified; transitioning to ${FAILURE_LABEL}..."
  else
    ESCALATION_LABELS="agent:failed-needs-human,${FAILURE_LABEL}"
    LABEL_COLOR="B60205"
    LABEL_DESC="GAS Failure Class: ${FAILURE_CLASS}"
    STATUS_CONTEXT="ADK / Pipeline Escalation"
    echo "⚠️ [GAS State Escalation] Worker exited with code ${exit_code}; transitioning issue #${TARGET_ISSUE_NUMBER} to agent:failed-needs-human..."
    gh label create "agent:failed-needs-human" --repo "${GITHUB_REPO}" --color "D93F0B" --description "Autonomous Agent escalated to human review" --force >/dev/null 2>&1 || true
  fi

  gh label create "${FAILURE_LABEL}" --repo "${GITHUB_REPO}" --color "${LABEL_COLOR}" --description "${LABEL_DESC}" --force >/dev/null 2>&1 || true
  gh issue edit "${TARGET_ISSUE_NUMBER}" --repo "${GITHUB_REPO}" --remove-label "agent:in-progress" --add-label "${ESCALATION_LABELS}" >/dev/null 2>&1 || true
  if [ -n "${PR_URL:-}" ]; then
    gh pr edit "${PR_URL}" --repo "${GITHUB_REPO}" --add-label "${ESCALATION_LABELS}" >/dev/null 2>&1 || true
  fi
  if [ -n "${INITIAL_PR_SHA:-}" ]; then
    gh api "repos/${GITHUB_REPO}/statuses/${INITIAL_PR_SHA}" \
      -X POST \
      -f state="failure" \
      -f context="${STATUS_CONTEXT}" \
      -f description="[${FAILURE_CLASS}] ${FAILURE_GLOSSARY:0:110}" \
      -f target_url="${PR_URL:-https://github.com/${GITHUB_REPO}}" >/dev/null 2>&1 || true
  fi

  if [ "${FAILURE_CLASS}" = "spec_gap" ]; then
    # Surface exactly what the spec-blind fresh reader had to invent, so the author knows what to add.
    python3 -c "
import json
try:
    d = json.load(open('/tmp/claude_execution_log.json'))
except Exception:
    d = {}
restatement = (d.get('goldfish_restatement') or '(no restatement produced)').strip()
findings = d.get('goldfish_findings') or []
lines = []
for f in findings:
    if not isinstance(f, dict):
        continue
    sev = str(f.get('severity', 'minor'))
    msg = str(f.get('message', '')).strip()
    if msg:
        lines.append(f'- \`{sev}\` — {msg}')
body = '\n'.join(lines) or '- _(no structured findings returned)_'
print('### 🐠 Fresh-reader restatement attempt\n')
print('> ' + restatement.replace(chr(10), chr(10) + '> '))
print('\n### Unanswered questions\n')
print(body)
" > /tmp/goldfish_report.md 2>/dev/null || echo "" > /tmp/goldfish_report.md
    {
      printf '%s\n\n' "🐠 **GAS Goldfish Spec-Completeness Pre-Screen: \`refuse\`** (\`${FAILURE_LABEL}\`)"
      printf '%s\n' "A spec-blind fresh reader — no repo access, no wiki, no prior context — read only this issue's acceptance criteria and could not restate *what changes, where, and how we would know it worked*. The pipeline stopped **before** any code was written, so nothing was committed and no budget was spent on implementation."
      printf '\n'
      cat /tmp/goldfish_report.md
      printf '\n%s\n' "---"
      printf '%s\n' "- **GAS Cause Glossary:** ${FAILURE_GLOSSARY}"
      printf '%s\n' "- **Action:** Removed \`agent:in-progress\`. Sharpen the acceptance criteria and re-apply \`agent:ready\`, or add the \`agent:force\` label to bypass this gate."
    } > /tmp/goldfish_comment.md
    gh issue comment "${TARGET_ISSUE_NUMBER}" --repo "${GITHUB_REPO}" --body-file /tmp/goldfish_comment.md >/dev/null 2>&1 || true
  else
    gh issue comment "${TARGET_ISSUE_NUMBER}" --repo "${GITHUB_REPO}" --body "🛑 **Google ADK Pipeline Escalated (\`agent:failed-needs-human\` / \`${FAILURE_LABEL}\`)**

- **Draft Pull Request:** ${PR_URL:-N/A}
- **GAS Failure Classification:** \`${FAILURE_CLASS}\` (\`${FAILURE_LABEL}\`)
- **GAS Cause Glossary:** ${FAILURE_GLOSSARY}
- **Exit Code:** \`${exit_code}\`
- **Action:** Removed \`agent:in-progress\` lock and escalated for human inspection." >/dev/null 2>&1 || true
  fi
}
trap handle_worker_exit EXIT

if id -u agentuser >/dev/null 2>&1; then
  chown -R agentuser:agentuser /workspace /tmp/sanitized_issue_prompt.txt /tmp/adk_stage_events.jsonl
  # Written by root in step [1/6] but read by agentuser. Without this the read fails, and because
  # `_load_issue_meta` swallows the error the Goldfish gate would skip silently on every issue.
  chmod 0644 /tmp/adk_issue_meta.json 2>/dev/null || true
  su -s /bin/bash agentuser -c "
    export PATH='/opt/venv/bin:/usr/local/bin:/usr/bin:/bin'
    export HOME='/home/agentuser'
    export CLAUDE_CODE_USE_VERTEX=1
    export CLOUD_ML_REGION='${CLOUD_ML_REGION}'
    export ANTHROPIC_VERTEX_PROJECT_ID='${ANTHROPIC_VERTEX_PROJECT_ID}'
    export ARCHITECT_MODEL='${ARCHITECT_MODEL}'
    export CODER_MODEL='${CODER_MODEL}'
    export REVIEWER_MODEL='${REVIEWER_MODEL}'
    export GOLDFISH_MODEL='${GOLDFISH_MODEL}'
    export GOLDFISH_ENABLED='${GOLDFISH_ENABLED}'
    export PYTHONUNBUFFERED=1
    git config --global --add safe.directory /workspace
    cd /workspace
    /opt/venv/bin/python3 -u /app/adk_orchestrator.py
  "
  chown -R root:root /workspace
else
  /opt/venv/bin/python3 -u /app/adk_orchestrator.py
fi

sleep 3
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
    cov = d.get("diff_coverage_pct", 100.0)
    print(f"ADK Loops: {loops}/3 | Total Turns: {turns} | Diff Coverage: {cov:.1f}% | Duration: {dur_s}s | Est. Cost: ${cost:.4f}")
except Exception as e:
    print(f"Telemetry unavailable ({e})")
')
echo "📊 [Google ADK + Claude CLI Telemetry] ${CLAUDE_TELEMETRY}"

echo "=== [4/6] Running Outer Deterministic Security & Test Gate ==="
python3 /app/verify_agent_diff.py

echo "=== [5/6] Pushing Isolated Branch with GAS Provenance Commit Trailers ==="
rm -f /workspace/.venv /workspace/.coverage*
git config user.name "bq-finops-agent"
git config user.email "bq-finops-agent@users.noreply.github.com"
git add -A

PROVENANCE_TRAILERS=$(python3 -c '
import json, os
try:
    d = json.load(open("/tmp/claude_execution_log.json"))
except Exception:
    d = {}
issue = os.environ.get("TARGET_ISSUE_NUMBER", "unknown")
arch = os.environ.get("ARCHITECT_MODEL", "claude-opus-5-5")
coder = os.environ.get("CODER_MODEL", "claude-sonnet-5")
rev = os.environ.get("REVIEWER_MODEL", "claude-opus-5-5")
loops = d.get("loop_iterations", 1)
turns = d.get("num_turns", 0)
cov = float(d.get("diff_coverage_pct", 100.0))
cost = float(d.get("total_cost_usd", 0.0))
print(f"ADK-Issue: #{issue}\nADK-Architect-Model: {arch}\nADK-Coder-Model: {coder}\nADK-Reviewer-Model: {rev}\nADK-Loops: {loops}/3\nADK-Turns: {turns}\nADK-Diff-Coverage: {cov:.1f}%\nADK-Cost-USD: {cost:.4f}\nADK-Gate-Status: verified")
')

git commit -m "feat: implement issue #${TARGET_ISSUE_NUMBER}

${PROVENANCE_TRAILERS}"
FINAL_SHA=$(git rev-parse HEAD)
git push -f origin "${BRANCH_NAME}"

echo "=== [6/6] Finalizing PR Dashboard, Sticky Comment, Provenance Labels & Native Commit Statuses ==="
export GH_TOKEN="${GITHUB_PAT}"
/opt/venv/bin/python3 -u /app/pr_live_telemetry.py \
  --mode finalize \
  --repo "${GITHUB_REPO}" \
  --pr-url "${PR_URL}" \
  --issue "${TARGET_ISSUE_NUMBER}" \
  --sha "${FINAL_SHA}" \
  --telemetry "${CLAUDE_TELEMETRY}"

echo "✅ PR finalized with Sticky Dashboard, Milestone Comments & Commit Statuses: ${PR_URL}"

gh label create "agent:pr-opened" --repo "${GITHUB_REPO}" --color "0E8A16" --description "Autonomous Agent PR opened" --force >/dev/null 2>&1 || true
gh label create "agent:generated" --repo "${GITHUB_REPO}" --color "1D76DB" --description "GAS/ADK Provenance: Autonomous Agent Generated" --force >/dev/null 2>&1 || true
gh label create "adk:verified" --repo "${GITHUB_REPO}" --color "0E8A16" --description "GAS/ADK Provenance: Verified by Opus 5.5 + Pinned Gates" --force >/dev/null 2>&1 || true
gh label create "gate:passed" --repo "${GITHUB_REPO}" --color "0E8A16" --description "GAS/ADK Provenance: Diff-Coverage & AST Mock-Gate Passed" --force >/dev/null 2>&1 || true
gh pr edit "${PR_URL}" --repo "${GITHUB_REPO}" --add-label "agent:generated,adk:verified,gate:passed" >/dev/null 2>&1 || true
gh issue edit "${TARGET_ISSUE_NUMBER}" --repo "${GITHUB_REPO}" --remove-label "agent:in-progress" --add-label "agent:pr-opened" >/dev/null 2>&1 || true
gh issue comment "${TARGET_ISSUE_NUMBER}" --repo "${GITHUB_REPO}" --body "🤖 **Google ADK 3-Agent Pipeline Completed**

- **Pull Request:** ${PR_URL}
- **Provenance Labels:** \`agent:generated\`, \`adk:verified\`, \`gate:passed\`
- **Telemetry:** \`${CLAUDE_TELEMETRY}\`" >/dev/null 2>&1 || true
PIPELINE_SUCCEEDED=1
