#!/usr/bin/env bash
set -euo pipefail

GITHUB_REPO="${GITHUB_REPO:-mbettan/bq-finops-optimizer-private}"

echo "=== [1/6] Independent Security & Timeline Re-Verification ==="
python3 /app/verify_issue_actor.py

echo "=== [2/6] Shallow Cloning Repository (1 Issue = 1 Branch) ==="
BRANCH_NAME="agent/issue-${TARGET_ISSUE_NUMBER}"
git clone --depth=1 "https://x-access-token:${GITHUB_PAT}@github.com/${GITHUB_REPO}.git" /workspace
cd /workspace
git checkout -b "${BRANCH_NAME}"

echo "=== [3/6] Executing Headless Claude Code via Google Cloud Vertex AI (Zero Static API Keys) ==="
export CLAUDE_CODE_USE_VERTEX=1
export CLOUD_ML_REGION="${VERTEX_REGION:-us-east5}"
export ANTHROPIC_VERTEX_PROJECT_ID="${GCP_PROJECT_ID:-bq-finops-optimizer}"

claude -p "$(cat /tmp/sanitized_issue_prompt.txt)" \
  --permission-mode acceptEdits \
  --max-turns 35 \
  --output-format json \
  --allowedTools \
    "Read" \
    "Edit" \
    "Write" \
    "Grep" \
    "Glob" \
    "Bash(./.venv/bin/pytest *)" \
    "Bash(./.venv/bin/ruff check *)" \
    "Bash(node tests/test_calculator_engine.js)" \
    "Bash(./scripts/sync_docs_bundle.sh)" \
    "Bash(git status)" \
    "Bash(git diff *)" \
  --disallowedTools \
    "Bash(curl *)" \
    "Bash(wget *)" \
    "Bash(git push *)" \
    "Bash(gh *)" \
    "Bash(python3 -c *)" \
    "Bash(pip *)" \
    "Bash(npm *)" \
    "WebFetch" \
    "WebSearch" > /tmp/claude_execution_log.json

echo "=== [4/6] Running Outer Deterministic Security & Test Gate ==="
python3 /app/verify_agent_diff.py

echo "=== [5/6] Pushing Isolated Branch (Fine-Grained PAT: Workflows=No Access) ==="
git config user.name "bq-finops-agent"
git config user.email "bq-finops-agent@users.noreply.github.com"
git add -A
git commit -m "feat: implement issue #${TARGET_ISSUE_NUMBER}"
git push origin "${BRANCH_NAME}"

echo "=== [6/6] Creating Draft PR for Second-Gate Security Review ==="
export GH_TOKEN="${GITHUB_PAT}"
PR_URL=$(gh pr create \
  --repo "${GITHUB_REPO}" \
  --head "${BRANCH_NAME}" \
  --base main \
  --draft \
  --title "feat: implement #${TARGET_ISSUE_NUMBER} (Autonomous Agent)" \
  --body "Automated implementation for #${TARGET_ISSUE_NUMBER}.

### 🛡️ Sandbox & Deterministic Gate Verification
- [x] **Actor & TOCTOU Check:** Verified approval by \`mbettan\` (\`ID: 14251830\`)
- [x] **Protected Path Isolation:** Verified zero modifications to \`.github/\`, \`deploy/\`, \`Dockerfile\`, or \`tests/conftest.py\`
- [x] **Offline Test Suite:** \`761+\` unit tests passed with socket-level network blocker active
- [x] **Bundle & CSP Sync:** Verified \`docs/static/\` and inline script SHA-256 CSP hash parity

⏳ **Next Step:** Automated Second-Gate Security Review (\`agent-pr-security-gate.yml\`) is now running on this diff before human review by @mbettan.")

echo "✅ Draft PR created successfully: ${PR_URL}"
