#!/usr/bin/env python3
"""
Google ADK Multi-Agent Orchestrator + Native Claude Code CLI (`@anthropic-ai/claude-code`).
Executes inside the ephemeral Cloud Run Worker container under unprivileged UID 10001 (`agentuser`).

Pipeline Architecture (`SequentialAgent` -> `LoopAgent`):
  1. ArchitectAgent (`claude-opus-5-5`, Read-Only CLI):
     Reads the sanitized issue and codebase (`Read`, `Grep`, `Glob`) and produces a structured
     Implementation & Security Plan in `session.state["architecture_plan"]`.
  2. CodeAndReviewLoop (`LoopAgent`, max_iterations=3):
     a) CoderAgent (`claude-sonnet-5`, Native `claude -p` CLI):
        Implements the plan (and addresses any `session.state["review_feedback"]` from previous
        loop iterations) using `Read`, `Edit`, `Write`, `pytest`, and `ruff`.
     b) ReviewerAgent (`claude-opus-5-5`, Read-Only CLI):
        Audits `git diff base-anchor` against the issue, the Architect's plan, and CLAUDE.md
        security invariants.
        - On `VERDICT: PASS`: Saves `/tmp/opus_review_report.md` and emits `EventActions(escalate=True)`
          to exit the ADK `LoopAgent`.
        - On `VERDICT: REVISE`: Stores line-by-line feedback in `session.state["review_feedback"]`
          so `LoopAgent` automatically loops back to `CoderAgent`.
"""
import asyncio
from collections.abc import AsyncGenerator
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any, Dict, List, Tuple

from google.adk.agents import BaseAgent, InvocationContext, LoopAgent, SequentialAgent
from google.adk.apps import App
from google.adk.events import Event, EventActions
from google.adk.runners import InMemoryRunner
from google.genai import types

ARCHITECT_MODEL = os.environ.get("ARCHITECT_MODEL", "claude-opus-5-5")
CODER_MODEL = os.environ.get("CODER_MODEL", "claude-sonnet-5")
REVIEWER_MODEL = os.environ.get("REVIEWER_MODEL", "claude-opus-5-5")
MAX_REVIEW_LOOPS = int(os.environ.get("MAX_REVIEW_LOOPS", "3"))

WORKSPACE_DIR = Path("/workspace")
PROMPT_FILE = Path("/tmp/sanitized_issue_prompt.txt")
TELEMETRY_FILE = Path("/tmp/claude_execution_log.json")
OPUS_REPORT_FILE = Path("/tmp/opus_review_report.md")


def _run_claude_cli(
    prompt: str,
    model: str,
    allowed_tools: List[str],
    disallowed_tools: List[str],
    max_turns: int = 25,
    accept_edits: bool = False,
) -> Tuple[str, int, float, float]:
    """
    Invoke the native `claude -p` CLI binary synchronously with `shell=False` and stdin piping.
    Returns (result_text, num_turns, duration_sec, cost_usd).
    """
    env = {
        "PATH": "/opt/venv/bin:/usr/local/bin:/usr/bin:/bin",
        "HOME": os.environ.get("HOME", "/home/agentuser"),
        "CLAUDE_CODE_USE_VERTEX": "1",
        "CLOUD_ML_REGION": os.environ.get("CLOUD_ML_REGION", "global"),
        "ANTHROPIC_VERTEX_PROJECT_ID": os.environ.get("ANTHROPIC_VERTEX_PROJECT_ID", "bq-finops-optimizer"),
        "ANTHROPIC_MODEL": model,
    }

    cmd: List[str] = [
        "claude",
        "-p",
        prompt,
        "--max-turns",
        str(max_turns),
        "--output-format",
        "json",
    ]
    if accept_edits:
        cmd.extend(["--permission-mode", "acceptEdits"])
    if allowed_tools:
        cmd.append("--allowedTools")
        cmd.extend(allowed_tools)
    if disallowed_tools:
        cmd.append("--disallowedTools")
        cmd.extend(disallowed_tools)

    proc = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=str(WORKSPACE_DIR) if WORKSPACE_DIR.exists() else None,
        env=env,
        check=False,
    )

    raw_out = proc.stdout.decode("utf-8", errors="replace").strip()
    try:
        payload: Dict[str, Any] = json.loads(raw_out)
        result_text = str(payload.get("result") or "")
        turns = int(payload.get("num_turns") or 1)
        dur_s = round(float(payload.get("duration_ms") or 0.0) / 1000.0, 1)
        cost = float(payload.get("total_cost_usd") or 0.0)
        return result_text, turns, dur_s, cost
    except Exception:
        if proc.returncode != 0:
            err_out = proc.stderr.decode("utf-8", errors="replace").strip()
            raise RuntimeError(f"claude CLI ({model}) exited {proc.returncode}: {err_out[:500]}")
        return raw_out, 1, 0.0, 0.0


def _collect_workspace_diff() -> str:
    """Collect current git diff against base-anchor plus any untracked files."""
    cwd = str(WORKSPACE_DIR) if WORKSPACE_DIR.exists() else "."
    # Stage intent-to-add on untracked files temporarily or read untracked files so git diff sees them
    diff_parts: List[str] = []
    try:
        tracked_diff = subprocess.check_output(
            ["git", "diff", "base-anchor"],
            cwd=cwd,
            stderr=subprocess.DEVNULL,
        ).decode("utf-8", errors="replace")
        diff_parts.append(tracked_diff)
    except Exception:
        pass

    try:
        untracked = subprocess.check_output(
            ["git", "ls-files", "--others", "--exclude-standard", "-z"],
            cwd=cwd,
        ).decode("utf-8", errors="replace").split("\0")
        for rel_path in sorted(f for f in untracked if f and f != ".venv"):
            full_path = Path(cwd) / rel_path
            if full_path.is_file():
                content = full_path.read_text(encoding="utf-8", errors="replace")
                diff_parts.append(f"\n--- /dev/null\n+++ b/{rel_path}\n{content}\n")
    except Exception:
        pass

    return "\n".join(diff_parts).strip()


class ArchitectAgent(BaseAgent):
    """Agent 1: Claude Opus 5.5 Architect that inspects the codebase and produces an implementation plan."""

    async def _run_async_impl(self, ctx: InvocationContext) -> AsyncGenerator[Event, None]:
        issue_prompt = str(ctx.session.state.get("issue_prompt", ""))
        print(f"🧠 [ADK Agent 1: ArchitectAgent ({ARCHITECT_MODEL})] Designing implementation plan...")

        architect_prompt = f"""You are Agent 1 (Principal Software Architect, {ARCHITECT_MODEL}) in a 3-agent Google ADK pipeline.
Your job is to inspect the repository in Read-Only mode (`Read`, `Grep`, `Glob`) and write a concise, actionable Implementation & Test Plan for Agent 2 (CoderAgent).
Do NOT attempt to edit or write files.

{issue_prompt}

Provide:
1. Exact target files and functions/classes to create or modify.
2. Edge cases and type/input validation requirements.
3. Exact unit tests to add in `tests/` (remembering `conftest.py` blocks all live sockets).
4. Security invariants to preserve (Zero `bigquery.tables.getData`, no protected paths modified)."""

        plan_text, turns, dur_s, cost = await asyncio.to_thread(
            _run_claude_cli,
            architect_prompt,
            ARCHITECT_MODEL,
            ["Read", "Grep", "Glob"],
            ["Edit", "Write", "Bash", "WebFetch", "WebSearch"],
            12,
            False,
        )

        delta = {
            "architecture_plan": plan_text,
            "total_turns": int(ctx.session.state.get("total_turns", 0)) + turns,
            "total_duration_s": round(float(ctx.session.state.get("total_duration_s", 0.0)) + dur_s, 1),
            "total_cost_usd": float(ctx.session.state.get("total_cost_usd", 0.0)) + cost,
        }
        ctx.session.state.update(delta)

        print(f"✅ [ADK Agent 1: ArchitectAgent] Plan generated ({turns} turns, {dur_s}s, ${cost:.4f}).")
        yield Event(
            author=self.name,
            invocation_id=ctx.invocation_id,
            content=types.Content(role="model", parts=[types.Part.from_text(text=plan_text)]),
            actions=EventActions(state_delta=delta),
        )


class CoderAgent(BaseAgent):
    """Agent 2: Claude Sonnet 5 Coder that implements the plan and fixes any ReviewerAgent feedback."""

    async def _run_async_impl(self, ctx: InvocationContext) -> AsyncGenerator[Event, None]:
        iteration = int(ctx.session.state.get("loop_iteration", 0)) + 1
        ctx.session.state["loop_iteration"] = iteration

        issue_prompt = str(ctx.session.state.get("issue_prompt", ""))
        arch_plan = str(ctx.session.state.get("architecture_plan", ""))
        review_feedback = str(ctx.session.state.get("review_feedback", ""))

        print(f"🛠️ [ADK Agent 2: CoderAgent ({CODER_MODEL})] Iteration {iteration}/{MAX_REVIEW_LOOPS}...")

        feedback_section = ""
        if review_feedback:
            feedback_section = f"""
<reviewer_opus_critique iteration="{iteration - 1}">
Agent 3 (ReviewerAgent, {REVIEWER_MODEL}) found issues in your previous implementation that MUST be resolved now:
{review_feedback}
</reviewer_opus_critique>
"""

        coder_prompt = f"""You are Agent 2 (Autonomous Software Engineer, {CODER_MODEL}) in a 3-agent Google ADK pipeline.
Implement the feature requested below by following Agent 1's Architecture Plan and resolving any critique from Agent 3.

{issue_prompt}

<architect_opus_plan>
{arch_plan}
</architect_opus_plan>
{feedback_section}
Run `./.venv/bin/pytest <your_test_file>` and `./.venv/bin/ruff check --select E9,F63,F7,F82 <modified_py_files>` before finishing."""

        coder_out, turns, dur_s, cost = await asyncio.to_thread(
            _run_claude_cli,
            coder_prompt,
            CODER_MODEL,
            [
                "Read",
                "Edit",
                "Write",
                "Grep",
                "Glob",
                "Bash(./.venv/bin/pytest *)",
                "Bash(./.venv/bin/ruff check *)",
                "Bash(node tests/test_calculator_engine.js)",
                "Bash(./scripts/sync_docs_bundle.sh)",
                "Bash(git status)",
                "Bash(git diff *)",
            ],
            [
                "Bash(curl *)",
                "Bash(wget *)",
                "Bash(git push *)",
                "Bash(gh *)",
                "Bash(python3 -c *)",
                "Bash(pip *)",
                "Bash(npm *)",
                "WebFetch",
                "WebSearch",
            ],
            30,
            True,
        )

        delta = {
            "loop_iteration": iteration,
            "total_turns": int(ctx.session.state.get("total_turns", 0)) + turns,
            "total_duration_s": round(float(ctx.session.state.get("total_duration_s", 0.0)) + dur_s, 1),
            "total_cost_usd": float(ctx.session.state.get("total_cost_usd", 0.0)) + cost,
        }
        ctx.session.state.update(delta)

        print(f"✅ [ADK Agent 2: CoderAgent] Pass {iteration} complete ({turns} turns, {dur_s}s, ${cost:.4f}).")
        yield Event(
            author=self.name,
            invocation_id=ctx.invocation_id,
            content=types.Content(role="model", parts=[types.Part.from_text(text=coder_out)]),
            actions=EventActions(state_delta=delta),
        )


class ReviewerAgent(BaseAgent):
    """Agent 3: Claude Opus 5.5 Adversarial Code & Security Reviewer that approves or loops back to Agent 2."""

    async def _run_async_impl(self, ctx: InvocationContext) -> AsyncGenerator[Event, None]:
        iteration = int(ctx.session.state.get("loop_iteration", 1))
        issue_prompt = str(ctx.session.state.get("issue_prompt", ""))
        arch_plan = str(ctx.session.state.get("architecture_plan", ""))
        current_diff = _collect_workspace_diff()

        print(f"🔍 [ADK Agent 3: ReviewerAgent ({REVIEWER_MODEL})] Auditing diff (Iteration {iteration}/{MAX_REVIEW_LOOPS})...")

        if not current_diff:
            feedback = "VERDICT: REVISE — No file modifications or untracked test files were produced in /workspace."
            delta = {"review_feedback": feedback, "review_approved": False}
            ctx.session.state.update(delta)
            yield Event(
                author=self.name,
                invocation_id=ctx.invocation_id,
                content=types.Content(role="model", parts=[types.Part.from_text(text=feedback)]),
                actions=EventActions(state_delta=delta, escalate=False),
            )
            return

        reviewer_prompt = f"""You are Agent 3 (Adversarial Code & Security Reviewer, {REVIEWER_MODEL}) in a 3-agent Google ADK pipeline.
Inspect the git diff produced by Agent 2 ({CODER_MODEL}) against the issue specification, Agent 1's Architecture Plan, and our security rules:
1. COMPLETENESS & CORRECTNESS: Does the code accurately implement the requested feature and edge cases with unit tests?
2. DATA-PLANE ISOLATION: Ensure zero references to `bigquery.tables.getData` or direct queries on user tables.
3. PROTECTED PATHS: Ensure zero modifications to `.github/`, `deploy/`, `Dockerfile`, `CLAUDE.md`, or `tests/conftest.py`.
4. SECURITY: Ensure no SSRF, command injection, unescaped innerHTML, or leaked credentials.

{issue_prompt}

<architect_opus_plan>
{arch_plan}
</architect_opus_plan>

<git_diff>
{current_diff[:90000]}
</git_diff>

Provide a concise Markdown review table.
End your response with EXACTLY one of:
- `VERDICT: PASS` (if the implementation and unit tests are ready for PR)
- `VERDICT: REVISE` (followed by specific bullet points for Agent 2 to fix in the next loop iteration)."""

        review_text, turns, dur_s, cost = await asyncio.to_thread(
            _run_claude_cli,
            reviewer_prompt,
            REVIEWER_MODEL,
            ["Read", "Grep", "Glob"],
            ["Edit", "Write", "Bash", "WebFetch", "WebSearch"],
            6,
            False,
        )

        approved = "VERDICT: PASS" in review_text and "VERDICT: REVISE" not in review_text
        delta = {
            "total_turns": int(ctx.session.state.get("total_turns", 0)) + turns,
            "total_duration_s": round(float(ctx.session.state.get("total_duration_s", 0.0)) + dur_s, 1),
            "total_cost_usd": float(ctx.session.state.get("total_cost_usd", 0.0)) + cost,
            "review_approved": approved,
            "review_feedback": "" if approved else review_text,
        }
        ctx.session.state.update(delta)

        if approved:
            print(f"✅ [ADK Agent 3: ReviewerAgent ({REVIEWER_MODEL})] VERDICT: PASS on iteration {iteration}!")
            OPUS_REPORT_FILE.write_text(review_text, encoding="utf-8")
            yield Event(
                author=self.name,
                invocation_id=ctx.invocation_id,
                content=types.Content(role="model", parts=[types.Part.from_text(text=review_text)]),
                actions=EventActions(state_delta=delta, escalate=True),
            )
        else:
            print(f"🔄 [ADK Agent 3: ReviewerAgent ({REVIEWER_MODEL})] VERDICT: REVISE on iteration {iteration}; looping back to Agent 2...")
            yield Event(
                author=self.name,
                invocation_id=ctx.invocation_id,
                content=types.Content(role="model", parts=[types.Part.from_text(text=review_text)]),
                actions=EventActions(state_delta=delta, escalate=False),
            )


def build_adk_app() -> App:
    """Construct the Google ADK Sequential + Loop multi-agent application."""
    architect = ArchitectAgent(
        name="ArchitectAgent",
        description="Claude Opus 5.5 Architect that creates the implementation plan.",
    )
    coder = CoderAgent(
        name="CoderAgent",
        description="Claude Sonnet 5 Coder that implements code & fixes review feedback.",
    )
    reviewer = ReviewerAgent(
        name="ReviewerAgent",
        description="Claude Opus 5.5 Reviewer that critiques the diff and escalates on PASS.",
    )

    code_and_review_loop = LoopAgent(
        name="CodeAndReviewLoop",
        description="Iterative Coder (Sonnet 5) + Reviewer (Opus 5.5) loop.",
        sub_agents=[coder, reviewer],
        max_iterations=MAX_REVIEW_LOOPS,
    )

    root_orchestrator = SequentialAgent(
        name="FinOpsIssueOrchestrator",
        description="End-to-end Architect -> (Coder <-> Reviewer) ADK pipeline.",
        sub_agents=[architect, code_and_review_loop],
    )

    return App(name="bq_finops_adk_agent", root_agent=root_orchestrator)


async def run_pipeline() -> None:
    if not PROMPT_FILE.exists():
        sys.exit(f"FATAL: Sanitized issue prompt not found at {PROMPT_FILE}")

    issue_prompt = PROMPT_FILE.read_text(encoding="utf-8")
    app = build_adk_app()
    runner = InMemoryRunner(app=app)

    session = await runner.session_service.create_session(
        app_name=app.name,
        user_id="cloud_run_worker",
        state={
            "issue_prompt": issue_prompt,
            "loop_iteration": 0,
            "total_turns": 0,
            "total_duration_s": 0.0,
            "total_cost_usd": 0.0,
            "review_approved": False,
        },
    )

    trigger_msg = types.Content(
        role="user",
        parts=[types.Part.from_text(text="Execute the 3-agent ADK Architect -> Coder <-> Reviewer workflow.")],
    )

    async for _ in runner.run_async(
        user_id="cloud_run_worker",
        session_id=session.id,
        new_message=trigger_msg,
    ):
        pass

    final_session = await runner.session_service.get_session(
        app_name=app.name,
        user_id="cloud_run_worker",
        session_id=session.id,
    )
    state = final_session.state if final_session else session.state

    telemetry_summary = {
        "num_turns": state.get("total_turns", 0),
        "duration_ms": int(float(state.get("total_duration_s", 0.0)) * 1000),
        "total_cost_usd": state.get("total_cost_usd", 0.0),
        "loop_iterations": state.get("loop_iteration", 1),
        "review_approved": state.get("review_approved", False),
    }
    TELEMETRY_FILE.write_text(json.dumps(telemetry_summary, indent=2), encoding="utf-8")

    if not state.get("review_approved", False):
        sys.exit(
            f"FATAL: ADK CodeAndReviewLoop exhausted {MAX_REVIEW_LOOPS} iterations without "
            f"earning VERDICT: PASS from ReviewerAgent ({REVIEWER_MODEL}). Aborting before push."
        )


if __name__ == "__main__":
    asyncio.run(run_pipeline())
