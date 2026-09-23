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
STAGE_EVENTS_FILE = Path("/tmp/adk_stage_events.jsonl")


def _emit_pr_stage_event(
    stage: str,
    agent_name: str,
    model: str,
    status: str,
    turns: int = 0,
    duration_s: float = 0.0,
    cost_usd: float = 0.0,
    iteration: int = 1,
    details: str = "",
) -> None:
    """
    Append a structured stage completion event to `/tmp/adk_stage_events.jsonl`.
    A privileged root background watcher in `worker_entrypoint.sh` tails this file and posts
    live progress comments & checklist updates to the GitHub Pull Request without exposing
    `GITHUB_PAT` to the unprivileged `agentuser` (UID 10001) sandbox.
    """
    event_payload = {
        "stage": stage,
        "agent": agent_name,
        "model": model,
        "status": status,
        "iteration": iteration,
        "turns": turns,
        "duration_s": round(float(duration_s), 1),
        "cost_usd": round(float(cost_usd), 4),
        "details": details[:12000],
    }
    try:
        with STAGE_EVENTS_FILE.open("a", encoding="utf-8") as f:
            f.write(json.dumps(event_payload, ensure_ascii=False) + "\n")
            f.flush()
    except Exception as exc:
        print(f"[StageEvent Warning] Could not write stage event: {exc}")


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


import re

PROTECTED_PREFIXES = (
    ".github/",
    "deploy/",
    "Dockerfile",
    "CLAUDE.md",
    "tests/conftest.py",
)

# MetaGPT Sec. 3.2 & Appendix E.2: Role-Specific Context Subscription Matrix
# Prevents Information Overload by restricting each agent to only the state keys required by its SOP role.
ROLE_SUBSCRIPTIONS: Dict[str, List[str]] = {
    "ArchitectAgent": ["issue_prompt"],
    "CoderAgent_Initial": ["issue_prompt", "architecture_plan", "sop_allowed_files", "sop_targeted_tests"],
    "CoderAgent_Revision": ["architecture_plan", "sop_allowed_files", "sop_targeted_tests", "executable_feedback", "review_feedback"],
    "ReviewerAgent": ["architecture_plan", "sop_allowed_files", "executable_feedback_summary"],
}


def _subscribe_role_context(role_key: str, state: Dict[str, Any]) -> Dict[str, Any]:
    """
    MetaGPT Publish-Subscribe filter (Sec. 3.2 / Appendix E.2):
    Extracts only the role-subscribed keys from the shared session state pool to prevent context bloat.
    """
    allowed_keys = ROLE_SUBSCRIPTIONS.get(role_key, [])
    return {k: state.get(k) for k in allowed_keys if k in state and state.get(k) not in (None, "", [])}


def _extract_sop_metadata(plan_text: str) -> Dict[str, List[str]]:
    """
    MetaGPT Sec. 3.2 & Sec. 4.4 (Table 6): Parse structured SOP JSON block (`ALLOWED_FILE_LIST`
    and `TARGETED_TEST_FILES`) emitted by ArchitectAgent during Upfront Prompt Expansion.
    """
    allowed_files: List[str] = []
    targeted_tests: List[str] = []

    json_match = re.search(r"```json\s*(\{.*?\})\s*```", plan_text, flags=re.DOTALL)
    if json_match:
        try:
            parsed = json.loads(json_match.group(1))
            if isinstance(parsed.get("allowed_file_list"), list):
                allowed_files = [
                    os.path.normpath(str(p)).replace("\\", "/").removeprefix("./")
                    for p in parsed["allowed_file_list"]
                    if str(p).strip()
                ]
            if isinstance(parsed.get("targeted_test_files"), list):
                targeted_tests = [
                    os.path.normpath(str(p)).replace("\\", "/").removeprefix("./")
                    for p in parsed["targeted_test_files"]
                    if str(p).strip()
                ]
        except Exception:
            pass

    return {
        "sop_allowed_files": allowed_files,
        "sop_targeted_tests": targeted_tests,
    }


def _collect_changed_files(cwd: str) -> List[str]:
    """Return normalized relative paths of all modified/added/untracked files in workspace."""
    files: set[str] = set()
    for cmd in (
        ["git", "diff", "--name-only", "-z", "base-anchor"],
        ["git", "ls-files", "--others", "--exclude-standard", "-z"],
    ):
        try:
            out = subprocess.check_output(cmd, cwd=cwd, stderr=subprocess.DEVNULL).decode("utf-8", errors="replace")
            for f in out.split("\0"):
                norm = os.path.normpath(f).replace("\\", "/").removeprefix("./") if f.strip() else ""
                if norm and norm not in (".", ".venv"):
                    files.add(norm)
        except Exception:
            pass
    return sorted(files)


def _run_executable_feedback_gate(
    sop_allowed_files: List[str],
    sop_targeted_tests: List[str],
) -> Tuple[bool, str]:
    """
    MetaGPT Sec. 3.3 (Fig. 2 Right, Table 1): Deterministic Pre-Review Executable Feedback Gate.
    Executes inside the LoopAgent BEFORE invoking ReviewerAgent (claude-opus-5-5):
      1. Protected Path & SOP File-Scope Verification
      2. Pre-Compilation AST/Syntax Check (`ruff check --select E9,F63,F7,F82`)
      3. Frontend Bundle & Node Engine Check (if `src/static/` touched)
      4. Targeted & Changed Unit Test Execution (`pytest` with offline socket blocker)
    Returns (passed: bool, feedback_report: str).
    If `passed` is False, ReviewerAgent short-circuits back to CoderAgent in <2s for $0.00 LLM cost.
    """
    cwd = str(WORKSPACE_DIR) if WORKSPACE_DIR.exists() else "."
    changed_files = _collect_changed_files(cwd)
    if not changed_files:
        return (
            False,
            "EXECUTABLE FEEDBACK FAILURE [No Changes]: No modified or untracked files found in /workspace.",
        )

    # 1. Protected paths & SOP scope check
    for path in changed_files:
        if any(path == p or path.startswith(p) for p in PROTECTED_PREFIXES):
            return (
                False,
                f"EXECUTABLE FEEDBACK FAILURE [Protected Path Violation]: '{path}' is a protected path ({PROTECTED_PREFIXES}). Revert changes to '{path}'.",
            )

    if sop_allowed_files:
        out_of_scope = [
            f for f in changed_files
            if f not in sop_allowed_files and not f.startswith("tests/") and not f.startswith("docs/static/")
        ]
        if out_of_scope:
            return (
                False,
                f"EXECUTABLE FEEDBACK FAILURE [SOP Scope Creep]: Modified files {out_of_scope} were not authorized in ArchitectAgent's `allowed_file_list` ({sop_allowed_files}).",
            )

    py_bin = "/opt/venv/bin" if Path("/opt/venv/bin/pytest").exists() else os.path.dirname(sys.executable)
    ruff_bin = str(Path(py_bin) / "ruff")
    pytest_bin = str(Path(py_bin) / "pytest")

    # 2. Pre-Compilation Syntax / Undefined Names Check (ruff)
    py_files = [f for f in changed_files if f.endswith(".py") and (Path(cwd) / f).exists()]
    if py_files and Path(ruff_bin).exists():
        ruff_proc = subprocess.run(
            [ruff_bin, "check", "--select", "E9,F63,F7,F82", *py_files],
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )
        if ruff_proc.returncode != 0:
            out = ruff_proc.stdout.decode("utf-8", errors="replace").strip()
            return (
                False,
                f"EXECUTABLE FEEDBACK FAILURE [Pre-Compilation Ruff Check]:\n```\n{out[:3000]}\n```",
            )

    # 3. Targeted & Modified Pytest Execution
    test_targets: List[str] = []
    for t in sop_targeted_tests + [f for f in changed_files if f.startswith("tests/") and f.endswith(".py")]:
        if t not in test_targets and (Path(cwd) / t).exists():
            test_targets.append(t)
    if not test_targets:
        test_targets = ["tests/test_utils.py"] if (Path(cwd) / "tests/test_utils.py").exists() else ["tests"]

    pytest_cmd = [pytest_bin if Path(pytest_bin).exists() else "pytest", "-q", "--tb=short", *test_targets]
    pytest_proc = subprocess.run(
        pytest_cmd,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    pytest_out = pytest_proc.stdout.decode("utf-8", errors="replace").strip()
    if pytest_proc.returncode != 0:
        return (
            False,
            f"EXECUTABLE FEEDBACK FAILURE [Pytest Runtime Traceback on {test_targets}]:\n```\n{pytest_out[-4000:]}\n```",
        )

    summary = (
        f"EXECUTABLE FEEDBACK PASSED:Changed={changed_files} | "
        f"Ruff(E9,F63,F7,F82)=0 errors | Pytest({', '.join(test_targets)})={pytest_out.splitlines()[-1] if pytest_out else 'PASSED'}"
    )
    return True, summary


class ArchitectAgent(BaseAgent):
    """
    Agent 1: Claude Opus 5.5 Architect implementing MetaGPT Upfront Prompt Expansion (Sec. 4.4, Table 6)
    and Standardized Operating Procedure (SOP) Structured Handover Schema (Sec. 3.2, Fig. 3).
    """

    async def _run_async_impl(self, ctx: InvocationContext) -> AsyncGenerator[Event, None]:
        sub_ctx = _subscribe_role_context("ArchitectAgent", ctx.session.state)
        issue_prompt = str(sub_ctx.get("issue_prompt", ""))
        print(f"🧠 [ADK Agent 1: ArchitectAgent ({ARCHITECT_MODEL})] Running Upfront Prompt Expansion & SOP Design...")

        architect_prompt = f"""You are Agent 1 (Principal Software Architect, {ARCHITECT_MODEL}) in a MetaGPT-inspired 3-agent Google ADK pipeline.
Inspect the repository in Read-Only mode (`Read`, `Grep`, `Glob`) and perform **Upfront Prompt Expansion** into a strict **SOP Handover Specification** for Agent 2 (CoderAgent).
Do NOT edit or write any files.

{issue_prompt}

You MUST output your response in the following standardized SOP structure:

### 1. SOP_METADATA_JSON
```json
{{
  "allowed_file_list": ["<exact relative paths of files to modify or create>"],
  "targeted_test_files": ["<exact relative paths of pytest files to run>"]
}}
```

### 2. INTERFACE_AND_DATA_STRUCTURES
- Exact function/method signatures, parameter types, default values, return types, and docstring contracts.

### 3. LOGIC_ANALYSIS_BY_FILE
- For each file in `allowed_file_list`, specify the exact insertion point, algorithm, and edge-case handling (`None`, `NaN`, `bool`, `inf`, negative numbers, boundary conditions).

### 4.SECURITY_AND_REPO_INVARIANTS
- Zero `bigquery.tables.getData` usage, no protected paths touched, offline `pytest` compatibility (`@pytest.mark.usefixtures("mock_bq_all")` if testing validators/CLI), and whether `src/static/` bundle sync is needed.

### 5. ANYTHING_UNCLEAR_RESOLVED
- Explicitly resolve any potential ambiguity in the user issue so Agent 2 (`{CODER_MODEL}`) can implement the code in a single pass without guessing."""

        plan_text, turns, dur_s, cost = await asyncio.to_thread(
            _run_claude_cli,
            architect_prompt,
            ARCHITECT_MODEL,
            ["Read", "Grep", "Glob"],
            ["Edit", "Write", "Bash", "WebFetch", "WebSearch"],
            12,
            False,
        )

        sop_meta = _extract_sop_metadata(plan_text)
        delta = {
            "architecture_plan": plan_text,
            "sop_allowed_files": sop_meta["sop_allowed_files"],
            "sop_targeted_tests": sop_meta["sop_targeted_tests"],
            "total_turns": int(ctx.session.state.get("total_turns", 0)) + turns,
            "total_duration_s": round(float(ctx.session.state.get("total_duration_s", 0.0)) + dur_s, 1),
            "total_cost_usd": float(ctx.session.state.get("total_cost_usd", 0.0)) + cost,
        }
        ctx.session.state.update(delta)

        print(
            f"✅ [ADK Agent 1: ArchitectAgent] SOP Plan generated ({turns} turns, {dur_s}s, ${cost:.4f}) | "
            f"Allowed files: {sop_meta['sop_allowed_files']}"
        )
        _emit_pr_stage_event(
            stage="1/3",
            agent_name="Agent 1: ArchitectAgent (Upfront SOP Plan)",
            model=ARCHITECT_MODEL,
            status="PASSED",
            turns=turns,
            duration_s=dur_s,
            cost_usd=cost,
            iteration=1,
            details=plan_text,
        )
        yield Event(
            author=self.name,
            invocation_id=ctx.invocation_id,
            content=types.Content(role="model", parts=[types.Part.from_text(text=plan_text)]),
            actions=EventActions(state_delta=delta),
        )


class CoderAgent(BaseAgent):
    """
    Agent 2: Claude Sonnet 5 Coder with Role-Specific Context Subscription (MetaGPT Sec. 3.2 & Appendix E.2).
    On Iteration 1, subscribes to the expanded SOP Plan.
    On Iteration 2+, subscribes only to the SOP Plan + Executable/Reviewer delta feedback to prevent Information Overload.
    """

    async def _run_async_impl(self, ctx: InvocationContext) -> AsyncGenerator[Event, None]:
        iteration = int(ctx.session.state.get("loop_iteration", 0)) + 1
        ctx.session.state["loop_iteration"] = iteration

        role_key = "CoderAgent_Initial" if iteration == 1 else "CoderAgent_Revision"
        sub_ctx = _subscribe_role_context(role_key, ctx.session.state)

        arch_plan = str(sub_ctx.get("architecture_plan", ""))
        sop_allowed_files = sub_ctx.get("sop_allowed_files", [])
        sop_targeted_tests = sub_ctx.get("sop_targeted_tests", [])
        exec_feedback = str(sub_ctx.get("executable_feedback", ""))
        review_feedback = str(sub_ctx.get("review_feedback", ""))

        print(
            f"🛠️ [ADK Agent 2: CoderAgent ({CODER_MODEL})] Iteration {iteration}/{MAX_REVIEW_LOOPS} "
            f"(Subscribed profile: {role_key}, keys={list(sub_ctx.keys())})..."
        )

        feedback_section = ""
        if exec_feedback or review_feedback:
            feedback_section = f"""
<iterative_feedback iteration="{iteration - 1}">
The previous iteration did NOT pass verification. Fix ONLY the defects reported below while adhering to `allowed_file_list` ({sop_allowed_files}):
{exec_feedback}
{review_feedback}
</iterative_feedback>
"""

        issue_section = f"\n{sub_ctx['issue_prompt']}\n" if "issue_prompt" in sub_ctx else ""
        test_hint = " ".join(sop_targeted_tests) if sop_targeted_tests else "<your_test_file>"

        coder_prompt = f"""You are Agent 2 (Autonomous Software Engineer, {CODER_MODEL}) in a MetaGPT-inspired 3-agent Google ADK pipeline.
Implement the specification in Agent 1's SOP Architecture Plan below.
Strictly restrict your file edits/creations to `allowed_file_list`: {sop_allowed_files}.
{issue_section}
<architect_sop_plan>
{arch_plan}
</architect_sop_plan>
{feedback_section}
Before finishing, execute `./.venv/bin/pytest -q {test_hint}` and `./.venv/bin/ruff check --select E9,F63,F7,F82 <modified_py_files>`."""

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
        changed_now = _collect_changed_files(str(WORKSPACE_DIR) if WORKSPACE_DIR.exists() else ".")
        _emit_pr_stage_event(
            stage="2/3",
            agent_name=f"Agent 2: CoderAgent (Pass {iteration}/{MAX_REVIEW_LOOPS})",
            model=CODER_MODEL,
            status="COMPLETED",
            turns=turns,
            duration_s=dur_s,
            cost_usd=cost,
            iteration=iteration,
            details=f"**Modified/Created Files:** `{changed_now}`\n\n{coder_out}",
        )
        yield Event(
            author=self.name,
            invocation_id=ctx.invocation_id,
            content=types.Content(role="model", parts=[types.Part.from_text(text=coder_out)]),
            actions=EventActions(state_delta=delta),
        )


class ReviewerAgent(BaseAgent):
    """
    Agent 3: MetaGPT Deterministic Pre-Review Executable Feedback Gate (Sec. 3.3, Fig. 2 Right)
    followed by Claude Opus 5.5 Adversarial Diff Critique with Role-Specific Subscription (Sec. 3.2).
    """

    async def _run_async_impl(self, ctx: InvocationContext) -> AsyncGenerator[Event, None]:
        iteration = int(ctx.session.state.get("loop_iteration", 1))
        sop_allowed_files = list(ctx.session.state.get("sop_allowed_files") or [])
        sop_targeted_tests = list(ctx.session.state.get("sop_targeted_tests") or [])

        # Step 3a: Deterministic Pre-Review Executable Feedback Gate (MetaGPT Sec. 3.3)
        print(f"⚙️ [ADK Pre-Review Executable Feedback Gate] Running ruff + pytest check (Iteration {iteration}/{MAX_REVIEW_LOOPS})...")
        exec_passed, exec_report = await asyncio.to_thread(
            _run_executable_feedback_gate,
            sop_allowed_files,
            sop_targeted_tests,
        )

        if not exec_passed:
            print(
                f"⚠️ [ADK Pre-Review Executable Feedback Gate] FAILED on iteration {iteration} "
                f"(Short-circuiting back to CoderAgent for $0.00 Opus cost):\n{exec_report}"
            )
            _emit_pr_stage_event(
                stage="2.5/3",
                agent_name=f"Pre-Review Executable Feedback Gate (Iteration {iteration}/{MAX_REVIEW_LOOPS})",
                model="deterministic-ruff-pytest",
                status="REVISE (Short-Circuit $0.00)",
                turns=0,
                duration_s=0.5,
                cost_usd=0.0,
                iteration=iteration,
                details=exec_report,
            )
            delta = {
                "executable_feedback": exec_report,
                "review_feedback": exec_report,
                "review_approved": False,
            }
            ctx.session.state.update(delta)
            yield Event(
                author=self.name,
                invocation_id=ctx.invocation_id,
                content=types.Content(role="model", parts=[types.Part.from_text(text=exec_report)]),
                actions=EventActions(state_delta=delta, escalate=False),
            )
            return

        print(f"✅ [ADK Pre-Review Executable Feedback Gate] {exec_report}")
        ctx.session.state["executable_feedback_summary"] = exec_report

        # Step 3b: Role-Specific Context Subscription for ReviewerAgent (MetaGPT Sec. 3.2)
        sub_ctx = _subscribe_role_context("ReviewerAgent", ctx.session.state)
        arch_plan = str(sub_ctx.get("architecture_plan", ""))
        exec_summary = str(sub_ctx.get("executable_feedback_summary", ""))
        current_diff = _collect_workspace_diff()

        print(f"🔍 [ADK Agent 3: ReviewerAgent ({REVIEWER_MODEL})] Auditing diff (Iteration {iteration}/{MAX_REVIEW_LOOPS})...")

        reviewer_prompt = f"""You are Agent 3 (Adversarial Code & Security Reviewer, {REVIEWER_MODEL}) in a MetaGPT-inspired 3-agent Google ADK pipeline.
Deterministic Pre-Review Executable Feedback has ALREADY PASSED (`{exec_summary}`).
Inspect the git diff against Agent 1's SOP Architecture Plan and our security invariants:
1. COMPLETENESS & EDGE CASES: Does the diff implement every requirement, type annotation, and edge case in `<architect_sop_plan>`?
2. DATA-PLANE ISOLATION: Ensure zero references to `bigquery.tables.getData` or direct queries on user tables.
3. PROTECTED PATHS & SCOPE: Ensure modifications adhere to `{sop_allowed_files}` and never touch `.github/`, `deploy/`, `Dockerfile`, `CLAUDE.md`, or `tests/conftest.py`.
4. SECURITY: Ensure no SSRF, command injection, unescaped innerHTML, or credential leaks.

<executable_feedback_status>
{exec_summary}
</executable_feedback_status>

<architect_sop_plan>
{arch_plan}
</architect_sop_plan>

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
            "executable_feedback": "",
            "review_approved": approved,
            "review_feedback": "" if approved else review_text,
        }
        ctx.session.state.update(delta)

        _emit_pr_stage_event(
            stage="3/3",
            agent_name=f"Agent 3: ReviewerAgent (Iteration {iteration}/{MAX_REVIEW_LOOPS})",
            model=REVIEWER_MODEL,
            status="VERDICT: PASS" if approved else "VERDICT: REVISE",
            turns=turns,
            duration_s=dur_s,
            cost_usd=cost,
            iteration=iteration,
            details=f"**Pre-Review Executable Feedback:** `{exec_summary}`\n\n{review_text}",
        )

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
