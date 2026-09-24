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
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
from typing import Any, Dict, List, Literal, Optional, Tuple

from google.adk.agents import BaseAgent, InvocationContext, LoopAgent, SequentialAgent
from google.adk.apps import App
from google.adk.events import Event, EventActions
from google.adk.runners import InMemoryRunner
from google.genai import types
from pydantic import BaseModel, Field

ARCHITECT_MODEL = os.environ.get("ARCHITECT_MODEL", "claude-opus-5-5")
CODER_MODEL = os.environ.get("CODER_MODEL", "claude-sonnet-5")
REVIEWER_MODEL = os.environ.get("REVIEWER_MODEL", "claude-opus-5-5")
# TODO(cost): move Goldfish to a cheaper model. It is a ~1-turn, tool-free, text-in/JSON-out spec
# read -- the cheapest unit of work in the pipeline -- but it currently defaults to the Coder's
# Sonnet rung. A Haiku-class Anthropic rung needs NO code change: just set GOLDFISH_MODEL on the
# Cloud Run jobs. It defaults to CODER_MODEL only because Haiku availability on this Vertex
# project was never verified.
# WARNING when doing this: `_parse_intake_verdict` fails OPEN to `pass`, so an unavailable or
# misspelled model ID will NOT surface as a crash -- Goldfish will silently approve every issue
# while still posting a green `GAS Intake / 0.` check. Confirm the new rung by watching a
# deliberately vague issue actually get refused; do not trust the check turning green.
# Note a non-Anthropic rung (e.g. Gemini) is a bigger change, not an env var: this value is
# forwarded to ANTHROPIC_MODEL for the `claude` CLI in `_run_claude_cli`. Goldfish is the only
# agent that could take a direct `google.genai` path, since spec-blindness means it needs no
# tools or repo access -- which would also let `IntakeVerdict` be a real response schema.
GOLDFISH_MODEL = os.environ.get("GOLDFISH_MODEL", CODER_MODEL)
GOLDFISH_ENABLED = os.environ.get("GOLDFISH_ENABLED", "true").strip().lower() != "false"
MAX_REVIEW_LOOPS = int(os.environ.get("MAX_REVIEW_LOOPS", "3"))
MAX_SESSION_COST_USD = float(os.environ.get("MAX_SESSION_COST_USD", "8.00"))

WORKSPACE_DIR = Path("/workspace")
PROMPT_FILE = Path("/tmp/sanitized_issue_prompt.txt")
SOP_PLAN_FILE = Path("/tmp/sop_plan.md")
TELEMETRY_FILE = Path("/tmp/claude_execution_log.json")
OPUS_REPORT_FILE = Path("/tmp/opus_review_report.md")
STAGE_EVENTS_FILE = Path("/tmp/adk_stage_events.jsonl")
PRE_TOOL_HOOK_SCRIPT = Path("/tmp/adk_pre_tool_hook.py")
ISSUE_META_FILE = Path("/tmp/adk_issue_meta.json")
LENSES_PINNED_FILE = Path("/opt/pinned/lenses.pinned.yaml")

# `illya-nau/GAS` `config/lenses.yaml` fallback when `/opt/pinned/lenses.pinned.yaml` is absent.
DEFAULT_LENS_KINDS: Dict[str, List[str]] = {
    "chore": ["CORRECTNESS"],
    "bug": ["CORRECTNESS", "REGRESSION"],
    "feature": ["CORRECTNESS", "SECURITY", "REGRESSION", "OPERABILITY"],
}
LENS_SCOPES: Dict[str, str] = {
    "CORRECTNESS": (
        "Does the diff implement every requirement, mathematical formula, type annotation, and "
        "boundary/edge case (`None`, `NaN`, `bool`, `inf`, negative numbers) in `<architect_sop_plan>`?"
    ),
    "SECURITY": (
        "Ensure zero references to `bigquery.tables.getData`, strict adherence to the SOP allowed-file "
        "scope (zero protected paths or `*conftest.py` files touched), and no SSRF, command injection, "
        "unescaped innerHTML, or credential leaks."
    ),
    "REGRESSION": (
        "Verify the diff changes ZERO behavior for existing callers who asked for none (no unintended "
        "changes to existing function signatures, return types, or untouched code)."
    ),
    "OPERABILITY": (
        "Verify clean docstring contracts, deterministic exception messages, offline testability "
        '(`@pytest.mark.usefixtures("mock_bq_all")` if needed), and static bundle parity.'
    ),
}


class DefectFinding(BaseModel):
    """Structured defect finding emitted by ReviewerAgent (Paper Sec. 3, p. 9 + GAS lenses.yaml)."""

    file_path: str = Field(description="Relative path of file containing the defect.")
    line_start: int = Field(default=1, description="First line of the offending code block.")
    line_end: int = Field(default=1, description="Last line of the offending code block.")
    category: Literal["CORRECTNESS", "SECURITY", "REGRESSION", "OPERABILITY", "MAINTAINABILITY"] = Field(
        default="CORRECTNESS", description="GAS Review Lens category."
    )
    critique: str = Field(description="Specific technical description of why the code is unacceptable.")
    actionable_remediation: str = Field(description="Clear instruction detailing the required code change.")


class ArchitecturalReviewVerdict(BaseModel):
    """Strictly validated Pydantic review verdict schema (Paper Sec. 3, p. 9 + GAS 4-Lens Spec-Blind Review)."""

    decision: Literal["APPROVE", "REQUEST_CHANGES"] = Field(description="Review determination.")
    lens_verdicts: Dict[str, Literal["PASS", "REJECT"]] = Field(
        default_factory=lambda: {
            "CORRECTNESS": "PASS",
            "SECURITY": "PASS",
            "REGRESSION": "PASS",
            "OPERABILITY": "PASS",
        },
        description="Per-lens verdict across the GAS Spec-Blind Review lenses selected for this work_kind.",
    )
    blocking_findings: List[DefectFinding] = Field(
        default_factory=list, description="List of blocking defects."
    )


class IntakeFinding(BaseModel):
    """
    `illya-nau/GAS` `src/goldfish/goldfish/verdict.py` `IntakeFinding`:
    A separate concern the spec does not address. Findings NEVER change the Goldfish decision.
    """

    severity: Literal["blocker", "major", "minor", "nit"] = Field(default="minor")
    message: str = Field(description="What the spec fails to address.")


class IntakeVerdict(BaseModel):
    """
    `illya-nau/GAS` `src/goldfish/goldfish/verdict.py` `IntakeVerdict`:
    Typed spec-completeness adjudication. `decision` uses `pass`/`refuse` (never the reviewer's
    `pass`/`reject`). A turn that produces no verdict is treated as `NoIntakeVerdictProducedError`.
    """

    decision: Literal["pass", "refuse"] = Field(description="Spec completeness determination.")
    restatement: str = Field(default="", description="The fresh reader's restatement of the spec.")
    findings: List[IntakeFinding] = Field(default_factory=list)


def setup_distributed_observability(repo_name: str, issue_id: str) -> None:
    """Initialize Google ADK native OpenTelemetry hooks (Paper Sec. 5, p. 13)."""
    os.environ.setdefault("OTEL_SERVICE_NAME", "bq-finops-adk-agent")
    os.environ.setdefault(
        "OTEL_RESOURCE_ATTRIBUTES",
        f"service.name=bq-finops-adk-agent,vcs.repository={repo_name},ci.issue_id={issue_id},deployment.environment=cloud-run-jobs",
    )
    os.environ.setdefault("OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT", "true")
    try:
        from google.adk.telemetry.setup import maybe_set_otel_providers

        if os.environ.get("USE_CLOUD_TRACE", "false").lower() == "true":
            from google.adk.telemetry.google_cloud import get_gcp_exporters

            maybe_set_otel_providers([get_gcp_exporters(enable_cloud_tracing=True)])
        elif os.environ.get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"):
            maybe_set_otel_providers()
    except Exception as exc:
        print(f"[OTel Notice] Continuing without external OTel exporter: {exc}")


def _install_claude_pre_tool_hook(sop_allowed_files: List[str]) -> None:
    """
    Install a deterministic Action-Level Policy Hook (`PreToolUse`) for Claude Code CLI (Paper pp. 2, 10-11).
    Blocks `Edit` / `Write` operations targeting protected repository paths or files outside `sop_allowed_files`
    BEFORE execution, and cleans up `.claude/` immediately after `CoderAgent` exits so `git status` stays clean.
    """
    hook_code = f'''#!/usr/bin/env python3
import json, os, re, shlex, sys
PROTECTED = {list(PROTECTED_PREFIXES)!r}
ALLOWED = {list(sop_allowed_files)!r}
# Git subcommands that mutate working-tree or history state. The harness -- not the agent -- owns
# git. On issue #67 the CoderAgent answered a diff-coverage shortfall by running
# `git checkout -- src/utils.py` three times, destroying its own work and taking coverage from
# 76.5% down to 36.8%. Reverting can never satisfy a gate that measures the diff: erasing the diff
# only shrinks the evidence. Commit/push are blocked too, so the agent cannot bypass the gates.
GIT_MUTATORS = (
    "checkout", "restore", "reset", "stash", "clean", "revert",
    "rebase", "merge", "cherry-pick", "commit", "push", "apply", "am",
)
GIT_FLAGS_WITH_VALUE = ("-C", "-c", "--git-dir", "--work-tree", "--namespace", "--exec-path")
try:
    payload = json.load(sys.stdin)
except Exception:
    sys.exit(0)
tool_name = str(payload.get("tool_name", ""))
tool_input = payload.get("tool_input") or {{}}
if tool_name == "Bash" and isinstance(tool_input, dict):
    for segment in re.split(r"&&|\\|\\||;|\\|", str(tool_input.get("command") or "")):
        try:
            tokens = shlex.split(segment)
        except Exception:
            tokens = segment.split()
        for idx, tok in enumerate(tokens):
            if tok != "git" and not tok.endswith("/git"):
                continue
            j = idx + 1
            while j < len(tokens) and tokens[j].startswith("-"):
                j += 2 if tokens[j] in GIT_FLAGS_WITH_VALUE else 1
            if j < len(tokens) and tokens[j].lower() in GIT_MUTATORS:
                sys.stderr.write(
                    "POLICY HOOK BLOCKED: `git " + tokens[j] + "` mutates repository state, which "
                    "the harness owns. Never revert, stash or restructure to satisfy a gate -- the "
                    "gate measures your diff, so erasing it cannot help. Edit the files directly "
                    "and add tests. Read-only git (status/diff/log/show) remains allowed.\\\\n"
                )
                sys.exit(2)
            break
if tool_name in ("Edit", "Write", "MultiEdit") and isinstance(tool_input, dict):
    raw_path = str(tool_input.get("file_path") or tool_input.get("path") or "")
    if raw_path:
        rel = os.path.normpath(raw_path).replace("\\\\", "/")
        if rel.startswith("/workspace/"):
            rel = rel[len("/workspace/"):]
        rel = rel.removeprefix("./")
        if any(rel == p or rel.startswith(p) for p in PROTECTED) or rel.startswith(".git/") or os.path.basename(rel) == "conftest.py":
            sys.stderr.write(f"POLICY HOOK BLOCKED: Writing to protected or conftest.py path '{{rel}}' is prohibited.\\n")
            sys.exit(2)
        if ALLOWED and rel not in ALLOWED and not rel.startswith("tests/") and not rel.startswith("docs/static/"):
            sys.stderr.write(f"POLICY HOOK BLOCKED: '{{rel}}' is outside ArchitectAgent allowed_file_list {{ALLOWED}}.\\n")
            sys.exit(2)
sys.exit(0)
'''
    try:
        PRE_TOOL_HOOK_SCRIPT.write_text(hook_code, encoding="utf-8")
        PRE_TOOL_HOOK_SCRIPT.chmod(0o755)
        if WORKSPACE_DIR.exists():
            claude_dir = WORKSPACE_DIR / ".claude"
            claude_dir.mkdir(parents=True, exist_ok=True)
            settings = {
                "hooks": {
                    "PreToolUse": [
                        {
                            "matcher": "Edit|Write|MultiEdit|Bash",
                            "hooks": [{"type": "command", "command": f"/opt/venv/bin/python3 {PRE_TOOL_HOOK_SCRIPT}"}],
                        }
                    ]
                }
            }
            (claude_dir / "settings.local.json").write_text(json.dumps(settings, indent=2), encoding="utf-8")
    except Exception as exc:
        print(f"[PolicyHook Warning] Could not install PreToolUse hook: {exc}")


def _cleanup_claude_pre_tool_hook() -> None:
    """Remove ephemeral `.claude/` directory from `/workspace` so `git status` remains completely clean."""
    try:
        claude_dir = WORKSPACE_DIR / ".claude"
        if claude_dir.exists():
            shutil.rmtree(claude_dir, ignore_errors=True)
    except Exception:
        pass


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


def _summarize_tool_input(tool_name: str, tool_input: Any) -> str:
    """Create a concise, single-line summary of a Claude Code tool invocation for live logs and PR traces."""
    if not isinstance(tool_input, dict):
        return f"{tool_name}()"
    if "file_path" in tool_input:
        return f"{tool_name}({tool_input['file_path']})"
    if "path" in tool_input and "pattern" in tool_input:
        return f"{tool_name}(pattern={tool_input['pattern']!r}, path={tool_input['path']!r})"
    if "pattern" in tool_input:
        return f"{tool_name}(pattern={tool_input['pattern']!r})"
    if "command" in tool_input:
        cmd_str = str(tool_input["command"]).strip().replace("\n", " ")
        return f"{tool_name}({cmd_str[:80]})"
    keys = list(tool_input.keys())[:2]
    return f"{tool_name}({', '.join(f'{k}={tool_input[k]!r}' for k in keys)})"


def _run_claude_cli(
    prompt: str,
    model: str,
    allowed_tools: List[str],
    disallowed_tools: List[str],
    max_turns: int = 25,
    accept_edits: bool = False,
    stage_id: str = "",
    agent_label: str = "",
) -> Tuple[str, int, float, float]:
    """
    Invoke the native `claude -p` CLI binary with `--output-format stream-json --verbose` (`shell=False`).
    Streams turn-by-turn tool invocations to Cloud Logging in real time, emits live PR progress heartbeats,
    and appends a chronological Tool Execution Trace + Token Cache summary to `result_text`.
    Returns (enriched_result_text, num_turns, duration_sec, cost_usd).
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
        "stream-json",
        "--verbose",
    ]
    if accept_edits:
        cmd.extend(["--permission-mode", "acceptEdits"])
    if allowed_tools:
        cmd.append("--allowedTools")
        cmd.extend(allowed_tools)
    if disallowed_tools:
        cmd.append("--disallowedTools")
        cmd.extend(disallowed_tools)

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=str(WORKSPACE_DIR) if WORKSPACE_DIR.exists() else None,
        env=env,
        text=True,
        bufsize=1,
    )

    tool_trace: List[str] = []
    result_payload: Dict[str, Any] = {}
    assistant_texts: List[str] = []
    turn_idx = 0

    assert proc.stdout is not None
    for raw_line in proc.stdout:
        line = raw_line.strip()
        if not line:
            continue
        try:
            ev = json.loads(line)
        except Exception:
            continue

        ev_type = ev.get("type")
        if ev_type == "assistant":
            msg = ev.get("message", {})
            content_list = msg.get("content", []) if isinstance(msg, dict) else []
            for item in content_list:
                if not isinstance(item, dict):
                    continue
                if item.get("type") == "tool_use":
                    turn_idx += 1
                    t_name = str(item.get("name", "Tool"))
                    t_summary = _summarize_tool_input(t_name, item.get("input"))
                    tool_trace.append(f"`{turn_idx}. {t_summary}`")
                    print(f"   ↳ [{agent_label or model} | Tool #{turn_idx}] 🔧 {t_summary}", flush=True)
                    if stage_id and (turn_idx == 1 or turn_idx % 3 == 0):
                        _emit_pr_stage_event(
                            stage=stage_id,
                            agent_name=agent_label or model,
                            model=model,
                            status=f"🔄 Running (Tool #{turn_idx}: {t_name})",
                            turns=turn_idx,
                            duration_s=0.0,
                            cost_usd=0.0,
                            iteration=1,
                            details="",
                        )
                elif item.get("type") == "text" and item.get("text"):
                    assistant_texts.append(str(item["text"]))
        elif ev_type == "result":
            result_payload = ev

    proc.wait()
    if result_payload:
        result_text = str(result_payload.get("result") or ("\n".join(assistant_texts[-2:]) if assistant_texts else ""))
        turns = int(result_payload.get("num_turns") or max(turn_idx, 1))
        dur_s = round(float(result_payload.get("duration_ms") or 0.0) / 1000.0, 1)
        cost = float(result_payload.get("total_cost_usd") or 0.0)
        usage = result_payload.get("usage", {}) if isinstance(result_payload.get("usage"), dict) else {}
        in_tok = int(usage.get("input_tokens") or 0)
        cache_read = int(usage.get("cache_read_input_tokens") or 0)
        out_tok = int(usage.get("output_tokens") or 0)

        trace_block = ""
        if tool_trace or in_tok or cache_read or out_tok:
            trace_lines = [
                "\n\n---",
                f"#### 🔬 Agent Execution Telemetry (`{model}`)",
                f"- **Turns:** `{turns}` | **Wall Time:** `{dur_s}s` | **Est. Cost:** `${cost:.4f}`",
                f"- **Token Usage:** `Input: {in_tok:,}` | `Prompt Cache Read: {cache_read:,}` | `Output: {out_tok:,}`",
            ]
            if tool_trace:
                trace_lines.append(f"- **Chronological Tool Trace ({len(tool_trace)} calls):** " + " $\\rightarrow$ ".join(tool_trace[:25]))
            trace_block = "\n".join(trace_lines)

        return result_text + trace_block, turns, dur_s, cost

    if proc.returncode != 0:
        err_out = proc.stderr.read().strip() if proc.stderr else ""
        raise RuntimeError(f"claude CLI ({model}) exited {proc.returncode}: {err_out[:500]}")
    return "\n".join(assistant_texts), max(turn_idx, 1), 0.0, 0.0


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
    "CoderAgent_Revision": [
        "architecture_plan",
        "sop_allowed_files",
        "sop_targeted_tests",
        "executable_feedback",
        "review_feedback",
        "blocking_findings_json",
        "convergence_trajectory",
    ],
    "ReviewerAgent": ["architecture_plan", "sop_allowed_files", "executable_feedback_summary"],
}


def _subscribe_role_context(role_key: str, state: Dict[str, Any]) -> Dict[str, Any]:
    """
    MetaGPT Publish-Subscribe filter (Sec. 3.2 / Appendix E.2):
    Extracts only the role-subscribed keys from the shared session state pool to prevent context bloat.
    """
    allowed_keys = ROLE_SUBSCRIPTIONS.get(role_key, [])
    return {k: state.get(k) for k in allowed_keys if k in state and state.get(k) not in (None, "", [])}


def _compute_diff_hash(diff_text: str) -> str:
    """Compute a 12-char SHA-256 fingerprint of the workspace unified diff to detect ΔDiff == 0 stagnation."""
    if not diff_text or not diff_text.strip():
        return "empty_diff_0"
    return hashlib.sha256(diff_text.strip().encode("utf-8")).hexdigest()[:12]


def _extract_pytest_distance(feedback_report: str) -> Tuple[int, List[str]]:
    """
    Compute the mathematical test failure distance metric D_test = |T_failed| + |T_errored| (Paper p. 9)
    and extract the sorted list of failing pytest node IDs (`FAILED tests/...::test_...`).
    """
    if not feedback_report or "EXECUTABLE FEEDBACK PASSED" in feedback_report:
        return 0, []

    failed_count = 0
    errored_count = 0
    m_fail = re.search(r"(\d+)\s+failed", feedback_report)
    if m_fail:
        failed_count = int(m_fail.group(1))
    m_err = re.search(r"(\d+)\s+errors?", feedback_report)
    if m_err:
        errored_count = int(m_err.group(1))

    failed_nodes = sorted(set(re.findall(r"(?:FAILED|ERROR)\s+([\w./:-]+)", feedback_report)))
    d_test = failed_count + errored_count
    if d_test == 0 and "EXECUTABLE FEEDBACK FAILURE" in feedback_report:
        d_test = max(len(failed_nodes), 1)
    return d_test, failed_nodes


def _load_issue_meta() -> Dict[str, Any]:
    """Read `/tmp/adk_issue_meta.json` (written by `verify_issue_actor.py`) with safe defaults."""
    try:
        meta = json.loads(ISSUE_META_FILE.read_text(encoding="utf-8"))
        if isinstance(meta, dict):
            return meta
    except Exception:
        pass
    return {}


def _select_lenses_for_work_kind(work_kind: str) -> List[str]:
    """
    `illya-nau/GAS` `config/lenses.yaml` `kinds:` — work-kind adaptive lens selection.
    `chore: [CORRECTNESS]`, `bug: [CORRECTNESS, REGRESSION]`, `feature: [all 4]`.
    Parsed from the image-baked `/opt/pinned/lenses.pinned.yaml` with a stdlib-only reader
    (no PyYAML dependency); an unknown work_kind resolves to the strictest set (all 4 lenses).
    """
    kinds: Dict[str, List[str]] = dict(DEFAULT_LENS_KINDS)
    try:
        text = LENSES_PINNED_FILE.read_text(encoding="utf-8")
        block = re.search(r"^kinds:\s*$(.*?)(?=^\S|\Z)", text, flags=re.MULTILINE | re.DOTALL)
        if block:
            parsed: Dict[str, List[str]] = {}
            for line in block.group(1).splitlines():
                entry = re.match(r"\s+([A-Za-z0-9_-]+):\s*\[([^\]]*)\]\s*$", line)
                if entry:
                    names = [n.strip().upper() for n in entry.group(2).split(",") if n.strip()]
                    if names:
                        parsed[entry.group(1).strip().lower()] = names
            if parsed:
                kinds = parsed
    except Exception:
        pass

    selected = kinds.get(str(work_kind or "").strip().lower())
    if not selected:
        selected = DEFAULT_LENS_KINDS["feature"]
    # Only ever return lenses this image carries a pinned scope for, order-stable.
    valid = [lens for lens in selected if lens in LENS_SCOPES]
    return valid or DEFAULT_LENS_KINDS["feature"]


def _build_lens_prompt_fragments(work_kind: str) -> Tuple[List[str], str, str, str]:
    """
    Build the ReviewerAgent prompt fragments for the lens subset `work_kind` selects.

    Extracted from `ReviewerAgent` so the adaptive-lens behaviour is unit-testable: inside the
    agent it is only reachable after a full pipeline run survives the Architect, the Coder, and
    the executable-feedback gate, which makes it expensive and unreliable to verify live.

    Returns `(active_lenses, lens_block, lens_json_rows, lens_categories)`.
    """
    active_lenses = _select_lenses_for_work_kind(work_kind)
    lens_block = "\n".join(
        f"{i}. **LENS {i} — `{lens}`:** {LENS_SCOPES[lens]}"
        for i, lens in enumerate(active_lenses, start=1)
    )
    lens_json_rows = ",\n".join(f'    "{lens}": "PASS"' for lens in active_lenses)
    lens_categories = "|".join(f'"{lens}"' for lens in active_lenses)
    return active_lenses, lens_block, lens_json_rows, lens_categories


def _parse_intake_verdict(intake_text: str) -> IntakeVerdict:
    """
    `illya-nau/GAS` `src/goldfish/goldfish/goldfish.py`: parse the Goldfish turn into a typed
    `IntakeVerdict`. The tool call is FORCED — a turn that ends without emitting a verdict is a
    `NoIntakeVerdictProducedError` equivalent, and free text alone NEVER counts as a verdict.
    Fail-open to `pass` so a malformed Goldfish turn can never block a well-specified issue.
    """
    for match in re.finditer(r"```json\s*(\{.*?\})\s*```", intake_text, flags=re.DOTALL):
        try:
            raw_obj = json.loads(match.group(1))
            if isinstance(raw_obj, dict) and "decision" in raw_obj:
                raw_obj["decision"] = str(raw_obj.get("decision", "pass")).strip().lower()
                if raw_obj["decision"] not in ("pass", "refuse"):
                    continue
                return IntakeVerdict.model_validate(raw_obj)
        except Exception:
            continue

    if re.search(r"INTAKE_VERDICT:\s*REFUSE", intake_text, flags=re.IGNORECASE):
        return IntakeVerdict(
            decision="refuse",
            restatement=intake_text[-2000:],
            findings=[IntakeFinding(severity="blocker", message="Goldfish refused (unstructured output).")],
        )
    return IntakeVerdict(decision="pass", restatement=intake_text[-2000:])


def _parse_review_verdict(review_text: str) -> ArchitecturalReviewVerdict:
    """
    Parse ReviewerAgent's output into a validated Pydantic `ArchitecturalReviewVerdict` (Paper p. 9).
    Extracts fenced JSON (`REVIEW_VERDICT_JSON`) if present, with a graceful fallback to `VERDICT: PASS/REVISE`.
    """
    for match in re.finditer(r"```json\s*(\{.*?\})\s*```", review_text, flags=re.DOTALL):
        try:
            raw_obj = json.loads(match.group(1))
            if isinstance(raw_obj, dict) and ("decision" in raw_obj or "blocking_findings" in raw_obj):
                return ArchitecturalReviewVerdict.model_validate(raw_obj)
        except Exception:
            continue

    approved = "VERDICT: PASS" in review_text and "VERDICT: REVISE" not in review_text
    if approved:
        return ArchitecturalReviewVerdict(decision="APPROVE", blocking_findings=[])

    return ArchitecturalReviewVerdict(
        decision="REQUEST_CHANGES",
        blocking_findings=[
            DefectFinding(
                file_path="workspace",
                line_start=1,
                line_end=1,
                category="CORRECTNESS",
                critique="Reviewer requested changes (see review report).",
                actionable_remediation=review_text[-2500:],
            )
        ],
    )


def _format_blocking_findings_for_coder(verdict: ArchitecturalReviewVerdict) -> str:
    """Serialize structured Pydantic `DefectFinding` list for targeted CoderAgent remediation (Paper p. 9)."""
    if not verdict.blocking_findings:
        return ""
    rows = [
        "### Structured `blocking_findings` (`ArchitecturalReviewVerdict`):",
        "| File | Lines | Category | Critique | Actionable Remediation |",
        "| :--- | :--- | :--- | :--- | :--- |",
    ]
    for f in verdict.blocking_findings:
        rows.append(
            f"| `{f.file_path}` | `L{f.line_start}-L{f.line_end}` | **{f.category}** | {f.critique} | {f.actionable_remediation} |"
        )
    return "\n".join(rows)


def _strip_verdict_json_for_display(review_text: str) -> str:
    """
    Remove raw machine-oriented `REVIEW_VERDICT_JSON` fenced JSON blocks from the human-facing
    GitHub PR comment after `_parse_review_verdict()` has validated them with Pydantic.
    """
    cleaned = re.sub(
        r"(?:#+\s*REVIEW_VERDICT_JSON[^\n]*\n+)?```json\s*\{\s*\"decision\"\s*:.*?\}\s*```\n?",
        "",
        review_text,
        flags=re.DOTALL | re.IGNORECASE,
    )
    return re.sub(r"\n{3,}", "\n\n", cleaned).strip()


def _extract_added_lines_by_file(diff_text: str) -> Dict[str, set]:
    """
    Parse unified diff (`git diff -U0`) to return a mapping of `{relative_path: set_of_added_line_numbers}`.
    Used for line-level `LINT_SCOPE=diff` filtering so pre-existing legacy lines never trigger false positives.
    """
    added_lines: Dict[str, set] = {}
    current_file: str | None = None
    curr_line_no = 0
    for raw_line in diff_text.splitlines():
        if raw_line.startswith("+++ b/"):
            current_file = raw_line[6:].strip()
            added_lines.setdefault(current_file, set())
        elif raw_line.startswith("@@ ") and current_file:
            m = re.search(r"\+(\d+)(?:,(\d+))?", raw_line)
            if m:
                curr_line_no = int(m.group(1))
        elif current_file and curr_line_no > 0:
            if raw_line.startswith("+") and not raw_line.startswith("+++"):
                added_lines[current_file].add(curr_line_no)
                curr_line_no += 1
            elif not raw_line.startswith("-") and not raw_line.startswith("\\"):
                curr_line_no += 1
    return added_lines


def _verify_no_self_mocking_in_diff(diff_text: str, test_files: List[str], cwd: str) -> Tuple[bool, str]:
    """
    `illya-nau/GAS` `test-integrity` AST `mock-gate`:
    Extracts newly added/modified top-level function definitions (`def <fn>(...)`) in `src/*.py` from `diff_text`,
    and verifies via Python AST that modified unit tests in `tests/*.py` do NOT `patch(...)` or
    `monkeypatch.setattr(...)` the very symbol under test instead of exercising its real implementation.
    """
    import ast

    added_src_funcs: set[str] = set()
    current_file: str | None = None
    for line in diff_text.splitlines():
        if line.startswith("+++ b/"):
            current_file = line[6:].strip()
        elif current_file and current_file.startswith("src/") and current_file.endswith(".py"):
            if line.startswith("+") and not line.startswith("+++"):
                m = re.match(r"^\+\s*(?:async\s+)?def\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\(", line)
                if m:
                    fn_name = m.group(1)
                    if not fn_name.startswith("_"):
                        added_src_funcs.add(fn_name)

    if not added_src_funcs:
        return True, ""

    for rel_test in test_files:
        full_test = Path(cwd) / rel_test
        if not full_test.is_file() or not rel_test.endswith(".py"):
            continue
        try:
            tree = ast.parse(full_test.read_text(encoding="utf-8", errors="replace"), filename=rel_test)
        except Exception:
            continue

        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func_repr = ""
            if isinstance(node.func, ast.Name):
                func_repr = node.func.id
            elif isinstance(node.func, ast.Attribute):
                func_repr = f"{getattr(node.func.value, 'id', '')}.{node.func.attr}"

            if func_repr in ("patch", "mock.patch", "unittest.mock.patch", "patch.object", "monkeypatch.setattr"):
                for arg in node.args:
                    if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                        target_str = arg.value
                        for src_fn in added_src_funcs:
                            if target_str == src_fn or target_str.endswith(f".{src_fn}"):
                                return (
                                    False,
                                    f"EXECUTABLE FEEDBACK FAILURE [GAS Mock-Gate Violation]: Unit test `{rel_test}:{getattr(node, 'lineno', 1)}` "
                                    f"mocks the newly implemented function under test (`{target_str}`) via `{func_repr}`. "
                                    f"Tests must invoke the real `{src_fn}()` implementation and only mock external collaborators.",
                                )
    return True, ""


def _extract_sop_metadata(plan_text: str) -> Dict[str, List[str]]:
    """
    MetaGPT Sec. 3.2 & Sec. 4.4 (Table 6): Parse structured SOP JSON block (`ALLOWED_FILE_LIST`
    and `TARGETED_TEST_FILES`) emitted by ArchitectAgent during Upfront Prompt Expansion.
    Supports multi-block scans, trailing-comma regex fallbacks, and markdown list extraction.
    """
    allowed_files: List[str] = []
    targeted_tests: List[str] = []

    # 1. Search all fenced code blocks (json or unmarked) for a dict with "allowed_file_list"
    for match in re.finditer(r"```(?:json)?\s*(\{.*?\})\s*```", plan_text, flags=re.DOTALL):
        try:
            parsed = json.loads(match.group(1))
            if isinstance(parsed, dict) and "allowed_file_list" in parsed:
                if isinstance(parsed["allowed_file_list"], list):
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
                if allowed_files:
                    break
        except Exception:
            continue

    # 2. Fallback regex for "allowed_file_list": [...]
    if not allowed_files:
        m = re.search(r'["\']?allowed_file_list["\']?\s*:\s*\[([^\]]*)\]', plan_text, flags=re.IGNORECASE | re.DOTALL)
        if m:
            allowed_files = [
                os.path.normpath(f).replace("\\", "/").removeprefix("./")
                for f in re.findall(r'["\']([^"\']+)["\']', m.group(1))
                if f.strip()
            ]

    # 3. Fallback regex for "targeted_test_files": [...]
    if not targeted_tests:
        m = re.search(r'["\']?targeted_test_files["\']?\s*:\s*\[([^\]]*)\]', plan_text, flags=re.IGNORECASE | re.DOTALL)
        if m:
            targeted_tests = [
                os.path.normpath(f).replace("\\", "/").removeprefix("./")
                for f in re.findall(r'["\']([^"\']+)["\']', m.group(1))
                if f.strip()
            ]

    # 4. Fallback: bullet points under "Allowed Files" or "allowed_file_list"
    if not allowed_files:
        m = re.search(
            r'(?:allowed_file_list|allowed files)[^\n]*\n((?:\s*[-*]\s*[`"\'\']?[a-zA-Z0-9_\-\./]+[`"\'\']?\s*\n)+)',
            plan_text,
            flags=re.IGNORECASE,
        )
        if m:
            lines = m.group(1).splitlines()
            for line in lines:
                f = re.sub(r'^\s*[-*]\s*[`"\'\']?|[`"\'\']?\s*$', '', line).strip()
                if f and ('.' in f or '/' in f):
                    allowed_files.append(os.path.normpath(f).replace("\\", "/").removeprefix("./"))

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
                if norm and norm not in (".", ".venv") and not norm.startswith(".claude"):
                    files.add(norm)
        except Exception:
            pass
    return sorted(files)


DIFF_COVERAGE_FLOOR_PCT = float(os.environ.get("DIFF_COVERAGE_FLOOR_PCT", "80.0"))
LAST_DIFF_COVERAGE_PCT: float = 100.0


def _compute_diff_coverage(
    cov_json_path: str | Path,
    added_lines_map: Dict[str, set],
    floor_pct: float = 80.0,
) -> Tuple[bool, float, List[str], str]:
    """
    `illya-nau/GAS` `cicd/integrity_checks/integrity_checks/diff_coverage.py`:
    Computes the percentage of newly added/modified executable lines in `src/*.py` (from `added_lines_map`)
    that were executed during the unit test suite (`executed_lines` vs `missing_lines` in `coverage.py` JSON).
    Enforces `floor_pct` (default 80.0%).
    """
    cov_file = Path(cov_json_path)
    if not cov_file.is_file():
        return True, 100.0, [], ""

    try:
        report = json.loads(cov_file.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        return True, 100.0, [], ""

    files_dict = report.get("files")
    if not isinstance(files_dict, dict):
        return True, 100.0, [], ""

    # Normalize keys in files_dict to git-relative paths (e.g. 'src/utils.py')
    norm_entries: Dict[str, dict] = {}
    for k, v in files_dict.items():
        clean_k = os.path.normpath(str(k)).replace("\\", "/").removeprefix("./")
        if "/workspace/" in clean_k:
            clean_k = clean_k.split("/workspace/", 1)[1]
        norm_entries[clean_k] = v

    total_executable_added = 0
    total_covered_added = 0
    gaps: List[str] = []

    for rel_path, added_set in sorted(added_lines_map.items()):
        norm_rel = os.path.normpath(rel_path).replace("\\", "/").removeprefix("./")
        if not norm_rel.startswith("src/") or not norm_rel.endswith(".py") or not added_set:
            continue

        entry = norm_entries.get(norm_rel)
        if not entry:
            continue

        executed = set(int(x) for x in (entry.get("executed_lines") or []))
        missing = set(int(x) for x in (entry.get("missing_lines") or []))
        measured = executed | missing

        added_exec = added_set & measured
        if not added_exec:
            continue

        covered_in_file = added_exec & executed
        missing_in_file = sorted(added_exec & missing)
        total_executable_added += len(added_exec)
        total_covered_added += len(covered_in_file)
        if missing_in_file:
            gaps.append(f"{norm_rel}: uncovered added lines {missing_in_file}")

    if total_executable_added == 0:
        return True, 100.0, [], "100.0% (0 new executable branch lines)"

    pct = round((total_covered_added / total_executable_added) * 100.0, 1)
    passed = pct >= floor_pct
    if not passed:
        needed = max(0, math.ceil(floor_pct / 100.0 * total_executable_added) - total_covered_added)
        err_msg = (
            f"EXECUTABLE FEEDBACK FAILURE [GAS Diff-Coverage Gate]: Diff coverage on newly added `src/` lines is "
            f"`{pct:.1f}%` ({total_covered_added}/{total_executable_added} lines), which is below the `{floor_pct:.1f}%` floor.\n"
            f"REMEDY: add tests that execute at least {needed} more of the added line(s) listed below. "
            f"Write ONLY new test cases in the test file.\n"
            f"DO NOT revert, restructure, reformat, or delete source lines to shrink the diff. This gate measures "
            f"your diff, so making the diff smaller does not raise coverage -- it lowers it and burns an iteration. "
            f"Leave the implementation exactly as it is unless a test reveals a genuine bug.\n"
            f"Uncovered added lines:\n- " + "\n- ".join(gaps)
        )
        return False, pct, gaps, err_msg

    return True, pct, gaps, f"{pct:.1f}% ({total_covered_added}/{total_executable_added} added lines >= {floor_pct:.0f}%)"


_CONCISE_RULE_CODE = re.compile(r"^.+?:\d+:\d+: ([A-Za-z][A-Za-z0-9-]*)", re.MULTILINE)


def _finding_counts(concise_stdout: str) -> Dict[str, int]:
    """
    `illya-nau/GAS` `src/worker/worker/autofix.py`:
    Extracts rule code counts from `ruff --output-format concise` (`path:line:col: CODE msg`)
    so Cloud Logging never prints source lines containing hardcoded secret literals (`S105/S106/S107`).
    """
    counts: Dict[str, int] = {}
    for code in _CONCISE_RULE_CODE.findall(concise_stdout):
        counts[code] = counts.get(code, 0) + 1
    return counts


def _run_harness_ruff_autofix(
    py_files: List[str],
    cwd: str,
    ruff_bin: str,
    pinned_ruff: Optional[str],
) -> Dict[str, int]:
    """
    `illya-nau/GAS` `src/worker/worker/autofix.py`:
    Deterministic pre-commit lint auto-fix (worker harness, not the agent).
    Applies `ruff check --fix` + `ruff format` strictly to the `.py` files the agent changed,
    under the same `/opt/pinned/ruff.pinned.toml` config enforced by the gate, sparing the loop
    a round-trip on mechanical classes (`I001`, `F401`, `UP`).
    """
    if not py_files or not Path(ruff_bin).exists():
        return {}
    cfg_args = ["--config", pinned_ruff] if pinned_ruff else []
    try:
        subprocess.run(
            [ruff_bin, "check", "--fix", "--output-format=concise", *cfg_args, *py_files],
            cwd=cwd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        subprocess.run(
            [ruff_bin, "format", *cfg_args, *py_files],
            cwd=cwd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        check_proc = subprocess.run(
            [ruff_bin, "check", "--output-format=concise", *cfg_args, *py_files],
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        return _finding_counts(check_proc.stdout.decode("utf-8", errors="replace"))
    except Exception:
        return {}


def classify_failure_cause(termination_reason: str, exit_code: int = 1) -> Tuple[str, str, str]:
    """
    `illya-nau/GAS` `src/orchestrator/orchestrator/failure_classification.py` &
    `src/harness/harness/cause_glossary.py`:
    Closed 3-valued classification of failure cause: `spec_gap` | `agent_error` | `infrastructure`.
    Returns `(failure_class, github_label, cause_glossary_sentence)`.
    """
    reason_upper = (termination_reason or "").upper()
    if any(k in reason_upper for k in ("SPEC_GAP", "UNPLANNABLE", "ARCHITECT_ABORT", "SOP_INVALID")):
        return (
            "spec_gap",
            "failure:spec-gap",
            "Spec Gap: The issue specification could not be decomposed into valid repository files or lacked actionable acceptance criteria.",
        )
    if any(
        k in reason_upper
        for k in (
            "DIFF_STAGNATION",
            "TEST_NON_CONVERGENCE",
            "COST_CEILING",
            "MAX_ITERATIONS",
            "EXHAUSTED",
            "NO_CHANGES",
            "REVISE",
            "EXECUTABLE FEEDBACK FAILURE",
        )
    ):
        return (
            "agent_error",
            "failure:agent-error",
            "Agent Error: The autonomous coding/review loop exhausted its refinement budget or failed deterministic verification gates.",
        )
    return (
        "infrastructure",
        "failure:infrastructure",
        f"Infrastructure Failure (exit {exit_code}): An infrastructure, runtime, or upstream API error interrupted the worker container before verdict completion.",
    )


def _verify_modified_module_imports(py_files: List[str], cwd: str, python_bin: str) -> Tuple[bool, str]:
    """
    `illya-nau/GAS` `cicd/integrity_checks/integrity_checks/import_check.py`:
    Validates every modified `src/**/*.py` module name against `^[A-Za-z0-9._]+$` and imports it
    via `importlib.import_module(name)` (never shell interpolation) to catch circular imports or top-level errors.
    """
    name_re = re.compile(r"^[A-Za-z0-9._]+$")
    mod_names: List[str] = []
    for rel_py in py_files:
        norm_py = os.path.normpath(rel_py).replace("\\", "/").removeprefix("./")
        if norm_py.startswith("src/") and norm_py.endswith(".py"):
            dotted = norm_py[:-3].replace("/", ".")
            if dotted.endswith(".__init__"):
                dotted = dotted[:-9]
            if dotted and name_re.match(dotted):
                mod_names.append(dotted)

    if not mod_names:
        return True, ""

    proc = subprocess.run(
        [
            python_bin,
            "-c",
            "import importlib, sys; sys.path.insert(0, '.'); [importlib.import_module(m) for m in sys.argv[1:]]",
            *mod_names,
        ],
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    if proc.returncode != 0:
        out = proc.stdout.decode("utf-8", errors="replace").strip()
        return (
            False,
            f"EXECUTABLE FEEDBACK FAILURE [GAS Import Smoke-Check ({mod_names})]:\n```\n{out[:2500]}\n```",
        )
    return True, f"Imported({', '.join(mod_names)})"


def _run_executable_feedback_gate(
    sop_allowed_files: List[str],
    sop_targeted_tests: List[str],
) -> Tuple[bool, str]:
    """
    MetaGPT Sec. 3.3 (Fig. 2 Right, Table 1): Deterministic Pre-Review Executable Feedback Gate.
    Executes inside the LoopAgent BEFORE invoking ReviewerAgent (claude-opus-5-5):
      1. Protected Path, `*conftest.py` Anti-Tampering (`GAS base_conftests`), & SOP File-Scope Verification
      2. Pre-Compilation AST/Syntax Check (`/opt/pinned/ruff.pinned.toml` + diff-hunk F/B check)
      3. `GAS import_check.py` (`importlib.import_module` smoke-check on modified `src/` modules)
      4. `GAS mock-gate` + `pytest` (`/opt/pinned/pytest.pinned.ini`) + `GAS diff-coverage` (`>= 80%` floor)
    Returns (passed: bool, feedback_report: str).
    If `passed` is False, ReviewerAgent short-circuits back to CoderAgent in <2s for $0.00 LLM cost.
    """
    global LAST_DIFF_COVERAGE_PCT
    cwd = str(WORKSPACE_DIR) if WORKSPACE_DIR.exists() else "."
    changed_files = _collect_changed_files(cwd)
    if not changed_files:
        return (
            False,
            "EXECUTABLE FEEDBACK FAILURE [No Changes]: No modified or untracked files found in /workspace.",
        )

    # 1. Protected paths, recursive *conftest.py guard (GAS base_conftests), & SOP scope check
    for path in changed_files:
        norm_p = os.path.normpath(path).replace("\\", "/").removeprefix("./")
        if os.path.basename(norm_p) == "conftest.py":
            return (
                False,
                f"EXECUTABLE FEEDBACK FAILURE [GAS base_conftests Violation]: Adding or modifying '{path}' is prohibited across all directories.",
            )
        if any(norm_p == p or norm_p.startswith(p) for p in PROTECTED_PREFIXES):
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
    python_bin = str(Path(py_bin) / "python3") if (Path(py_bin) / "python3").exists() else sys.executable
    pinned_ruff = "/opt/pinned/ruff.pinned.toml" if Path("/opt/pinned/ruff.pinned.toml").exists() else None
    pinned_pytest = "/opt/pinned/pytest.pinned.ini" if Path("/opt/pinned/pytest.pinned.ini").exists() else None
    pinned_cov = "/opt/pinned/coverage.pinned.rc" if Path("/opt/pinned/coverage.pinned.rc").exists() else None

    added_lines_map: Dict[str, set] = {}
    try:
        u0_diff = subprocess.check_output(
            ["git", "diff", "-U0", "base-anchor"],
            cwd=cwd,
            stderr=subprocess.DEVNULL,
        ).decode("utf-8", errors="replace")
        added_lines_map = _extract_added_lines_by_file(u0_diff)
    except Exception:
        pass

    # 2. GAS Deterministic Pre-Commit Lint Auto-Fix (`src/worker/worker/autofix.py`)
    #    + Pre-Compilation Syntax / Undefined Names Check (`ruff` with `/opt/pinned/ruff.pinned.toml`)
    py_files = [f for f in changed_files if f.endswith(".py") and (Path(cwd) / f).exists()]
    if py_files and Path(ruff_bin).exists():
        autofix_counts = _run_harness_ruff_autofix(py_files, cwd, ruff_bin, pinned_ruff)
        if autofix_counts:
            print(f"🧹 [GAS autofix.py] Remaining concise rule counts after harness auto-fix: {dict(autofix_counts)}")

        ruff_cmd = [ruff_bin, "check", "--output-format=concise"]
        if pinned_ruff:
            ruff_cmd.extend(["--config", pinned_ruff])
        else:
            ruff_cmd.extend(["--select", "E9,F63,F7,F82"])
        ruff_cmd.extend(py_files)
        ruff_proc = subprocess.run(
            ruff_cmd,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )
        if ruff_proc.returncode != 0:
            out = ruff_proc.stdout.decode("utf-8", errors="replace").strip()
            return (
                False,
                f"EXECUTABLE FEEDBACK FAILURE [Pre-Compilation Ruff Check (LINT_SCOPE=diff)]:\n```\n{out[:3000]}\n```",
            )

        # Line-level diff-hunk F/B check (strictly on newly added/modified line numbers in git diff -U0)
        try:
            fb_proc = subprocess.run(
                [ruff_bin, "check", "--output-format=json", "--select", "E9,F,B", "--ignore", "B008", *py_files],
                cwd=cwd,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                check=False,
            )
            if fb_proc.returncode != 0 and fb_proc.stdout:
                diagnostics = json.loads(fb_proc.stdout.decode("utf-8", errors="replace"))
                diff_violations: List[str] = []
                for diag in diagnostics:
                    rel_diag_file = os.path.relpath(str(diag.get("filename", "")), cwd).replace("\\", "/")
                    line_row = int((diag.get("location") or {}).get("row") or 0)
                    file_added_set = added_lines_map.get(rel_diag_file)
                    if (rel_diag_file not in added_lines_map and rel_diag_file in py_files) or (
                        file_added_set and line_row in file_added_set
                    ):
                        code = diag.get("code", "RUFF")
                        msg = diag.get("message", "")
                        diff_violations.append(f"{rel_diag_file}:{line_row}: [{code}] {msg}")
                if diff_violations:
                    return (
                        False,
                        "EXECUTABLE FEEDBACK FAILURE [Diff-Hunk Ruff Check (LINT_SCOPE=diff on newly added lines)]:\n```\n"
                        + "\n".join(diff_violations[:25])
                        + "\n```",
                    )
        except Exception:
            pass

    # 2b. GAS `import_check.py` (importlib.import_module smoke-check on modified src/**/*.py)
    if py_files:
        imp_ok, imp_msg = _verify_modified_module_imports(py_files, cwd, python_bin)
        if not imp_ok:
            return False, imp_msg

    # 3. Targeted & Modified Pytest Execution (with /opt/pinned/pytest.pinned.ini & /opt/pinned/coverage.pinned.rc)
    test_targets: List[str] = []
    for t in sop_targeted_tests + [f for f in changed_files if f.startswith("tests/") and f.endswith(".py")]:
        if t not in test_targets and (Path(cwd) / t).exists():
            test_targets.append(t)
    if not test_targets:
        test_targets = ["tests/test_utils.py"] if (Path(cwd) / "tests/test_utils.py").exists() else ["tests"]

    # 3a. GAS AST `mock-gate` (Verify tests do not mock the newly implemented function under test)
    diff_for_mock_gate = _collect_workspace_diff()
    mock_gate_ok, mock_gate_err = _verify_no_self_mocking_in_diff(diff_for_mock_gate, test_targets, cwd)
    if not mock_gate_ok:
        return False, mock_gate_err

    cov_json_file = Path("/tmp/adk_coverage.json")
    cov_json_file.unlink(missing_ok=True)

    has_pytest_cov = Path("/opt/venv/bin/pytest").exists()
    pytest_cmd = [pytest_bin if Path(pytest_bin).exists() else "pytest"]
    if pinned_pytest:
        pytest_cmd.extend(["--rootdir=.", "--override-ini=addopts=", "-c", pinned_pytest])
    if has_pytest_cov:
        pytest_cmd.extend(["--cov=src", f"--cov-report=json:{cov_json_file}"])
        if pinned_cov:
            pytest_cmd.append(f"--cov-config={pinned_cov}")
    pytest_cmd.extend(["-q", "--tb=short", *test_targets])
    pytest_env = {**os.environ, "COVERAGE_FILE": "/tmp/.adk_coverage"}
    pytest_proc = subprocess.run(
        pytest_cmd,
        cwd=cwd,
        env=pytest_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    for cov_artifact in Path(cwd).glob(".coverage*"):
        cov_artifact.unlink(missing_ok=True)
    pytest_out = pytest_proc.stdout.decode("utf-8", errors="replace").strip()
    if pytest_proc.returncode != 0:
        return (
            False,
            f"EXECUTABLE FEEDBACK FAILURE [Pytest Runtime Traceback on {test_targets}]:\n```\n{pytest_out[-4000:]}\n```",
        )

    # 3b. GAS `diff-coverage` Gate (>= 80.0% floor on newly added executable lines in src/*.py)
    cov_ok, cov_pct, _cov_gaps, cov_summary = _compute_diff_coverage(
        cov_json_file, added_lines_map, floor_pct=DIFF_COVERAGE_FLOOR_PCT
    )
    LAST_DIFF_COVERAGE_PCT = cov_pct
    if not cov_ok:
        return False, cov_summary

    summary = (
        f"EXECUTABLE FEEDBACK PASSED:Changed={changed_files} | "
        f"Ruff(LINT_SCOPE=diff, /opt/pinned/ruff.pinned.toml)=0 errors | "
        f"GAS Mock-Gate=PASSED | GAS Diff-Coverage={cov_pct:.1f}% (>={DIFF_COVERAGE_FLOOR_PCT:.0f}% floor) | "
        f"Pytest({', '.join(test_targets)})={pytest_out.splitlines()[-1] if pytest_out else 'PASSED'}"
    )
    return True, summary


class GoldfishAgent(BaseAgent):
    """
    Agent 0: `illya-nau/GAS` `src/goldfish/goldfish/goldfish.py` Spec-Completeness Pre-Screen.

    A fresh reader who has never seen this project. It is SPEC-BLIND by construction: it receives
    ONLY `work_kind`, `project_kind`, and `acceptance_criteria` (the escaped issue title + body) --
    no repository access (`Read`/`Grep`/`Glob` are all disallowed), no wiki block, no skills block,
    no findings-from-a-previous-run block. It grades only whether the spec itself carries enough
    information for a stranger to act on.

    On `refuse` it escalates immediately (`EventActions(escalate=True)`), aborting the pipeline in
    a single turn for ~$0.02 rather than burning the full ~$0.80 Architect -> Coder <-> Reviewer run
    on an issue no one could implement. Findings never change the decision (GAS `goldfish.md`).
    """

    async def _run_async_impl(self, ctx: InvocationContext) -> AsyncGenerator[Event, None]:
        meta = _load_issue_meta()
        work_kind = str(meta.get("work_kind") or "feature")
        project_kind = str(meta.get("project_kind") or "python-bigquery-finops-cli")
        title = str(meta.get("title") or "")
        criteria = str(meta.get("acceptance_criteria") or "")
        bypass = bool(meta.get("goldfish_bypass"))

        if not GOLDFISH_ENABLED or bypass or not criteria.strip():
            reason = (
                "disabled via GOLDFISH_ENABLED=false"
                if not GOLDFISH_ENABLED
                else ("bypassed via `agent:force` label" if bypass else "no acceptance criteria available")
            )
            print(f"⏭️ [ADK Agent 0: GoldfishAgent] Skipped ({reason}).")
            # Emit anyway: the PR dashboard row and the `GAS Intake / 0.` commit status must resolve,
            # otherwise every bypassed run leaves a permanently pending check on the PR.
            _emit_pr_stage_event(
                stage="0/3",
                agent_name="Agent 0: GoldfishAgent (Spec-Completeness Pre-Screen)",
                model=GOLDFISH_MODEL,
                status=f"SKIPPED ({reason})",
                turns=0,
                duration_s=0.0,
                cost_usd=0.0,
                iteration=1,
                details=f"**GAS Goldfish Spec-Completeness Pre-Screen:** skipped — {reason}.",
            )
            delta = {"goldfish_decision": "pass", "work_kind": work_kind, "goldfish_skipped": True}
            ctx.session.state.update(delta)
            yield Event(
                author=self.name,
                invocation_id=ctx.invocation_id,
                content=types.Content(role="model", parts=[types.Part.from_text(text=f"Goldfish skipped ({reason}).")]),
                actions=EventActions(state_delta=delta),
            )
            return

        print(f"🐠 [ADK Agent 0: GoldfishAgent ({GOLDFISH_MODEL})] Spec-completeness pre-screen (work_kind={work_kind})...")

        goldfish_prompt = f"""You are a fresh reader who has never seen this project before.
You receive a work_kind, a project_kind, and a spec someone wants built. You have NO repository access and MUST NOT ask for any.

Restate the spec in your own words: WHAT changes, WHERE, and HOW you would know it is done.
If restating it forces you to INVENT any of those three, the spec is incomplete: emit `"decision": "refuse"` and say exactly what you had to invent.
Otherwise emit `"decision": "pass"`.
List any separate concerns the spec does not address as `findings`; they do NOT change your decision.

<work_kind>{work_kind}</work_kind>
<project_kind>{project_kind}</project_kind>
<acceptance_criteria title="{title}">
{criteria[:20000]}
</acceptance_criteria>

Respond with a short restatement followed by EXACTLY one fenced `INTAKE_VERDICT_JSON` block conforming to `IntakeVerdict`:
```json
{{
  "decision": "pass",
  "restatement": "<one-paragraph restatement: what changes, where, and how you would know it is done>",
  "findings": [
    {{"severity": "minor", "message": "<a separate concern the spec does not address>"}}
  ]
}}
```
(`severity` must be one of `blocker`, `major`, `minor`, `nit`. Free text alone never counts as a verdict — you MUST emit the JSON block.)"""

        intake_text, turns, dur_s, cost = await asyncio.to_thread(
            _run_claude_cli,
            goldfish_prompt,
            GOLDFISH_MODEL,
            [],
            ["Read", "Grep", "Glob", "Edit", "Write", "Bash", "WebFetch", "WebSearch"],
            2,
            False,
            "0/3",
            "GoldfishAgent",
        )

        verdict = _parse_intake_verdict(intake_text)
        refused = verdict.decision == "refuse"
        findings_md = "\n".join(f"- `{f.severity}` — {f.message}" for f in verdict.findings) or "- _None_"
        details = (
            f"**GAS Goldfish Spec-Completeness Pre-Screen** (`work_kind={work_kind}`, `project_kind={project_kind}`)\n\n"
            f"**Decision:** `{verdict.decision.upper()}`\n\n"
            f"**Restatement (fresh reader):**\n> {verdict.restatement[:2000]}\n\n"
            f"**Findings (do not change the decision):**\n{findings_md}"
        )

        delta: Dict[str, Any] = {
            "goldfish_decision": verdict.decision,
            "goldfish_restatement": verdict.restatement,
            "goldfish_findings": [f.model_dump() for f in verdict.findings],
            "work_kind": work_kind,
            "total_turns": int(ctx.session.state.get("total_turns", 0)) + turns,
            "total_duration_s": round(float(ctx.session.state.get("total_duration_s", 0.0)) + dur_s, 1),
            "total_cost_usd": float(ctx.session.state.get("total_cost_usd", 0.0)) + cost,
        }

        _emit_pr_stage_event(
            stage="0/3",
            agent_name="Agent 0: GoldfishAgent (Spec-Completeness Pre-Screen)",
            model=GOLDFISH_MODEL,
            status="REFUSED (spec_gap)" if refused else "PASSED",
            turns=turns,
            duration_s=dur_s,
            cost_usd=cost,
            iteration=1,
            details=details,
        )

        if refused:
            hard_break_msg = (
                "HARD_BREAK [SPEC_GAP]: GAS Goldfish refused the issue specification — a fresh reader "
                "had to invent what changes, where, or how completion would be verified. "
                "Terminating before the Architect -> Coder <-> Reviewer pipeline is dispatched."
            )
            print(f"🛑 {hard_break_msg}")
            delta["termination_reason"] = hard_break_msg
            delta["review_approved"] = False
            ctx.session.state.update(delta)
            yield Event(
                author=self.name,
                invocation_id=ctx.invocation_id,
                content=types.Content(role="model", parts=[types.Part.from_text(text=f"{hard_break_msg}\n\n{details}")]),
                actions=EventActions(state_delta=delta, escalate=True),
            )
            return

        print(f"✅ [ADK Agent 0: GoldfishAgent] Spec PASSED ({turns} turns, {dur_s}s, ${cost:.4f}); dispatching pipeline.")
        ctx.session.state.update(delta)
        yield Event(
            author=self.name,
            invocation_id=ctx.invocation_id,
            content=types.Content(role="model", parts=[types.Part.from_text(text=details)]),
            actions=EventActions(state_delta=delta),
        )


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

CRITICAL INSPECTION BUDGET:
You have a budget of up to 15 tool inspections. Keep your repository survey tightly focused. Once you have inspected the key integration points, you MUST STOP calling tools and output your final SOP specification with the `### 1. SOP_METADATA_JSON` block containing `allowed_file_list` and `targeted_test_files`.

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

### 4. SECURITY_AND_REPO_INVARIANTS
- Zero `bigquery.tables.getData` usage, no protected paths touched, offline `pytest` compatibility (`@pytest.mark.usefixtures("mock_bq_all")` if testing validators/CLI), `LINT_SCOPE="diff"` (only lint modified files via `/opt/pinned/ruff.pinned.toml`), and whether `src/static/` bundle sync is needed.

### 5. ANYTHING_UNCLEAR_RESOLVED
- Explicitly resolve any potential ambiguity in the user issue so Agent 2 (`{CODER_MODEL}`) can implement the code in a single pass without guessing."""

        plan_text, turns, dur_s, cost = await asyncio.to_thread(
            _run_claude_cli,
            architect_prompt,
            ARCHITECT_MODEL,
            ["Read", "Grep", "Glob"],
            ["Edit", "Write", "Bash", "WebFetch", "WebSearch"],
            25,
            False,
            "1/3",
            "ArchitectAgent",
        )

        sop_meta = _extract_sop_metadata(plan_text)

        # If Architect ran out of turns or emitted free text without JSON, run a quick 1-turn synthesis
        if not sop_meta["sop_allowed_files"]:
            print("⚠️ [ADK Agent 1: ArchitectAgent] Plan lacked allowed_file_list; running 1-turn synthesis...", flush=True)
            synth_prompt = f"""You are Agent 1 (Principal Software Architect, {ARCHITECT_MODEL}).
In your previous turn, you inspected the repository for the following issue:
{issue_prompt}

Your preliminary analysis output was:
{plan_text[:4000]}

You MUST now immediately emit the required SOP specification and `allowed_file_list`.
Do NOT call any tools. Output the standardized SOP structure:

### 1. SOP_METADATA_JSON
```json
{{
  "allowed_file_list": ["<exact relative paths of files to modify or create>"],
  "targeted_test_files": ["<exact relative paths of pytest files to run>"]
}}
```

### 2. INTERFACE_AND_DATA_STRUCTURES
### 3. LOGIC_ANALYSIS_BY_FILE
### 4. SECURITY_AND_REPO_INVARIANTS
### 5. ANYTHING_UNCLEAR_RESOLVED"""
            synth_text, s_turns, s_dur, s_cost = await asyncio.to_thread(
                _run_claude_cli,
                synth_prompt,
                ARCHITECT_MODEL,
                [],
                ["Read", "Grep", "Glob", "Edit", "Write", "Bash", "WebFetch", "WebSearch"],
                2,
                False,
                "1/3",
                "ArchitectAgent (Synthesis)",
            )
            synth_meta = _extract_sop_metadata(synth_text)
            if synth_meta["sop_allowed_files"]:
                plan_text = synth_text + "\n\n" + plan_text
                sop_meta = synth_meta
                turns += s_turns
                dur_s += s_dur
                cost += s_cost

        if not sop_meta["sop_allowed_files"]:
            raise RuntimeError(
                f"ArchitectAgent failed to emit a non-empty `allowed_file_list` in SOP plan. "
                f"Cannot proceed to CoderAgent without authorized files."
            )

        delta = {
            "architecture_plan": plan_text,
            "sop_allowed_files": sop_meta["sop_allowed_files"],
            "sop_targeted_tests": sop_meta["sop_targeted_tests"],
            "total_turns": int(ctx.session.state.get("total_turns", 0)) + turns,
            "total_duration_s": round(float(ctx.session.state.get("total_duration_s", 0.0)) + dur_s, 1),
            "total_cost_usd": float(ctx.session.state.get("total_cost_usd", 0.0)) + cost,
        }
        ctx.session.state.update(delta)

        try:
            SOP_PLAN_FILE.write_text(plan_text, encoding="utf-8")
        except Exception as exc:
            print(f"[SOPPlan Warning] Could not persist SOP plan to {SOP_PLAN_FILE}: {exc}")

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
    Agent 2: Claude Sonnet 5 Coder with Role-Specific Context Subscription (MetaGPT Sec. 3.2 & Appendix E.2)
    and Action-Level PreToolUse Policy Hook (Paper pp. 2, 10-11).
    """

    async def _run_async_impl(self, ctx: InvocationContext) -> AsyncGenerator[Event, None]:
        iteration = int(ctx.session.state.get("loop_iteration", 0)) + 1
        ctx.session.state["loop_iteration"] = iteration

        role_key = "CoderAgent_Initial" if iteration == 1 else "CoderAgent_Revision"
        sub_ctx = _subscribe_role_context(role_key, ctx.session.state)

        arch_plan = str(sub_ctx.get("architecture_plan", ""))
        sop_allowed_files = list(sub_ctx.get("sop_allowed_files") or [])
        sop_targeted_tests = list(sub_ctx.get("sop_targeted_tests") or [])
        exec_feedback = str(sub_ctx.get("executable_feedback", ""))
        blocking_findings_table = str(sub_ctx.get("blocking_findings_json", ""))
        review_feedback = str(sub_ctx.get("review_feedback", ""))
        convergence_trajectory = str(sub_ctx.get("convergence_trajectory", ""))

        print(
            f"🛠️ [ADK Agent 2: CoderAgent ({CODER_MODEL})] Iteration {iteration}/{MAX_REVIEW_LOOPS} "
            f"(Subscribed profile: {role_key}, keys={list(sub_ctx.keys())})..."
        )

        feedback_section = ""
        if exec_feedback or blocking_findings_table or review_feedback:
            structured_critique = blocking_findings_table if blocking_findings_table else review_feedback
            feedback_section = f"""
<iterative_feedback iteration="{iteration - 1}">
The previous iteration did NOT pass verification. Fix ONLY the defects reported below while adhering to `allowed_file_list` ({sop_allowed_files}):
{convergence_trajectory}
{exec_feedback}
{structured_critique}
</iterative_feedback>
"""

        issue_section = f"\n{sub_ctx['issue_prompt']}\n" if "issue_prompt" in sub_ctx else ""
        test_hint = " ".join(sop_targeted_tests) if sop_targeted_tests else "<your_test_file>"
        py_allowed_hint = " ".join(f for f in sop_allowed_files if f.endswith(".py")) or "<modified_py_files>"

        coder_prompt = f"""You are Agent 2 (Autonomous Software Engineer, {CODER_MODEL}) in a MetaGPT-inspired 3-agent Google ADK pipeline.
Implement the specification in Agent 1's SOP Architecture Plan below.
Strictly restrict your file edits/creations to `allowed_file_list`: {sop_allowed_files}.

CRITICAL EXECUTION & TURN BUDGET (MAX 50 TURNS):
- Agent 1's `<architect_sop_plan>` below ALREADY surveyed the codebase and provides exact file paths, line numbers, function signatures, and implementation blueprints.
- Spend AT MOST 6-8 tool calls reading/grepping files before you begin writing code (`Write` / `Edit`). NEVER spend an entire pass reading files without creating/editing files!
- Implement ALL required files in `allowed_file_list` ({sop_allowed_files}) — including backend modules, API endpoints, static UI files (and running `./scripts/sync_docs_bundle.sh` if `static/` was modified), AND the complete unit test suite (`{test_hint}`) covering >=80% of all newly added `src/` lines — within this pass.

[AUTHORITATIVE VERIFICATION OVERRIDE]:
In this automated pipeline, verification is governed strictly by diff-scoped checks (`LINT_SCOPE="diff"`).
Do NOT run full-repo `./.venv/bin/ruff check .` across untouched files (any conflicting instruction in CLAUDE.md is superseded for this autonomous run).

{issue_section}
<architect_sop_plan>
{arch_plan}
</architect_sop_plan>
{feedback_section}
CRITICAL LINT & TEST SCOPE (`LINT_SCOPE="diff"`):
1. Run targeted unit tests: `./.venv/bin/pytest --rootdir=. --override-ini=addopts= -c /opt/pinned/pytest.pinned.ini -q {test_hint}`
2. Run diff-scoped Ruff ONLY on your modified Python files: `./.venv/bin/ruff check --config /opt/pinned/ruff.pinned.toml {py_allowed_hint}`
3. NEVER run unfiltered `./.venv/bin/ruff check .` across the entire repository, and NEVER waste turns running `git stash`, `git show HEAD:...`, or inspecting pre-existing warnings in untouched lines/files. Finish immediately once your targeted `pytest` and diff-scoped `ruff` commands pass."""

        coder_allowed_tools = [
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
        ]
        coder_disallowed_tools = [
            "Bash(curl *)",
            "Bash(wget *)",
            "Bash(git push *)",
            "Bash(gh *)",
            "Bash(python3 -c *)",
            "Bash(pip *)",
            "Bash(npm *)",
            "WebFetch",
            "WebSearch",
        ]

        _install_claude_pre_tool_hook(sop_allowed_files)
        try:
            coder_out, turns, dur_s, cost = await asyncio.to_thread(
                _run_claude_cli,
                coder_prompt,
                CODER_MODEL,
                coder_allowed_tools,
                coder_disallowed_tools,
                50,
                True,
                "2/3",
                f"CoderAgent (Pass {iteration})",
            )
        finally:
            _cleanup_claude_pre_tool_hook()

        changed_now = _collect_changed_files(str(WORKSPACE_DIR) if WORKSPACE_DIR.exists() else ".")
        if not changed_now:
            print(
                f"⚠️ [ADK Agent 2: CoderAgent] Pass {iteration} produced 0 modified files after {turns} turns. "
                f"Running immediate write-only recovery pass..."
            )
            recovery_prompt = f"""CRITICAL RECOVERY DIRECTIVE:
You just exhausted {turns} turns reading files without creating or editing ANY files in `/workspace` (`git status` is completely clean)!
STOP reading/grepping files immediately. Use `Write` and `Edit` RIGHT NOW to implement ALL required files in `allowed_file_list`: {sop_allowed_files}, including the unit test suite (`{test_hint}`), run `./scripts/sync_docs_bundle.sh` if `static/` was modified, and verify with `./.venv/bin/pytest --rootdir=. --override-ini=addopts= -c /opt/pinned/pytest.pinned.ini -q {test_hint}` and `./.venv/bin/ruff check --config /opt/pinned/ruff.pinned.toml {py_allowed_hint}`.

<architect_sop_plan>
{arch_plan}
</architect_sop_plan>"""
            _install_claude_pre_tool_hook(sop_allowed_files)
            try:
                rec_out, r_turns, r_dur, r_cost = await asyncio.to_thread(
                    _run_claude_cli,
                    recovery_prompt,
                    CODER_MODEL,
                    coder_allowed_tools,
                    coder_disallowed_tools,
                    40,
                    True,
                    "2/3",
                    f"CoderAgent (Pass {iteration} Recovery)",
                )
                coder_out = coder_out + "\n\n---\n### Write-First Recovery Pass\n" + rec_out
                turns += r_turns
                dur_s = round(dur_s + r_dur, 1)
                cost += r_cost
            finally:
                _cleanup_claude_pre_tool_hook()
            changed_now = _collect_changed_files(str(WORKSPACE_DIR) if WORKSPACE_DIR.exists() else ".")

        delta = {
            "loop_iteration": iteration,
            "total_turns": int(ctx.session.state.get("total_turns", 0)) + turns,
            "total_duration_s": round(float(ctx.session.state.get("total_duration_s", 0.0)) + dur_s, 1),
            "total_cost_usd": float(ctx.session.state.get("total_cost_usd", 0.0)) + cost,
        }
        ctx.session.state.update(delta)

        print(f"✅ [ADK Agent 2: CoderAgent] Pass {iteration} complete ({turns} turns, {dur_s}s, ${cost:.4f}).")
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
    + Mathematical Convergence & Stagnation Evaluation (Paper pp. 9-10)
    + Claude Opus 5.5 Adversarial Diff Critique with Pydantic `ArchitecturalReviewVerdict` (Paper p. 9).
    """

    async def _run_async_impl(self, ctx: InvocationContext) -> AsyncGenerator[Event, None]:
        iteration = int(ctx.session.state.get("loop_iteration", 1))
        sop_allowed_files = list(ctx.session.state.get("sop_allowed_files") or [])
        sop_targeted_tests = list(ctx.session.state.get("sop_targeted_tests") or [])
        cost_ceiling = float(ctx.session.state.get("app:max_session_cost_usd", MAX_SESSION_COST_USD))

        # Capture current workspace diff and SHA-256 fingerprint (Paper p. 10: ΔDiff stagnation check)
        current_diff = _collect_workspace_diff()
        curr_diff_hash = _compute_diff_hash(current_diff)
        diff_hash_history = list(ctx.session.state.get("diff_hash_history") or [])
        prev_diff_hash = diff_hash_history[-1] if diff_hash_history else None
        diff_hash_history.append(curr_diff_hash)

        # Step 3a: Deterministic Pre-Review Executable Feedback Gate (MetaGPT Sec. 3.3)
        print(f"⚙️ [ADK Pre-Review Executable Feedback Gate] Running ruff + pytest check (Iteration {iteration}/{MAX_REVIEW_LOOPS}, diff_sha={curr_diff_hash})...")
        exec_passed, exec_report = await asyncio.to_thread(
            _run_executable_feedback_gate,
            sop_allowed_files,
            sop_targeted_tests,
        )

        curr_d_test, curr_failed_nodes = _extract_pytest_distance(exec_report if not exec_passed else "")
        d_test_history = list(ctx.session.state.get("d_test_history") or [])
        prev_d_test = d_test_history[-1] if d_test_history else None
        prev_failed_nodes = list(ctx.session.state.get("prev_failed_nodes") or [])
        d_test_history.append(curr_d_test)

        # Evaluate Loop-Boundary Mathematical Hard-Breaks (Paper pp. 9-10)
        if iteration >= 2 and prev_diff_hash and curr_diff_hash == prev_diff_hash and not exec_passed:
            hard_break_msg = (
                f"HARD_BREAK [DIFF_STAGNATION]: Iteration {iteration} produced an identical diff fingerprint "
                f"(`{curr_diff_hash}`, ΔDiff = 0) as Iteration {iteration - 1}. Terminating loop to prevent cognitive deadlock."
            )
            print(f"🛑 {hard_break_msg}")
            _emit_pr_stage_event(
                stage="2.5/3",
                agent_name=f"Pre-Review Executable Feedback Gate (Iteration {iteration}/{MAX_REVIEW_LOOPS})",
                model="deterministic-ruff-pytest",
                status="FAILED (ΔDiff=0 Stagnation)",
                turns=0,
                duration_s=0.2,
                cost_usd=0.0,
                iteration=iteration,
                details=f"{hard_break_msg}\n\n{exec_report}",
            )
            delta = {
                "diff_hash_history": diff_hash_history,
                "d_test_history": d_test_history,
                "termination_reason": hard_break_msg,
                "review_approved": False,
            }
            ctx.session.state.update(delta)
            yield Event(
                author=self.name,
                invocation_id=ctx.invocation_id,
                content=types.Content(role="model", parts=[types.Part.from_text(text=hard_break_msg)]),
                actions=EventActions(state_delta=delta, escalate=True),
            )
            return

        if not exec_passed:
            # Check Test Failure Distance Stagnation (D_test^(N) >= D_test^(N-1) with identical failing test node IDs)
            if (
                iteration >= 2
                and prev_d_test is not None
                and curr_d_test > 0
                and curr_d_test >= prev_d_test
                and curr_failed_nodes
                and curr_failed_nodes == prev_failed_nodes
            ):
                hard_break_msg = (
                    f"HARD_BREAK [TEST_STAGNATION]: Test failure distance did not improve "
                    f"(D_test: {prev_d_test} -> {curr_d_test}) and the exact same test assertions failed across consecutive cycles: {curr_failed_nodes}."
                )
                print(f"🛑 {hard_break_msg}")
                _emit_pr_stage_event(
                    stage="2.5/3",
                    agent_name=f"Pre-Review Executable Feedback Gate (Iteration {iteration}/{MAX_REVIEW_LOOPS})",
                    model="deterministic-ruff-pytest",
                    status=f"FAILED (D_test={curr_d_test} Stagnation)",
                    turns=0,
                    duration_s=0.3,
                    cost_usd=0.0,
                    iteration=iteration,
                    details=f"{hard_break_msg}\n\n{exec_report}",
                )
                delta = {
                    "diff_hash_history": diff_hash_history,
                    "d_test_history": d_test_history,
                    "termination_reason": hard_break_msg,
                    "review_approved": False,
                }
                ctx.session.state.update(delta)
                yield Event(
                    author=self.name,
                    invocation_id=ctx.invocation_id,
                    content=types.Content(role="model", parts=[types.Part.from_text(text=hard_break_msg)]),
                    actions=EventActions(state_delta=delta, escalate=True),
                )
                return

            # Check Loop-Boundary Cost Ceiling before launching next Coder retry
            curr_total_cost = float(ctx.session.state.get("total_cost_usd", 0.0))
            if curr_total_cost >= cost_ceiling:
                hard_break_msg = (
                    f"HARD_BREAK [COST_CEILING_EXCEEDED]: Cumulative session cost (${curr_total_cost:.4f}) "
                    f"reached ceiling (${cost_ceiling:.2f}) at loop boundary {iteration}."
                )
                print(f"🛑 {hard_break_msg}")
                delta = {
                    "diff_hash_history": diff_hash_history,
                    "d_test_history": d_test_history,
                    "termination_reason": hard_break_msg,
                    "review_approved": False,
                }
                ctx.session.state.update(delta)
                yield Event(
                    author=self.name,
                    invocation_id=ctx.invocation_id,
                    content=types.Content(role="model", parts=[types.Part.from_text(text=hard_break_msg)]),
                    actions=EventActions(state_delta=delta, escalate=True),
                )
                return

            trajectory_note = (
                f"📈 Convergence Metrics: D_test trajectory = {d_test_history} (current failing/errored count = {curr_d_test}), "
                f"diff_sha = {curr_diff_hash}."
            )
            print(
                f"⚠️ [ADK Pre-Review Executable Feedback Gate] FAILED on iteration {iteration} "
                f"(Short-circuiting back to CoderAgent for $0.00 Opus cost | {trajectory_note}):\n{exec_report}"
            )
            _emit_pr_stage_event(
                stage="2.5/3",
                agent_name=f"Pre-Review Executable Feedback Gate (Iteration {iteration}/{MAX_REVIEW_LOOPS})",
                model="deterministic-ruff-pytest",
                status=f"REVISE (D_test={curr_d_test}, $0.00)",
                turns=0,
                duration_s=0.5,
                cost_usd=0.0,
                iteration=iteration,
                details=f"**{trajectory_note}**\n\n{exec_report}",
            )
            delta = {
                "temp:raw_pytest_output": exec_report,
                "executable_feedback": exec_report,
                "review_feedback": exec_report,
                "blocking_findings_json": "",
                "convergence_trajectory": trajectory_note,
                "diff_hash_history": diff_hash_history,
                "d_test_history": d_test_history,
                "prev_failed_nodes": curr_failed_nodes,
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

        print(f"✅ [ADK Pre-Review Executable Feedback Gate] {exec_report} (D_test=0, diff_sha={curr_diff_hash})")
        ctx.session.state["executable_feedback_summary"] = exec_report

        # Step 3b: Role-Specific Context Subscription for ReviewerAgent (MetaGPT Sec. 3.2)
        sub_ctx = _subscribe_role_context("ReviewerAgent", ctx.session.state)
        arch_plan = str(sub_ctx.get("architecture_plan", ""))
        exec_summary = str(sub_ctx.get("executable_feedback_summary", ""))

        print(f"🔍 [ADK Agent 3: ReviewerAgent ({REVIEWER_MODEL})] Auditing diff (Iteration {iteration}/{MAX_REVIEW_LOOPS})...")

        # GAS `config/lenses.yaml` `kinds:` — work-kind adaptive lens selection.
        work_kind = str(ctx.session.state.get("work_kind") or _load_issue_meta().get("work_kind") or "feature")
        active_lenses, lens_block, lens_json_rows, lens_categories = _build_lens_prompt_fragments(work_kind)
        ctx.session.state["active_lenses"] = active_lenses
        print(
            f"🔬 [GAS Adaptive Lenses] work_kind=`{work_kind}` → {len(active_lenses)}/4 lenses: {active_lenses}"
        )

        reviewer_prompt = f"""You are Agent 3 (Independent Spec-Blind Code & Security Reviewer, {REVIEWER_MODEL}) in a MetaGPT + GAS 3-agent Google ADK pipeline.
Per `GAS` Spec-Blind Policy (`/opt/pinned/lenses.pinned.yaml`), you receive ONLY the acceptance specification (`<architect_sop_plan>`), deterministic gate receipts (`<executable_feedback_status>`), and the unified `<git_diff>` — never the author's (`CoderAgent`) own reasoning, prompt, or history.
Deterministic Pre-Review Executable Feedback has ALREADY PASSED (`{exec_summary}`).
Note: Outer Step 4 (`verify_agent_diff.py`) deterministically executes the full `CLAUDE.md` §2 offline gate suite (`pytest --rootdir=. --override-ini=addopts= -c /opt/pinned/pytest.pinned.ini -m "not integration" --strict-markers`, `ruff check --config /opt/pinned/ruff.pinned.toml`, `sync_docs_bundle.sh`, and `node tests/test_calculator_engine.js`) prior to `git push`.

This issue's GAS `work_kind` is `{work_kind}`, which selects exactly {len(active_lenses)} Spec-Blind Review Lens(es) from `/opt/pinned/lenses.pinned.yaml` (`kinds:` map). Evaluate the diff across ONLY these lenses — do NOT report on lenses outside this set:
{lens_block}

Note on the SOP file scope for this run: `{sop_allowed_files}`.

<executable_feedback_status>
{exec_summary}
</executable_feedback_status>

<architect_sop_plan>
{arch_plan}
</architect_sop_plan>

<git_diff>
{current_diff[:90000]}
</git_diff>

Provide a concise {len(active_lenses)}-row Markdown Lens Review Table (`| GAS Lens | Scope | Verdict | Findings |`), followed by a structured `REVIEW_VERDICT_JSON` block conforming to `ArchitecturalReviewVerdict`:
```json
{{
  "decision": "APPROVE",
  "lens_verdicts": {{
{lens_json_rows}
  }},
  "blocking_findings": []
}}
```
(If requesting changes, set `"decision": "REQUEST_CHANGES"`, mark the failing lens(es) `"REJECT"` in `"lens_verdicts"`, and populate `"blocking_findings"` with `file_path`, `line_start`, `line_end`, `category` [{lens_categories}], `critique`, and `actionable_remediation`.)
End your response with EXACTLY one of:
- `VERDICT: PASS` (if all {len(active_lenses)} selected lens(es) pass and the implementation is ready for PR)
- `VERDICT: REVISE` (followed by specific bullet points for Agent 2 to fix in the next loop iteration)."""

        review_text, turns, dur_s, cost = await asyncio.to_thread(
            _run_claude_cli,
            reviewer_prompt,
            REVIEWER_MODEL,
            ["Read", "Grep", "Glob"],
            ["Edit", "Write", "Bash", "WebFetch", "WebSearch"],
            6,
            False,
            "3/3",
            f"ReviewerAgent (Pass {iteration})",
        )

        verdict = _parse_review_verdict(review_text)
        approved = (verdict.decision == "APPROVE") and ("VERDICT: REVISE" not in review_text)
        blocking_table = _format_blocking_findings_for_coder(verdict)
        display_review_text = _strip_verdict_json_for_display(review_text)
        new_total_cost = float(ctx.session.state.get("total_cost_usd", 0.0)) + cost

        convergence_summary = (
            f"**Convergence Ledger:** `D_test={curr_d_test}` (history: `{d_test_history}`) | "
            f"`Diff Coverage={LAST_DIFF_COVERAGE_PCT:.1f}%` | `Diff SHA-256={curr_diff_hash}` | "
            f"`Session Cost=${new_total_cost:.4f} / ${cost_ceiling:.2f}`"
        )

        delta = {
            "temp:raw_git_diff": current_diff[:10000],
            "total_turns": int(ctx.session.state.get("total_turns", 0)) + turns,
            "total_duration_s": round(float(ctx.session.state.get("total_duration_s", 0.0)) + dur_s, 1),
            "total_cost_usd": new_total_cost,
            "diff_coverage_pct": LAST_DIFF_COVERAGE_PCT,
            "diff_hash_history": diff_hash_history,
            "d_test_history": d_test_history,
            "prev_failed_nodes": [],
            "executable_feedback": "",
            "review_approved": approved,
            "blocking_findings_json": "" if approved else blocking_table,
            "convergence_trajectory": convergence_summary,
            "review_feedback": "" if approved else display_review_text,
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
            details=f"**Pre-Review Executable Feedback:** `{exec_summary}`\n{convergence_summary}\n\n{display_review_text}",
        )

        if approved:
            print(f"✅ [ADK Agent 3: ReviewerAgent ({REVIEWER_MODEL})] VERDICT: PASS on iteration {iteration}!")
            OPUS_REPORT_FILE.write_text(f"{convergence_summary}\n\n{display_review_text}", encoding="utf-8")
            yield Event(
                author=self.name,
                invocation_id=ctx.invocation_id,
                content=types.Content(role="model", parts=[types.Part.from_text(text=display_review_text)]),
                actions=EventActions(state_delta=delta, escalate=True),
            )
        else:
            # Loop-boundary cost ceiling check before starting next retry
            if new_total_cost >= cost_ceiling:
                hard_break_msg = (
                    f"HARD_BREAK [COST_CEILING_EXCEEDED]: Session cost (${new_total_cost:.4f}) exceeded "
                    f"ceiling (${cost_ceiling:.2f}) after ReviewerAgent pass {iteration}."
                )
                print(f"🛑 {hard_break_msg}")
                ctx.session.state["termination_reason"] = hard_break_msg
                yield Event(
                    author=self.name,
                    invocation_id=ctx.invocation_id,
                    content=types.Content(role="model", parts=[types.Part.from_text(text=hard_break_msg)]),
                    actions=EventActions(state_delta={"termination_reason": hard_break_msg}, escalate=True),
                )
                return

            print(f"🔄 [ADK Agent 3: ReviewerAgent ({REVIEWER_MODEL})] VERDICT: REVISE on iteration {iteration}; looping back to Agent 2...")
            yield Event(
                author=self.name,
                invocation_id=ctx.invocation_id,
                content=types.Content(role="model", parts=[types.Part.from_text(text=review_text)]),
                actions=EventActions(state_delta=delta, escalate=False),
            )


def build_adk_app() -> App:
    """Construct the Google ADK Sequential + Loop multi-agent application."""
    goldfish = GoldfishAgent(
        name="GoldfishAgent",
        description="GAS spec-blind intake pre-screen that refuses under-specified issues in 1 turn.",
    )
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
        description="End-to-end Goldfish -> Architect -> (Coder <-> Reviewer) ADK pipeline.",
        sub_agents=[goldfish, architect, code_and_review_loop],
    )

    return App(name="bq_finops_adk_agent", root_agent=root_orchestrator)


async def run_pipeline() -> None:
    if not PROMPT_FILE.exists():
        sys.exit(f"FATAL: Sanitized issue prompt not found at {PROMPT_FILE}")

    repo_name = os.environ.get("GITHUB_REPO", "mbettan/bq-finops-optimizer-private")
    issue_id = os.environ.get("TARGET_ISSUE_NUMBER", "unknown")
    setup_distributed_observability(repo_name=repo_name, issue_id=issue_id)

    issue_prompt = PROMPT_FILE.read_text(encoding="utf-8")
    issue_meta = _load_issue_meta()
    seed_work_kind = str(issue_meta.get("work_kind") or "feature")
    app = build_adk_app()
    runner = InMemoryRunner(app=app)

    session = await runner.session_service.create_session(
        app_name=app.name,
        user_id="cloud_run_worker",
        state={
            "app:repo": repo_name,
            "app:issue_number": issue_id,
            "app:max_iterations": MAX_REVIEW_LOOPS,
            "app:max_session_cost_usd": MAX_SESSION_COST_USD,
            "issue_prompt": issue_prompt,
            "work_kind": seed_work_kind,
            "active_lenses": _select_lenses_for_work_kind(seed_work_kind),
            "goldfish_decision": "pending",
            "loop_iteration": 0,
            "total_turns": 0,
            "total_duration_s": 0.0,
            "total_cost_usd": 0.0,
            "diff_coverage_pct": 100.0,
            "diff_hash_history": [],
            "d_test_history": [],
            "review_approved": False,
        },
    )

    trigger_msg = types.Content(
        role="user",
        parts=[
            types.Part.from_text(
                text="Execute the GAS/ADK Goldfish -> Architect -> Coder <-> Reviewer workflow."
            )
        ],
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

    approved = bool(state.get("review_approved", False))
    goldfish_decision = str(state.get("goldfish_decision", "pass"))
    goldfish_refused = goldfish_decision == "refuse"
    active_lenses = list(state.get("active_lenses") or _select_lenses_for_work_kind(seed_work_kind))
    term_reason = str(
        state.get("termination_reason", "VERDICT: PASS" if approved else "MAX_ITERATIONS_EXHAUSTED")
    )
    if approved:
        fail_class, fail_label, fail_glossary = (
            "none",
            "gate:passed",
            f"All GAS/ADK gates and the {len(active_lenses)}-lens Spec-Blind review passed.",
        )
    elif goldfish_refused:
        # GAS `failure_classification.py`: a Goldfish refusal is `spec_gap` by construction, and
        # escalates to `agent:needs-clarification` rather than the generic failure label.
        fail_class, fail_label = "spec_gap", "agent:needs-clarification"
        fail_glossary = (
            "Spec Gap (GAS Goldfish): A fresh reader could not restate what changes, where, and how "
            "completion would be verified without inventing at least one of the three. "
            "Clarify the issue and re-apply `agent:implement`."
        )
    else:
        fail_class, fail_label, fail_glossary = classify_failure_cause(term_reason, exit_code=1)

    telemetry_summary = {
        "num_turns": state.get("total_turns", 0),
        "duration_ms": int(float(state.get("total_duration_s", 0.0)) * 1000),
        "total_cost_usd": state.get("total_cost_usd", 0.0),
        "cost_ceiling_usd": state.get("app:max_session_cost_usd", MAX_SESSION_COST_USD),
        "loop_iterations": state.get("loop_iteration", 1),
        "diff_coverage_pct": state.get("diff_coverage_pct", LAST_DIFF_COVERAGE_PCT),
        "diff_hash_history": state.get("diff_hash_history", []),
        "d_test_history": state.get("d_test_history", []),
        "termination_reason": term_reason,
        "review_approved": approved,
        "failure_class": fail_class,
        "failure_label": fail_label,
        "failure_glossary": fail_glossary,
        "work_kind": str(state.get("work_kind") or seed_work_kind),
        "active_lenses": active_lenses,
        "goldfish_decision": goldfish_decision,
        "goldfish_restatement": str(state.get("goldfish_restatement", ""))[:2000],
        "goldfish_findings": state.get("goldfish_findings", []),
    }
    TELEMETRY_FILE.write_text(json.dumps(telemetry_summary, indent=2), encoding="utf-8")

    if not approved:
        sys.exit(
            f"FATAL [{fail_class} / {fail_label}]: ADK pipeline terminated without approval ({term_reason}). {fail_glossary}"
        )


if __name__ == "__main__":
    asyncio.run(run_pipeline())
