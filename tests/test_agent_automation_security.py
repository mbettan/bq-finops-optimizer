"""
Unit tests for the Autonomous Issue-to-PR Agent Security Gates:
- deploy/agent_automation/poller.py
- deploy/agent_automation/verify_issue_actor.py
- deploy/agent_automation/verify_agent_diff.py
"""
import importlib.util
from pathlib import Path
from unittest.mock import patch
import pytest

ROOT_DIR = Path(__file__).resolve().parent.parent


def _load_module(name: str, rel_path: str):
    spec = importlib.util.spec_from_file_location(name, ROOT_DIR / rel_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_poller_rejects_unauthorized_actor_and_removes_label():
    poller = _load_module("poller", "deploy/agent_automation/poller.py")
    fake_issue = {
        "number": 15,
        "user": {"id": 999999},
        "updated_at": "2026-09-22T18:00:00Z",
    }
    calls = []

    def fake_api(method, url, gh_pat, payload=None):
        calls.append((method, url))
        if method == "GET":
            return [
                {
                    "event": "labeled",
                    "label": {"name": "agent:implement"},
                    "actor": {"id": 888888, "login": "malicious_user"},
                    "created_at": "2026-09-22T18:05:00Z",
                }
            ]
        return None

    with patch.object(poller, "_gh_api_request", side_effect=fake_api):
        approved = poller.verify_and_lock_issue(
            fake_issue,
            "mbettan/bq-finops-optimizer-private",
            "fake_pat",
            "bq-finops-agent-worker",
        )
        assert approved is False
        assert any(m == "DELETE" for m, _ in calls)


def test_poller_aborts_on_toctou_midflight_edit():
    poller = _load_module("poller", "deploy/agent_automation/poller.py")
    fake_issue = {
        "number": 15,
        "user": {"id": 777777},
        "updated_at": "2026-09-22T18:10:00Z",  # Edited AFTER mbettan applied label
    }

    def fake_api(method, url, gh_pat, payload=None):
        return [
            {
                "event": "labeled",
                "label": {"name": "agent:implement"},
                "actor": {"id": 14251830, "login": "mbettan"},
                "created_at": "2026-09-22T18:05:00Z",
            }
        ]

    with patch.object(poller, "_gh_api_request", side_effect=fake_api):
        approved = poller.verify_and_lock_issue(
            fake_issue,
            "mbettan/bq-finops-optimizer-private",
            "fake_pat",
            "bq-finops-agent-worker",
        )
        assert approved is False


def test_poller_approves_and_locks_valid_owner_issue():
    poller = _load_module("poller", "deploy/agent_automation/poller.py")
    fake_issue = {
        "number": 15,
        "user": {"id": 14251830},
        "updated_at": "2026-09-22T18:00:00Z",
    }
    calls = []

    def fake_api(method, url, gh_pat, payload=None):
        calls.append((method, url))
        if method == "GET":
            return [
                {
                    "event": "labeled",
                    "label": {"name": "agent:implement"},
                    "actor": {"id": 14251830, "login": "mbettan"},
                    "created_at": "2026-09-22T18:05:00Z",
                }
            ]
        return None

    with patch.object(poller, "_gh_api_request", side_effect=fake_api):
        approved = poller.verify_and_lock_issue(
            fake_issue,
            "mbettan/bq-finops-optimizer-private",
            "fake_pat",
            "bq-finops-agent-worker",
        )
        assert approved is True
        assert any(m == "PUT" and url.endswith("/lock") for m, url in calls)
        assert any(m == "POST" and url.endswith("/labels") for m, url in calls)
        assert any(m == "DELETE" for m, _ in calls)


def test_verify_agent_diff_blocks_protected_paths():
    diff_gate = _load_module("verify_agent_diff", "deploy/agent_automation/verify_agent_diff.py")
    with patch.object(
        diff_gate.subprocess,
        "check_output",
        return_value=b".github/workflows/ci.yml\0src/main.py\0",
    ):
        with pytest.raises(SystemExit) as exc:
            diff_gate.main()
        assert "FATAL SECURITY GATE VIOLATION" in str(exc.value)


def _load_adk_orchestrator():
    import sys
    import types

    for mod_name in (
        "google",
        "google.adk",
        "google.adk.agents",
        "google.adk.apps",
        "google.adk.events",
        "google.adk.runners",
        "google.genai",
    ):
        if mod_name not in sys.modules:
            sys.modules[mod_name] = types.ModuleType(mod_name)

    sys.modules["google.adk.agents"].BaseAgent = object
    sys.modules["google.adk.agents"].InvocationContext = object
    sys.modules["google.adk.agents"].LoopAgent = object
    sys.modules["google.adk.agents"].SequentialAgent = object
    sys.modules["google.adk.apps"].App = object
    sys.modules["google.adk.events"].Event = object
    sys.modules["google.adk.events"].EventActions = object
    sys.modules["google.adk.runners"].InMemoryRunner = object
    sys.modules["google.genai"].types = types.SimpleNamespace()

    return _load_module("adk_orchestrator", "deploy/agent_automation/adk_orchestrator.py")


def test_metagpt_sop_metadata_extraction_and_role_subscriptions():
    orch = _load_adk_orchestrator()
    sample_plan = '''
### 1. SOP_METADATA_JSON
```json
{
  "allowed_file_list": ["./src/utils.py", "tests\\\\test_utils.py"],
  "targeted_test_files": ["tests/test_utils.py"]
}
```
'''
    meta = orch._extract_sop_metadata(sample_plan)
    assert meta["sop_allowed_files"] == ["src/utils.py", "tests/test_utils.py"]
    assert meta["sop_targeted_tests"] == ["tests/test_utils.py"]

    full_state = {
        "issue_prompt": "Raw issue text",
        "architecture_plan": "Structured SOP Plan",
        "sop_allowed_files": ["src/utils.py"],
        "sop_targeted_tests": ["tests/test_utils.py"],
        "executable_feedback": "Pytest failed line 42",
        "review_feedback": "Fix docstring",
        "executable_feedback_summary": "PASSED",
        "unrelated_noise": "Should be filtered out",
    }

    # Revision pass for CoderAgent must exclude raw issue_prompt to avoid Information Overload
    rev_ctx = orch._subscribe_role_context("CoderAgent_Revision", full_state)
    assert "issue_prompt" not in rev_ctx
    assert "unrelated_noise" not in rev_ctx
    assert rev_ctx["executable_feedback"] == "Pytest failed line 42"

    # ReviewerAgent subscribes only to architecture_plan, sop_allowed_files, and executable_feedback_summary
    reviewer_ctx = orch._subscribe_role_context("ReviewerAgent", full_state)
    assert set(reviewer_ctx.keys()) == {"architecture_plan", "sop_allowed_files", "executable_feedback_summary"}


def test_executable_feedback_gate_short_circuits_on_scope_creep_and_stage_event(tmp_path):
    orch = _load_adk_orchestrator()
    with patch.object(orch, "_collect_changed_files", return_value=["src/utils.py", "src/unauthorized.py"]):
        passed, report = orch._run_executable_feedback_gate(
            sop_allowed_files=["src/utils.py"],
            sop_targeted_tests=["tests/test_utils.py"],
        )
        assert passed is False
        assert "SOP Scope Creep" in report
        assert "src/unauthorized.py" in report

    stage_file = tmp_path / "adk_stage_events.jsonl"
    with patch.object(orch, "STAGE_EVENTS_FILE", stage_file):
        orch._emit_pr_stage_event(
            stage="1/3",
            agent_name="Agent 1: ArchitectAgent",
            model="claude-opus-5-5",
            status="PASSED",
            turns=5,
            duration_s=27.6,
            cost_usd=0.2471,
            iteration=1,
            details="SOP Plan Body",
        )
        lines = stage_file.read_text(encoding="utf-8").splitlines()
        assert len(lines) == 1
        assert '"stage": "1/3"' in lines[0]
        assert '"agent": "Agent 1: ArchitectAgent"' in lines[0]


def test_convergence_math_pydantic_verdict_and_pre_tool_hook(tmp_path):
    import json
    import subprocess
    import sys

    orch = _load_adk_orchestrator()

    # 1. Diff SHA-256 fingerprint (ΔDiff == 0 detection)
    h1 = orch._compute_diff_hash("--- a/src/utils.py\n+++ b/src/utils.py\n+x = 1\n")
    h2 = orch._compute_diff_hash("--- a/src/utils.py\n+++ b/src/utils.py\n+x = 1\n")
    h3 = orch._compute_diff_hash("--- a/src/utils.py\n+++ b/src/utils.py\n+x = 2\n")
    assert h1 == h2
    assert h1 != h3
    assert orch._compute_diff_hash("") == "empty_diff_0"

    # 2. Pytest failure distance metric D_test = |T_failed| + |T_errored|
    sample_pytest_fail = (
        "EXECUTABLE FEEDBACK FAILURE [Pytest Runtime Traceback]:\n"
        "FAILED tests/test_utils.py::test_negative_value - ValueError\n"
        "ERROR tests/test_utils.py::test_bad_fixture\n"
        "2 failed, 1 error, 15 passed in 0.12s"
    )
    d_test, nodes = orch._extract_pytest_distance(sample_pytest_fail)
    assert d_test == 3
    assert nodes == [
        "tests/test_utils.py::test_bad_fixture",
        "tests/test_utils.py::test_negative_value",
    ]

    # 3. Structured Pydantic ArchitecturalReviewVerdict + DefectFinding parsing
    review_with_json = '''
Here is the audit table.
```json
{
  "decision": "REQUEST_CHANGES",
  "blocking_findings": [
    {
      "file_path": "src/utils.py",
      "line_start": 42,
      "line_end": 48,
      "category": "SECURITY",
      "critique": "Missing boolean rejection before numeric comparison.",
      "actionable_remediation": "Add isinstance(val, bool) check before val == 0."
    }
  ]
}
```
VERDICT: REVISE
'''
    verdict = orch._parse_review_verdict(review_with_json)
    assert verdict.decision == "REQUEST_CHANGES"
    assert len(verdict.blocking_findings) == 1
    assert verdict.blocking_findings[0].file_path == "src/utils.py"
    assert verdict.blocking_findings[0].category == "SECURITY"
    coder_table = orch._format_blocking_findings_for_coder(verdict)
    assert "L42-L48" in coder_table
    assert "isinstance(val, bool)" in coder_table

    # 4. PreToolUse Policy Hook enforcement
    hook_script = tmp_path / "adk_pre_tool_hook.py"
    fake_ws = tmp_path / "workspace"
    fake_ws.mkdir()
    with patch.object(orch, "PRE_TOOL_HOOK_SCRIPT", hook_script), patch.object(orch, "WORKSPACE_DIR", fake_ws):
        orch._install_claude_pre_tool_hook(["src/utils.py"])
        assert (fake_ws / ".claude" / "settings.local.json").exists()

        # Allowed write -> exit 0
        res_ok = subprocess.run(
            [sys.executable, str(hook_script)],
            input=json.dumps({"tool_name": "Edit", "tool_input": {"file_path": "/workspace/src/utils.py"}}),
            text=True,
            capture_output=True,
            check=False,
        )
        assert res_ok.returncode == 0

        # Protected path write -> exit 2 (blocked)
        res_prot = subprocess.run(
            [sys.executable, str(hook_script)],
            input=json.dumps({"tool_name": "Write", "tool_input": {"file_path": "/workspace/.github/workflows/ci.yml"}}),
            text=True,
            capture_output=True,
            check=False,
        )
        assert res_prot.returncode == 2
        assert "POLICY HOOK BLOCKED" in res_prot.stderr

        # Out-of-scope source file write -> exit 2 (blocked)
        res_scope = subprocess.run(
            [sys.executable, str(hook_script)],
            input=json.dumps({"tool_name": "Edit", "tool_input": {"file_path": "/workspace/src/main.py"}}),
            text=True,
            capture_output=True,
            check=False,
        )
        assert res_scope.returncode == 2
        assert "outside ArchitectAgent allowed_file_list" in res_scope.stderr

        orch._cleanup_claude_pre_tool_hook()
        assert not (fake_ws / ".claude").exists()


def test_gas_lint_scope_diff_pinned_configs_and_mock_gate(tmp_path):
    orch = _load_adk_orchestrator()

    # 1. Line-level diff-hunk extraction (_extract_added_lines_by_file)
    sample_u0_diff = (
        "diff --git a/src/utils.py b/src/utils.py\n"
        "--- a/src/utils.py\n"
        "+++ b/src/utils.py\n"
        "@@ -645,0 +646,3 @@\n"
        "+def clamp_percentage(val):\n"
        "+    return max(0.0, min(100.0, float(val)))\n"
        "+\n"
    )
    added_map = orch._extract_added_lines_by_file(sample_u0_diff)
    assert added_map["src/utils.py"] == {646, 647, 648}
    assert 9 not in added_map["src/utils.py"]  # Legacy line 9 is ignored!

    # 2. GAS AST `mock-gate` (_verify_no_self_mocking_in_diff)
    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()
    bad_test_file = tests_dir / "test_utils.py"
    bad_test_file.write_text(
        "from unittest.mock import patch\n"
        "def test_fake_clamp():\n"
        "    with patch('src.utils.clamp_percentage', return_value=50.0):\n"
        "        assert True\n",
        encoding="utf-8",
    )
    ok_bad, err_bad = orch._verify_no_self_mocking_in_diff(
        sample_u0_diff, ["tests/test_utils.py"], str(tmp_path)
    )
    assert ok_bad is False
    assert "GAS Mock-Gate Violation" in err_bad
    assert "clamp_percentage" in err_bad

    # Valid test that mocks an external collaborator (e.g. google.cloud.bigquery.Client) must PASS
    good_test_file = tests_dir / "test_utils_good.py"
    good_test_file.write_text(
        "from unittest.mock import patch\n"
        "def test_real_clamp():\n"
        "    with patch('google.cloud.bigquery.Client'):\n"
        "        assert True\n",
        encoding="utf-8",
    )
    ok_good, err_good = orch._verify_no_self_mocking_in_diff(
        sample_u0_diff, ["tests/test_utils_good.py"], str(tmp_path)
    )
    assert ok_good is True
    assert err_good == ""

    # 3. Strip machine REVIEW_VERDICT_JSON from human-facing PR comment after Pydantic parse
    raw_review = (
        "### Review Summary\nAll edge cases covered.\n\n"
        "### REVIEW_VERDICT_JSON\n"
        "```json\n"
        '{\n  "decision": "APPROVE",\n  "blocking_findings": []\n}\n'
        "```\n\n"
        "VERDICT: PASS"
    )
    parsed = orch._parse_review_verdict(raw_review)
    assert parsed.decision == "APPROVE"
    cleaned_display = orch._strip_verdict_json_for_display(raw_review)
    assert '"decision": "APPROVE"' not in cleaned_display
    assert "VERDICT: PASS" in cleaned_display
    assert "All edge cases covered." in cleaned_display

    # 4. Verify pinned config files exist and are valid
    assert Path("deploy/agent_automation/pinned/ruff.pinned.toml").is_file()
    assert Path("deploy/agent_automation/pinned/pytest.pinned.ini").is_file()



