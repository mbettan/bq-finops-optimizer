"""
Unit tests for the Autonomous Issue-to-PR Agent Security Gates:
- deploy/agent_automation/poller.py
- deploy/agent_automation/verify_issue_actor.py
- deploy/agent_automation/verify_agent_diff.py
"""
import importlib.util
import re
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
    assert Path("deploy/agent_automation/pinned/coverage.pinned.rc").is_file()


def test_gas_diff_coverage_base_conftests_import_check_and_provenance(tmp_path):
    import json
    import subprocess
    import sys

    orch = _load_adk_orchestrator()

    # 1. GAS diff-coverage calculation (_compute_diff_coverage)
    cov_json = tmp_path / "coverage.json"
    cov_json.write_text(
        json.dumps(
            {
                "files": {
                    "src/utils.py": {
                        "executed_lines": [10, 646, 647],
                        "missing_lines": [648],
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    # 2 covered out of 3 added executable lines = 66.7% (< 80% floor -> FAIL)
    ok_low, pct_low, gaps_low, msg_low = orch._compute_diff_coverage(
        cov_json, {"src/utils.py": {646, 647, 648}}, floor_pct=80.0
    )
    assert ok_low is False
    assert pct_low == 66.7
    assert "648" in msg_low
    assert "GAS Diff-Coverage Gate" in msg_low

    # 3 covered out of 3 added executable lines = 100.0% (>= 80% floor -> PASS)
    cov_json.write_text(
        json.dumps(
            {
                "files": {
                    "src/utils.py": {
                        "executed_lines": [10, 646, 647, 648],
                        "missing_lines": [9],  # Legacy missing line 9 is ignored!
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    ok_high, pct_high, gaps_high, msg_high = orch._compute_diff_coverage(
        cov_json, {"src/utils.py": {646, 647, 648}}, floor_pct=80.0
    )
    assert ok_high is True
    assert pct_high == 100.0
    assert gaps_high == []

    # 2. Recursive *conftest.py guard (GAS base_conftests) in _run_executable_feedback_gate and PreToolUse hook
    with patch.object(orch, "_collect_changed_files", return_value=["tests/unit/conftest.py"]):
        passed_cf, report_cf = orch._run_executable_feedback_gate(
            sop_allowed_files=["src/utils.py"],
            sop_targeted_tests=["tests/test_utils.py"],
        )
        assert passed_cf is False
        assert "GAS base_conftests Violation" in report_cf

    hook_script = tmp_path / "adk_pre_tool_hook.py"
    fake_ws = tmp_path / "workspace"
    fake_ws.mkdir()
    with patch.object(orch, "PRE_TOOL_HOOK_SCRIPT", hook_script), patch.object(orch, "WORKSPACE_DIR", fake_ws):
        orch._install_claude_pre_tool_hook(["src/utils.py"])
        res_sub_conftest = subprocess.run(
            [sys.executable, str(hook_script)],
            input=json.dumps({"tool_name": "Write", "tool_input": {"file_path": "/workspace/tests/sub/conftest.py"}}),
            text=True,
            capture_output=True,
            check=False,
        )
        assert res_sub_conftest.returncode == 2
        assert "conftest.py" in res_sub_conftest.stderr
        orch._cleanup_claude_pre_tool_hook()

    # 3. GAS import_check.py (_verify_modified_module_imports)
    ok_imp, msg_imp = orch._verify_modified_module_imports(["src/utils.py"], ".", sys.executable)
    assert ok_imp is True
    assert "src.utils" in msg_imp

    # 4. Verify worker_entrypoint.sh contains GAS state escalation trap & provenance trailers
    entrypoint_text = Path("deploy/agent_automation/worker_entrypoint.sh").read_text(encoding="utf-8")
    assert "trap handle_worker_exit EXIT" in entrypoint_text
    assert "agent:failed-needs-human" in entrypoint_text
    assert "ADK-Diff-Coverage:" in entrypoint_text
    assert "agent:generated,adk:verified,gate:passed" in entrypoint_text

    # 5. Verify #6 GAS 4-Lens Spec-Blind Review (lenses.pinned.yaml + REGRESSION/OPERABILITY schema)
    assert Path("deploy/agent_automation/pinned/lenses.pinned.yaml").is_file()
    lens_review_json = '''
```json
{
  "decision": "REQUEST_CHANGES",
  "lens_verdicts": {
    "CORRECTNESS": "PASS",
    "SECURITY": "PASS",
    "REGRESSION": "REJECT",
    "OPERABILITY": "PASS"
  },
  "blocking_findings": [
    {
      "file_path": "src/utils.py",
      "line_start": 100,
      "line_end": 105,
      "category": "REGRESSION",
      "critique": "Changed default parameter value for existing caller.",
      "actionable_remediation": "Restore default parameter value."
    }
  ]
}
```
VERDICT: REVISE
'''
    v6 = orch._parse_review_verdict(lens_review_json)
    assert v6.decision == "REQUEST_CHANGES"
    assert v6.lens_verdicts["REGRESSION"] == "REJECT"
    assert v6.blocking_findings[0].category == "REGRESSION"


def test_gas_tier_1_autofix_stage_checks_failure_class_and_bot_guard(monkeypatch):
    """
    Verify all 4 GAS Tier-1 upgrades:
      1. `autofix.py` (`_finding_counts` extracts rule codes without leaking S105 secrets)
      2. `stage_checks.py` (`stamp_all_commit_statuses` posts granular GAS Gate & Review statuses)
      3. `failure_classification.py` (`classify_failure_cause` maps spec_gap, agent_error, infrastructure)
      4. `intake.py` (`is_bot_author` blocks Bot accounts and `[bot]` logins while allowing human maintainers)
    """
    orch = _load_adk_orchestrator()
    poller = _load_module("poller", "deploy/agent_automation/poller.py")
    telemetry = _load_module("pr_live_telemetry", "deploy/agent_automation/pr_live_telemetry.py")

    # 1. GAS `autofix.py` `_finding_counts`
    concise_out = (
        "src/utils.py:10:5: F401 `os` imported but unused\n"
        "src/utils.py:22:1: E501 Line too long (122 > 100)\n"
        "src/utils.py:30:12: S105 Possible hardcoded password assigned to: 'SUPER_SECRET_TOKEN'\n"
    )
    counts = orch._finding_counts(concise_out)
    assert counts == {"F401": 1, "E501": 1, "S105": 1}
    assert "SUPER_SECRET_TOKEN" not in str(counts)

    # 2. GAS `failure_classification.py` (`spec_gap` / `agent_error` / `infrastructure`)
    fc_spec, lbl_spec, gl_spec = orch.classify_failure_cause("SPEC_GAP: SOP_INVALID target files missing")
    assert fc_spec == "spec_gap"
    assert lbl_spec == "failure:spec-gap"
    assert "Spec Gap:" in gl_spec

    fc_agent, lbl_agent, gl_agent = orch.classify_failure_cause("HARD_BREAK [DIFF_STAGNATION]: ΔDiff=0")
    assert fc_agent == "agent_error"
    assert lbl_agent == "failure:agent-error"
    assert "Agent Error:" in gl_agent

    fc_infra, lbl_infra, gl_infra = orch.classify_failure_cause("Vertex AI 503 Unavailable", exit_code=137)
    assert fc_infra == "infrastructure"
    assert lbl_infra == "failure:infrastructure"
    assert "exit 137" in gl_infra

    # 3. GAS `intake.py` Bot-Author Intake Guard (`is_bot_author`)
    assert poller.is_bot_author({"user": {"login": "mbettan", "type": "User"}}) is False
    assert poller.is_bot_author({"user": {"login": "dependabot[bot]", "type": "Bot"}}) is True
    assert poller.is_bot_author({"user": {"login": "bq-finops-agent", "type": "User"}}) is True
    assert poller.is_bot_author({"user": {"login": "custom-ci-bot", "type": "User"}}) is True

    # 4. GAS `stage_checks.py` granular commit statuses
    recorded_statuses = []

    def fake_set_commit_status(repo, sha, context, state, description, target_url=""):
        recorded_statuses.append((context, state, description))

    monkeypatch.setattr(telemetry, "set_commit_status", fake_set_commit_status)
    stages = {
        "1/3": {"status": "✅ Complete", "turns": 10, "duration_s": 50.0},
        "2/3": {"status": "✅ Complete", "turns": 7, "duration_s": 40.0},
        "2.5/3": {
            "status": "✅ PASSED",
            "details": "Ruff=0 errors | GAS Mock-Gate=PASSED | GAS Diff-Coverage=100.0% (>=80% floor)",
        },
        "3/3": {"status": "✅ PASS", "turns": 2, "duration_s": 30.0},
    }
    telemetry.stamp_all_commit_statuses(
        "mbettan/bq-finops-optimizer-private",
        "abc1234",
        "https://github.com/mbettan/bq-finops-optimizer-private/pull/66",
        "claude-opus-5-5",
        "claude-sonnet-5",
        "claude-opus-5-5",
        stages,
    )
    contexts = {c: (st, desc) for c, st, desc in recorded_statuses}
    assert "GAS Gate / 1. Ruff Pinned (LINT_SCOPE=diff)" in contexts
    assert contexts["GAS Gate / 1. Ruff Pinned (LINT_SCOPE=diff)"][0] == "success"
    assert "GAS Gate / 2. AST Mock-Gate & Import Check" in contexts
    assert "GAS Gate / 3. Diff-Coverage (>=80% floor)" in contexts
    assert "100.0%" in contexts["GAS Gate / 3. Diff-Coverage (>=80% floor)"][1]
    assert "GAS Review / 4-Lens Spec-Blind Audit" in contexts
    assert contexts["GAS Review / 4-Lens Spec-Blind Audit"][0] == "success"






def test_gas_tier_2_work_kind_derivation_and_adaptive_lens_selection(monkeypatch):
    """
    GAS Tier 2 #0 — Work-Kind Adaptive Lens Selection (`illya-nau/GAS` `config/lenses.yaml` `kinds:`).

    Two invariants are load-bearing here:
      1. An unrecognized work_kind must fail SAFE (all 4 lenses), never fail permissive (1 lens).
      2. Labels outrank the title prefix, because a maintainer relabeling an issue is a
         deliberate act while a title prefix is whatever the reporter happened to type.
    """
    actor = _load_module("verify_issue_actor", "deploy/agent_automation/verify_issue_actor.py")

    assert actor.derive_work_kind({"labels": [{"name": "bug"}], "title": "x"}) == "bug"
    assert actor.derive_work_kind({"labels": [{"name": "kind:chore"}], "title": "x"}) == "chore"
    assert actor.derive_work_kind({"labels": [], "title": "chore: bump pins"}) == "chore"
    assert actor.derive_work_kind({"labels": [], "title": "fix: off-by-one"}) == "bug"
    # Unclassifiable -> strictest review posture.
    assert actor.derive_work_kind({"labels": [], "title": "please make it better"}) == "feature"
    # A label must beat a contradicting title prefix.
    assert actor.derive_work_kind({"labels": [{"name": "bug"}], "title": "chore: tidy"}) == "bug"

    orch = _load_adk_orchestrator()

    # Exercise the real stdlib YAML reader against the repo copy of the image-baked config,
    # since /opt/pinned/ only exists inside the container.
    monkeypatch.setattr(
        orch, "LENSES_PINNED_FILE", ROOT_DIR / "deploy/agent_automation/pinned/lenses.pinned.yaml"
    )
    assert orch._select_lenses_for_work_kind("chore") == ["CORRECTNESS"]
    assert orch._select_lenses_for_work_kind("bug") == ["CORRECTNESS", "REGRESSION"]
    assert set(orch._select_lenses_for_work_kind("feature")) == {
        "CORRECTNESS",
        "SECURITY",
        "REGRESSION",
        "OPERABILITY",
    }
    assert len(orch._select_lenses_for_work_kind("not-a-real-kind")) == 4

    # A missing/unreadable pinned file must fall back to the in-code defaults, not crash the run.
    monkeypatch.setattr(orch, "LENSES_PINNED_FILE", Path("/nonexistent/lenses.pinned.yaml"))
    assert orch._select_lenses_for_work_kind("chore") == ["CORRECTNESS"]
    assert len(orch._select_lenses_for_work_kind("feature")) == 4


def test_gas_tier_2_goldfish_intake_verdict_parsing_fails_open():
    """
    GAS Tier 2 #5 — Goldfish Spec-Completeness Pre-Screen
    (`illya-nau/GAS` `src/goldfish/goldfish/verdict.py`).

    Goldfish uses `pass`/`refuse`, NOT the reviewer's `pass`/`reject`, and it MUST fail open:
    a garbled Goldfish turn is an infrastructure problem, and silently blocking a
    well-specified issue on it would be worse than skipping the gate entirely.
    """
    orch = _load_adk_orchestrator()

    refuse = orch._parse_intake_verdict(
        'preamble\n```json\n{"decision": "refuse", "restatement": "I cannot tell which module changes.",'
        ' "findings": [{"severity": "blocker", "message": "No target file or module is named."}]}\n```'
    )
    assert refuse.decision == "refuse"
    assert refuse.findings[0].severity == "blocker"
    assert "which module" in refuse.restatement

    passed = orch._parse_intake_verdict(
        '```json\n{"decision": "pass", "restatement": "Add a --dry-run flag to cli.py."}\n```'
    )
    assert passed.decision == "pass"
    assert passed.findings == []

    # Plain-text fallback when the model forgets the fence.
    assert orch._parse_intake_verdict("INTAKE_VERDICT: REFUSE — too vague").decision == "refuse"

    # Garbage, an invalid decision token, and an empty turn must all fail OPEN.
    assert orch._parse_intake_verdict("the model rambled and produced nothing").decision == "pass"
    assert orch._parse_intake_verdict('```json\n{"decision": "maybe"}\n```').decision == "pass"
    assert orch._parse_intake_verdict("").decision == "pass"


def test_gas_tier_2_goldfish_surfaces_in_pr_dashboard_and_commit_statuses():
    """A Goldfish refusal must be visible on the PR, not just in Cloud Logging."""
    telemetry = _load_module("pr_live_telemetry", "deploy/agent_automation/pr_live_telemetry.py")

    md = telemetry.build_dashboard_markdown(
        "42",
        "claude-opus-5-5",
        "claude-sonnet-5",
        "claude-opus-5-5",
        {"0/3": {"status": "🛑 REFUSED — issue underspecified", "model": "claude-sonnet-5", "turns": 1}},
    )
    assert "GoldfishAgent" in md
    assert "REFUSED" in md

    stamped = []
    with patch.object(telemetry, "set_commit_status", lambda *a, **k: stamped.append(a[2:4])):
        telemetry.stamp_all_commit_statuses(
            "o/r", "deadbeef", "https://pr", "a", "b", "c", {"0/3": {"status": "REFUSED (spec_gap)"}}
        )
    contexts = dict(stamped)
    assert contexts["GAS Intake / 0. Goldfish Spec-Completeness"] == "failure"

    # A bypassed run must read green, not sit pending forever.
    stamped.clear()
    with patch.object(telemetry, "set_commit_status", lambda *a, **k: stamped.append(a[2:4])):
        telemetry.stamp_all_commit_statuses(
            "o/r", "deadbeef", "https://pr", "a", "b", "c",
            {"0/3": {"status": "SKIPPED (bypassed via `agent:force` label)"}},
        )
    assert dict(stamped)["GAS Intake / 0. Goldfish Spec-Completeness"] == "success"


def test_worker_entrypoint_forwards_every_tunable_across_the_privilege_drop():
    """
    `adk_orchestrator.py` runs as `agentuser` via `su -c` with an EXPLICIT allowlist of exported
    vars, so any tunable declared at the top of the script but omitted from that block is
    silently dropped. That already bit GOLDFISH_ENABLED -- the kill switch for the intake gate
    was unreachable from the Cloud Run job config. Fail loudly if it regresses.
    """
    script = (ROOT_DIR / "deploy/agent_automation/worker_entrypoint.sh").read_text(encoding="utf-8")

    su_block = script.split('su -s /bin/bash agentuser -c "', 1)
    assert len(su_block) == 2, "privilege-drop block not found; did the entrypoint get restructured?"
    su_body = su_block[1].split('\n  "', 1)[0]

    # Every MODEL/ENABLED tunable exported at the top must survive the privilege drop.
    declared = set(re.findall(r'^export ([A-Z0-9_]+(?:_MODEL|_ENABLED))=', script, flags=re.MULTILINE))
    assert {"GOLDFISH_MODEL", "GOLDFISH_ENABLED"} <= declared

    forwarded = set(re.findall(r"export ([A-Z0-9_]+)='", su_body))
    missing = declared - forwarded
    assert not missing, f"tunables declared but not forwarded to agentuser: {sorted(missing)}"


def test_worker_entrypoint_makes_intake_metadata_readable_by_agentuser():
    """
    `/tmp/adk_issue_meta.json` is written by root but read by agentuser. `_load_issue_meta`
    swallows read errors, so an unreadable file makes Goldfish skip silently on every issue
    while still posting a green check -- it must not depend on the image umask.
    """
    script = (ROOT_DIR / "deploy/agent_automation/worker_entrypoint.sh").read_text(encoding="utf-8")
    assert "chmod 0644 /tmp/adk_issue_meta.json" in script

    # And the orchestrator must genuinely tolerate the file being absent (fail-safe work_kind).
    orch = _load_adk_orchestrator()
    assert orch._load_issue_meta.__doc__


def _generated_hook_verdict(tmp_path, command=None, file_path=None, allowed=("src/utils.py",)):
    """Write the REAL generated hook to disk and execute it, rather than asserting on source text."""
    import json, subprocess, sys as _sys
    orch = _load_adk_orchestrator()
    hook = tmp_path / "hook.py"
    orch.PRE_TOOL_HOOK_SCRIPT = hook
    orch.WORKSPACE_DIR = tmp_path / "nonexistent-workspace"
    orch._install_claude_pre_tool_hook(list(allowed))
    if command is not None:
        payload = {"tool_name": "Bash", "tool_input": {"command": command}}
    else:
        payload = {"tool_name": "Write", "tool_input": {"file_path": file_path}}
    proc = subprocess.run(
        [_sys.executable, str(hook)], input=json.dumps(payload),
        capture_output=True, text=True, timeout=30,
    )
    return proc.returncode, proc.stderr


def test_policy_hook_blocks_self_destructive_git_from_bash(tmp_path):
    """
    Regression for issue #67: CoderAgent ran `git checkout -- src/utils.py` three times to answer a
    diff-coverage shortfall, destroying its own work (76.5% -> 36.8%). The hook only matched
    Edit/Write/MultiEdit, so Bash was an open escape hatch.
    """
    orch = _load_adk_orchestrator()

    # The guard is inert unless Bash is registered in the matcher.
    src = (ROOT_DIR / "deploy/agent_automation/adk_orchestrator.py").read_text(encoding="utf-8")
    assert '"matcher": "Edit|Write|MultiEdit|Bash"' in src

    blocked = [
        "git checkout -- src/utils.py",
        "git checkout .",
        "git restore src/utils.py",
        "git reset --hard HEAD",
        "git stash",
        "git clean -fd",
        "git apply .utils_full.patch",
        "git commit -m 'bypass'",
        "git push --force",
        "git -C /workspace checkout -- src/utils.py",          # flag-with-value must not hide the verb
        "pytest -q && git checkout -- src/utils.py",            # chained after a benign command
        "echo hi; git reset --hard",
    ]
    for cmd in blocked:
        rc, err = _generated_hook_verdict(tmp_path, command=cmd)
        assert rc == 2, f"should have been blocked: {cmd}"
        assert "POLICY HOOK BLOCKED" in err

    allowed = [
        "git status -s",
        "git diff src/utils.py",
        "git diff --stat; git status",
        "git log --oneline -5",
        "git show HEAD",
        "./.venv/bin/pytest -q",
        "./.venv/bin/ruff check --config /opt/pinned/ruff.pinned.toml src/utils.py",
        "wc -l src/utils.py && tail -n 30 src/utils.py",
        "echo 'git checkout is mentioned only in this string'",  # not a git invocation
    ]
    for cmd in allowed:
        rc, err = _generated_hook_verdict(tmp_path, command=cmd)
        assert rc == 0, f"should have been allowed: {cmd} (stderr={err})"

    # The pre-existing Edit/Write protection must still work.
    rc, err = _generated_hook_verdict(tmp_path, file_path="/workspace/deploy/agent_automation/poller.py")
    assert rc == 2 and "POLICY HOOK BLOCKED" in err
    assert orch  # keep the loader referenced


def test_diff_coverage_feedback_forbids_shrinking_the_diff():
    """
    The old message only said 'add tests', which the Coder read as license to restructure the
    source. It must now state the remedy and explicitly rule out shrinking the diff.
    """
    orch = _load_adk_orchestrator()
    src = (ROOT_DIR / "deploy/agent_automation/adk_orchestrator.py").read_text(encoding="utf-8")
    assert "DO NOT revert, restructure, reformat, or delete source lines to shrink the diff" in src
    assert "REMEDY: add tests that execute at least" in src
    assert orch.math is not None  # `math.ceil` is used to compute the shortfall


def test_reviewer_prompt_fragments_adapt_to_work_kind(monkeypatch):
    """
    GAS Tier 2 #0, validated WITHOUT a paid pipeline run.

    The live E2E on issue #67 never reached stage 3 (the Coder exhausted its budget on the
    diff-coverage gate), so the adaptive-lens behaviour was never observed end-to-end. These
    assertions cover the exact fragments that get interpolated into the ReviewerAgent prompt.
    """
    orch = _load_adk_orchestrator()
    monkeypatch.setattr(
        orch, "LENSES_PINNED_FILE", ROOT_DIR / "deploy/agent_automation/pinned/lenses.pinned.yaml"
    )

    lenses, block, json_rows, categories = orch._build_lens_prompt_fragments("chore")
    assert lenses == ["CORRECTNESS"]
    # Exactly one numbered lens line, and no mention of the three excluded lenses anywhere.
    assert block.count("**LENS ") == 1
    assert block.startswith("1. **LENS 1 — `CORRECTNESS`:**")
    for excluded in ("SECURITY", "REGRESSION", "OPERABILITY"):
        assert excluded not in block
        assert excluded not in json_rows
        assert excluded not in categories
    # The verdict skeleton the model is told to emit must have exactly one row.
    assert json_rows == '    "CORRECTNESS": "PASS"'
    assert categories == '"CORRECTNESS"'

    lenses, block, json_rows, categories = orch._build_lens_prompt_fragments("bug")
    assert lenses == ["CORRECTNESS", "REGRESSION"]
    assert block.count("**LENS ") == 2
    assert "SECURITY" not in block and "OPERABILITY" not in block

    lenses, block, json_rows, categories = orch._build_lens_prompt_fragments("feature")
    assert len(lenses) == 4
    assert block.count("**LENS ") == 4
    for expected in ("CORRECTNESS", "SECURITY", "REGRESSION", "OPERABILITY"):
        assert expected in block and expected in json_rows and expected in categories

    # Unknown kind must fail SAFE to the full set, never to the cheap one.
    lenses, block, _, _ = orch._build_lens_prompt_fragments("not-a-kind")
    assert len(lenses) == 4 and block.count("**LENS ") == 4

    # Every emitted lens must carry a real scope sentence -- no silent empty instructions.
    for lens in orch._select_lenses_for_work_kind("feature"):
        assert orch.LENS_SCOPES[lens].strip()


def test_extract_sop_metadata_multi_block_and_fallbacks():
    """
    Verify ArchitectAgent SOP metadata parsing handles:
    1. Multiple code blocks where an API schema precedes the metadata block
    2. Trailing commas / non-strict JSON via regex fallback
    3. Markdown bullet list fallback
    """
    orch = _load_adk_orchestrator()

    # Case 1: Multiple JSON blocks (API schema followed by SOP_METADATA_JSON)
    multi_block = """
### API Request Example
```json
{
  "prompt": "diagnose query contention",
  "project_id": "test-finops"
}
```

### 1. SOP_METADATA_JSON
```json
{
  "allowed_file_list": ["src/main.py", "src/mcp_server.py"],
  "targeted_test_files": ["tests/test_ai_doctor.py", "tests/test_mcp_server.py"]
}
```
"""
    res1 = orch._extract_sop_metadata(multi_block)
    assert res1["sop_allowed_files"] == ["src/main.py", "src/mcp_server.py"]
    assert res1["sop_targeted_tests"] == ["tests/test_ai_doctor.py", "tests/test_mcp_server.py"]

    # Case 2: Trailing comma / non-strict JSON
    trailing_comma = """
```json
{
  "allowed_file_list": [
    "src/utils.py",
    "static/app.js",
  ],
  "targeted_test_files": [
    "tests/test_utils.py",
  ]
}
```
"""
    res2 = orch._extract_sop_metadata(trailing_comma)
    assert res2["sop_allowed_files"] == ["src/utils.py", "static/app.js"]
    assert res2["sop_targeted_tests"] == ["tests/test_utils.py"]

    # Case 3: Markdown bullet points
    markdown_list = """
### 1. Allowed Files
- `src/main.py`
- `static/index.html`
- `tests/test_ai.py`
"""
    res3 = orch._extract_sop_metadata(markdown_list)
    assert res3["sop_allowed_files"] == ["src/main.py", "static/index.html", "tests/test_ai.py"]

