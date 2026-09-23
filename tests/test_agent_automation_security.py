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
