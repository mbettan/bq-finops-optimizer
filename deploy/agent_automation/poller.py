#!/usr/bin/env python3
"""
Pull-Only GitHub Issue Poller for FinOps Optimizer for BigQuery.
Uses Python standard library only (zero external package dependencies).
Runs as a serverless Cloud Run Job triggered by Cloud Scheduler every 5 minutes.
"""
import json
import os
import re
import subprocess
import sys
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional

OWNER_IMMUTABLE_ID = 14251830  # Immutable GitHub Database ID for mbettan
REPO_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
IDENT_RE = re.compile(r"^[A-Za-z0-9_-]+$")

TRIGGER_LABEL = "agent:implement"
IN_PROGRESS_LABEL = "agent:in-progress"


def _get_validated_env() -> Dict[str, str]:
    repo = os.environ.get("GITHUB_REPO", "mbettan/bq-finops-optimizer-private").strip()
    if not REPO_RE.match(repo):
        raise ValueError(f"Invalid GITHUB_REPO format: {repo}")

    project_id = os.environ.get("GCP_PROJECT_ID", "bq-finops-optimizer").strip()
    if not IDENT_RE.match(project_id):
        raise ValueError(f"Invalid GCP_PROJECT_ID format: {project_id}")

    region = os.environ.get("GCP_REGION", "us-central1").strip()
    if not IDENT_RE.match(region):
        raise ValueError(f"Invalid GCP_REGION format: {region}")

    worker_job = os.environ.get("WORKER_JOB_NAME", "bq-finops-agent-worker").strip()
    if not IDENT_RE.match(worker_job):
        raise ValueError(f"Invalid WORKER_JOB_NAME format: {worker_job}")

    gh_pat = os.environ.get("GITHUB_PAT", "").strip()
    if not gh_pat:
        raise RuntimeError("Missing required GITHUB_PAT environment variable.")

    return {
        "repo": repo,
        "project_id": project_id,
        "region": region,
        "worker_job": worker_job,
        "gh_pat": gh_pat,
    }


def _gh_api_request(
    method: str,
    url: str,
    gh_pat: str,
    payload: Optional[Dict[str, Any]] = None,
) -> Any:
    if not url.startswith("https://api.github.com/"):
        raise ValueError(f"Disallowed API URL: {url}")
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={
            "Authorization": f"Bearer {gh_pat}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Type": "application/json",
            "User-Agent": "bq-finops-agent-poller/1.0",
        },
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        raw = resp.read().decode("utf-8")
        return json.loads(raw) if raw.strip() else None


def verify_and_lock_issue(
    issue: Dict[str, Any],
    repo: str,
    gh_pat: str,
    worker_job: str,
) -> bool:
    issue_num = int(issue["number"])
    events_url = f"https://api.github.com/repos/{repo}/issues/{issue_num}/events"
    events: List[Dict[str, Any]] = _gh_api_request("GET", events_url, gh_pat) or []

    label_events = [
        e for e in events
        if e.get("event") == "labeled" and e.get("label", {}).get("name") == TRIGGER_LABEL
    ]
    if not label_events:
        print(f"[Issue #{issue_num}] Skipping: No '{TRIGGER_LABEL}' label event found.")
        return False

    latest_event = label_events[-1]
    actor_id = latest_event.get("actor", {}).get("id")
    actor_login = latest_event.get("actor", {}).get("login")
    label_timestamp = latest_event.get("created_at", "")

    # Gate 1: Immutable Numeric Actor Check
    if actor_id != OWNER_IMMUTABLE_ID:
        print(
            f"[SECURITY ALERT] Issue #{issue_num} labeled by unauthorized actor "
            f"{actor_login} (ID: {actor_id} != {OWNER_IMMUTABLE_ID}). Removing label."
        )
        encoded_label = urllib.parse.quote(TRIGGER_LABEL, safe="")
        _gh_api_request(
            "DELETE",
            f"https://api.github.com/repos/{repo}/issues/{issue_num}/labels/{encoded_label}",
            gh_pat,
        )
        return False

    # Gate 2: TOCTOU Mid-Flight Modification Check
    issue_author_id = issue.get("user", {}).get("id")
    issue_updated_at = issue.get("updated_at", "")
    if issue_author_id != OWNER_IMMUTABLE_ID and issue_updated_at > label_timestamp:
        print(
            f"[SECURITY ALERT] Issue #{issue_num} was modified at {issue_updated_at} "
            f"after maintainer approval timestamp ({label_timestamp}). Aborting TOCTOU risk."
        )
        return False

    # Gate 3: Lock Issue Conversation & Swap Label
    _gh_api_request(
        "PUT",
        f"https://api.github.com/repos/{repo}/issues/{issue_num}/lock",
        gh_pat,
        {"lock_reason": "resolved"},
    )

    _gh_api_request(
        "POST",
        f"https://api.github.com/repos/{repo}/issues/{issue_num}/labels",
        gh_pat,
        {"labels": [IN_PROGRESS_LABEL]},
    )
    encoded_label = urllib.parse.quote(TRIGGER_LABEL, safe="")
    _gh_api_request(
        "DELETE",
        f"https://api.github.com/repos/{repo}/issues/{issue_num}/labels/{encoded_label}",
        gh_pat,
    )

    _gh_api_request(
        "POST",
        f"https://api.github.com/repos/{repo}/issues/{issue_num}/comments",
        gh_pat,
        {
            "body": (
                f"🔒 **Autonomous Agent Dispatched**\n"
                f"- Verified Maintainer Approval (`ID: {OWNER_IMMUTABLE_ID}`)\n"
                f"- Issue locked to prevent mid-flight prompt modification.\n"
                f"- Spinning up ephemeral Cloud Run Worker micro-VM (`{worker_job}`)..."
            )
        },
    )
    return True


def main() -> None:
    cfg = _get_validated_env()
    encoded_trigger = urllib.parse.quote(TRIGGER_LABEL, safe="")
    url = f"https://api.github.com/repos/{cfg['repo']}/issues?labels={encoded_trigger}&state=open&per_page=10"
    issues = _gh_api_request("GET", url, cfg["gh_pat"]) or []

    if not issues:
        print("No open issues labeled 'agent:implement'. Exiting cleanly.")
        return

    for issue in issues:
        if "pull_request" in issue:
            continue
        issue_num = int(issue["number"])
        if verify_and_lock_issue(issue, cfg["repo"], cfg["gh_pat"], cfg["worker_job"]):
            print(f"[Issue #{issue_num}] Verified & locked. Dispatching Cloud Run Worker Job...")
            subprocess.run(
                [
                    "/usr/bin/env", "gcloud", "run", "jobs", "execute", cfg["worker_job"],
                    f"--project={cfg['project_id']}",
                    f"--region={cfg['region']}",
                    f"--update-env-vars=TARGET_ISSUE_NUMBER={issue_num},GITHUB_REPO={cfg['repo']}",
                    "--async",
                ],
                check=True,
            )


if __name__ == "__main__":
    main()
