#!/usr/bin/env python3
"""
Deterministic Pre-Push Security Gate.
Executes outside Claude Code's control before git push is permitted.
"""
import subprocess
import sys

FORBIDDEN_PATHS = (
    ".github/",
    "deploy/",
    "Dockerfile",
    ".gitattributes",
    ".gitignore",
    "CLAUDE.md",
    "tests/conftest.py",
    "scripts/sync_docs_bundle.sh",
    "scripts/sync_pricing.js",
)


def main() -> None:
    import os
    diff_base = []
    try:
        diff_base = subprocess.check_output(
            ["/usr/bin/env", "git", "diff", "--name-only", "-z", "base-anchor"],
            stderr=subprocess.DEVNULL,
        ).decode().split("\0")
    except Exception:
        pass

    diff_working = subprocess.check_output(
        ["/usr/bin/env", "git", "diff", "--name-only", "-z", "HEAD"]
    ).decode().split("\0")
    diff_cached = subprocess.check_output(
        ["/usr/bin/env", "git", "diff", "--name-only", "-z", "--cached"]
    ).decode().split("\0")
    untracked = subprocess.check_output(
        ["/usr/bin/env", "git", "ls-files", "--others", "--exclude-standard", "-z"]
    ).decode().split("\0")

    changed_files = sorted({f for f in (diff_base + diff_working + diff_cached + untracked) if f})

    if not changed_files:
        sys.exit("ABORT: Agent produced zero file changes.")

    for path in changed_files:
        norm_path = os.path.normpath(path).lstrip("./")
        for prefix in FORBIDDEN_PATHS:
            clean_prefix = prefix.rstrip("/")
            if norm_path == clean_prefix or norm_path.startswith(clean_prefix + "/"):
                sys.exit(f"FATAL SECURITY GATE VIOLATION: Agent attempted to touch protected path: {path}")

    print("Running ./scripts/sync_docs_bundle.sh...")
    subprocess.run(["./scripts/sync_docs_bundle.sh"], check=True)

    import shutil
    ruff_bin = shutil.which("./.venv/bin/ruff") or shutil.which("/opt/venv/bin/ruff") or shutil.which("ruff") or "ruff"
    pytest_bin = shutil.which("./.venv/bin/pytest") or shutil.which("/opt/venv/bin/pytest") or shutil.which("pytest") or "pytest"

    py_changed = [f for f in changed_files if f.endswith(".py")]
    if py_changed:
        print(f"Running Ruff linter on modified files: {py_changed} ({ruff_bin})...")
        subprocess.run([ruff_bin, "check", "--select", "E9,F63,F7,F82"] + py_changed, check=True)

    print(f"Running offline pytest suite ({pytest_bin})...")
    subprocess.run([pytest_bin, "-m", "not integration", "--strict-markers"], check=True)

    print("Running Node calculator & pricing parity tests...")
    import os
    if os.path.exists("docs/PRICING_CALCULATOR_SPEC.md"):
        subprocess.run(["/usr/bin/env", "node", "scripts/sync_pricing.js", "--check"], check=True)
    subprocess.run(["/usr/bin/env", "node", "tests/test_calculator_engine.js"], check=True)

    print("✅ All deterministic pre-push security & quality gates passed!")


if __name__ == "__main__":
    main()
