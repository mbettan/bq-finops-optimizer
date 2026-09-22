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
    status_out = subprocess.check_output(["/usr/bin/env", "git", "status", "--porcelain"]).decode().splitlines()
    changed_files = [line[3:].strip() for line in status_out if line]

    if not changed_files:
        sys.exit("ABORT: Agent produced zero file changes.")

    for path in changed_files:
        for prefix in FORBIDDEN_PATHS:
            if path == prefix or path.startswith(prefix):
                sys.exit(f"FATAL SECURITY GATE VIOLATION: Agent attempted to touch protected path: {path}")

    print("Running ./scripts/sync_docs_bundle.sh...")
    subprocess.run(["./scripts/sync_docs_bundle.sh"], check=True)

    print("Running Ruff linter...")
    subprocess.run(["./.venv/bin/ruff", "check", "."], check=True)

    print("Running offline pytest suite...")
    subprocess.run(["./.venv/bin/pytest"], check=True)

    print("Running Node calculator & pricing parity tests...")
    subprocess.run(["/usr/bin/env", "node", "scripts/sync_pricing.js", "--check"], check=True)
    subprocess.run(["/usr/bin/env", "node", "tests/test_calculator_engine.js"], check=True)

    print("✅ All deterministic pre-push security & quality gates passed!")


if __name__ == "__main__":
    main()
