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

    import re
    for path in changed_files:
        norm_path = os.path.normpath(path).replace("\\", "/").removeprefix("./")
        if os.path.basename(norm_path) == "conftest.py":
            sys.exit(f"FATAL SECURITY GATE VIOLATION [GAS base_conftests]: Agent attempted to add or modify a conftest.py file: {path}")
        for prefix in FORBIDDEN_PATHS:
            clean_prefix = prefix.rstrip("/")
            if norm_path == clean_prefix or norm_path.startswith(clean_prefix + "/"):
                sys.exit(f"FATAL SECURITY GATE VIOLATION: Agent attempted to touch protected path: {path}")

    print("Running ./scripts/sync_docs_bundle.sh...")
    subprocess.run(["./scripts/sync_docs_bundle.sh"], check=True)

    import shutil
    ruff_bin = shutil.which("./.venv/bin/ruff") or shutil.which("/opt/venv/bin/ruff") or shutil.which("ruff") or "ruff"
    pytest_bin = shutil.which("./.venv/bin/pytest") or shutil.which("/opt/venv/bin/pytest") or shutil.which("pytest") or "pytest"
    python_bin = shutil.which("./.venv/bin/python3") or shutil.which("/opt/venv/bin/python3") or sys.executable
    pinned_ruff = "/opt/pinned/ruff.pinned.toml" if os.path.exists("/opt/pinned/ruff.pinned.toml") else None
    pinned_pytest = "/opt/pinned/pytest.pinned.ini" if os.path.exists("/opt/pinned/pytest.pinned.ini") else None

    py_changed = [f for f in changed_files if f.endswith(".py") and os.path.exists(f)]
    if py_changed:
        ruff_args = [ruff_bin, "check"]
        if pinned_ruff:
            ruff_args.extend(["--config", pinned_ruff])
        else:
            ruff_args.extend(["--select", "E9,F63,F7,F82"])
        ruff_args.extend(py_changed)
        print(f"Running Ruff linter on modified files (LINT_SCOPE=diff): {py_changed} ({ruff_args})...")
        subprocess.run(ruff_args, check=True)

        # GAS import_check.py: verify every modified src/**/*.py module imports cleanly via importlib.import_module
        mod_names = []
        name_re = re.compile(r"^[A-Za-z0-9._]+$")
        for rel_py in py_changed:
            norm_py = os.path.normpath(rel_py).replace("\\", "/").removeprefix("./")
            if norm_py.startswith("src/") and norm_py.endswith(".py"):
                dotted = norm_py[:-3].replace("/", ".")
                if dotted.endswith(".__init__"):
                    dotted = dotted[:-9]
                if dotted and name_re.match(dotted):
                    mod_names.append(dotted)
        if mod_names:
            print(f"Running GAS importlib.import_module smoke-check on modified modules: {mod_names}...")
            subprocess.run(
                [
                    python_bin,
                    "-c",
                    "import importlib, sys; sys.path.insert(0, '.'); [importlib.import_module(m) for m in sys.argv[1:]]",
                    *mod_names,
                ],
                check=True,
            )

    pytest_args = [pytest_bin]
    if pinned_pytest:
        pytest_args.extend(["--rootdir=.", "--override-ini=addopts=", "-c", pinned_pytest])
    pytest_args.extend(["-m", "not integration", "--strict-markers"])
    print(f"Running offline pytest suite ({pytest_args})...")
    subprocess.run(pytest_args, check=True)

    print("Running Node calculator & pricing parity tests...")
    if os.path.exists("docs/PRICING_CALCULATOR_SPEC.md"):
        subprocess.run(["/usr/bin/env", "node", "scripts/sync_pricing.js", "--check"], check=True)
    subprocess.run(["/usr/bin/env", "node", "tests/test_calculator_engine.js"], check=True)

    print("✅ All deterministic pre-push security & quality gates passed!")


if __name__ == "__main__":
    main()
