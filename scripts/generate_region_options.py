#!/usr/bin/env python3
"""Regenerate the region <select> markup in the HTML bundles from BQ_REGIONS.

The registry in src/constants.py is the single source of truth.
This script rewrites the <select id="cfg-region"> block in place so the markup
can never drift from the backend's notion of a valid region.

Usage:
    python3 scripts/generate_region_options.py            # rewrite in place
    python3 scripts/generate_region_options.py --check    # verify, exit 1 on drift
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from src.constants import BQ_REGIONS, BQ_REGION_GROUPS  # noqa: E402

TARGETS = (
    REPO_ROOT / "static" / "index.html",
    REPO_ROOT / "docs" / "simulator.html",
)

# Matches the whole <select id="cfg-region"> ... </select> block.
SELECT_RE = re.compile(
    r'(?P<open><select\s+id="cfg-region"[^>]*>)(?P<body>.*?)(?P<close></select>)',
    re.DOTALL,
)


def build_options(indent: str) -> str:
    """Render <optgroup>-grouped <option> elements."""
    lines: list[str] = []
    for group in BQ_REGION_GROUPS:
        members = [r for r in BQ_REGIONS if r[2] == group]
        if not members:
            continue
        lines.append(f'{indent}<optgroup label="{group}">')
        for value, label, _group, org_views in members:
            # Regions without org-scoped INFORMATION_SCHEMA are listed but
            # flagged, so the 404 from the error taxonomy is anticipated.
            suffix = "" if org_views else " \u26a0"
            title = (
                ""
                if org_views
                else ' title="This region does not expose organization-scoped '
                'INFORMATION_SCHEMA views."'
            )
            lines.append(
                f'{indent}    <option value="{value}"{title}>{label}{suffix}</option>'
            )
        lines.append(f"{indent}</optgroup>")
    return "\n".join(lines)


def rewrite(path: Path, check_only: bool) -> bool:
    """Returns True if the file is (or was made) up to date."""
    original = path.read_text(encoding="utf-8")
    match = SELECT_RE.search(original)
    if not match:
        print(f"[ERROR] No <select id=\"cfg-region\"> found in {path}", file=sys.stderr)
        return False

    # Preserve the indentation style already used inside the block.
    body = match.group("body")
    indent_match = re.search(r"\n(\s+)<optgroup", body) or re.search(r"\n(\s+)<option", body)
    indent = indent_match.group(1) if indent_match else "    "

    new_body = "\n" + build_options(indent) + "\n" + indent[:-4]
    updated = (
        original[: match.start("body")] + new_body + original[match.end("body") :]
    )

    if updated == original:
        print(f"[OK] {path.relative_to(REPO_ROOT)} already up to date")
        return True

    if check_only:
        print(
            f"[DRIFT] {path.relative_to(REPO_ROOT)} is out of sync with BQ_REGIONS",
            file=sys.stderr,
        )
        return False

    path.write_text(updated, encoding="utf-8")
    print(f"[WRITE] {path.relative_to(REPO_ROOT)} regenerated")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="verify only; exit non-zero if the markup has drifted",
    )
    args = parser.parse_args()

    ok = True
    for target in TARGETS:
        if not target.exists():
            print(f"[SKIP] {target} does not exist")
            continue
        ok = rewrite(target, args.check) and ok
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
