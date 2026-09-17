#!/usr/bin/env bash
# Mirrors the canonical frontend bundle into the GitHub Pages copy.
#
# The frontend ships twice: `static/` is served by the FastAPI app, and
# `docs/static/` is served by GitHub Pages for the simulator. The two must stay
# byte-identical -- `tests/test_bundle_sync.py` enforces that in CI. Run this
# script after touching anything under `static/` to satisfy the gate.
#
# Usage:
#   scripts/sync_docs_bundle.sh           # copy static/ -> docs/static/
#   scripts/sync_docs_bundle.sh --check   # report drift, change nothing
set -euo pipefail

cd "$(dirname "$0")/.."

# Discover the bundle rather than hardcoding it: a new asset under static/ must
# not be able to ship un-mirrored because someone forgot to update an array.
FILES=()
while IFS= read -r f; do FILES+=("$(basename "$f")"); done \
  < <(find static -maxdepth 1 -type f \( -name '*.js' -o -name '*.css' \) | sort)

# With `set -u` an empty array would make the loop a silent no-op and exit 0
# having verified nothing.
if (( ${#FILES[@]} == 0 )); then
  echo "error: no bundle files discovered under static/" >&2
  exit 1
fi

check_only=0
if [[ "${1:-}" == "--check" ]]; then
  check_only=1
elif [[ $# -gt 0 ]]; then
  echo "usage: $0 [--check]" >&2
  exit 2
fi

# Fail loudly rather than silently creating a half-synced tree.
for f in "${FILES[@]}"; do
  if [[ ! -f "static/$f" ]]; then
    echo "error: source file static/$f does not exist" >&2
    exit 1
  fi
done

if [[ ! -d "docs/static" ]]; then
  echo "error: destination directory docs/static does not exist" >&2
  exit 1
fi

drift=0
for f in "${FILES[@]}"; do
  if [[ -f "docs/static/$f" ]] && cmp -s "static/$f" "docs/static/$f"; then
    echo "ok      static/$f == docs/static/$f"
    continue
  fi

  drift=1
  if (( check_only )); then
    echo "DRIFT   static/$f != docs/static/$f"
  else
    cp "static/$f" "docs/static/$f"
    echo "synced  static/$f -> docs/static/$f"
  fi
done

# Mirror RELEASE_NOTES.md into docs/RELEASE_NOTES.md so simulator fetches same-origin
if [[ -f "docs/RELEASE_NOTES.md" ]] && cmp -s "RELEASE_NOTES.md" "docs/RELEASE_NOTES.md"; then
  echo "ok      RELEASE_NOTES.md == docs/RELEASE_NOTES.md"
else
  drift=1
  if (( check_only )); then
    echo "DRIFT   RELEASE_NOTES.md != docs/RELEASE_NOTES.md"
  else
    cp "RELEASE_NOTES.md" "docs/RELEASE_NOTES.md"
    echo "synced  RELEASE_NOTES.md -> docs/RELEASE_NOTES.md"
  fi
fi

python3 - "$check_only" << 'EOF' || drift=1
import sys
import re
import hashlib
import base64
from pathlib import Path

check_only = int(sys.argv[1])
idx_path = Path("static/index.html")
sim_path = Path("docs/simulator.html")

idx_text = idx_path.read_text(encoding="utf-8")
sim_text = sim_path.read_text(encoding="utf-8")

sim_head = sim_text[:sim_text.index("<body>")]
idx_body = idx_text[idx_text.index("<body>"):].replace('    <script src="static/app.js"></script>\n', '')
expected_sim = sim_head + idx_body

# Compute SHA-256 hash of the inline <script> block in <head> and keep CSP script-src in sync
head_no_comments = re.sub(r'<!--.*?-->', '', sim_head, flags=re.S)
inline_scripts = re.findall(r'<script(?![^>]*src)[^>]*>(.*?)</script>', head_no_comments, flags=re.S)
if inline_scripts:
    script_hash = base64.b64encode(hashlib.sha256(inline_scripts[0].encode('utf-8')).digest()).decode('ascii')
    expected_sim = re.sub(r"'sha256-[A-Za-z0-9+/=]+'", f"'sha256-{script_hash}'", expected_sim)

if sim_text == expected_sim:
    print("ok      static/index.html <body> & CSP hash == docs/simulator.html")
    sys.exit(0)
if check_only:
    print("DRIFT   static/index.html <body> or CSP hash != docs/simulator.html")
    sys.exit(1)
sim_path.write_text(expected_sim, encoding="utf-8")
print("synced  static/index.html <body> & CSP hash -> docs/simulator.html")
sys.exit(1)
EOF

if (( check_only )) && (( drift )); then
  echo "" >&2
  echo "Bundles are out of sync. Run: scripts/sync_docs_bundle.sh" >&2
  exit 1
fi

if (( drift )); then
  echo ""
  echo "Bundle sync complete. Commit the updated docs/ files."
else
  echo ""
  echo "Bundles already in sync; nothing to do."
fi
