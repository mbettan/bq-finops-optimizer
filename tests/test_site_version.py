"""The public site, simulator and llms.txt must advertise the current version."""

import re
from pathlib import Path

from src.constants import __version__

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"


def _read(path):
    return path.read_text(encoding="utf-8")


def test_json_ld_software_version():
    assert f'"softwareVersion": "{__version__}"' in _read(DOCS / "index.html")


def test_prerendered_release_card_version():
    assert f'<span id="release-version-text">v{__version__}</span>' in _read(DOCS / "index.html")


def test_local_latest_release_tag():
    html = _read(DOCS / "index.html")
    match = re.search(r"LOCAL_LATEST_RELEASE\s*=\s*\{\s*tag_name:\s*'v([^']+)'", html)
    assert match, "LOCAL_LATEST_RELEASE not found in docs/index.html"
    assert match.group(1) == __version__


def test_llms_txt_current_version():
    assert f"Current version: **v{__version__}**" in _read(DOCS / "llms.txt")


def test_simulator_fallback_version():
    assert f"releases[0].version : '{__version__}'" in _read(DOCS / "simulator.html")


def test_release_notes_top_heading():
    match = re.search(r"^## .*? v(\d+\.\d+\.\d+)", _read(ROOT / "RELEASE_NOTES.md"), re.M)
    assert match and match.group(1) == __version__
