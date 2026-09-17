"""
Frontend bundle sync gate.

The frontend is shipped twice: `static/` is mounted by the FastAPI app, and
`docs/static/` is published by GitHub Pages for the simulator
(`docs/simulator.html` links `./static/style.css`). Nothing in the build
process keeps the two in step, so a contributor who edits one and not the
other ships a Pages site that silently disagrees with the product.

These tests assert SHA-256 equality across both copies and name the remedy in
the failure message. This generalizes the per-feature `BUNDLES` check already
present in `tests/test_batch_candidates.py`.
"""

import hashlib
import pathlib
import subprocess
import sys

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent

SYNC_SCRIPT = "scripts/sync_docs_bundle.sh"

MIRRORED_SUFFIXES = {".js", ".css"}
PAGES_ONLY = {"calculator.js", "pricing.js"}   # intentionally have no static/ counterpart

def _discover() -> list[str]:
    return sorted(p.name for p in (REPO_ROOT / "static").iterdir()
                  if p.is_file() and p.suffix in MIRRORED_SUFFIXES)

BUNDLE_FILES = _discover()

REMEDY = f"Run `{SYNC_SCRIPT}` to mirror static/ into docs/static/."


def _sha256(path: pathlib.Path) -> str:
    """Stream the file so the 417 KB bundle is not held in memory twice."""
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


class TestBundleSync:
    """`static/<f>` and `docs/static/<f>` must be byte-identical."""

    @pytest.mark.parametrize("filename", BUNDLE_FILES)
    def test_bundles_are_byte_identical(self, filename):
        source = REPO_ROOT / "static" / filename
        mirror = REPO_ROOT / "docs" / "static" / filename

        assert source.is_file(), f"missing canonical bundle: static/{filename}"
        assert mirror.is_file(), (
            f"missing Pages bundle: docs/static/{filename}. {REMEDY}"
        )

        source_digest = _sha256(source)
        mirror_digest = _sha256(mirror)

        assert source_digest == mirror_digest, (
            f"Bundle drift: static/{filename} (sha256 {source_digest[:12]}…) "
            f"!= docs/static/{filename} (sha256 {mirror_digest[:12]}…). "
            f"{REMEDY}"
        )

    @pytest.mark.parametrize("filename", BUNDLE_FILES)
    def test_bundle_sizes_match(self, filename):
        """A cheap, readable first signal when the SHA assertion fires."""
        source = REPO_ROOT / "static" / filename
        mirror = REPO_ROOT / "docs" / "static" / filename

        assert source.stat().st_size == mirror.stat().st_size, (
            f"Size mismatch for {filename}: "
            f"static/={source.stat().st_size} bytes, "
            f"docs/static/={mirror.stat().st_size} bytes. {REMEDY}"
        )

    def test_every_static_asset_is_mirrored(self):
        """A new file under static/ must reach docs/static/ in the same commit."""
        missing = [n for n in _discover()
                   if not (REPO_ROOT / "docs" / "static" / n).is_file()]
        assert not missing, f"static/ assets absent from docs/static/: {missing}"

    def test_no_orphaned_mirror_assets(self):
        """And a file deleted from static/ must not linger in the Pages copy."""
        src = set(_discover())
        orphans = sorted(p.name for p in (REPO_ROOT / "docs" / "static").iterdir()
                         if p.is_file() and p.suffix in MIRRORED_SUFFIXES
                         and p.name not in src and p.name not in PAGES_ONLY)
        assert not orphans, f"docs/static/ has stale assets: {orphans}"

    def test_simulator_html_body_matches_index_html_body(self):
        """Ensure docs/simulator.html <body> never drifts from static/index.html <body>."""
        idx_text = (REPO_ROOT / "static" / "index.html").read_text(encoding="utf-8")
        sim_text = (REPO_ROOT / "docs" / "simulator.html").read_text(encoding="utf-8")
        idx_body = idx_text[idx_text.index("<body>"):].replace('    <script src="static/app.js"></script>\n', '')
        sim_body = sim_text[sim_text.index("<body>"):]
        assert idx_body == sim_body, (
            f"docs/simulator.html <body> has drifted from static/index.html <body>. {REMEDY}"
        )

    def test_release_notes_mirrored_to_docs(self):
        """Ensure RELEASE_NOTES.md is mirrored into docs/RELEASE_NOTES.md for same-origin simulator fetch."""
        src = REPO_ROOT / "RELEASE_NOTES.md"
        dst = REPO_ROOT / "docs" / "RELEASE_NOTES.md"
        assert dst.is_file(), f"missing docs/RELEASE_NOTES.md. {REMEDY}"
        assert _sha256(src) == _sha256(dst), f"RELEASE_NOTES.md != docs/RELEASE_NOTES.md. {REMEDY}"

    def test_simulator_csp_sha256_matches_inline_script(self):
        """Ensure the inline <script> in docs/simulator.html matches the SHA-256 hash in its CSP meta tag."""
        import re
        import base64
        sim_text = (REPO_ROOT / "docs" / "simulator.html").read_text(encoding="utf-8")
        head_text = sim_text[:sim_text.index("<body>")]
        head_no_comments = re.sub(r"<!--.*?-->", "", head_text, flags=re.S)
        scripts = re.findall(r"<script(?![^>]*src)[^>]*>(.*?)</script>", head_no_comments, flags=re.S)
        assert len(scripts) == 1, f"Expected 1 inline <script> in simulator.html <head>, found {len(scripts)}"
        expected_hash = "sha256-" + base64.b64encode(hashlib.sha256(scripts[0].encode("utf-8")).digest()).decode("ascii")
        assert expected_hash in head_text, (
            f"CSP script-src hash in docs/simulator.html does not match inline script ({expected_hash}). {REMEDY}"
        )

    def test_simulator_handles_all_frontend_api_endpoints(self):
        """Ensure every /api/... route fetched by static/app.js is handled in docs/simulator.html."""
        import re
        app_js = (REPO_ROOT / "static" / "app.js").read_text(encoding="utf-8")
        sim_html = (REPO_ROOT / "docs" / "simulator.html").read_text(encoding="utf-8")
        endpoints = sorted(set(re.findall(r"/api/[a-zA-Z0-9_/-]+", app_js)))
        missing = [ep for ep in endpoints if ep not in sim_html]
        assert not missing, f"docs/simulator.html is missing mock handlers for frontend API endpoints: {missing}"


class TestSyncScript:
    """The gate is only reasonable if the one-command remedy actually exists."""

    def test_sync_script_exists_and_is_executable(self):
        script = REPO_ROOT / SYNC_SCRIPT
        assert script.is_file(), f"{SYNC_SCRIPT} is missing"
        # Acceptance criterion 2: the script must be runnable as-is (POSIX execute bit).
        if not sys.platform.startswith("win"):
            assert script.stat().st_mode & 0o111, f"{SYNC_SCRIPT} is not executable"

    @pytest.mark.skipif(
        sys.platform.startswith("win"), reason="bash is not available on windows-latest"
    )
    def test_sync_script_is_valid_bash(self):
        result = subprocess.run(
            ["bash", "-n", str(REPO_ROOT / SYNC_SCRIPT)],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, (
            f"{SYNC_SCRIPT} is not valid bash: {result.stderr}"
        )
