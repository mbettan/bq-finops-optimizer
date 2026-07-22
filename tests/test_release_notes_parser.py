"""
Release Notes parser tests — verify that _parse_release_notes() correctly
handles both the BQ-style date-based format and the legacy version-based format.

The parser feeds the /api/about endpoint and the sidebar version badge, so
regressions here surface as blank "About" panels or missing version numbers.
"""

from __future__ import annotations

import textwrap
from unittest.mock import patch
from pathlib import Path

import pytest

from src.main import _parse_release_notes, __version__


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

BQ_STYLE_NOTES = textwrap.dedent("""\
    # Release Notes — BigQuery FinOps Optimizer

    ---

    ## July 21, 2026 — v1.2.2

    **Feature**
    The Fluid Scaling tab now includes an interactive Config Builder.

    **Fixed**
    Clipboard copy broken on non-HTTPS origins.

    **Change**
    Enhanced sidebar navigation hover effects.

    ---

    ## July 13, 2026 — v1.2.0

    **Security**
    Resolved a DOM-based Stored XSS in the Slots Profiler.

    **Fixed**
    MV Cost Auditor phantom counts corrected.

    ---

    ## Core Modules (Introduced in v1.0.0)

    - Storage Cost Optimizer
    - On-Demand vs. Editions Job Analyzer
""")

LEGACY_STYLE_NOTES = textwrap.dedent("""\
    # Release Notes

    ---

    ## v1.2.1 — 2026-07-17

    This release focuses on AI Doctor.

    ### 🔑 Key Highlights

    #### 1. AI Doctor UI Terminology Update
    Replaced references to Vertex AI.

    #### 2. 🏗️ Organization-Level Schema Auditing
    Engineered a dynamic discovery layer.

    ---

    ## v1.1.0 — 2026-07-08

    ### 🔑 Key Highlights

    #### 1. 🛡️ Dynamic Billing Limits
    Introduced safety cap parameter.
""")

EMPTY_NOTES = ""

MIXED_NOTES = textwrap.dedent("""\
    # Release Notes

    ---

    ## July 21, 2026

    **Feature**
    New config builder added.

    ---

    ## v1.2.1 — 2026-07-17

    ### 🔑 Key Highlights

    #### 1. AI Doctor Fix
    Fixed a parsing bug.
""")


def _parse_from_string(content: str) -> dict:
    """Helper: patch Path.read_text to return the given string."""
    with patch.object(Path, "read_text", return_value=content):
        return _parse_release_notes()


# ---------------------------------------------------------------------------
# BQ-style format tests
# ---------------------------------------------------------------------------

class TestBQStyleFormat:
    """Tests for the ## Month Day, Year format with **Tag** entries."""

    def test_parses_date_headings(self):
        result = _parse_from_string(BQ_STYLE_NOTES)
        assert len(result["releases"]) == 2

    def test_first_release_date(self):
        result = _parse_from_string(BQ_STYLE_NOTES)
        assert result["releases"][0]["release_date"] == "July 21, 2026"

    def test_second_release_date(self):
        result = _parse_from_string(BQ_STYLE_NOTES)
        assert result["releases"][1]["release_date"] == "July 13, 2026"

    def test_version_extracted_from_date_heading(self):
        """Date headings with version suffix should extract the version."""
        result = _parse_from_string(BQ_STYLE_NOTES)
        assert result["releases"][0]["version"] == "1.2.2"
        assert result["releases"][1]["version"] == "1.2.0"

    def test_highlight_count_july_21(self):
        result = _parse_from_string(BQ_STYLE_NOTES)
        assert len(result["releases"][0]["highlights"]) == 3

    def test_highlight_count_july_13(self):
        result = _parse_from_string(BQ_STYLE_NOTES)
        assert len(result["releases"][1]["highlights"]) == 2

    def test_highlights_have_tag_prefix(self):
        result = _parse_from_string(BQ_STYLE_NOTES)
        for r in result["releases"]:
            for h in r["highlights"]:
                assert h.startswith("["), f"Missing tag prefix: {h[:50]}"

    def test_feature_tag_extracted(self):
        result = _parse_from_string(BQ_STYLE_NOTES)
        tags = [h.split("]")[0][1:] for h in result["releases"][0]["highlights"]]
        assert "Feature" in tags

    def test_fixed_tag_extracted(self):
        result = _parse_from_string(BQ_STYLE_NOTES)
        tags = [h.split("]")[0][1:] for h in result["releases"][0]["highlights"]]
        assert "Fixed" in tags

    def test_security_tag_extracted(self):
        result = _parse_from_string(BQ_STYLE_NOTES)
        tags = [h.split("]")[0][1:] for h in result["releases"][1]["highlights"]]
        assert "Security" in tags

    def test_core_modules_not_parsed_as_release(self):
        """The ## Core Modules footer should not become a release entry."""
        result = _parse_from_string(BQ_STYLE_NOTES)
        dates = [r["release_date"] for r in result["releases"]]
        for d in dates:
            assert "Core" not in d

    def test_first_sentence_extraction(self):
        """Body should be truncated to the first sentence."""
        result = _parse_from_string(BQ_STYLE_NOTES)
        feature_highlight = result["releases"][0]["highlights"][0]
        assert "Config Builder" in feature_highlight
        # Should not include the full paragraph — only the first sentence
        assert len(feature_highlight) < 200


# ---------------------------------------------------------------------------
# Legacy format tests
# ---------------------------------------------------------------------------

class TestLegacyFormat:
    """Tests for the ## vX.Y.Z — YYYY-MM-DD format with #### highlights."""

    def test_parses_version_headings(self):
        result = _parse_from_string(LEGACY_STYLE_NOTES)
        assert len(result["releases"]) == 2

    def test_version_extracted(self):
        result = _parse_from_string(LEGACY_STYLE_NOTES)
        assert result["releases"][0]["version"] == "1.2.1"
        assert result["releases"][1]["version"] == "1.1.0"

    def test_date_extracted(self):
        result = _parse_from_string(LEGACY_STYLE_NOTES)
        assert result["releases"][0]["release_date"] == "2026-07-17"

    def test_highlights_extracted(self):
        result = _parse_from_string(LEGACY_STYLE_NOTES)
        assert len(result["releases"][0]["highlights"]) == 2

    def test_emoji_stripped_from_highlights(self):
        result = _parse_from_string(LEGACY_STYLE_NOTES)
        for h in result["releases"][0]["highlights"]:
            assert "🏗️" not in h

    def test_number_prefix_stripped(self):
        result = _parse_from_string(LEGACY_STYLE_NOTES)
        for h in result["releases"][0]["highlights"]:
            assert not h[0].isdigit()


# ---------------------------------------------------------------------------
# Mixed format tests
# ---------------------------------------------------------------------------

class TestMixedFormat:
    """Tests for files containing both BQ-style and legacy headings."""

    def test_parses_both_formats(self):
        result = _parse_from_string(MIXED_NOTES)
        assert len(result["releases"]) == 2

    def test_first_is_date_style(self):
        result = _parse_from_string(MIXED_NOTES)
        assert result["releases"][0]["release_date"] == "July 21, 2026"

    def test_second_is_legacy_style(self):
        result = _parse_from_string(MIXED_NOTES)
        assert result["releases"][1]["version"] == "1.2.1"

    def test_both_have_highlights(self):
        result = _parse_from_string(MIXED_NOTES)
        for r in result["releases"]:
            assert len(r["highlights"]) > 0


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    """Edge cases and structural integrity."""

    def test_empty_file(self):
        result = _parse_from_string(EMPTY_NOTES)
        assert result["version"] == __version__
        assert result["releases"] == []

    def test_missing_file(self):
        with patch.object(Path, "read_text", side_effect=FileNotFoundError):
            result = _parse_release_notes()
        assert result["version"] == __version__
        assert result["releases"] == []

    def test_json_serializable(self):
        """The result must be JSON-serializable for the /api/about endpoint."""
        import json
        result = _parse_from_string(BQ_STYLE_NOTES)
        json.dumps(result)  # Should not raise

    def test_required_top_level_keys(self):
        result = _parse_from_string(BQ_STYLE_NOTES)
        for key in ["name", "version", "release_date", "repo_url",
                     "changelog_url", "releases"]:
            assert key in result, f"Missing key: {key}"

    def test_no_internal_keys_leaked(self):
        result = _parse_from_string(BQ_STYLE_NOTES)
        for r in result["releases"]:
            assert "_pending_tag" not in r
            assert "_has_highlights_section" not in r

    def test_top_level_version_uses_dunder(self):
        """Top-level version always uses __version__ for the sidebar badge."""
        result = _parse_from_string(BQ_STYLE_NOTES)
        assert result["version"] == __version__


# ---------------------------------------------------------------------------
# Integration: parse the actual RELEASE_NOTES.md
# ---------------------------------------------------------------------------

class TestActualReleaseNotes:
    """Smoke test against the real RELEASE_NOTES.md file."""

    def test_real_file_parses_without_error(self):
        result = _parse_release_notes()
        assert result["version"] == __version__
        assert len(result["releases"]) >= 1

    def test_real_file_has_highlights(self):
        result = _parse_release_notes()
        assert len(result["releases"][0]["highlights"]) > 0

    def test_real_version_matches_dunder(self):
        result = _parse_release_notes()
        assert result["version"] == __version__
