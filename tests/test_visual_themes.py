"""Dual-theme structural invariants.

Deliberately NOT pixel snapshots. A screenshot suite against a data-driven
dashboard goes permanently red the first time a number changes, and a
permanently-red job gets ignored -- which is how the light theme shipped
unverified in the first place. These assert the invariants that actually
broke in v1.4.4 instead.
"""

import urllib.error
import urllib.request

import pytest

playwright = pytest.importorskip("playwright.sync_api")

APP_URL = "http://127.0.0.1:8080/"
THEMES = ("dark-theme", "light-theme")


def _server_is_up() -> bool:
    try:
        urllib.request.urlopen(APP_URL, timeout=2)
        return True
    except (urllib.error.URLError, OSError):
        return False


@pytest.fixture(scope="module")
def page():
    if not _server_is_up():
        pytest.skip("app is not running on " + APP_URL)
    with playwright.sync_playwright() as p:
        try:
            browser = p.chromium.launch(headless=True)
        except playwright.Error as exc:
            if "Executable doesn't exist" in str(exc):
                pytest.skip("Playwright browsers not installed")
            raise
        # See tests/test_contrast.py for why reduced motion is required here:
        # body transitions background-color/color, so an immediate read after a
        # theme flip samples the cross-fade mid-flight.
        ctx = browser.new_context(reduced_motion="reduce")
        pg = ctx.new_page()
        pg.goto(APP_URL, wait_until="domcontentloaded")
        yield pg
        browser.close()


def _apply(page, theme):
    page.evaluate(
        "(t) => { const r = document.documentElement;"
        "  r.classList.remove('dark-theme', 'light-theme'); r.classList.add(t); }",
        theme,
    )


def test_theme_classes_actually_exist_in_the_stylesheet():
    """Guard against testing a theme that isn't there.

    The first version of these tests set `className = 'light'` / `'dark'`.
    Neither matches anything -- the real classes are `light-theme` and
    `dark-theme` -- so both parametrisations measured the same unthemed page
    and passed while checking nothing.
    """
    css = (
        __import__("pathlib").Path(__file__).resolve().parents[1]
        / "static" / "style.css"
    ).read_text(encoding="utf-8")
    for theme in THEMES:
        assert f".{theme}" in css, f"{theme} is not defined in style.css"


def test_themes_produce_different_surfaces(page):
    """The two themes must actually differ, on <html>-level tokens."""
    seen = {}
    for theme in THEMES:
        _apply(page, theme)
        seen[theme] = page.evaluate(
            "() => getComputedStyle(document.body).backgroundColor"
        )
    assert seen["dark-theme"] != seen["light-theme"], (
        f"both themes resolved to the same background: {seen}"
    )


@pytest.mark.parametrize("theme", THEMES)
def test_theme_tokens_resolve(page, theme):
    """Every core token must resolve to a usable value in both themes.

    An unresolved custom property yields '' and silently falls back to
    inherit -- the failure mode behind the `.panel-subtitle` bug, where
    `var(--muted, #94a3b8)` produced an invalid colour and the fallback never
    fired.
    """
    _apply(page, theme)
    tokens = page.evaluate(
        """(names) => {
            const cs = getComputedStyle(document.documentElement);
            const out = {};
            names.forEach(n => out[n] = cs.getPropertyValue(n).trim());
            return out;
        }""",
        ["--bg-primary", "--text-primary", "--text-secondary",
         "--text-tertiary", "--accent-color", "--glass-bg"],
    )
    unresolved = [k for k, v in tokens.items() if not v]
    assert not unresolved, f"{theme}: unresolved tokens {unresolved}"


@pytest.mark.parametrize("theme", THEMES)
def test_theme_class_lives_on_html_not_body(page, theme):
    """Tokens are declared on <html>.

    `docs/simulator.html` previously put the class on <body>, which broke root
    `color-scheme`, root scrollbar tokens, and applyTheme()'s
    getComputedStyle(document.documentElement) read.
    """
    _apply(page, theme)
    assert page.evaluate(
        "(t) => document.documentElement.classList.contains(t)", theme
    ), f"{theme} did not apply to <html>"


def test_reduced_motion_disables_theme_transition(page):
    """The reduced-motion block must actually neutralise the cross-fade.

    This context runs with reduced motion on, so body must report no
    transition. If this regresses, the contrast suite starts sampling
    mid-transition colours and reporting phantom failures.
    """
    transition = page.evaluate("() => getComputedStyle(document.body).transition")
    assert transition in ("none", "none 0s ease 0s", "all 0s ease 0s"), (
        f"expected transitions to be disabled under reduced motion, got {transition!r}"
    )
