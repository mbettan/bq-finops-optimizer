"""WCAG AA contrast assertions over COMPUTED styles, in both themes.

Why this exists
---------------
v1.4.4 shipped a Corporate Light theme claiming "strict WCAG AA contrast
compliance (>= 4.5:1)". Two defects reached review with no automated check:

  1. `.risk-badge--*` were authored as light-palette hex with `!important` and
     no `.light-theme` scope, so they applied in BOTH themes -- near-white
     chips on a near-black card.
  2. `--high` and `--medium` were the same colour (#b67901) and both failed AA
     in the theme they were designed for.

Both only appear in COMPUTED style, after the cascade resolves. Parsing the
CSS source cannot find them; this has to run in a browser.

Measurement notes
-----------------
* Backgrounds are alpha-composited up the ancestor chain. The dark badges use
  `rgba(..., 0.12)` fills, so comparing text against the declared background
  alone would report a wildly wrong ratio.
* Badge text is TEXT, so it is held to 4.5:1 (WCAG 1.4.3), not the 3:1 that
  applies to non-text graphics (1.4.11).
* Probes use the SAME markup `renderStorageRiskBadge()` emits (app.js:219-225)
  -- base class AND modifier, mounted inside a real panel -- so the composited
  background matches production. A bare div on <body> would measure a surface
  the badge never actually sits on.
"""

import urllib.error
import urllib.request

import pytest

playwright = pytest.importorskip("playwright.sync_api")

APP_URL = "http://127.0.0.1:8080/"

# Mirrors renderStorageRiskBadge() in static/app.js:219-225.
BADGE_HTML = (
    '<span class="risk-badge risk-badge--{level}">'
    '<i class="fa-solid fa-circle-info"></i>{label}</span>'
)


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
        # reduced_motion="reduce" is load-bearing, not cosmetic. `body` carries
        # `transition: background-color 0.2s, ... color 0.15s`, so reading
        # getComputedStyle immediately after flipping the theme class samples
        # the cross-fade MID-FLIGHT and returns the outgoing theme's colours --
        # which reports confident, entirely fictional contrast failures.
        # Honouring reduced-motion disables those transitions (see the
        # @media (prefers-reduced-motion: reduce) block in style.css), so
        # computed values settle synchronously and the measurement is real.
        ctx = browser.new_context(reduced_motion="reduce")
        pg = ctx.new_page()
        pg.goto(APP_URL, wait_until="domcontentloaded")
        yield pg
        browser.close()


# Walks ancestors compositing background-color until opaque, then returns the
# contrast ratio of the element's own colour against that resolved backdrop.
_CONTRAST_JS = r"""(selector) => {
    const el = document.querySelector(selector);
    if (!el) return null;

    const parse = (s) => {
        const m = (s || '').match(/[\d.]+/g);
        if (!m) return [0, 0, 0, 0];
        return [ +m[0], +m[1], +m[2], m.length > 3 ? +m[3] : 1 ];
    };
    const over = (fg, bg) => {
        const ao = fg[3] + bg[3] * (1 - fg[3]);
        if (!ao) return [0, 0, 0, 0];
        return [0, 1, 2].map(i =>
            (fg[i] * fg[3] + bg[i] * bg[3] * (1 - fg[3])) / ao
        ).concat(ao);
    };
    const lum = ([r, g, b]) => {
        const f = [r, g, b].map(v => {
            v /= 255;
            return v <= 0.03928 ? v / 12.92 : Math.pow((v + 0.055) / 1.055, 2.4);
        });
        return 0.2126 * f[0] + 0.7152 * f[1] + 0.0722 * f[2];
    };

    let bg = [0, 0, 0, 0];
    for (let n = el; n && n.nodeType === 1; n = n.parentElement) {
        bg = over(bg, parse(getComputedStyle(n).backgroundColor));
        if (bg[3] >= 1) break;
    }
    // The page canvas is the final backstop if every ancestor was transparent.
    if (bg[3] < 1) bg = over(bg, parse(getComputedStyle(document.body).backgroundColor));
    if (bg[3] < 1) bg = over(bg, [255, 255, 255, 1]);

    const fg = over(parse(getComputedStyle(el).color), bg);
    const l1 = lum(fg), l2 = lum(bg);
    return Math.round(((Math.max(l1, l2) + 0.05) / (Math.min(l1, l2) + 0.05)) * 100) / 100;
}"""


def _apply_theme(page, theme):
    """Set the real theme class.

    Tokens live on <html>, and the class names are `dark-theme`/`light-theme`.
    Setting `dark`/`light` matches no rule and would silently measure an
    unthemed page in BOTH parametrisations -- a green test that checks nothing.
    """
    assert theme in ("dark-theme", "light-theme")
    page.evaluate(
        "(t) => { const r = document.documentElement;"
        "  r.classList.remove('dark-theme', 'light-theme'); r.classList.add(t); }",
        theme,
    )


def _inject(page, html):
    """Mount markup inside a real panel so ancestor backgrounds composite the
    way they do in production. Returns a selector for the mounted node."""
    page.evaluate(
        """(html) => {
            document.querySelectorAll('#contrast-probe').forEach(n => n.remove());
            const host = document.querySelector('.glass-panel, .panel, main, .app-container')
                      || document.body;
            const wrap = document.createElement('div');
            wrap.id = 'contrast-probe';
            wrap.innerHTML = html;
            host.appendChild(wrap);
        }""",
        html,
    )
    return "#contrast-probe > *"


@pytest.mark.parametrize("theme", ["dark-theme", "light-theme"])
@pytest.mark.parametrize("level,label", [
    ("critical", "Critical Risk"),
    ("high", "High Risk"),
    ("medium", "Medium Risk"),
    ("low", "Low Risk"),
])
def test_risk_badge_contrast(page, theme, level, label):
    """Badge text must clear AA in BOTH themes.

    The original defect was unscoped rules letting the light palette bleed into
    dark mode. Parametrising over both themes is the part that catches it.
    """
    _apply_theme(page, theme)
    sel = _inject(page, BADGE_HTML.format(level=level, label=label))
    ratio = page.evaluate(_CONTRAST_JS, sel)
    assert ratio is not None, "%s badge did not mount" % level
    assert ratio >= 4.5, "%s .risk-badge--%s: %s:1 < 4.5:1" % (theme, level, ratio)


@pytest.mark.parametrize("theme", ["dark-theme", "light-theme"])
@pytest.mark.parametrize("token", ["--text-primary", "--text-secondary",
                                   "--text-tertiary", "--muted-foreground"])
def test_text_token_contrast(page, theme, token):
    """Foreground tokens must clear AA against the surface they render on.

    style.css:125-126 asserts this in a comment; four tokens did not hold to
    it. A comment is not a check.
    """
    _apply_theme(page, theme)
    sel = _inject(page, '<p style="color: var(%s)">Sample body text</p>' % token)
    ratio = page.evaluate(_CONTRAST_JS, sel)
    assert ratio is not None, "%s probe did not mount" % token
    assert ratio >= 4.5, "%s %s: %s:1 < 4.5:1" % (theme, token, ratio)


@pytest.mark.parametrize("theme", ["dark-theme", "light-theme"])
def test_panel_subtitle_resolves_to_a_real_colour(page, theme):
    """Regression guard for the bare-HSL-triplet bug.

    `.panel-subtitle` was `color: var(--muted, #94a3b8)` where `--muted` is a
    bare `H S% L%` triplet meant for `hsl(var(--muted))`. That is invalid at
    computed-value time, so `color` silently fell back to inherit and the
    declared fallback never fired. Contrast alone would NOT catch it (inherited
    text often still passes), so assert it resolves to a real colour too.
    """
    _apply_theme(page, theme)
    sel = _inject(page, '<p class="panel-subtitle">Subtitle text</p>')
    resolved = page.evaluate(
        "(s) => getComputedStyle(document.querySelector(s)).color", sel
    )
    assert resolved and resolved.startswith("rgb"), (
        ".panel-subtitle colour did not resolve to a colour: %r" % resolved
    )
    ratio = page.evaluate(_CONTRAST_JS, sel)
    assert ratio >= 4.5, "%s .panel-subtitle: %s:1 < 4.5:1" % (theme, ratio)
