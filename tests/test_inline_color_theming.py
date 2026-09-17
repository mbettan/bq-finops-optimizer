"""Every inline colour must survive the light theme.

Why this exists
---------------
Most of this dashboard is rendered by injecting HTML strings, and those
strings carry inline `style="color: #..."` declarations authored against the
dark palette. Inline styles outrank every class selector, so `.light-theme`
cannot restyle them through the normal cascade. `style.css` compensates with
a rescue layer of `.light-theme [style*="color: #hex"]` attribute selectors
that remaps each known dark hex onto a theme token.

That layer is a hand-maintained allowlist, so it rots silently: a renderer
that introduces a new hex gets no remap, and the text keeps its dark-palette
colour on a white card. That is how the Active Assist empty state shipped a
`#fef08a` heading -- pale yellow on a pale-yellow tint, effectively invisible
in light mode (~1.3:1) -- alongside a `#facc15` status pill (~1.5:1) and two
`#94a3b8` "no rows" messages (~2.5:1).

Neither existing suite catches this. `tests/test_contrast.py` measures
computed styles, but only for probes it mounts itself; an empty state that
renders only after a fetch returns zero rows is never on the page when it
runs. These checks are static, so they need no browser and no server.

`docs/static/style.css` is a byte-identical mirror (see test_bundle_sync.py),
so a remap added here covers `docs/simulator.html` too.

Charts are the same failure with a harsher surface. Chart.js paints labels
onto a `<canvas>`, so there is no element for the rescue layer to match and
no possible CSS fix -- the Compute Profile scatter hardcoded `#f8fafc` axis,
tick and legend colours, which rendered as near-invisible ghosts on the white
card. Chart colours must come from `chartPalette()` instead, so the last check
here bans colour literals in chart options.
"""

from __future__ import annotations

import pathlib
import re

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent

# The shipped frontend. docs/ is a mirror and is gated separately.
SOURCES = ("static/app.js", "static/index.html")
STYLE_CSS = REPO_ROOT / "static" / "style.css"

# `color:` but not `background-color:` / `border-color:`, which the rescue
# layer handles with its own selectors.
INLINE_COLOR = re.compile(r"(?<![-\w])color:(\s*)(#[0-9a-fA-F]{6})")

RESCUED = re.compile(r'\.light-theme \[style\*="color: (#[0-9a-fA-F]{6})"\]')

# Hexes that are deliberately NOT remapped. Each needs a reason, because the
# cost of a wrong entry here is invisible text in production.
THEME_AGNOSTIC = {
    # slate-500: 4.8:1 on the light card surface, used for de-emphasised
    # placeholders ("N/A", em dashes). Clears AA where it matters.
    "#64748b",
    # Dark ink on a filled #f59e0b button (index.html, DDL consent "Proceed").
    # The button keeps its amber fill in both themes, so remapping this to a
    # light foreground would erase the label.
    "#0f172a",
}


def _read(rel: str) -> str:
    return (REPO_ROOT / rel).read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def rescued_hexes() -> set[str]:
    hexes = {m.lower() for m in RESCUED.findall(STYLE_CSS.read_text(encoding="utf-8"))}
    assert hexes, (
        "found no .light-theme [style*=\"color: #...\"] rules in style.css -- "
        "the rescue layer was removed or renamed, and this test is now "
        "asserting nothing"
    )
    return hexes


@pytest.mark.parametrize("source", SOURCES)
def test_every_inline_color_is_remapped_for_the_light_theme(source, rescued_hexes):
    """A dark-palette hex with no remap renders unreadably on a white card."""
    unmapped: dict[str, int] = {}
    for line_no, line in enumerate(_read(source).splitlines(), start=1):
        for _, value in INLINE_COLOR.findall(line):
            value = value.lower()
            if value in rescued_hexes or value in THEME_AGNOSTIC:
                continue
            unmapped.setdefault(value, line_no)

    assert not unmapped, (
        f"{source} uses inline colours the light theme cannot reach: "
        + ", ".join(f"{h} (first at line {n})" for h, n in sorted(unmapped.items()))
        + ". Either render it with a theme token (var(--warning), "
        "var(--text-secondary), ...) or add it to the matching "
        '`.light-theme [style*="color: #hex"]` group in static/style.css.'
    )


@pytest.mark.parametrize("source", SOURCES)
def test_inline_colors_use_the_spacing_the_rescue_selectors_require(source):
    """`[style*=...]` is a literal substring match, so spacing is load-bearing.

    `color:#94a3b8` and `color: #94a3b8` are the same declaration to the
    browser but only the second matches the rescue selector. Eight such
    unspaced declarations in app.js were skipping the light theme entirely.
    """
    offenders = [
        (line_no, line.strip()[:110])
        for line_no, line in enumerate(_read(source).splitlines(), start=1)
        for gap, _ in INLINE_COLOR.findall(line)
        if gap != " "
    ]
    assert not offenders, (
        f"{source} declares inline colours without the single space that "
        '`.light-theme [style*="color: #hex"]` matches on:\n'
        + "\n".join(f"  line {n}: {text}" for n, text in offenders)
    )


# A `color:` key in a JS object literal assigned a quoted constant. Excludes
# `backgroundColor:` / `borderColor:` (capital C) -- those carry data meaning
# (series identity, risk hue) and are not the theme's business.
CHART_COLOR_LITERAL = re.compile(r"""(?<![A-Za-z])color:\s*(['"])(#|rgb|hsl)""")


def test_chart_option_colors_are_not_hardcoded():
    """Canvas text has no DOM node, so a literal here is unfixable by CSS.

    Chart.js resolves `color` for ticks, axis titles and legend labels. A
    per-chart literal outranks `Chart.defaults`, so it survives every theme
    switch: the scatter chart's `#f8fafc` labels stayed near-white on a white
    card no matter what the stylesheet said. Route them through
    `chartPalette()` so `retintChart()` can follow them.
    """
    offenders = [
        (line_no, line.strip()[:110])
        for line_no, line in enumerate(_read("static/app.js").splitlines(), start=1)
        if CHART_COLOR_LITERAL.search(line)
    ]
    assert not offenders, (
        "static/app.js hardcodes colours the theme cannot reach on canvas:\n"
        + "\n".join(f"  line {n}: {text}" for n, text in offenders)
        + "\nUse chartPalette() (title/tick/grid/guide) instead, and tag "
        "non-data annotation datasets `_themeRole: 'guide'` so retintChart() "
        "picks them up on a theme switch."
    )
