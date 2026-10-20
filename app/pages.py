"""
Content pages: the guides in content/*.md and the generated zone pages.

Each page is rendered once per process from Markdown and cached. Titles and
meta descriptions come from the table in content/seo-meta.md, so that file
stays the single place to edit them. HTML comments in the Markdown are
dropped before rendering: the drafts park text there (the ads and affiliate
paragraphs) that must not reach the site, not even in the page source.

Zone pages add a generated section below their intro from content/zones.md:
a calendar of predicted first-choice odds for every date of the season (the
same predicted_by_date the calculator's heatmap shows), a month-by-month
table with the past lotteries alongside, and a link into the calculator.
"""
from __future__ import annotations

import calendar
import datetime as dt
import html
import os
import re
import sys
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional

import markdown

from .odds_engine import ENGINE_LOCK, estimate_season_odds

BASE_DIR = Path(__file__).resolve().parent.parent
CONTENT_DIR = BASE_DIR / "content"

# Canonical URLs, sitemap and Open Graph tags use this origin.
SITE_URL = os.getenv("SITE_URL", "https://permitodds.com").rstrip("/")
SITE_NAME = "Permit Odds"

# Same years and target year as the calculator's heatmap.
PERMIT_YEAR = 2027
DATA_YEARS = [2022, 2023, 2024, 2025, 2026]
SEASON_MONTHS = [5, 6, 7, 8, 9, 10]
DEFAULT_GROUP_SIZE = 2

# Slug -> source file, in sitemap order. Zone pages come from sections of
# zones.md and are added by _zone_sections().
GUIDE_PAGES = [
    ("/lottery", "how-the-lottery-works.md"),
    ("/faq", "faq.md"),
    ("/methodology", "methodology.md"),
    ("/about", "about.md"),
    ("/privacy", "privacy.md"),
    ("/terms", "terms.md"),
]
ZONES_FILE = "zones.md"

# Zone slug -> engine zone name and the label used in links.
ZONES = [
    ("/zones/core", "Core", "Core Enchantment"),
    ("/zones/colchuck", "Colchuck", "Colchuck"),
    ("/zones/snow", "Snow", "Snow"),
    ("/zones/eightmile", "Eightmile", "Eightmile / Caroline"),
    ("/zones/stuart", "Stuart", "Stuart"),
]
# Only the Core's odds depend on group size (see methodology.md).
GROUP_SIZE_ZONES = {"Core"}

_HTML_COMMENT = re.compile(r"<!--.*?-->", re.S)
PLACEHOLDER = re.compile(r"\[(?:EDIT|CONFIRM|NUMBER)\b[^\]]*\]")

# ---------------------------------------------------------------- sources

def _read(name: str) -> str:
    return (CONTENT_DIR / name).read_text(encoding="utf-8")


@lru_cache(maxsize=None)
def seo_meta() -> Dict[str, Dict[str, str]]:
    """Slug -> {title, description} from the table in seo-meta.md."""
    meta = {}
    for line in _read("seo-meta.md").splitlines():
        line = line.strip()
        if not line.startswith("|"):
            continue
        cells = [c.strip() for c in line.strip("|").split("|")]
        if len(cells) < 3:
            continue
        slug = cells[0].strip("`").strip()
        if slug.startswith("/"):
            meta[slug] = {"title": cells[1], "description": cells[2]}
    return meta


@lru_cache(maxsize=None)
def _zone_sections() -> Dict[str, Dict[str, str]]:
    """Zone slug -> {heading, body} from the '## ... Zone' sections of
    zones.md. The '**Slug:**' line is metadata and the text above the first
    section is a note to the editor; neither is published."""
    parts = re.split(r"^## +(.*?Zone) *$", _HTML_COMMENT.sub("", _read(ZONES_FILE)), flags=re.M)
    out = {}
    for i in range(1, len(parts), 2):
        heading, body = parts[i].strip(), parts[i + 1]
        m = re.search(r"^\*\*Slug:\*\* *`([^`]+)`", body, flags=re.M)
        if not m:
            raise ValueError(f"No **Slug:** line under '## {heading}' in {ZONES_FILE}")
        body = body[: m.start()] + body[m.end():]
        body = re.sub(r"^\s*---\s*$", "", body, flags=re.M)
        out[m.group(1).strip()] = {"heading": heading, "body": body.strip()}
    return out


def slugs() -> List[str]:
    """Every content URL, in sitemap order (the calculator is "/")."""
    guides = [s for s, _ in GUIDE_PAGES]
    return ["/", guides[0], "/zones"] + [s for s, _, _ in ZONES] + guides[1:]


def to_html(md_text: str) -> str:
    out = markdown.markdown(_HTML_COMMENT.sub("", md_text), extensions=["tables", "toc"])
    return (out.replace("<table>", '<div class="table-scroll"><table>')
               .replace("</table>", "</table></div>"))


# ---------------------------------------------------------------- chrome

def esc(s) -> str:
    return html.escape(str(s), quote=True)


def site_header(here: str = "") -> str:
    def link(href, label):
        current = here == href or (href == "/zones" and here.startswith("/zones"))
        attr = ' aria-current="page"' if current else ""
        return f'<a href="{href}"{attr}>{label}</a>'

    return (
        '<header class="site-header">'
        '<a class="site-brand" href="/">Permit Odds</a>'
        '<nav class="site-nav" aria-label="Main">'
        + link("/", "Calculator")
        + link("/zones", "Zones")
        + link("/lottery", "How the lottery works")
        + link("/faq", "FAQ")
        + "</nav></header>"
    )


def site_footer() -> str:
    return (
        '<footer class="site-footer">'
        '<nav aria-label="Site">'
        '<a href="/about">About</a><a href="/methodology">Methodology</a>'
        '<a href="/privacy">Privacy</a><a href="/terms">Terms</a>'
        "</nav>"
        "<p>Not affiliated with the U.S. Forest Service or Recreation.gov. "
        "Odds are estimates, not guarantees.</p>"
        "</footer>"
    )


def page_shell(slug: str, body: str) -> str:
    meta = seo_meta().get(slug)
    if meta is None:
        raise KeyError(f"No row for {slug} in seo-meta.md")
    url = SITE_URL + slug
    title, desc = esc(meta["title"]), esc(meta["description"])
    return (
        "<!DOCTYPE html>\n"
        '<html lang="en">\n<head>\n'
        '<meta charset="UTF-8">\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1">\n'
        f"<title>{title}</title>\n"
        f'<meta name="description" content="{desc}">\n'
        f'<link rel="canonical" href="{esc(url)}">\n'
        f'<meta property="og:site_name" content="{SITE_NAME}">\n'
        '<meta property="og:type" content="website">\n'
        f'<meta property="og:title" content="{title}">\n'
        f'<meta property="og:description" content="{desc}">\n'
        f'<meta property="og:url" content="{esc(url)}">\n'
        '<link rel="icon" href="/static/favicon.svg" type="image/svg+xml">\n'
        '<link rel="stylesheet" href="/static/site.css">\n'
        '<script src="/static/analytics.js" defer></script>\n'
        '</head>\n<body class="content-page">\n'
        '<div class="main-wrapper">\n'
        + site_header(slug)
        + '\n<main class="card prose">\n' + body + "\n</main>\n"
        + site_footer()
        + "\n</div>\n</body>\n</html>\n"
    )


# ---------------------------------------------------------------- zone data

def _db_dir() -> str:
    return os.getenv("ODDS_DB_DIR") or str(BASE_DIR / "odds_databases")


@lru_cache(maxsize=None)
def zone_season(zone: str, group_size: int) -> dict:
    """estimate_season_odds for the zone pages; ~2-3 s uncached. Callers
    keep group_size in 1..8 (and fixed outside the Core), so the cache is
    bounded at 12 entries."""
    with ENGINE_LOCK:
        return estimate_season_odds(zone, group_size, PERMIT_YEAR, DATA_YEARS, _db_dir())


def _mean(values) -> Optional[float]:
    vals = [v for v in values if v is not None]
    return sum(vals) / len(vals) if vals else None


def _median(values) -> Optional[float]:
    vals = sorted(v for v in values if v is not None)
    if not vals:
        return None
    mid = len(vals) // 2
    return vals[mid] if len(vals) % 2 else (vals[mid - 1] + vals[mid]) / 2


def fmt_pct(p: Optional[float]) -> str:
    if p is None:
        return "&mdash;"
    if round(100 * p) < 1:
        return f"{100 * p:.1f}%"
    return f"{100 * p:.0f}%"


def odds_class(p: Optional[float]) -> str:
    # Same bands as the calculator's heatmap (hmColorClass in index.html).
    if p is None:
        return "hm-nodata"
    if p >= 0.30:
        return "hm-green"
    if p >= 0.05:
        return "hm-orange"
    return "hm-red"


def month_calendar(year: int, month: int, predicted: Dict[str, Optional[float]]) -> str:
    cells = []
    for week in calendar.Calendar(firstweekday=6).monthdatescalendar(year, month):
        for d in week:
            if d.month != month:
                cells.append('<span class="hm-day hm-out"></span>')
                continue
            iso = d.isoformat()
            if iso not in predicted:      # outside May 15 - Oct 31
                cells.append(f'<span class="hm-day hm-out">{d.day}</span>')
                continue
            p = predicted[iso]
            label = f"{d:%b} {d.day}: " + ("no data" if p is None else f"{100 * p:.1f}% predicted odds")
            cells.append(
                f'<span class="hm-day {odds_class(p)}" title="{esc(label)}">'
                f'{d.day}<span class="hm-day-odds">{fmt_pct(p)}</span></span>'
            )
    return (
        '<section class="zone-month">'
        f"<h3>{calendar.month_name[month]}</h3>"
        '<div class="hm-weekdays" aria-hidden="true">'
        + "".join(f"<span>{c}</span>" for c in "SMTWTFS")
        + '</div><div class="hm-grid">' + "".join(cells) + "</div></section>"
    )


def zone_odds_section(slug: str, zone: str, label: str, group_size: int) -> str:
    season = zone_season(zone, group_size)
    predicted = season["predicted_by_date"]
    dates = {iso: dt.date.fromisoformat(iso) for iso in predicted}
    party = "solo" if group_size == 1 else f"party of {group_size}"

    parts = [f'<h2 id="odds">Predicted {PERMIT_YEAR} odds by date, {party}</h2>']
    parts.append(
        f"<p>Each day shows the estimated chance that a {label} first choice for that entry date "
        f"wins in the {PERMIT_YEAR} lottery. The median day of the season is "
        f"<strong>{fmt_pct(_median(predicted.values()))}</strong>. "
        'The <a href="/methodology">methodology page</a> explains how these are estimated.</p>'
    )
    if zone in GROUP_SIZE_ZONES:
        links = []
        for g in range(1, 9):
            attr = ' aria-current="true"' if g == group_size else ""
            href = slug if g == DEFAULT_GROUP_SIZE else f"{slug}?size={g}"
            links.append(f'<a href="{href}#odds"{attr}>{g}</a>')
        parts.append(
            '<p class="size-picker"><span>Group size:</span>' + "".join(links) + "</p>"
        )
    parts.append(
        '<div class="hm-legend">'
        '<span><span class="hm-chip hm-green"></span>&ge; 30%</span>'
        '<span><span class="hm-chip hm-orange"></span>5&ndash;30%</span>'
        '<span><span class="hm-chip hm-red"></span>&lt; 5%</span>'
        '<span><span class="hm-chip hm-nodata"></span>No data</span></div>'
    )
    parts.append(
        '<div class="zone-calendar">'
        + "".join(month_calendar(PERMIT_YEAR, m, predicted) for m in SEASON_MONTHS)
        + "</div>"
    )

    # Month by month: the prediction next to each past lottery.
    years = season["data_years"]
    head = "".join(f"<th>{y}</th>" for y in years)
    rows = []
    for m in SEASON_MONTHS:
        in_month = [iso for iso, d in dates.items() if d.month == m]
        pred = _mean(predicted[iso] for iso in in_month)
        past = "".join(
            f"<td>{fmt_pct(_mean(season['odds_by_date'][iso].get(y) for iso in in_month))}</td>"
            for y in years
        )
        rows.append(f"<tr><th>{calendar.month_abbr[m]}</th><td><strong>{fmt_pct(pred)}</strong></td>{past}</tr>")
    parts.append(f'<h2 id="by-month">Average odds by month, {party}</h2>')
    parts.append(
        f"<p>The {PERMIT_YEAR} column averages the predictions above. The other columns average "
        "the simulated odds of the comparable dates in each past lottery.</p>"
    )
    parts.append(
        '<div class="table-scroll"><table class="month-table"><thead><tr>'
        f"<th>Month</th><th>{PERMIT_YEAR} (predicted)</th>{head}</tr></thead>"
        "<tbody>" + "".join(rows) + "</tbody></table></div>"
    )

    calc = f"/?zone={zone}" + (f"&size={group_size}" if zone in GROUP_SIZE_ZONES else "")
    parts.append(
        f'<p class="cta"><a class="cta-button" href="{esc(calc)}">Run your exact choices in the calculator</a></p>'
    )
    others = "".join(
        f'<li><a href="{s}">{esc(lbl)}</a></li>' for s, _, lbl in ZONES if s != slug
    )
    parts.append(
        "<h2>Other zones</h2><ul class=\"zone-links\">" + others + "</ul>"
        '<p>For strategy across zones, see the <a href="/faq">FAQ</a> and '
        '<a href="/lottery">how the lottery works</a>.</p>'
    )
    return "\n".join(parts)


# ---------------------------------------------------------------- pages

@lru_cache(maxsize=None)
def render_guide(slug: str) -> str:
    name = dict(GUIDE_PAGES)[slug]
    return page_shell(slug, to_html(_read(name)))


@lru_cache(maxsize=None)
def render_zone(slug: str, group_size: int = DEFAULT_GROUP_SIZE) -> str:
    zone, label = next((z, lbl) for s, z, lbl in ZONES if s == slug)
    if zone not in GROUP_SIZE_ZONES:
        group_size = DEFAULT_GROUP_SIZE
    group_size = min(max(group_size, 1), 8)
    section = _zone_sections()[slug]
    body = (
        to_html(f"# {section['heading']}\n\n{section['body']}")
        + "\n" + zone_odds_section(slug, zone, label, group_size)
    )
    return page_shell(slug, body)


@lru_cache(maxsize=None)
def render_zones_index() -> str:
    rows = []
    for slug, zone, label in ZONES:
        predicted = zone_season(zone, DEFAULT_GROUP_SIZE)["predicted_by_date"]
        by_month = {
            m: _mean(p for iso, p in predicted.items() if dt.date.fromisoformat(iso).month == m)
            for m in (7, 9)
        }
        rows.append(
            f'<tr><th><a href="{slug}">{esc(label)}</a></th>'
            f"<td>{fmt_pct(_median(predicted.values()))}</td>"
            f"<td>{fmt_pct(by_month[7])}</td><td>{fmt_pct(by_month[9])}</td></tr>"
        )
    body = (
        "<h1>Enchantments Permit Zones</h1>"
        "<p>The Enchantment Permit Area is split into five zones, and an overnight permit is for one "
        "of them. Odds differ by more than ten times between the hardest and the easiest. Each zone "
        f"page has a calendar of predicted {PERMIT_YEAR} odds for every date of the season.</p>"
        f'<h2>Predicted {PERMIT_YEAR} first-choice odds, party of {DEFAULT_GROUP_SIZE}</h2>'
        '<div class="table-scroll"><table class="month-table"><thead><tr>'
        "<th>Zone</th><th>Median day</th><th>July average</th><th>September average</th>"
        "</tr></thead><tbody>" + "".join(rows) + "</tbody></table></div>"
        '<p>Group size changes the odds only in the Core; see the '
        '<a href="/zones/core">Core page</a> for every size. '
        '<a href="/lottery">How the lottery works</a> describes each zone and the draw.</p>'
    )
    return page_shell("/zones", body)


def warm_cache() -> None:
    """Render every content page once, so the first visitor after a deploy
    does not wait ~12 s for the zone data. Run in a background thread at
    startup; it takes ENGINE_LOCK one zone at a time, so calculator requests
    queue for at most one zone's computation."""
    try:
        for slug, _ in GUIDE_PAGES:
            render_guide(slug)
        for slug, _, _ in ZONES:
            render_zone(slug)
        render_zones_index()
    except Exception as e:  # never take the site down over a cache
        print("Content page warm-up failed:", e, file=sys.stderr)


def robots_txt() -> str:
    return f"User-agent: *\nDisallow: /debug/\nDisallow: /analytics/\n\nSitemap: {SITE_URL}/sitemap.xml\n"


def sitemap_xml() -> str:
    urls = "".join(f"  <url><loc>{esc(SITE_URL + s)}</loc></url>\n" for s in slugs())
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n' + urls + "</urlset>\n"
    )
