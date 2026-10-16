"""
Stage 4: render a dated, self-contained HTML report.

The data-quality summary (rows pulled, rows removed and why, check results,
null rates) comes first so no chart is read before its provenance. Each
question then gets a short narrative, a chart where one helps, and a table.
Charts are matplotlib PNGs embedded as base64; the file has no external
dependencies.
"""

from __future__ import annotations

import base64
import datetime as dt
import html
import io
import math
from pathlib import Path
from typing import Dict, List, Optional

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

from .checks import CheckResult, failures  # noqa: E402
from .transform import BURST_EVENTS, BURST_WINDOW, Tables  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUT_DIR = ROOT / "reports"

CSS = """
body { font: 15px/1.5 -apple-system, Segoe UI, Helvetica, Arial, sans-serif; color: #222; max-width: 1100px; margin: 2rem auto; padding: 0 1rem; }
h1 { font-size: 1.7rem; margin-bottom: .2rem; } h2 { font-size: 1.25rem; margin-top: 2.5rem; border-bottom: 1px solid #ddd; padding-bottom: .3rem; }
h3 { font-size: 1rem; margin: 1.2rem 0 .4rem; color: #444; }
.meta { color: #666; font-size: .9rem; } .narrative { max-width: 800px; }
table { border-collapse: collapse; font-size: .85rem; margin: .5rem 0 1rem; } th, td { border: 1px solid #e3e3e3; padding: .25rem .55rem; text-align: right; }
th { background: #f6f6f6; } td:first-child, th:first-child { text-align: left; }
.grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 1.5rem; align-items: start; }
img { max-width: 100%; height: auto; }
.badge { display: inline-block; padding: .05rem .5rem; border-radius: 3px; font-size: .8rem; font-weight: 600; }
.ok { background: #e6f4ea; color: #1e7b34; } .warn { background: #fff4e0; color: #8a5a00; } .fail { background: #fde8e8; color: #a12020; }
.banner { background: #fde8e8; border: 1px solid #f3b4b4; padding: .8rem 1rem; border-radius: 4px; margin: 1rem 0; }
.kpis { display: flex; flex-wrap: wrap; gap: 1rem; margin: 1rem 0; }
.kpi { background: #f7f7f7; padding: .6rem 1rem; border-radius: 4px; min-width: 120px; } .kpi b { display: block; font-size: 1.4rem; }
ul.checks { list-style: none; padding: 0; } ul.checks li { margin: .15rem 0; }
"""


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _pct(x, digits=0) -> str:
    return "n/a" if x is None or (isinstance(x, float) and math.isnan(x)) else f"{x * 100:.{digits}f}%"


def _num(x, digits=0) -> str:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "n/a"
    return f"{x:,.{digits}f}"


def _esc(s) -> str:
    return html.escape(str(s))


def _table(df, max_rows: int = 30, index: bool = True, pct_cols=("share",), int_cols=()) -> str:
    if df is None or len(df) == 0:
        return "<p class='meta'>No data.</p>"
    d = df.head(max_rows).copy()
    for c in d.columns:
        if c in pct_cols:
            d[c] = d[c].map(lambda v: _pct(v, 1))
        elif c in int_cols:
            d[c] = d[c].map(lambda v: _num(v))
        elif pd.api.types.is_float_dtype(d[c]):
            d[c] = d[c].map(lambda v: _num(v, 1))
    note = f"<p class='meta'>Showing {max_rows} of {len(df)} rows.</p>" if len(df) > max_rows else ""
    return d.to_html(index=index, escape=True, border=0, na_rep="") + note


def _fig_html(fig, alt: str) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=110, bbox_inches="tight")
    plt.close(fig)
    return f"<img alt='{_esc(alt)}' src='data:image/png;base64,{base64.b64encode(buf.getvalue()).decode()}'>"


# ---------------------------------------------------------------------------
# Charts
# ---------------------------------------------------------------------------

def chart_daily(per_day: pd.DataFrame) -> str:
    if per_day.empty or per_day[["visits", "queries"]].sum().sum() == 0:
        return ""
    fig, ax = plt.subplots(figsize=(9, 3.2))
    ax.plot(per_day.index, per_day["visits"], label="visits", color="#2b6cb0")
    ax.plot(per_day.index, per_day["queries"], label="queries", color="#dd6b20")
    w = per_day["in_lottery_window"].values
    if w.any():
        ax.fill_between(per_day.index, 0, per_day[["visits", "queries"]].max().max() * 1.05, where=w,
                        color="#f6e05e", alpha=0.25, label="lottery window")
    ax.set_ylabel("per day")
    ax.legend(loc="upper left", frameon=False)
    ax.grid(axis="y", alpha=0.3)
    fig.autofmt_xdate()
    return _fig_html(fig, "Visits and queries per day")


def chart_bar(series: pd.Series, title: str, horizontal: bool = True, top: int = 12) -> str:
    if series is None or len(series) == 0 or float(series.sum()) == 0:
        return ""
    s = series.head(top)
    fig, ax = plt.subplots(figsize=(6, 0.35 * len(s) + 1.2) if horizontal else (6, 3))
    if horizontal:
        ax.barh([str(i) for i in s.index][::-1], s.values[::-1], color="#2b6cb0")
    else:
        ax.bar([str(i) for i in s.index], s.values, color="#2b6cb0")
    ax.set_title(title, fontsize=10, loc="left")
    ax.grid(axis="x" if horizontal else "y", alpha=0.3)
    return _fig_html(fig, title)


def chart_zone_date(by_zone_date: pd.DataFrame, permit_year) -> str:
    if by_zone_date is None or by_zone_date.empty or by_zone_date.values.sum() == 0:
        return ""
    fig, ax = plt.subplots(figsize=(10, 0.5 * len(by_zone_date) + 1.4))
    im = ax.imshow(by_zone_date.values, aspect="auto", cmap="YlOrRd", interpolation="nearest")
    ax.set_yticks(range(len(by_zone_date.index)))
    ax.set_yticklabels(by_zone_date.index)
    cols = by_zone_date.columns
    firsts = [i for i, d in enumerate(cols) if d.day == 1]
    ax.set_xticks(firsts)
    ax.set_xticklabels([cols[i].strftime("%b") for i in firsts])
    ax.set_title(f"Choices per zone and entry date, permit year {permit_year}", fontsize=10, loc="left")
    fig.colorbar(im, ax=ax, fraction=0.02, pad=0.02, label="choices")
    return _fig_html(fig, "Choices per zone and entry date")


# ---------------------------------------------------------------------------
# Sections
# ---------------------------------------------------------------------------

def _kpis(items) -> str:
    return "<div class='kpis'>" + "".join(f"<div class='kpi'><b>{_esc(v)}</b>{_esc(k)}</div>" for k, v in items) + "</div>"


def quality_section(tables: Tables, checks: List[CheckResult], meta: Dict) -> str:
    removed = tables.removed
    rem_rows = "".join(f"<tr><td>{_esc(k)}</td><td>{v}</td></tr>" for k, v in removed.items())
    check_items = "".join(
        f"<li><span class='badge {'ok' if c.ok else ('fail' if c.severity == 'error' else 'warn')}'>"
        f"{'ok' if c.ok else c.severity}</span> <b>{_esc(c.name)}</b> — {_esc(c.message)}</li>"
        for c in checks)
    nr = tables.null_rates.copy()
    nr["null_share"] = nr["null_share"].map(lambda v: _pct(v, 1))
    return f"""
<h2>Data quality</h2>
{_kpis([("rows pulled", _num(tables.n_pulled)), ("rows kept", _num(tables.n_kept)),
        ("rows removed", _num(tables.n_pulled - tables.n_kept)),
        ("pulled at", (meta.get("pulled_at") or "")[:19].replace("T", " ")), ("source", meta.get("origin") or meta.get("source") or "")])}
<div class='grid'>
<div><h3>Checks</h3><ul class='checks'>{check_items}</ul></div>
<div><h3>Rows removed, by reason</h3><table><tr><th>reason</th><th>rows</th></tr>{rem_rows}</table>
<p class='meta'>legacy_schema = imported v1 rows (listed under "Legacy rows" below); test_source = /debug/log_test rows; api_source = calls without a visitor id (scripts, direct API use, server errors on non-page requests); bot = crawler or headless user agents; bot_burst = visits making {BURST_EVENTS}+ query/heatmap requests within {int(BURST_WINDOW.total_seconds())} s, whatever their user agent; rate_limited = the server's own 429 rows (the page reports a throttled visitor separately).</p></div>
<div><h3>Null rates on kept rows</h3>{_table(nr, max_rows=40, index=False)}</div>
</div>{legacy_section(tables)}"""


def legacy_section(tables: Tables) -> str:
    """Imported v1 rows, with their original timestamps. They are excluded
    from every metric (no visits, referrers or geography), but shown here so
    nothing pulled is invisible."""
    ae = tables.all_events
    leg = ae[ae["removed_reason"] == "legacy_schema"] if "removed_reason" in ae.columns else ae.iloc[0:0]
    if leg.empty:
        return ""
    by_day = (leg.assign(day=leg["day"].dt.strftime("%Y-%m-%d"))
              .groupby("day").agg(rows=("id", "size"),
                                  first_utc=("event_time_utc", "min"),
                                  last_utc=("event_time_utc", "max"),
                                  visitors=("visitor_id", "nunique")))
    zones = []
    for ch in leg["choices"]:
        zones.extend(c.get("zone") for c in ch if isinstance(c, dict))
    zone_counts = pd.Series(zones, dtype="object").value_counts().rename("choices").to_frame() if zones else None
    text = (f"{len(leg)} rows imported from the v1 <code>query_events</code> table, "
            f"from {_esc(leg['event_time_utc'].min())} to {_esc(leg['event_time_utc'].max())} (UTC, as originally recorded). "
            f"{leg['visitor_id'].nunique()} distinct v1 session ids. Excluded from the sections below.")
    return f"""
<h3>Legacy rows (schema v1)</h3>
<p class='narrative'>{text}</p>
<div class='grid'>
<div>{_table(by_day, max_rows=60)}</div>
<div>{_table(zone_counts) if zone_counts is not None else ''}</div>
</div>"""


def traffic_section(m: Dict, tables: Tables) -> str:
    t, w, pd_ = m["totals"], m["window"], m["per_day"]
    busiest = pd_["visits"].idxmax() if len(pd_) and pd_["visits"].max() > 0 else None
    text = (f"{_num(t['visits'])} visits from {_num(t['visitors'])} distinct browsers "
            f"({_num(t['new_visitors'])} seen for the first time), running {_num(t['queries'])} queries. ")
    if busiest is not None:
        text += f"The busiest day was {busiest:%Y-%m-%d} with {int(pd_.loc[busiest, 'visits'])} visits. "
    if not math.isnan(w.get("share_visits", float("nan"))):
        text += (f"{_pct(w['share_visits'])} of visits and {_pct(w['share_queries'])} of queries fell inside the "
                 f"lottery window ({w['window']}).")
    else:
        text += "No visits in range."
    season = m["per_season"]
    week = m["per_week"]
    if len(week):
        week = week.assign(week=week.index.strftime("%Y-%m-%d")).set_index("week")
    return f"""
<h2>1. How many people use the tool</h2>
<p class='narrative'>{text}</p>
{chart_daily(pd_)}
<div class='grid'>
<div><h3>Per lottery season</h3>{_table(season)}</div>
<div><h3>Per week</h3>{_table(week, max_rows=60)}</div>
<div><h3>Lottery window by season</h3>{_table(w['per_season'])}</div>
</div>"""


def iterate_section(m: Dict) -> str:
    q = m["queries_per_visit"]
    if q["n_visits"] == 0:
        text = "No visits in range."
    else:
        text = (f"Visitors ran {_num(q['mean'], 2)} queries per visit on average (median {_num(q['median'])}, "
                f"90th percentile {_num(q['p90'])}, most {q['max']}). {_pct(q['share_zero'])} of visits ran no query at all "
                f"and {_pct(q['share_two_plus'])} ran two or more, i.e. iterated. Visits with at least one query "
                f"lasted a median of {_num(q['median_duration_s'] / 60, 1)} minutes.")
    hist = q["histogram"]
    return f"""
<h2>2. Whether people iterate (queries per visit)</h2>
<p class='narrative'>{text}</p>
<div class='grid'><div>{chart_bar(hist, 'Visits by number of queries', horizontal=False)}</div>
<div><h3>Distribution</h3>{_table(hist.rename('visits').to_frame())}</div></div>"""


def demand_section(m: Dict) -> str:
    d = m["demand"]
    if d["n_choices"] == 0:
        return "<h2>3. What people ask about</h2><p class='narrative'>No queries in range.</p>"
    bz, bzf, bg, bm = d["by_zone"], d["by_zone_first"], d["by_group_size"], d["by_month"]
    top_zone = bz.index[0]
    top_first = bzf.index[0] if len(bzf) else top_zone
    top_gs = bg["count"].idxmax()
    top_month = bm["count"].idxmax()
    top_zd = d["top_zone_dates"].iloc[0] if len(d["top_zone_dates"]) else None
    text = (f"{_num(d['n_choices'])} choices were submitted across {_num(d['n_queries'])} queries. "
            f"The most requested zone is {top_zone} ({_pct(bz.loc[top_zone, 'share'])} of all choices; "
            f"{top_first} leads first choices at {_pct(bzf.loc[top_first, 'share'])}). "
            f"The most common group size is {int(top_gs)} ({_pct(bg.loc[top_gs, 'share'])}) and the most requested "
            f"month is {dt.date(2000, int(top_month), 1):%B} ({_pct(bm.loc[top_month, 'share'])}).")
    if top_zd is not None:
        n = int(top_zd["choices"])
        text += f" The single most requested zone and date is {top_zd['zone']} on {top_zd['permit_date']} ({n} choice{'s' if n != 1 else ''})."
    return f"""
<h2>3. What people ask about: zones, entry dates, group sizes</h2>
<p class='narrative'>{text}</p>
{chart_zone_date(d['by_zone_date'], d['permit_year'])}
<div class='grid'>
<div><h3>Zone (all choices)</h3>{_table(bz)}<h3>Zone (first choice only)</h3>{_table(bzf)}</div>
<div><h3>Group size</h3>{_table(bg)}<h3>Choices per query</h3>{_table(d['n_choices_per_query'])}</div>
<div><h3>Month</h3>{_table(bm)}<h3>Distinct browsers per zone</h3>{_table(d['visitors_by_zone'].rename('browsers').to_frame())}</div>
<div><h3>Top zone and date</h3>{_table(d['top_zone_dates'], index=False)}</div>
<div><h3>Zone by month</h3>{_table(d['by_zone_month'])}</div>
</div>"""


def devices_section(m: Dict) -> str:
    dv = m["devices"]["device"]
    if dv.empty:
        return "<h2>4. Mobile vs desktop</h2><p class='narrative'>No visits in range.</p>"
    parts = [f"{_pct(dv.loc[k, 'share'])} {k}" for k in ("mobile", "desktop", "tablet") if k in dv.index]
    text = "Visits by device: " + ", ".join(parts) + "."
    return f"""
<h2>4. Mobile vs desktop</h2>
<p class='narrative'>{text}</p>
<div class='grid'>
<div>{chart_bar(dv['count'], 'Visits by device')}</div>
<div><h3>Device</h3>{_table(dv)}<h3>Viewport width</h3>{_table(m['devices']['viewport'])}</div>
<div><h3>OS</h3>{_table(m['devices']['os'])}<h3>Browser</h3>{_table(m['devices']['browser'])}</div>
</div>"""


def referrers_section(m: Dict) -> str:
    r = m["referrers"]
    if r["by_host"] is None or (r["by_host"].empty and math.isnan(r["share_direct"])):
        return "<h2>5. Where visitors come from</h2><p class='narrative'>No visits in range.</p>"
    text = f"{_pct(r['share_direct'])} of visits arrived with no referrer (typed, bookmarked, or from an app that strips it). "
    if len(r["by_host"]):
        text += "Top referring sites: " + ", ".join(f"{h} ({int(c)})" for h, c in r["by_host"]["count"].head(5).items()) + ". "
    if len(r["by_country"]) and not (r["by_country"].index == "unknown").all():
        text += "Top countries: " + ", ".join(f"{c} ({_pct(s)})" for c, s in r["by_country"]["share"].head(5).items()) + "."
    else:
        text += "Country is not available (no geolocation header in front of the app)."
    return f"""
<h2>5. Where visitors come from</h2>
<p class='narrative'>{text}</p>
<div class='grid'>
<div>{chart_bar(r['by_host']['count'], 'Visits by referring site') if len(r['by_host']) else ''}</div>
<div><h3>Referrer</h3>{_table(r['by_host'])}<h3>UTM source</h3>{_table(r['by_utm_source'])}</div>
<div><h3>Country</h3>{_table(r['by_country'])}</div>
</div>"""


def latency_section(m: Dict) -> str:
    lat = m["latency"]
    q = lat.loc["query: engine time (server)"]
    c = lat.loc["query: round trip seen by the page"]
    h = lat.loc["heatmap: engine time (server)"]
    if q["n"] == 0:
        text = "No queries in range."
    else:
        text = (f"The engine answered a query in {_num(q['p50_ms'])} ms at the median and {_num(q['p95_ms'])} ms at the 95th percentile "
                f"(n={int(q['n'])}). Seen from the page, the round trip was {_num(c['p50_ms'])} ms median / {_num(c['p95_ms'])} ms p95 "
                f"(n={int(c['n'])}). Heatmap requests took {_num(h['p50_ms'])} ms median / {_num(h['p95_ms'])} ms p95 (n={int(h['n'])}).")
    return f"""
<h2>6. Whether the tool is slow</h2>
<p class='narrative'>{text}</p>
{_table(lat, int_cols=('n',))}"""


def heatmap_section(m: Dict) -> str:
    h = m["heatmap"]
    if math.isnan(h.get("share_visits_with_heatmap", float("nan"))):
        return "<h2>7. How the heatmap is used</h2><p class='narrative'>No visits in range.</p>"
    text = (f"{_pct(h['share_visits_with_heatmap'])} of visits opened the odds calendar, {_num(h['opens_per_using_visit'], 1)} times each on average "
            f"({h['n_opens']} opens in total; re-opening a cached calendar is not counted). "
            f"{_pct(h.get('share_using_visits_that_queried'))} of those visits went on to run a query, and "
            f"{_pct(h['share_queries_after_heatmap'])} of all queries came after a calendar view in the same visit.")
    return f"""
<h2>7. How the heatmap is used</h2>
<p class='narrative'>{text}</p>
<div class='grid'>
<div>{chart_bar(h['by_zone']['count'], 'Calendar opens by zone') if len(h['by_zone']) else ''}</div>
<div><h3>Zone</h3>{_table(h['by_zone'])}<h3>Opened from choice</h3>{_table(h['by_from_choice'])}</div>
<div><h3>Month shown first</h3>{_table(h['by_month'])}</div>
</div>"""


def ads_section(m: Dict) -> str:
    a = m["ads"]
    if not a["available"]:
        return ("<h2>8. Ad exposure</h2><p class='narrative'>No <code>ad_view</code> events in this range. "
                "This section fills in once the ads workflow starts logging them.</p>")
    text = (f"{_num(a['n_ad_views'])} ad views. {_pct(a['share_visits_exposed'])} of visits saw at least one ad; "
            f"{_num(a['views_per_visit'], 2)} views per visit overall, {_num(a['views_per_exposed_visit'], 2)} per exposed visit, "
            f"{_num(a['views_per_query'], 2)} per query.")
    return f"""
<h2>8. Ad exposure</h2>
<p class='narrative'>{text}</p>
<div class='grid'><div><h3>By slot</h3>{_table(a['by_slot'])}</div></div>"""


# ---------------------------------------------------------------------------

def render_report(
    tables: Tables,
    checks: List[CheckResult],
    metrics: Dict,
    *,
    since: str,
    until: Optional[str],
    source: str,
    raw_file: Optional[Path] = None,
    meta: Optional[Dict] = None,
    out_dir: Optional[Path] = None,
    generated: Optional[dt.datetime] = None,
) -> Path:
    meta = dict(meta or {})
    meta.setdefault("source", source)
    generated = generated or dt.datetime.now()
    out_dir = Path(out_dir or DEFAULT_OUT_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)
    bad = failures(checks)
    banner = ("" if not bad else
              "<div class='banner'><b>Data-quality checks failed.</b> This report was rendered anyway; treat every number below as suspect.<ul>"
              + "".join(f"<li>{_esc(c.name)}: {_esc(c.message)}</li>" for c in bad) + "</ul></div>")
    body = "\n".join([
        f"<h1>Permit tool usage report</h1>",
        f"<p class='meta'>Events from {_esc(since)} to {_esc(until or 'now')} · source: {_esc(source)} · "
        f"generated {generated:%Y-%m-%d %H:%M} · days counted in {_esc(tables.tz)}"
        + (f" · raw file: {_esc(raw_file.name)}" if raw_file else "") + "</p>",
        banner,
        quality_section(tables, checks, meta),
        traffic_section(metrics, tables),
        iterate_section(metrics),
        demand_section(metrics),
        devices_section(metrics),
        referrers_section(metrics),
        latency_section(metrics),
        heatmap_section(metrics),
        ads_section(metrics),
    ])
    doc = f"<!doctype html><html><head><meta charset='utf-8'><title>Permit tool usage report {generated:%Y-%m-%d}</title><style>{CSS}</style></head><body>{body}</body></html>"
    path = out_dir / f"{generated:%Y-%m-%d}_report_since_{since[:10]}.html"
    path.write_text(doc, encoding="utf-8")
    return path
