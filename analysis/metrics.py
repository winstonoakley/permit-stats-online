"""
Stage 3: one function per question in ANALYTICS_NOTES.md section 7.7.

Every function takes tidy tables from transform.build_tables() and returns
plain pandas objects or dicts; nothing here renders. compute_all() bundles
them for the report.
"""

from __future__ import annotations

from typing import Dict, Optional
from urllib.parse import urlsplit

import pandas as pd

from . import DEFAULT_TZ, LOTTERY_WINDOW
from .transform import Tables, in_lottery_window


def _naive(ts) -> pd.Timestamp:
    t = pd.Timestamp(ts)
    return t.tz_localize(None) if t.tzinfo is not None else t


def _range_days(since: str, until: Optional[str], tz: str) -> pd.DatetimeIndex:
    start = _naive(since).normalize()
    end = (_naive(until).normalize() - pd.Timedelta(days=1)) if until else pd.Timestamp.now(tz).tz_localize(None).normalize()
    return pd.date_range(start, max(start, end), freq="D", name="day")


def _share(s: pd.Series) -> pd.DataFrame:
    total = int(s.sum())
    return pd.DataFrame({"count": s.astype(int), "share": (s / total) if total else 0.0})


# ---------------------------------------------------------------------------
# How many people use the tool
# ---------------------------------------------------------------------------

_DAILY = ["visits", "visitors", "new_visitors", "queries", "page_views", "heatmaps"]


def _agg_visits(g) -> pd.DataFrame:
    return g.agg(visits=("visit_id", "size"), visitors=("visitor_id", "nunique"),
                 new_visitors=("is_new_visitor", "sum"), queries=("n_queries", "sum"),
                 page_views=("n_page_views", "sum"), heatmaps=("n_heatmaps", "sum")).astype(int)


def visits_per_day(visits: pd.DataFrame, since: str, until: Optional[str], tz: str = DEFAULT_TZ) -> pd.DataFrame:
    idx = _range_days(since, until, tz)
    if visits.empty:
        out = pd.DataFrame(0, index=idx, columns=_DAILY)
    else:
        out = _agg_visits(visits.groupby("day")).reindex(idx, fill_value=0)
    out["in_lottery_window"] = in_lottery_window(out.index.to_series()).values
    return out


def visits_per_week(visits: pd.DataFrame) -> pd.DataFrame:
    if visits.empty:
        return pd.DataFrame(columns=_DAILY)
    return _agg_visits(visits.groupby(pd.Grouper(key="day", freq="W-MON", label="left", closed="left")))


def visits_per_season(visits: pd.DataFrame) -> pd.DataFrame:
    if visits.empty:
        return pd.DataFrame(columns=_DAILY)
    out = _agg_visits(visits.groupby("season"))
    out.index.name = "season"
    return out


def lottery_window_share(visits: pd.DataFrame) -> Dict:
    (m0, d0), (m1, d1) = LOTTERY_WINDOW
    label = f"{m0:02d}-{d0:02d} to {m1:02d}-{d1:02d}"
    if visits.empty:
        return {"window": label, "visits_total": 0, "visits_in_window": 0, "share_visits": float("nan"),
                "queries_total": 0, "queries_in_window": 0, "share_queries": float("nan"),
                "per_season": pd.DataFrame()}
    w = visits[visits["in_lottery_window"]]
    per = visits.groupby("season").apply(
        lambda s: pd.Series({"visits": len(s), "visits_in_window": int(s["in_lottery_window"].sum()),
                             "queries": int(s["n_queries"].sum()),
                             "queries_in_window": int(s.loc[s["in_lottery_window"], "n_queries"].sum())}),
        include_groups=False)
    per["share_visits"] = per["visits_in_window"] / per["visits"]
    return {
        "window": label,
        "visits_total": len(visits), "visits_in_window": len(w),
        "share_visits": len(w) / len(visits),
        "queries_total": int(visits["n_queries"].sum()), "queries_in_window": int(w["n_queries"].sum()),
        "share_queries": (int(w["n_queries"].sum()) / int(visits["n_queries"].sum())) if visits["n_queries"].sum() else float("nan"),
        "per_season": per,
    }


# ---------------------------------------------------------------------------
# Whether people iterate
# ---------------------------------------------------------------------------

def queries_per_visit(visits: pd.DataFrame) -> Dict:
    if visits.empty:
        return {"n_visits": 0, "mean": float("nan"), "median": float("nan"), "p90": float("nan"), "max": 0,
                "share_zero": float("nan"), "share_two_plus": float("nan"), "histogram": pd.Series(dtype=int),
                "median_duration_s": float("nan")}
    q = visits["n_queries"]
    hist = q.clip(upper=10).value_counts().sort_index()
    hist.index = [("10+" if i == 10 else str(int(i))) for i in hist.index]
    return {
        "n_visits": len(q), "mean": float(q.mean()), "median": float(q.median()), "p90": float(q.quantile(0.9)),
        "max": int(q.max()), "share_zero": float((q == 0).mean()), "share_two_plus": float((q >= 2).mean()),
        "histogram": hist,
        "median_duration_s": float(visits.loc[q > 0, "duration_s"].median()) if (q > 0).any() else float("nan"),
    }


# ---------------------------------------------------------------------------
# Demand: zones, dates, group sizes
# ---------------------------------------------------------------------------

def demand(choices: pd.DataFrame) -> Dict:
    if choices.empty:
        return {"n_choices": 0, "n_queries": 0, "permit_year": None, "by_zone": pd.DataFrame(),
                "by_zone_first": pd.DataFrame(), "by_group_size": pd.DataFrame(), "by_month": pd.DataFrame(),
                "by_zone_month": pd.DataFrame(), "by_zone_date": pd.DataFrame(), "top_zone_dates": pd.DataFrame(),
                "visitors_by_zone": pd.Series(dtype=int)}
    first = choices[choices["choice_rank"] == 1]
    years = choices["permit_date"].dt.year.dropna()
    permit_year = int(years.mode().iloc[0]) if len(years) else None
    if permit_year:
        season = pd.date_range(f"{permit_year}-05-15", f"{permit_year}-10-31", freq="D")
        in_year = choices[choices["permit_date"].dt.year == permit_year]
        by_zone_date = (pd.crosstab(in_year["zone"], in_year["permit_date"])
                        .reindex(columns=season, fill_value=0))
    else:
        by_zone_date = pd.DataFrame()
    top = (choices.dropna(subset=["permit_date"]).groupby(["zone", "permit_date"]).size()
           .sort_values(ascending=False).head(20).reset_index(name="choices"))
    top["permit_date"] = top["permit_date"].dt.strftime("%Y-%m-%d")
    return {
        "n_choices": len(choices),
        "n_queries": int(choices["event_id"].nunique()),
        "permit_year": permit_year,
        "by_zone": _share(choices["zone"].value_counts()),
        "by_zone_first": _share(first["zone"].value_counts()),
        "by_group_size": _share(choices["group_size"].dropna().astype(int).value_counts().sort_index()),
        "by_month": _share(choices["month"].dropna().astype(int).value_counts().sort_index()),
        "by_zone_month": pd.crosstab(choices["zone"], choices["month"].dropna().astype(int)),
        "by_zone_date": by_zone_date,
        "top_zone_dates": top,
        "visitors_by_zone": choices.groupby("zone")["visitor_id"].nunique().sort_values(ascending=False),
        "n_choices_per_query": _share(choices.groupby("event_id")["n_choices"].first().value_counts().sort_index()),
    }


# ---------------------------------------------------------------------------
# Devices, referrers, latency, heatmap, ads
# ---------------------------------------------------------------------------

def device_split(visits: pd.DataFrame) -> Dict:
    if visits.empty:
        return {"device": pd.DataFrame(), "os": pd.DataFrame(), "browser": pd.DataFrame(), "viewport": pd.DataFrame()}
    vp = pd.cut(visits["viewport_w"].dropna(), bins=[0, 480, 768, 1024, 1440, 100000],
                labels=["<480", "480-767", "768-1023", "1024-1439", "1440+"])
    return {
        "device": _share(visits["device_type"].fillna("unknown").value_counts()),
        "os": _share(visits["os_family"].fillna("unknown").value_counts()),
        "browser": _share(visits["browser_family"].fillna("unknown").value_counts()),
        "viewport": _share(vp.value_counts().sort_index()) if len(vp) else pd.DataFrame(),
    }


def referrers(visits: pd.DataFrame) -> Dict:
    if visits.empty:
        return {"share_direct": float("nan"), "by_host": pd.DataFrame(), "by_utm_source": pd.DataFrame(),
                "by_country": pd.DataFrame()}
    host = visits["referrer"].dropna().map(lambda u: urlsplit(u).netloc or u)
    return {
        "share_direct": float(visits["referrer"].isna().mean()),
        "by_host": _share(host.value_counts()),
        "by_utm_source": _share(visits["utm_source"].dropna().value_counts()),
        "by_country": _share(visits["country"].fillna("unknown").value_counts()),
    }


def latency(events: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for etype, col, label in (("get_table", "server_ms", "query: engine time (server)"),
                              ("get_table", "client_ms", "query: round trip seen by the page"),
                              ("heatmap_view", "server_ms", "heatmap: engine time (server)")):
        s = events.loc[events["event_type"] == etype, col].dropna()
        rows.append({"metric": label, "n": len(s),
                     "p50_ms": float(s.quantile(0.5)) if len(s) else float("nan"),
                     "p95_ms": float(s.quantile(0.95)) if len(s) else float("nan"),
                     "max_ms": float(s.max()) if len(s) else float("nan")})
    return pd.DataFrame(rows).set_index("metric")


def heatmap_usage(visits: pd.DataFrame, events: pd.DataFrame) -> Dict:
    hm = events[events["event_type"] == "heatmap_view"]
    if visits.empty:
        return {"n_opens": 0, "share_visits_with_heatmap": float("nan"), "opens_per_using_visit": float("nan"),
                "share_queries_after_heatmap": float("nan"), "by_zone": pd.DataFrame(), "by_from_choice": pd.DataFrame(),
                "by_month": pd.DataFrame()}
    using = visits[visits["n_heatmaps"] > 0]
    detail = pd.DataFrame(list(hm["detail"])) if len(hm) else pd.DataFrame()
    # did a heatmap view precede each query within the same visit?
    ordered = events[events["visit_id"].notna()].sort_values(["visit_id", "ts", "file_order"])
    seen = ordered.groupby("visit_id")["event_type"].transform(lambda s: (s == "heatmap_view").cumsum())
    queries = ordered["event_type"] == "get_table"
    after = float((seen[queries] > 0).mean()) if queries.any() else float("nan")
    return {
        "n_opens": len(hm),
        "share_visits_with_heatmap": len(using) / len(visits),
        "opens_per_using_visit": float(using["n_heatmaps"].mean()) if len(using) else float("nan"),
        "share_using_visits_that_queried": float((using["n_queries"] > 0).mean()) if len(using) else float("nan"),
        "share_queries_after_heatmap": after,
        "by_zone": _share(detail["zone"].value_counts()) if "zone" in detail else pd.DataFrame(),
        "by_from_choice": _share(detail["from_choice"].dropna().astype(int).value_counts().sort_index()) if "from_choice" in detail else pd.DataFrame(),
        "by_month": _share(detail["month"].dropna().astype(int).value_counts().sort_index()) if "month" in detail else pd.DataFrame(),
    }


def ad_exposure(visits: pd.DataFrame, events: pd.DataFrame) -> Dict:
    ads = events[events["event_type"] == "ad_view"]
    if ads.empty or visits.empty:
        return {"available": False, "n_ad_views": int(len(ads))}
    exposed = visits[visits["n_ad_views"] > 0]
    detail = pd.DataFrame(list(ads["detail"]))
    return {
        "available": True,
        "n_ad_views": len(ads),
        "share_visits_exposed": len(exposed) / len(visits),
        "views_per_visit": float(visits["n_ad_views"].mean()),
        "views_per_exposed_visit": float(exposed["n_ad_views"].mean()),
        "views_per_query": (len(ads) / int(visits["n_queries"].sum())) if visits["n_queries"].sum() else float("nan"),
        "by_slot": _share(detail["slot"].fillna("unknown").value_counts()) if "slot" in detail else pd.DataFrame(),
    }


def compute_all(tables: Tables, since: str, until: Optional[str]) -> Dict:
    v, e, c = tables.visits, tables.events, tables.choices
    return {
        "per_day": visits_per_day(v, since, until, tables.tz),
        "per_week": visits_per_week(v),
        "per_season": visits_per_season(v),
        "window": lottery_window_share(v),
        "queries_per_visit": queries_per_visit(v),
        "demand": demand(c),
        "devices": device_split(v),
        "referrers": referrers(v),
        "latency": latency(e),
        "heatmap": heatmap_usage(v, e),
        "ads": ad_exposure(v, e),
        "totals": {
            "visits": len(v), "visitors": int(v["visitor_id"].nunique()) if len(v) else 0,
            "new_visitors": int(v["is_new_visitor"].sum()) if len(v) else 0,
            "queries": int(v["n_queries"].sum()) if len(v) else 0,
            "page_views": int(v["n_page_views"].sum()) if len(v) else 0,
            "errors": int((e["event_type"] == "error").sum()),
        },
    }
