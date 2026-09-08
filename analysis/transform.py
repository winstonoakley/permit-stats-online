"""
Stage 2: raw event rows -> tidy tables.

    events    one row per kept event (page traffic only), with local time
    choices   one row per zone/date/group-size choice from each get_table
    visits    one row per visit id: duration, event and query counts, device,
              country, referrer, whether it fell in the lottery window
    visitors  one row per visitor id

Rows from API callers, test rows, bots and legacy (schema v1) rows are
removed here and counted in Tables.removed. Null rates of the columns the
metrics rely on are recorded in Tables.null_rates.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List

import pandas as pd

from . import DEFAULT_TZ, KNOWN_EVENT_TYPES, LOTTERY_WINDOW, SUPPORTED_SCHEMA_VERSION

EVENT_COLUMNS = [
    "id", "schema_version", "event_type", "event_time_utc", "source", "status",
    "visitor_id", "visit_id", "seq_in_visit", "is_new_visitor",
    "page_path", "page_referrer", "utm_source", "utm_medium", "utm_campaign", "viewport_w",
    "country", "region", "geo_source",
    "device_type", "os_family", "browser_family", "browser_major", "is_bot",
    "app_version", "sim_version", "server_ms", "client_ms",
    "detail", "choices",
]
NUMERIC_COLUMNS = ["id", "schema_version", "seq_in_visit", "is_new_visitor", "viewport_w",
                   "browser_major", "is_bot", "server_ms", "client_ms"]
CHOICE_COLUMNS = ["event_id", "ts", "ts_local", "day", "visit_id", "visitor_id", "n_choices",
                  "choice_rank", "zone", "permit_date", "month", "day_of_month", "group_size",
                  "predicted", "conditioned"]
VISIT_COLUMNS = ["visit_id", "visitor_id", "start", "end", "start_local", "duration_s", "n_events",
                 "n_page_views", "n_queries", "n_heatmaps", "n_errors", "n_ad_views",
                 "is_new_visitor", "device_type", "os_family", "browser_family", "country", "region",
                 "viewport_w", "app_version", "referrer", "utm_source", "utm_medium", "utm_campaign",
                 "day", "season", "in_lottery_window"]
VISITOR_COLUMNS = ["visitor_id", "first_seen", "last_seen", "n_visits", "n_visits_in_window",
                   "n_queries", "device_type", "country"]

REMOVAL_REASONS = ("legacy_schema", "test_source", "api_source", "bot")


@dataclass
class Tables:
    events: pd.DataFrame        # kept events
    all_events: pd.DataFrame    # every pulled row, with `removed_reason`
    choices: pd.DataFrame
    visits: pd.DataFrame
    visitors: pd.DataFrame
    removed: Dict[str, int]     # reason -> rows removed
    null_rates: pd.DataFrame    # scope, column, n, null_share
    tz: str

    @property
    def n_pulled(self) -> int:
        return len(self.all_events)

    @property
    def n_kept(self) -> int:
        return len(self.events)


# ---------------------------------------------------------------------------

def in_lottery_window(days: pd.Series) -> pd.Series:
    """Boolean mask for naive local dates inside LOTTERY_WINDOW (inclusive)."""
    days = pd.to_datetime(days)
    md = days.dt.month * 100 + days.dt.day
    (m0, d0), (m1, d1) = LOTTERY_WINDOW
    return (md >= m0 * 100 + d0) & (md <= m1 * 100 + d1)


def events_frame(rows: Iterable[dict], tz: str = DEFAULT_TZ) -> pd.DataFrame:
    """All pulled rows as a frame in file order, with parsed timestamps."""
    rows = list(rows)
    df = pd.DataFrame(rows, columns=EVENT_COLUMNS) if not rows else pd.DataFrame(rows)
    for c in EVENT_COLUMNS:
        if c not in df.columns:
            df[c] = None
    df = df[EVENT_COLUMNS].copy()
    df["file_order"] = range(len(df))
    for c in NUMERIC_COLUMNS:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["ts"] = pd.to_datetime(df["event_time_utc"], utc=True, errors="coerce", format="ISO8601")
    df["ts_local"] = df["ts"].dt.tz_convert(tz)
    df["day"] = df["ts_local"].dt.tz_localize(None).dt.normalize()
    df["detail"] = df["detail"].apply(lambda d: d if isinstance(d, dict) else {})
    df["choices"] = df["choices"].apply(lambda c: c if isinstance(c, list) else [])
    return df


def filter_traffic(df: pd.DataFrame):
    """Keep page traffic from real browsers on the current schema. Returns
    (kept, removed_counts) and adds `removed_reason` to `df` in place."""
    reason = pd.Series([None] * len(df), index=df.index, dtype="object")
    legacy = df["schema_version"].notna() & (df["schema_version"] < SUPPORTED_SCHEMA_VERSION)
    reason[legacy] = "legacy_schema"
    reason[reason.isna() & (df["source"] == "test")] = "test_source"
    reason[reason.isna() & (df["source"] == "api")] = "api_source"
    reason[reason.isna() & (df["is_bot"] == 1)] = "bot"
    df["removed_reason"] = reason
    kept = df[reason.isna()].drop(columns=["removed_reason"]).copy()
    removed = {r: int((reason == r).sum()) for r in REMOVAL_REASONS}
    return kept, removed


def explode_choices(events: pd.DataFrame) -> pd.DataFrame:
    records: List[dict] = []
    queries = events[events["event_type"] == "get_table"]
    for _, e in queries.iterrows():
        for c in e["choices"]:
            if not isinstance(c, dict):
                continue
            records.append({
                "event_id": e["id"], "ts": e["ts"], "ts_local": e["ts_local"], "day": e["day"],
                "visit_id": e["visit_id"], "visitor_id": e["visitor_id"], "n_choices": len(e["choices"]),
                "choice_rank": c.get("choice_rank"), "zone": c.get("zone"), "permit_date": c.get("permit_date"),
                "month": c.get("month"), "day_of_month": c.get("day"), "group_size": c.get("group_size"),
                "predicted": c.get("predicted"), "conditioned": c.get("conditioned"),
            })
    df = pd.DataFrame(records, columns=CHOICE_COLUMNS)
    df["permit_date"] = pd.to_datetime(df["permit_date"], errors="coerce")
    for c in ("choice_rank", "month", "day_of_month", "group_size", "predicted", "conditioned", "n_choices"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def build_visits(events: pd.DataFrame) -> pd.DataFrame:
    ev = events[events["visit_id"].notna()].sort_values(["ts", "file_order"])
    if ev.empty:
        return pd.DataFrame(columns=VISIT_COLUMNS)
    g = ev.groupby("visit_id", sort=False)
    counts = (pd.crosstab(ev["visit_id"], ev["event_type"])
              .reindex(columns=list(KNOWN_EVENT_TYPES), fill_value=0)
              .rename(columns={"page_view": "n_page_views", "get_table": "n_queries", "heatmap_view": "n_heatmaps",
                               "error": "n_errors", "ad_view": "n_ad_views"}))
    visits = pd.DataFrame({
        "visitor_id": g["visitor_id"].first(),
        "start": g["ts"].min(),
        "end": g["ts"].max(),
        "start_local": g["ts_local"].min(),
        "n_events": g.size(),
        "is_new_visitor": g["is_new_visitor"].max().fillna(0).astype(int),
        "device_type": g["device_type"].first(),
        "os_family": g["os_family"].first(),
        "browser_family": g["browser_family"].first(),
        "country": g["country"].first(),
        "region": g["region"].first(),
        "viewport_w": g["viewport_w"].first(),
        "app_version": g["app_version"].first(),
        "referrer": g["page_referrer"].first(),
        "utm_source": g["utm_source"].first(),
        "utm_medium": g["utm_medium"].first(),
        "utm_campaign": g["utm_campaign"].first(),
    }).join(counts)
    visits["duration_s"] = (visits["end"] - visits["start"]).dt.total_seconds()
    visits["day"] = visits["start_local"].dt.tz_localize(None).dt.normalize()
    visits["season"] = visits["day"].dt.year
    visits["in_lottery_window"] = in_lottery_window(visits["day"])
    return visits.reset_index()[VISIT_COLUMNS]


def build_visitors(visits: pd.DataFrame) -> pd.DataFrame:
    if visits.empty:
        return pd.DataFrame(columns=VISITOR_COLUMNS)
    g = visits.sort_values("start").groupby("visitor_id", sort=False)
    visitors = pd.DataFrame({
        "first_seen": g["start"].min(),
        "last_seen": g["end"].max(),
        "n_visits": g.size(),
        "n_visits_in_window": g["in_lottery_window"].sum().astype(int),
        "n_queries": g["n_queries"].sum().astype(int),
        "device_type": g["device_type"].first(),
        "country": g["country"].first(),
    })
    return visitors.reset_index()[VISITOR_COLUMNS]


NULL_RATE_SCOPES = (
    ("all", None, ["visitor_id", "visit_id", "seq_in_visit", "device_type", "os_family", "browser_family",
                   "country", "region", "app_version"]),
    ("page_view", "page_view", ["page_referrer", "viewport_w", "page_path"]),
    ("get_table", "get_table", ["server_ms", "client_ms", "sim_version"]),
    ("heatmap_view", "heatmap_view", ["server_ms", "sim_version"]),
)


def null_rates(events: pd.DataFrame) -> pd.DataFrame:
    out = []
    for scope, etype, cols in NULL_RATE_SCOPES:
        sub = events if etype is None else events[events["event_type"] == etype]
        for c in cols:
            n = len(sub)
            out.append({"scope": scope, "column": c, "n": n,
                        "null_share": float(sub[c].isna().mean()) if n else float("nan")})
    return pd.DataFrame(out, columns=["scope", "column", "n", "null_share"])


def build_tables(rows: Iterable[dict], tz: str = DEFAULT_TZ) -> Tables:
    all_events = events_frame(rows, tz)
    kept, removed = filter_traffic(all_events)
    visits = build_visits(kept)
    return Tables(
        events=kept,
        all_events=all_events,
        choices=explode_choices(kept),
        visits=visits,
        visitors=build_visitors(visits),
        removed=removed,
        null_rates=null_rates(kept),
        tz=tz,
    )
