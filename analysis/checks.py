"""
Data-quality checks. Each returns a CheckResult; `severity == "error"`
failures abort the report unless the caller explicitly allows them.

Errors (the report would be wrong):
    - a row with a schema version newer than this code understands
    - unparseable, missing or duplicate ids / timestamps
    - timestamps out of order in the export (the export is time-ordered;
      disorder means a broken pull or a server clock problem)
    - rows outside the requested range
    - a full day with zero page events inside the lottery window
Warnings (worth a look, report still rendered):
    - legacy v1 rows present (removed from the tables)
    - a visit whose events span more than a day, or seq_in_visit gaps
    - no country on any row (no geo header in front of the app)
    - device_type missing on more than 5% of kept rows
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import List, Optional

import pandas as pd

from . import DataQualityError, LOTTERY_WINDOW, SUPPORTED_SCHEMA_VERSION
from .transform import Tables, in_lottery_window


@dataclass
class CheckResult:
    name: str
    ok: bool
    severity: str      # "error" | "warn"
    message: str


def _naive(ts) -> pd.Timestamp:
    t = pd.Timestamp(ts)
    return t.tz_localize(None) if t.tzinfo is not None else t


def lottery_window_days(since: str, until: Optional[str], last_complete_day: pd.Timestamp) -> pd.DatetimeIndex:
    """Calendar days inside the lottery window that the requested range
    fully covers and that are already complete."""
    start = _naive(since).normalize()
    end = min(_naive(until).normalize() - pd.Timedelta(days=1) if until else last_complete_day, last_complete_day)
    days = []
    for year in range(start.year, end.year + 1):
        (m0, d0), (m1, d1) = LOTTERY_WINDOW
        w0, w1 = pd.Timestamp(year, m0, d0), pd.Timestamp(year, m1, d1)
        if w1 < start or w0 > end:
            continue
        days.extend(pd.date_range(max(w0, start), min(w1, end), freq="D"))
    return pd.DatetimeIndex(days)


def run_checks(tables: Tables, since: str, until: Optional[str] = None,
               today: Optional[dt.date] = None) -> List[CheckResult]:
    all_ev, ev = tables.all_events, tables.events
    results: List[CheckResult] = []

    def add(name, ok, severity, message):
        results.append(CheckResult(name, bool(ok), severity, message))

    # --- schema ---------------------------------------------------------
    newer = all_ev["schema_version"].notna() & (all_ev["schema_version"] > SUPPORTED_SCHEMA_VERSION)
    add("schema_version", not newer.any(), "error",
        f"{int(newer.sum())} rows have schema_version > {SUPPORTED_SCHEMA_VERSION}; update the analysis package"
        if newer.any() else f"all rows at schema_version <= {SUPPORTED_SCHEMA_VERSION}")
    missing_sv = all_ev["schema_version"].isna()
    add("schema_version_present", not missing_sv.any(), "error",
        f"{int(missing_sv.sum())} rows have no schema_version" if missing_sv.any() else "schema_version present on every row")
    legacy = tables.removed.get("legacy_schema", 0)
    add("legacy_rows", legacy == 0, "warn",
        f"{legacy} legacy (v1) rows removed; they predate visits and geography" if legacy else "no legacy rows")

    # --- ids and timestamps ---------------------------------------------
    add("rows_present", len(all_ev) > 0, "error", f"{len(all_ev)} rows pulled" if len(all_ev) else "no rows pulled")
    bad_ts = all_ev["ts"].isna()
    add("timestamps_parse", not bad_ts.any(), "error",
        f"{int(bad_ts.sum())} rows have an unparseable event_time_utc" if bad_ts.any() else "all timestamps parse")
    dup = all_ev["id"].duplicated() | all_ev["id"].isna()
    add("ids_unique", not dup.any(), "error",
        f"{int(dup.sum())} duplicate or missing ids" if dup.any() else "ids unique")
    ordered = all_ev["ts"].dropna()
    disorder = (ordered.diff().dt.total_seconds() < 0)
    add("timestamps_ordered", not disorder.any(), "error",
        f"{int(disorder.sum())} rows arrive before an earlier row (export is time-ordered; pull or clock problem)"
        if disorder.any() else "export is in time order")

    lo = pd.Timestamp(since)
    lo = lo.tz_localize("UTC") if lo.tzinfo is None else lo
    before = (all_ev["ts"] < lo).sum()
    after = 0
    if until:
        hi = pd.Timestamp(until)
        hi = hi.tz_localize("UTC") if hi.tzinfo is None else hi
        after = (all_ev["ts"] >= hi).sum()
    add("rows_in_range", before == 0 and after == 0, "error",
        f"{int(before)} rows before since and {int(after)} rows at/after until" if before or after
        else "all rows inside the requested range")

    # --- lottery window coverage ----------------------------------------
    today = today or dt.date.today()
    last_complete = pd.Timestamp(today) - pd.Timedelta(days=1)
    window_days = lottery_window_days(since, until, last_complete)
    if len(window_days) == 0:
        add("lottery_window_coverage", True, "warn", "requested range contains no complete lottery-window days")
    else:
        have = set(ev["day"].dropna().unique())
        empty = [d for d in window_days if d not in have]
        add("lottery_window_coverage", not empty, "error",
            f"{len(empty)} lottery-window day(s) with zero page events: "
            + ", ".join(d.strftime("%Y-%m-%d") for d in empty[:10]) + (" ..." if len(empty) > 10 else "")
            if empty else f"every one of the {len(window_days)} covered lottery-window days has events")

    # --- visits ---------------------------------------------------------
    v = tables.visits
    long = v[v["duration_s"] > 24 * 3600] if not v.empty else v
    add("visit_duration", len(long) == 0, "warn",
        f"{len(long)} visit(s) span more than 24 h; the page's visit id may not be rotating" if len(long)
        else "no visit spans more than a day")
    gaps = 0
    withseq = ev[ev["visit_id"].notna() & ev["seq_in_visit"].notna()]
    if not withseq.empty:
        s = withseq.sort_values(["visit_id", "seq_in_visit"]).groupby("visit_id")["seq_in_visit"]
        gaps = int((s.max() - s.min() + 1 != s.size()).sum())
    add("seq_in_visit_gaps", gaps == 0, "warn",
        f"{gaps} visit(s) have gaps in seq_in_visit (events lost or dropped)" if gaps else "seq_in_visit contiguous")

    # --- enrichment coverage --------------------------------------------
    if len(ev):
        no_geo = ev["country"].isna().all()
        add("geo_present", not no_geo, "warn",
            "no row has a country: no geolocation header reaches the app" if no_geo
            else f"country present on {ev['country'].notna().mean():.0%} of rows")
        dev_null = ev["device_type"].isna().mean()
        add("device_present", dev_null <= 0.05, "warn",
            f"device_type missing on {dev_null:.0%} of kept rows" if dev_null > 0.05
            else f"device_type present on {1 - dev_null:.0%} of rows")

    return results


def failures(results: List[CheckResult]) -> List[CheckResult]:
    return [r for r in results if not r.ok and r.severity == "error"]


def assert_ok(results: List[CheckResult]) -> None:
    bad = failures(results)
    if bad:
        raise DataQualityError("data-quality checks failed:\n" + "\n".join(f"  - {r.name}: {r.message}" for r in bad))
