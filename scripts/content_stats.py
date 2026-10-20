#!/usr/bin/env python
"""Print the odds figures quoted in content/*.md, from the current engine.

The FAQ, the lottery page and the zone intros quote medians, monthly
averages and weekday averages. Re-run this after adding a data year or
changing the predictor, and update the prose to match:

    venv/Scripts/python.exe scripts/content_stats.py

All figures are first-choice predictions for permit_year 2027 from the same
predictor the calculator and the zone pages use (predicted_by_date from
estimate_season_odds), party of 2 unless stated.
"""
from __future__ import annotations

import datetime as dt
import statistics
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.odds_engine import estimate_season_odds  # noqa: E402

ZONES = ["Eightmile", "Stuart", "Snow", "Colchuck", "Core"]
DATA_YEARS = [2022, 2023, 2024, 2025, 2026]
PERMIT_YEAR = 2027
MONTHS = [5, 6, 7, 8, 9, 10]
DAY_GROUPS = {"Mon-Thu": {0, 1, 2, 3}, "Fri-Sat": {4, 5}, "Sun": {6}}


def pct(x):
    return "  -  " if x is None else f"{100 * x:5.1f}"


def mean(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None


def median(xs):
    xs = [x for x in xs if x is not None]
    return statistics.median(xs) if xs else None


def season(zone, gs):
    return estimate_season_odds(zone, gs, PERMIT_YEAR, DATA_YEARS, str(ROOT / "odds_databases"))


def summarize(zone, gs):
    r = season(zone, gs)
    pred = {dt.date.fromisoformat(k): v for k, v in r["predicted_by_date"].items()}
    by_month = {m: mean(v for d, v in pred.items() if d.month == m) for m in MONTHS}
    by_days = {name: mean(v for d, v in pred.items() if d.weekday() in days)
               for name, days in DAY_GROUPS.items()}
    sep_weekdays = mean(v for d, v in pred.items() if d.month == 9 and d.weekday() < 4)
    late_sep = mean(v for d, v in pred.items() if d.month == 9 and d.day >= 15)
    late_oct = mean(v for d, v in pred.items() if d.month == 10 and d.day >= 18)
    summer_weekends = mean(v for d, v in pred.items()
                           if d.month in (7, 8) and d.weekday() in (4, 5))
    # Historical: season mean of each data year's comparable-date odds.
    by_year = {y: mean(v.get(y) for v in r["odds_by_date"].values()) for y in DATA_YEARS}
    return {
        "median": median(pred.values()),
        "mean": mean(pred.values()),
        "by_month": by_month,
        "by_days": by_days,
        "sep_weekdays": sep_weekdays,
        "late_sep": late_sep,
        "late_oct": late_oct,
        "summer_weekends": summer_weekends,
        "by_year": by_year,
        "no_data": sum(1 for v in pred.values() if v is None),
    }


def main():
    stats = {z: summarize(z, 2) for z in ZONES}

    print(f"First-choice predictions for {PERMIT_YEAR}, party of 2, data {DATA_YEARS[0]}-{DATA_YEARS[-1]} (percent)\n")
    print(f"{'Zone':<10} {'median':>6} {'mean':>6} {'SepMTh':>6} {'lateSep':>7} {'lateOct':>7} {'JulAugFS':>8} {'nodata':>6}")
    for z, s in stats.items():
        print(f"{z:<10} {pct(s['median']):>6} {pct(s['mean']):>6} {pct(s['sep_weekdays']):>6} "
              f"{pct(s['late_sep']):>7} {pct(s['late_oct']):>7} {pct(s['summer_weekends']):>8} {s['no_data']:>6}")

    print("\nBy month (mean of daily predictions)")
    print(f"{'Zone':<10}" + "".join(f"{dt.date(2027, m, 1):%b}".rjust(7) for m in MONTHS))
    for z, s in stats.items():
        print(f"{z:<10}" + "".join(pct(s["by_month"][m]).rjust(7) for m in MONTHS))

    print("\nBy entry weekday (season mean)")
    print(f"{'Zone':<10}" + "".join(k.rjust(9) for k in DAY_GROUPS))
    for z, s in stats.items():
        print(f"{z:<10}" + "".join(pct(s["by_days"][k]).rjust(9) for k in DAY_GROUPS))

    print("\nHistorical season mean by data year (comparable-date odds)")
    print(f"{'Zone':<10}" + "".join(str(y).rjust(7) for y in DATA_YEARS))
    for z, s in stats.items():
        print(f"{z:<10}" + "".join(pct(s["by_year"][y]).rjust(7) for y in DATA_YEARS))

    print("\nCore by group size (median / Jul-Aug mean / season mean)")
    for gs in (1, 2, 3, 4, 6, 8):
        s = stats["Core"] if gs == 2 else summarize("Core", gs)
        julaug = mean([s["by_month"][7], s["by_month"][8]])
        print(f"  party of {gs}: {pct(s['median'])} / {pct(julaug)} / {pct(s['mean'])}")


if __name__ == "__main__":
    main()
