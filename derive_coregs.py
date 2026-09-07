"""
Derive the Core group-size scaling table (app.odds_engine.coregs) from the
odds databases and test it against the table currently in the code.

coregs[g] is the assumed ratio odds(g) / odds(g + 1) for Core. coreodds1()
uses it only when a date has no row for the requested group size and that
size lies outside the sizes observed on the date (below the smallest or
above the largest); interior gaps are linearly interpolated instead.

Each candidate table is the median ratio over Core dates where both sizes
are backed by at least --min-obs applications. The test hides the smallest
and largest observed size on every Core date and re-estimates it through
coreodds1() with each table, then scores against the hidden row.

Usage:
    python derive_coregs.py                       # derive from 2024-2026, test on 2022-2026
    python derive_coregs.py --derive 2022 2023 2024 2025 --test 2026   # out-of-sample check
"""

import argparse
import os
import sqlite3
import statistics
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import app.odds_engine as eng

DB_DIR = Path(__file__).parent / "odds_databases"


def load_core(year):
    """{date_id: {group_size: (avgodds, obs)}} for Core first-choice rows."""
    conn = sqlite3.connect(DB_DIR / f"odds_{year}.db")
    cz = conn.execute("SELECT zone_id FROM zone WHERE zonename = 'Core'").fetchone()[0]
    out = defaultdict(dict)
    for d, g, p, o in conn.execute(
        "SELECT dateid1, groupsize1, avgodds, obs FROM wins WHERE choicenum = 1 AND zoneid1 = ?",
        (cz,),
    ):
        out[d][int(g)] = (float(p), int(o))
    conn.close()
    return out


def derive(data, years, min_obs):
    """Median odds(g)/odds(g+1) per step, with the sample count behind it."""
    samples = defaultdict(list)
    for y in years:
        for sizes in data[y].values():
            for g in range(1, 8):
                if g in sizes and g + 1 in sizes:
                    (p1, o1), (p2, o2) = sizes[g], sizes[g + 1]
                    if o1 >= min_obs and o2 >= min_obs and p2 > 0:
                        samples[g].append(p1 / p2)
    return {g: (round(statistics.median(v), 2), len(v)) for g, v in sorted(samples.items())}


def evaluate(data, table, years, min_target_obs):
    """Leave-endpoint-out through eng.coreodds1. Returns [(size, pred, actual)]."""
    saved_table, saved_fetch = eng.coregs, eng.fetchCore1
    eng.coregs = table
    res = []
    try:
        for y in years:
            for sizes in data[y].values():
                if len(sizes) < 2:
                    continue
                for g in (min(sizes), max(sizes)):
                    p_act, o_act = sizes[g]
                    if o_act < min_target_obs:
                        continue
                    rows = [(gg, pp) for gg, (pp, _) in sizes.items() if gg != g]
                    eng.fetchCore1 = lambda _d, _g, rows=rows: rows
                    pred = eng.coreodds1(None, g)
                    if pred is not None:
                        res.append((g, pred, p_act))
    finally:
        eng.coregs, eng.fetchCore1 = saved_table, saved_fetch
    return res


def score(res):
    n = len(res)
    mae = sum(abs(p - a) for _, p, a in res) / n * 100
    bias = sum(p - a for _, p, a in res) / n * 100
    return n, mae, bias


def main():
    ap = argparse.ArgumentParser(description="Derive and test the Core group-size scaling table")
    ap.add_argument("--derive", type=int, nargs="+", default=[2024, 2025, 2026],
                    help="years to derive the table from")
    ap.add_argument("--test", type=int, nargs="+", default=[2022, 2023, 2024, 2025, 2026],
                    help="years to run the leave-endpoint-out test on")
    ap.add_argument("--min-obs", type=int, default=3,
                    help="both sizes need at least this many applications to count toward a ratio")
    args = ap.parse_args()

    years = sorted(set(args.derive) | set(args.test))
    data = {y: load_core(y) for y in years}

    per_year = {y: derive(data, [y], args.min_obs) for y in years}
    new = derive(data, args.derive, args.min_obs)
    old = eng.coregs

    print(f"\nMedian odds(g)/odds(g+1), both sizes obs >= {args.min_obs}\n")
    print(f"{'step':<6}{'in code':>9}" + "".join(f"{y:>8}" for y in years) + f"{'derived':>9}{'n':>6}")
    for g in range(1, 8):
        row = f"{g}->{g+1:<3}{old[g]:>9.2f}"
        row += "".join(f"{per_year[y][g][0]:>8.2f}" if g in per_year[y] else f"{'-':>8}" for y in years)
        row += f"{new[g][0]:>9.2f}{new[g][1]:>6}"
        print(row)

    new_table = {g: v for g, (v, _) in new.items()}
    print(f"\nDerived from {args.derive}:")
    print("coregs = {" + ", ".join(f"{g}:{v:.2f}" for g, v in new_table.items()) + "}")

    print(f"\nLeave-endpoint-out through coreodds1 on {args.test}")
    for label, min_obs in (("target obs >= 3", 3), ("all targets", 0)):
        print(f"\n  {label}")
        print(f"  {'table':<10}{'n':>6}{'MAE':>9}{'bias':>9}")
        for name, table in (("in code", old), ("derived", new_table)):
            n, mae, bias = score(evaluate(data, table, args.test, min_obs))
            print(f"  {name:<10}{n:>6}{mae:>7.2f}pp{bias:>+7.2f}pp")

    res_old = evaluate(data, old, args.test, 3)
    res_new = evaluate(data, new_table, args.test, 3)
    print("\n  By hidden group size (target obs >= 3)")
    print(f"  {'size':<6}{'n':>6}{'in-code MAE':>13}{'bias':>9}{'derived MAE':>13}{'bias':>9}")
    for g in range(1, 9):
        o = [r for r in res_old if r[0] == g]
        if not o:
            continue
        _, mo, bo = score(o)
        _, mn, bn = score([r for r in res_new if r[0] == g])
        print(f"  {g:<6}{len(o):>6}{mo:>11.2f}pp{bo:>+7.2f}pp{mn:>11.2f}pp{bn:>+7.2f}pp")
    print()


if __name__ == "__main__":
    main()
