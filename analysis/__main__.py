"""
Command line:

    python -m analysis report --since 2026-01-01 [--until 2026-04-01] [--source prod|local]
    python -m analysis pull   --since 2026-01-01 [--source prod|local]

prod needs ANALYTICS_URL (or --url) and ANALYTICS_TOKEN; local reads
--db (default odds_database/analytics.db or ANALYTICS_DB_PATH).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from . import DEFAULT_TZ, DataQualityError
from . import checks as checks_mod
from . import metrics as metrics_mod
from . import pull as pull_mod
from . import report as report_mod
from . import transform


def _add_source_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--since", required=True, help="ISO date, inclusive (e.g. 2026-01-01)")
    p.add_argument("--until", help="ISO date, exclusive")
    p.add_argument("--source", choices=["prod", "local"], default="prod")
    p.add_argument("--url", help="site base URL for prod (default: ANALYTICS_URL)")
    p.add_argument("--db", type=Path, help="analytics.db path for local (default: ANALYTICS_DB_PATH or odds_database/analytics.db)")
    p.add_argument("--raw-dir", type=Path, default=pull_mod.DEFAULT_RAW_DIR, help="raw JSONL cache directory")
    p.add_argument("--refresh", action="store_true", help="re-pull even if today's raw file exists")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="python -m analysis", description="Analytics pipeline for the permit odds tool")
    sub = p.add_subparsers(dest="cmd", required=True)

    pp = sub.add_parser("pull", help="fetch events and cache them as JSONL")
    _add_source_args(pp)

    rp = sub.add_parser("report", help="pull (or reuse today's pull), check, and render an HTML report")
    _add_source_args(rp)
    rp.add_argument("--out", type=Path, default=report_mod.DEFAULT_OUT_DIR, help="report directory")
    rp.add_argument("--no-pull", action="store_true", help="use the most recent cached pull for this range instead of fetching")
    rp.add_argument("--allow-failed-checks", action="store_true",
                    help="render the report even when a data-quality check fails (it will carry a warning banner)")
    rp.add_argument("--tz", default=DEFAULT_TZ, help="time zone for day boundaries")
    return p


def _pull(args) -> Path:
    if args.no_pull if hasattr(args, "no_pull") else False:
        cached = pull_mod.find_cached(args.raw_dir, args.source, args.since, args.until)
        if not cached:
            sys.exit(f"no cached pull for {args.source} since {args.since}; run without --no-pull")
        return cached
    return pull_mod.pull(args.source, args.since, args.until, base_url=args.url, db_path=args.db,
                         raw_dir=args.raw_dir, refresh=args.refresh)


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)

    raw = _pull(args)
    meta = pull_mod.read_meta(raw)
    print(f"raw: {raw} ({meta.get('rows', '?')} rows, pulled {meta.get('pulled_at', '?')[:19]})")
    if args.cmd == "pull":
        return 0

    rows = pull_mod.read_jsonl(raw)
    tables = transform.build_tables(rows, tz=args.tz)
    print(f"kept {tables.n_kept} of {tables.n_pulled} rows; removed {tables.removed}")

    results = checks_mod.run_checks(tables, args.since, args.until)
    for r in results:
        flag = "ok  " if r.ok else ("FAIL" if r.severity == "error" else "warn")
        print(f"  [{flag}] {r.name}: {r.message}")
    try:
        checks_mod.assert_ok(results)
    except DataQualityError as e:
        if not args.allow_failed_checks:
            print(f"\n{e}\n\nNo report written. Fix the data or re-run with --allow-failed-checks.", file=sys.stderr)
            return 2
        print(f"\n{e}\n\nRendering anyway (--allow-failed-checks).", file=sys.stderr)

    metrics = metrics_mod.compute_all(tables, args.since, args.until)
    path = report_mod.render_report(tables, results, metrics, since=args.since, until=args.until,
                                    source=args.source, raw_file=raw, meta=meta, out_dir=args.out)
    t = metrics["totals"]
    print(f"visits {t['visits']}, visitors {t['visitors']}, queries {t['queries']}")
    print(f"report: {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
