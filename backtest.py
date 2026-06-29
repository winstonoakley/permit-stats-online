"""
Backtest the prediction algorithm against actual outcomes.

Usage:
    python backtest.py --target 2025
    python backtest.py --target 2026 --n 2000 --seed 7
    python backtest.py --target 2025 --output results/my_run.txt
"""

import argparse
import math
import random
import sqlite3
import datetime as dt
from pathlib import Path
import sys


class _Tee:
    """Write to multiple streams at once (e.g. stdout + a file)."""
    def __init__(self, *streams):
        self._streams = streams

    def write(self, text):
        for s in self._streams:
            s.write(text)

    def flush(self):
        for s in self._streams:
            s.flush()

sys.path.insert(0, str(Path(__file__).parent))
import app.odds_engine as eng
from app.odds_engine import find_comp_date

DB_DIR = Path(__file__).parent / "odds_databases"

ZONES = ["Core", "Colchuck", "Snow", "Eightmile", "Stuart"]
COREZONE_IDS = {2022: 4, 2023: 7, 2024: 1, 2025: 4, 2026: 2}
PREDICT_WINDOW = 3


# ---------------------------------------------------------------------------
# Prediction logic (mirrors index.html forecastProbLogit)
# ---------------------------------------------------------------------------

def _clamp(x, lo, hi):
    return max(lo, min(hi, x))

def _logit(p):
    p = _clamp(p, 1e-6, 1 - 1e-6)
    return math.log(p / (1 - p))

def _inv_logit(x):
    ex = math.exp(x)
    return ex / (1 + ex)

def _fit_line(points):
    n = len(points)
    mt = sum(t for t, _ in points) / n
    my = sum(y for _, y in points) / n
    num = sum((t - mt) * (y - my) for t, y in points)
    den = sum((t - mt) ** 2 for t, _ in points)
    b = 0.0 if den == 0 else num / den
    return my - b * mt, b

def forecast_prob(series, target_year, window=PREDICT_WINDOW):
    """Logit-linear extrapolation matching the frontend algorithm."""
    clean = sorted(
        [(y, p) for y, p in series if math.isfinite(y) and math.isfinite(p)],
        key=lambda x: x[0],
    )
    if not clean:
        return None
    recent = clean[-window:]
    if len(recent) == 1:
        return _clamp(recent[0][1], 0, 1)
    pts = [(t, _logit(p)) for t, p in recent]
    a, b = _fit_line(pts)
    return _clamp(_inv_logit(a + b * target_year), 0.0005, 0.9995)


# ---------------------------------------------------------------------------
# Odds lookup
# ---------------------------------------------------------------------------

def lookup_odds(zone, month, day, group_size, data_year, permit_year):
    """Return odds (0..1) for a single choice in a given data year, or None on failure."""
    db_path = DB_DIR / f"odds_{data_year}.db"
    if not db_path.exists():
        return None

    conn = sqlite3.connect(db_path)
    eng.cur = conn.cursor()
    eng.corezoneid = COREZONE_IDS[data_year]
    try:
        permit_date = dt.date(permit_year, month, day)
        comp_date = find_comp_date(permit_date, data_year)
        date_str = comp_date.strftime("%m-%d-%Y")

        eng.cur.execute("SELECT zone_id FROM zone WHERE zonename = ?", (zone,))
        row = eng.cur.fetchone()
        if not row:
            return None
        zid = row[0]

        eng.cur.execute("SELECT date_id FROM date WHERE datestr = ?", (date_str,))
        row = eng.cur.fetchone()
        if not row:
            return None
        did = row[0]

        if zid == eng.corezoneid:
            odds = eng.coreodds1(did, group_size)
        else:
            r = eng.checkexact(1, zid, did, group_size, 0, 0, 0, 0, 0, 0)
            odds = r[0][0] if r else 0.0

        return float(odds) if odds is not None else None
    except Exception:
        return None
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Sampling
# ---------------------------------------------------------------------------

def sample_choices(n, permit_year):
    start = dt.date(permit_year, 5, 15)
    end = dt.date(permit_year, 10, 31)
    total_days = (end - start).days
    choices = []
    for _ in range(n):
        zone = random.choice(ZONES)
        d = start + dt.timedelta(random.randint(0, total_days))
        group_size = random.randint(1, 8)
        choices.append((zone, d.month, d.day, group_size))
    return choices


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

def compute_metrics(pairs):
    n = len(pairs)
    if n == 0:
        return {}

    errors = [p - a for p, a in pairs]
    abs_err = [abs(e) for e in errors]
    sq_err = [e ** 2 for e in errors]

    mae = sum(abs_err) / n
    rmse = math.sqrt(sum(sq_err) / n)
    mbe = sum(errors) / n
    brier = sum(sq_err) / n

    preds = [p for p, _ in pairs]
    actuals = [a for _, a in pairs]
    mp, ma = sum(preds) / n, sum(actuals) / n
    cov = sum((p - mp) * (a - ma) for p, a in pairs) / n
    sp = math.sqrt(sum((p - mp) ** 2 for p in preds) / n)
    sa = math.sqrt(sum((a - ma) ** 2 for a in actuals) / n)
    r = (cov / (sp * sa)) if sp > 0 and sa > 0 else float("nan")

    return {
        "n": n,
        "mae_pp": mae * 100,
        "rmse_pp": rmse * 100,
        "mbe_pp": mbe * 100,
        "brier_score": brier,
        "pearson_r": r,
        "r_squared": r ** 2 if math.isfinite(r) else float("nan"),
        "within_2pp": sum(1 for e in abs_err if e <= 0.02) / n * 100,
        "within_5pp": sum(1 for e in abs_err if e <= 0.05) / n * 100,
        "within_10pp": sum(1 for e in abs_err if e <= 0.10) / n * 100,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Backtest permit lottery prediction accuracy")
    parser.add_argument("--target", type=int, required=True, choices=[2025, 2026])
    parser.add_argument("--n", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Path to save results as a .txt file (default: backtest_<target>_<timestamp>.txt)",
    )
    parser.add_argument("--verbose", action="store_true", help="Print details of skipped choices")
    args = parser.parse_args()

    random.seed(args.seed)
    target = args.target
    prior_years = [y for y in sorted(COREZONE_IDS) if y < target]

    output_path = args.output or f"backtest_{target}_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    output_file = open(output_path, "w", encoding="utf-8")
    sys.stdout = _Tee(sys.__stdout__, output_file)

    print(f"\nBacktesting predictions for {target}")
    print(f"Prior years used for prediction: {prior_years}  (window={PREDICT_WINDOW})")
    print(f"Sampling {args.n} random choices (seed={args.seed})...\n")

    choices = sample_choices(args.n, target)

    pairs = []
    skipped_no_actual = 0
    skipped_no_history = 0

    for zone, month, day, group_size in choices:
        actual = lookup_odds(zone, month, day, group_size, target, target)
        if not actual:
            skipped_no_actual += 1
            if args.verbose:
                print(f"  [skip: no actual] {zone} {month:02d}-{day:02d} gs={group_size}")
            continue

        series = [
            (yr, odds)
            for yr in prior_years
            if (odds := lookup_odds(zone, month, day, group_size, yr, target)) is not None
        ]

        if len(series) < 2:
            skipped_no_history += 1
            if args.verbose:
                found = [yr for yr in prior_years
                         if lookup_odds(zone, month, day, group_size, yr, target) is not None]
                missing = [yr for yr in prior_years if yr not in found]
                print(f"  [skip: history]   {zone} {month:02d}-{day:02d} gs={group_size} "
                      f"| found={found} missing={missing}")
            continue

        predicted = forecast_prob(series, target)
        if predicted is None:
            skipped_no_history += 1
            if args.verbose:
                print(f"  [skip: no pred]   {zone} {month:02d}-{day:02d} gs={group_size} series={series}")
            continue

        pairs.append((predicted, actual))

    print(f"  Evaluated : {len(pairs)}")
    print(f"  Skipped (no actual data)      : {skipped_no_actual}")
    print(f"  Skipped (insufficient history): {skipped_no_history}\n")

    if not pairs:
        print("No usable samples — cannot compute metrics.")
        return

    m = compute_metrics(pairs)

    print("=" * 48)
    print(f"   PREDICTION ACCURACY — {target}")
    print("=" * 48)
    print(f"  Samples evaluated       : {m['n']:>6}")
    print()
    print(f"  MAE                     : {m['mae_pp']:>6.2f} pp")
    print(f"  RMSE                    : {m['rmse_pp']:>6.2f} pp")
    print(f"  Mean Bias Error         : {m['mbe_pp']:>+6.2f} pp  {'(over-predicting)' if m['mbe_pp'] > 0 else '(under-predicting)' if m['mbe_pp'] < 0 else ''}")
    print(f"  Brier Score             : {m['brier_score']:>8.5f}")
    print()
    print(f"  Pearson r               : {m['pearson_r']:>8.4f}")
    print(f"  R²                      : {m['r_squared']:>8.4f}")
    print()
    print(f"  Within +/-2 pp          : {m['within_2pp']:>6.1f}%")
    print(f"  Within +/-5 pp          : {m['within_5pp']:>6.1f}%")
    print(f"  Within +/-10 pp         : {m['within_10pp']:>6.1f}%")
    print("=" * 48)

    # Calibration table
    buckets = [(0, 0.05), (0.05, 0.10), (0.10, 0.20), (0.20, 0.40), (0.40, 1.01)]
    print("\n  CALIBRATION")
    print(f"  {'Pred range':<12} {'Count':>6} {'Avg pred':>10} {'Avg actual':>11} {'Bias':>8}")
    print("  " + "-" * 52)
    for lo, hi in buckets:
        bucket = [(p, a) for p, a in pairs if lo <= p < hi]
        if not bucket:
            continue
        avg_p = sum(p for p, _ in bucket) / len(bucket)
        avg_a = sum(a for _, a in bucket) / len(bucket)
        hi_label = "100%" if hi > 1 else f"{hi*100:.0f}%"
        label = f"{lo*100:.0f}%–{hi_label}"
        bias = (avg_p - avg_a) * 100
        print(f"  {label:<12} {len(bucket):>6} {avg_p*100:>9.1f}% {avg_a*100:>10.1f}% {bias:>+7.1f}pp")

    print()

    sys.stdout = sys.__stdout__
    output_file.close()
    print(f"Results saved to: {output_path}")


if __name__ == "__main__":
    main()
