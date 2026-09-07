"""
Backtest the prediction algorithm against actual outcomes.

Predicts a target year from the years before it using the same engine code
the site uses (app.odds_engine.lookup_first_choice_history +
predict_first_choice), then scores against the target year's recorded odds.

Usage:
    python backtest.py --target 2025
    python backtest.py --target 2026 --n 2000 --seed 7
    python backtest.py --target 2025 --method logit     # legacy trend forecast, for comparison
    python backtest.py --target 2025 --output results/my_run.txt
"""

import argparse
import math
import random
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

DB_DIR = Path(__file__).parent / "odds_databases"

ZONES = ["Core", "Colchuck", "Snow", "Eightmile", "Stuart"]
DATA_YEARS = [2022, 2023, 2024, 2025, 2026]
LOGIT_WINDOW = 3   # only used by --method logit


# ---------------------------------------------------------------------------
# Legacy predictor (the logit-linear trend the frontend used before the fix).
# Kept so the improvement can be re-measured; not used by the site.
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

def forecast_prob_logit(series, target_year, window=LOGIT_WINDOW):
    """Logit-linear extrapolation over the most recent `window` points."""
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


def print_calibration(pairs, label, key):
    """Bucket `pairs` by key(pred, actual, extra) and print pred vs actual."""
    buckets = [(0, 0.05), (0.05, 0.10), (0.10, 0.20), (0.20, 0.40), (0.40, 1.01)]
    print(f"\n  {label}")
    print(f"  {'Pred range':<12} {'Count':>6} {'Avg pred':>10} {'Avg actual':>11} {'Bias':>8}")
    print("  " + "-" * 52)
    for lo, hi in buckets:
        bucket = [x for x in pairs if lo <= key(x) < hi]
        if not bucket:
            continue
        avg_p = sum(x[0] for x in bucket) / len(bucket)
        avg_a = sum(x[1] for x in bucket) / len(bucket)
        hi_label = "100%" if hi > 1 else f"{hi*100:.0f}%"
        rng = f"{lo*100:.0f}%–{hi_label}"
        bias = (avg_p - avg_a) * 100
        print(f"  {rng:<12} {len(bucket):>6} {avg_p*100:>9.1f}% {avg_a*100:>10.1f}% {bias:>+7.1f}pp")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Backtest permit lottery prediction accuracy")
    parser.add_argument("--target", type=int, required=True, choices=[2025, 2026])
    parser.add_argument("--n", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--method", choices=["smoothed", "logit"], default="smoothed",
        help="smoothed = the site's predictor (default); logit = legacy trend forecast",
    )
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
    prior_years = [y for y in DATA_YEARS if y < target]
    db_dir = str(DB_DIR)

    output_path = args.output or f"backtest_{target}_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    output_file = open(output_path, "w", encoding="utf-8")
    sys.stdout = _Tee(sys.__stdout__, output_file)

    print(f"\nBacktesting predictions for {target}")
    print(f"Prior years used for prediction: {prior_years}")
    print(f"Method: {args.method}"
          + (f"  (kernel {eng.SMOOTH_WEIGHTS}, all prior years averaged)" if args.method == "smoothed"
             else f"  (logit-linear trend, window={LOGIT_WINDOW})"))
    print(f"Sampling {args.n} random choices (seed={args.seed})...\n")

    choices = sample_choices(args.n, target)

    pairs = []          # (predicted, actual, actual_obs)
    skipped_no_actual = 0
    skipped_no_history = 0

    for zone, month, day, group_size in choices:
        permit_date = dt.date(target, month, day)

        actual_rec = eng.lookup_first_choice_history(
            zone, group_size, permit_date, [target], db_dir, neighbour_weeks=0
        )[target]
        actual = actual_rec["odds"]
        if not actual:
            skipped_no_actual += 1
            if args.verbose:
                print(f"  [skip: no actual] {zone} {month:02d}-{day:02d} gs={group_size}")
            continue

        history = eng.lookup_first_choice_history(
            zone, group_size, permit_date, prior_years, db_dir
        )
        available = [y for y in prior_years if history[y]["odds"] is not None]
        if len(available) < 2:
            skipped_no_history += 1
            if args.verbose:
                missing = [y for y in prior_years if y not in available]
                print(f"  [skip: history]   {zone} {month:02d}-{day:02d} gs={group_size} "
                      f"| found={available} missing={missing}")
            continue

        if args.method == "logit":
            predicted = forecast_prob_logit([(y, history[y]["odds"]) for y in available], target)
        else:
            predicted = eng.predict_first_choice(history)

        if predicted is None:
            skipped_no_history += 1
            if args.verbose:
                print(f"  [skip: no pred]   {zone} {month:02d}-{day:02d} gs={group_size}")
            continue

        pairs.append((predicted, actual, actual_rec["obs"]))

    print(f"  Evaluated : {len(pairs)}")
    print(f"  Skipped (no actual data)      : {skipped_no_actual}")
    print(f"  Skipped (insufficient history): {skipped_no_history}\n")

    if not pairs:
        print("No usable samples — cannot compute metrics.")
        return

    m = compute_metrics([(p, a) for p, a, _ in pairs])

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

    print_calibration(pairs, "CALIBRATION (all samples)", key=lambda x: x[0])

    # The "actual" is itself a small-sample average. Show how the model does
    # on targets backed by at least a few applications, where noise is lower.
    solid = [x for x in pairs if x[2] is not None and x[2] >= 3]
    if solid:
        print_calibration(solid, f"CALIBRATION (actual backed by obs >= 3; n={len(solid)})", key=lambda x: x[0])

    print()

    sys.stdout = sys.__stdout__
    output_file.close()
    print(f"Results saved to: {output_path}")


if __name__ == "__main__":
    main()
