# Calibration notes: why high-probability predictions run hot

Sections 1 to 5 are the diagnosis, written against `heatmap-calendar` at
commit `9e3c64c` (2026-09-06) before anything was changed. Section 6
describes the fix that was then applied and its measured effect.

## 1. How one probability estimate is produced today

### 1.1 Request path

1. The page collects up to three choices (zone, date, group size) and POSTs
   them to `/estimate_odds` with `permit_year` (currently 2027) and
   `data_years` (`[2022, 2023, 2024, 2025, 2026]`).
2. `estimate_odds()` in `app/main.py` converts the payload to `Choice`
   dataclasses and calls `estimate_odds_for_choice_set()` in
   `app/odds_engine.py`. This is the only engine entry point the site uses.
   The second/third-choice machinery in the engine (`fetchCore2`,
   `fetchCore3`, `EstC2Odds`, `EstC3Odds`, and the `cnum = 2/3` branches of
   `checkexact`) is dead code on this path. Every choice is scored as if it
   were a first choice.
3. For each choice and each data year the engine:
   - opens `odds_databases/odds_<year>.db`;
   - looks up the Core zone id by name (`SELECT zone_id FROM zone WHERE
     zonename = 'Core'`), because the ids differ per year;
   - maps the permit date to a comparable date in that year with
     `find_comp_date()`: same day-of-year, nudged to the same weekday, kept in
     the same month, capped at Oct 31;
   - resolves the zone id and the comparable date id;
   - if the zone is Core, calls `coreodds1(date_id, group_size)`;
   - otherwise calls `checkexact(1, zone_id, date_id, ...)`, which selects
     `wins.avgodds` for `choicenum = 1`, that zone and that date, and
     **ignores group size entirely**. The first row is returned, or `None`.
4. `coreodds1()` fetches every Core `choicenum = 1` row for that date, one row
   per group size present that year. An exact group-size match returns that
   row's `avgodds`. Otherwise:
   - a size bracketed by two observed sizes is linearly interpolated;
   - a size below the smallest observed size is scaled up by multiplying
     through the handcrafted `coregs` ratios;
   - a size above the largest observed size is scaled down by dividing
     through `coregs`;
   - results are capped at 1.0. If none of the branches apply the function
     prints `Error in estimating group size odds` and returns `None`.
5. The endpoint returns `odds_by_year` per choice (one float or `None` per
   data year) plus the comparable dates. No prediction is made server-side.

### 1.2 Turning the per-year history into a single number (frontend)

`forecastProbLogit(series, PREDICT_YEAR, 3)` in `index.html`:

1. keeps the last three available years (a `None` year is dropped);
2. with a single point, returns it unchanged;
3. otherwise transforms each value with `logit(p)` (after clamping to
   `[1e-6, 1 - 1e-6]`), fits an ordinary least-squares line against year,
   evaluates the line at `PREDICT_YEAR = 2027`, and inverse-transforms;
4. clamps the result to `[0.0005, 0.9995]`.

That number is what the page shows as the predicted odds. The heatmap
calendar uses the same function (`hmPredictAll`) on the output of
`/heatmap_odds`, which runs the identical per-year lookups for every date in
the season.

### 1.3 The backtest

`backtest.py` re-implements the same logit-linear forecast in Python
(`forecast_prob`), samples 1000 random (zone, date, group size) choices, and
predicts 2025 from 2022 to 2024. The "actual" it scores against is the
2025 database's `avgodds` for the same lookup, which is itself a small-sample
average, not a realized win/loss.

## 2. What is actually in `odds_<year>.db`

`schema.sql` only describes the analytics logger; the odds databases were
inspected directly.

Tables: `zone(zone_id, zonename)`, `date(date_id, datestr)`, and
`wins(wins_id, choicenum, zoneid1..3, dateid1..3, groupsize1..3, obs,
avgodds, stddev, minodds, perc25th, perc50th, perc75th, maxodds)`.

- One `wins` row per distinct choice set that appeared in that year's
  lottery. `choicenum = 1` rows (about 1,900 per year) are the only ones the
  site uses.
- `avgodds` is the mean, across the `obs` applications that listed exactly
  that first choice, of a per-application win probability. `stddev` and the
  percentile columns describe the spread across those applications. The
  code that produced these numbers is not in the repo, so the exact
  definition of the per-application probability is unknown. It is not
  `1/obs`.
- **Non-Core rows carry `groupsize1 = 0`.** There is exactly one row per
  (zone, date), so group size cannot affect a non-Core estimate. Core has one
  row per group size present that year (7 or 8 sizes on most dates).
- **`obs = 0` rows exist**, 70 to 87 per year, and their `avgodds` is almost
  always exactly `1.0`. These are dates nobody applied for, recorded as a
  certain win. Most are Eightmile, Stuart and Snow in May and October.

Sample size is strongly tied to the odds themselves. In every year:

| `avgodds` range | rows | mean `obs` |
|---|---|---|
| < 0.05 | ~1,000 | ~31 |
| 0.05 to 0.10 | ~300 | ~13 |
| 0.10 to 0.20 | ~240 | ~6 |
| 0.20 to 0.40 | ~150 | ~3 |
| 0.40 to 0.70 | ~60 | ~1.8 |
| 0.70 to 0.999 | ~30 | ~0.9 |
| 1.0 | 50 to 86 | ~0.3 |

So a prediction that lands above 40% is, by construction, built from rows
backed by zero, one or two applications per year.

## 3. The backtest itself was measuring the wrong thing today

Before ranking causes, one finding changes the baseline.

`backtest.py` hardcodes `COREZONE_IDS = {2022: 4, 2023: 7, 2024: 1, ...}`.
Those ids matched the database files that existed on 2026-06-28 when the
saved runs were made. Commit `f08773f` on 2026-07-02 replaced
`odds_2022.db`, `odds_2023.db` and `odds_2024.db` with regenerated files
(new zone ordering, `obs` and percentile columns added, most `avgodds`
values nudged by a few thousandths). In the current files Core is zone id 2
in all three years. The engine is unaffected because it looks Core up by
name; the backtest is not.

Consequences when the backtest runs against today's files:

- Core history for 2022 to 2024 goes through the non-Core path, which
  returns the first row by rowid. That is the group-size-1 row on about
  two-thirds of dates, so a party of 4 is scored against size-1 odds, which
  are 3 to 4 times higher.
- In 2024, zone id 1 is Colchuck, so Colchuck is routed into `coreodds1()`,
  finds no group-size rows, prints `Error in estimating group size odds`
  200 times, and drops the 2024 point from every Colchuck series.

Runs of the 2025 backtest, same seed:

| Run | MAE | Brier | 0 to 5% bias | 40 to 100% bias |
|---|---|---|---|---|
| Saved 2026-06-28 (old databases, ids matched) | 8.25 pp | 0.0325 | -3.8 pp | +18.5 pp |
| Today, as-is (stale ids) | 10.48 pp | 0.0436 | -8.5 pp | +15.1 pp |
| Today, Core ids corrected in a scratch copy | 8.13 pp | 0.0326 | -6.7 pp | +9.3 pp |

The rest of this document uses the corrected-id run on the current
databases, since that is the only one that reflects what the site would do.
Its full calibration table:

| Pred range | n | Avg pred | Avg actual | Bias |
|---|---|---|---|---|
| 0 to 5% | 397 | 2.3% | 9.0% | -6.7 pp |
| 5 to 10% | 251 | 7.1% | 12.7% | -5.6 pp |
| 10 to 20% | 131 | 13.9% | 18.2% | -4.3 pp |
| 20 to 40% | 85 | 27.8% | 30.9% | -3.1 pp |
| 40 to 100% | 136 | 84.9% | 75.6% | +9.3 pp |

The high bucket still runs hot, and every other bucket runs cold. The
pattern the saved run showed is real, just smaller than +18.5 pp.

## 4. Ranked hypotheses for the high-bucket overconfidence

### H1. Logit-linear trend extrapolation over two or three noisy points (primary)

The forecast fits a two-parameter line through at most three points, in
logit space, and extrapolates it forward. There is no regularization, so
year-to-year sampling noise becomes "trend" and is pushed further in the
same direction. Two things make this bite hardest at the top:

- `logit(1.0)` after clamping is about 13.8. Any year whose value is an
  imputed `1.0` (an `obs = 0` row) dominates the fit or the average. In the
  high bucket, 85 of 136 series contain a `1.0`; 47 have an imputed `1.0`
  as their most recent (2024) point.
- Once the line has a positive slope, evaluating it a year ahead and
  inverse-transforming saturates: 58 of the 136 high-bucket predictions sit
  on the 0.9995 clamp ceiling.

Evidence from the corrected-id backtest, same 1000 samples:

| Predictor for the same series | MAE | Brier | 40 to 100% bias | worst low bucket |
|---|---|---|---|---|
| Current: logit line extrapolated to 2025 | 8.13 pp | 0.0326 | +9.3 pp | -6.7 pp |
| Same line, evaluated at 2024 (no forward step) | 5.98 pp | 0.0189 | +8.0 pp | -6.9 pp |
| Mean of logits (slope forced to zero) | 5.36 pp | 0.0166 | +11.7 pp | -1.8 pp |
| Last available year only | 5.77 pp | 0.0161 | -0.1 pp | -8.1 pp |
| Plain mean of the available years | 4.77 pp | 0.0108 | +2.9 pp | -1.0 pp |

Splits inside the high bucket under the current method:

| Subset | n | Avg pred | Avg actual | Bias |
|---|---|---|---|---|
| Fitted slope > 0 | 55 | 79.3% | 61.4% | +17.9 pp |
| Fitted slope <= 0 | 81 | 88.7% | 85.3% | +3.4 pp |
| All three years exactly 1.0 | 41 | 100.0% | 99.7% | +0.2 pp |
| Some years 1.0, some below | 44 | 96.9% | 79.0% | +17.9 pp |
| No 1.0 in the series | 51 | 62.4% | 53.3% | +9.1 pp |

For the "mixed" rows the plain mean would have predicted 80.3% against the
79.0% actual. The same mechanism explains the cold low buckets: a series
that starts high and falls gets a steep negative slope and is extrapolated
toward zero. In the 20 to 40% bucket, rows with a positive slope are +7 pp
hot and rows with a negative slope are -19 pp cold. Across the whole sample,
series containing a `1.0` that is not the last point are predicted at 56%
against a 75% actual.

Typical failure, Eightmile Oct 1, group of 5: history 0.797 (obs 0),
1.000 (obs 0), 0.630 (obs 3); predicted 98.8%; 2025 actual 30.8%.

Production is worse than the backtest in one respect: `PREDICT_YEAR` is
2027 and the last data year is 2026, but the window is still three points,
so the line is fitted on 2024 to 2026 and stepped one year ahead, the same
shape as the backtest.

### H2. Thin samples with no shrinkage, so extremes do not regress (secondary, structural)

Every high-odds row is backed by 0 to 2 applications, and the year-to-year
movement of such rows is mostly noise:

| `obs` behind the 2024 row | n | mean absolute change 2024 to 2025 |
|---|---|---|
| 0 | 87 | 11.6 pp |
| 1 | 63 | 15.5 pp |
| 2 to 3 | 101 | 10.4 pp |
| 4 to 9 | 204 | 7.9 pp |
| 10+ | 545 | 2.0 pp |

Because a choice enters the high bucket precisely when its past values were
high, and those past values include positive noise, the next year regresses
toward the middle. Pure persistence (2024 value against 2025 actual) shows
it directly: rows at 0.70 to 0.999 in 2024 averaged 82.7% and came in at
71.1%; rows at 0.40 to 0.70 averaged 53.4% and came in at 65.1%. Both ends
pull toward roughly 65 to 70%.

Nothing in the pipeline pools information across neighboring dates, the
same zone-month, or other group sizes, and `obs` is never used as a weight.
This is why even the plain mean is still +2.9 pp hot at the top: the
remaining bias is winner's-curse selection on tiny samples. Note that the
backtest "actual" is just as thin (68 of the 136 high-bucket actuals are
`obs = 0` rows, 20 more are `obs = 1`), so part of the measured error is
noise in the target rather than in the model; the sign is systematic, the
magnitude is inflated.

### H3. Zero-applicant dates are stored as a certain 100% with no uncertainty

`obs = 0` rows with `avgodds = 1.0` mean "nobody applied, so anyone would
have won". Treating that as a measured probability of exactly 1.0 is what
feeds H1 (infinite logit) and H2 (no weight for zero evidence). When the
three prior years are all `1.0` the prediction is fine (99.7% actual); the
damage is done when a `1.0` sits next to real observations. Whether these
rows should count as evidence at all, or only as a weak prior that the date
is low-demand, is a modeling decision that has not been made anywhere in
the code.

### H4. Group size is not modeled for non-Core zones (does not cause the bias, but limits the fix)

Non-Core rows have `groupsize1 = 0`, one row per zone-date. The site accepts
a group size for every zone but the estimate cannot respond to it outside
Core. The row's `stddev` for non-Core is inflated by pooling every group
size into one average. This does not push high predictions up, but it means
any group-size-aware correction can only ever apply to Core, and the
backtest's random group size is meaningless for 80% of samples.

### H5. The handcrafted `coregs` table (minor)

`coregs = {1: 4.64, 2: 1.41, 3: 1.15, 4: 1.18, 5: 1.07, 6: 1.2, 7: 1.03}`
is the assumed ratio `odds(g) / odds(g + 1)` for Core. Empirical medians
from the databases (dates where both sizes have `obs >= 3`):

| Step | code | 2022 | 2023 | 2024 | 2025 | 2026 |
|---|---|---|---|---|---|---|
| 1 to 2 | 4.64 | 3.62 | 3.30 | 3.14 | 2.90 | 2.89 |
| 2 to 3 | 1.41 | 1.33 | 1.31 | 1.24 | 1.26 | 1.22 |
| 3 to 4 | 1.15 | 1.14 | 1.11 | 1.12 | 1.11 | 1.12 |
| 4 to 5 | 1.18 | 1.12 | 1.14 | 1.12 | 1.12 | 1.10 |
| 5 to 6 | 1.07 | 1.08 | 1.00 | 1.07 | 1.07 | 1.06 |
| 6 to 7 | 1.20 | 1.12 | 1.08 | 1.14 | 1.11 | 1.11 |
| 7 to 8 | 1.03 | 1.00 | 1.09 | 1.00 | 1.00 | 1.00 |

The size-1 factor is high by roughly a third and drifting further from the
data each year; the rest are close. The table is only reached when a date
is missing a group-size row, which is rare (most Core dates have all
sizes), and Core contributes only 7 of the 136 high-bucket samples. In the
backtest the `core-extrap` rows are +14 pp hot at the top (n = 6) and
-12 pp cold at the bottom (n = 16). Real, but a rounding error next to H1.

### H6. Other things noticed, not calibration-related

- `checkexact()` uses `&` where `and` was meant in every `cnum = 2/3`
  branch (`z1 == corezoneid & z2 == corezoneid` parses as
  `z1 == (corezoneid & z2) == corezoneid`). Dead on the current path, but
  it will misbehave if second/third-choice scoring is ever revived.
- `coreodds1()` returns `None` via a bare `except` and a print when no
  branch applies (group size 0 rows, or a date with a single group-size
  row on the wrong side). The engine converts that to "Data Unavailable",
  which is fine, but the backtest silently drops the year.
- The frontend and backtest forecast code are duplicated by hand and must be
  kept in sync manually.

## 5. Implications for a fix (not implemented)

In order of expected payoff:

1. Fix the backtest first: derive Core ids by name, exactly as the engine
   does, so measurements are of the real pipeline. Consider reporting `obs`
   behind each actual so thin targets can be down-weighted or excluded when
   scoring.
2. Replace the unregularized logit-linear extrapolation. The plain mean of
   the available years already cuts MAE by 40% and Brier by two-thirds
   with no other change. Anything trend-aware needs shrinkage of the slope
   toward zero and should not be fitted in a space where `1.0` is infinite.
3. Add shrinkage toward a pooled baseline (for example zone-month, or the
   surrounding dates) weighted by `obs`, so a row backed by one application
   cannot carry the estimate on its own. This is what addresses the
   remaining regression-to-the-mean bias at the top.
4. Decide what `obs = 0` rows mean and encode it (a prior with a small
   pseudo-count rather than a measured 1.0).
5. Re-derive `coregs` from the data, or drop it in favor of interpolating
   in the pooled baseline. Low priority.

Any change to the forecast must be made in both `index.html`
(`forecastProbLogit`) and `backtest.py` (`forecast_prob`), or the two
should be unified so the site calls a server-side prediction.

## 6. Fix applied

### 6.1 What changed

- **Prediction moved server-side and unified.** `app/odds_engine.py` gained
  `lookup_first_choice_history()` and `predict_first_choice()`. The
  `/estimate_odds` response now carries `predicted` and `obs_by_year` per
  choice, and `/heatmap_odds` carries `predicted_by_date`. The page no
  longer forecasts anything itself; it only combines the per-choice
  predictions with the existing priority conditioning (P2 is scaled by
  1 - P1, P3 by 1 - P1 - P2). The logit, line-fit and
  `forecastProbLogit` helpers were removed from `index.html`.
- **New estimator.** For each data year, the comparable date's odds are
  averaged with the same weekday one week either side (weights 1, 0.5, 0.5)
  in probability space, then the plain mean is taken across all available
  years. No trend is fitted and no logit transform is used, so an imputed
  100% is just another value of 1.0 rather than an infinite lever.
- **Backtest reads Core by name** instead of the stale hardcoded ids, uses
  the engine's own lookup and predictor, and keeps the old logit forecast
  behind `--method logit` for comparison. It also prints a second
  calibration table restricted to targets backed by at least three
  applications.
- `sim_version` logged with each query is now `v1.1.0`.

### 6.2 How the kernel was chosen

Candidates were scored on the 2025 backtest and then checked on 2026
(same seed, 1000 samples each). The ±1 week kernel at half weight was the
best on Brier in 2025 and within noise of best in 2026, and it was the only
candidate whose 40%+ bucket was within a point on both years. Wider or
flatter kernels over-smoothed and turned the top bucket cold.

| Predictor | 2025 MAE | 2025 Brier | 2025 40%+ bias | 2026 MAE | 2026 Brier | 2026 40%+ bias |
|---|---|---|---|---|---|---|
| Old logit trend, window 3 | 8.13 pp | 0.0326 | +9.3 pp | 7.36 pp | 0.0260 | +9.2 pp |
| Plain mean of years | 4.77 pp | 0.0108 | +2.9 pp | 4.86 pp | 0.0080 | +2.8 pp |
| Kernel 1 / 0.5 (chosen) | 5.14 pp | 0.0103 | -0.2 pp | 5.02 pp | 0.0083 | +0.2 pp |
| Kernel 1 / 0.5 / 0.25 | 5.69 pp | 0.0117 | -2.8 pp | 5.59 pp | 0.0095 | -2.9 pp |
| Flat mean of 5 dates | 6.68 pp | 0.0157 | -6.3 pp | 6.54 pp | 0.0134 | -4.9 pp |

### 6.3 Result

`python backtest.py --target 2025` and `--target 2026` after the change:

| Pred range | 2025 n | 2025 pred / actual | 2025 bias | 2026 n | 2026 pred / actual | 2026 bias |
|---|---|---|---|---|---|---|
| 0 to 5% | 284 | 2.4% / 2.6% | -0.2 pp | 278 | 2.4% / 2.7% | -0.3 pp |
| 5 to 10% | 213 | 7.2% / 7.6% | -0.4 pp | 219 | 7.2% / 8.5% | -1.3 pp |
| 10 to 20% | 177 | 14.4% / 14.2% | +0.2 pp | 185 | 14.7% / 16.3% | -1.6 pp |
| 20 to 40% | 146 | 27.6% / 27.0% | +0.6 pp | 139 | 27.6% / 27.6% | 0.0 pp |
| 40 to 100% | 180 | 73.4% / 73.6% | -0.2 pp | 179 | 72.4% / 72.3% | +0.2 pp |

Overall: MAE 5.14 pp and Brier 0.0103 for 2025 (was 8.13 pp and 0.0326
with the old forecast on the same data); MAE 5.02 pp and Brier 0.0083 for
2026 (was 7.36 pp and 0.0260). R² rose from 0.65 to 0.87.

One caveat remains and is inherent to the data rather than the estimator.
Among choices predicted above 40%, the subset whose 2025 outcome was backed
by three or more applications came in at 46% against a 59% prediction.
Those are exactly the dates where demand showed up unexpectedly, and the
sample is small (42 rows). Conditioning on the outcome's `obs` selects for
surprise, so this is not evidence of miscalibration on its own, but it is a
reminder that the top bucket is driven by whether anyone else applies.

### 6.4 Not changed

- `coregs` is still the handcrafted table (H5). It only matters for Core
  dates missing a group-size row and would need re-deriving from the data.
- Non-Core zones still ignore group size (H4); that is a property of the
  database, not the estimator.
- No pooling toward a zone-month baseline was added (H2). The neighbour
  kernel does part of that job; a proper shrinkage model is a possible next
  step if the top bucket drifts again when 2027 actuals arrive.
