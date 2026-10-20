# How the Odds Are Estimated

This page explains what the numbers on this site are, where they come from, and where they are weak. It describes what the code does as of October 2026.

## The data

Recreation.gov publishes the results of each Enchantments lottery: every application, the three choices on it, and whether it won. This site uses the lotteries for the 2022 through 2026 seasons. The 2020 and 2021 lotteries are excluded because the zone quotas and the pandemic-year demand were different enough to mislead a forecast.

For each year the site holds a table of first-choice outcomes for every combination of zone and entry date that anyone applied for, and, where enough people applied for the same set, outcomes for full two- and three-choice combinations as well.

## Simulated odds, not raw win rates

The percentage for a zone and date in a given year is not simply "winners divided by applicants" for that date. A raw win rate is noisy when only a few people applied, and it is zero for any date where the handful of applicants happened to lose.

Instead, each year's lottery is re-run many times with the same applications in a different random order, following the same rule recreation.gov uses: applications are processed in draw order, and each one takes the first of its three choices that still has room. The odds for a choice are the share of simulated lotteries in which that choice won. A value of 10% means the choice won in 100 of 1,000 simulated draws. This smooths out the luck of the actual draw and gives a number for dates that had only a few applicants.

## Comparable dates

You are applying for a 2027 date, but the data is about 2022 through 2026 dates. Demand depends far more on the day of the week and the time of year than on the calendar date, so for each past year the site finds the comparable date: the day in that year with the same weekday, in the same month, closest to the same position in the month. A Saturday in the second week of August 2027 is compared to the Saturday in the second week of August in each earlier year. The comparable date used for each year is shown in small text under each cell in the historical table.

## Group size

For Colchuck, Snow, Eightmile / Caroline, and Stuart, group size is not used when looking up odds. The lottery counts quota for those zones in a way that makes group size a small effect, and the data does not separate it cleanly.

For the Core Zone, group size matters a lot, because Core quota is counted per person. The site looks up the odds for the exact group size when anyone applied with that size on the comparable date. When nobody did, it interpolates between the nearest observed sizes, or, at the ends of the range, scales from the nearest observed size using ratios measured from the data (the median over 2024–2026 Core dates: a party of one has roughly 2.9 times the odds of a party of two on the same date, a party of two roughly 1.24 times the odds of a party of three, and the ratio is close to 1.1 for each step above that).

## The 2027 prediction

The historical columns show each year separately. The prediction combines them in two steps.

1. **Smooth within each year.** A single date can be noisy when few people applied for it. So for each past year, the comparable date's odds are averaged with the odds on the same weekday one week before and one week after, with the comparable date counting twice as much as each neighbour. Dates a week apart in the same month draw very similar demand, so this damps the noise without blurring the season.
2. **Average across years.** The smoothed values for every year with data are averaged, each year counting equally. No trend is fitted.

An earlier version of the site fitted a trend line through the last three years and extended it forward. Tested on the 2025 and 2026 lotteries, that overstated the odds of the likeliest choices by about 9 percentage points, because a line through two or three noisy years swings hard. The current method, tested the same way (predicting each year from the years before it, over 1,000 random choices), has an average error of about 5 points, and in every range of predictions the average prediction is within 2 points of how often those choices actually won.

If no past year has data for a date, the site says so rather than guessing.

## Second and third choices

Your second choice only matters if your first choice loses, and your third only if both lose. The site handles this in two steps.

First, it estimates each choice's odds as though it were a first choice. Then it conditions them: the second choice's contribution is its first-choice odds multiplied by the probability that your first choice loses, and the third choice's contribution is scaled by the probability that both earlier choices lose. The total shown is the sum of the three conditioned values, which is your estimated probability of winning any of them. This mirrors how the draw processes an application.

The historical columns are conditioned the same way, so the per-year totals are comparable to the prediction.

## The heatmap calendar

The calendar behind each date field runs the same first-choice prediction for every day in the season for the zone and group size you picked, then colors each day by its predicted odds: green at 30% or better, orange from 5% to 30%, red below 5%, and gray where no data exists. For a second or third choice, the calendar scales each day by the chance that your earlier choices lose. The [zone pages](/zones) show the same first-choice calendar, with a group-size picker on the Core page. It is meant for picking dates, not for the final answer; the full three-choice calculation is what the results box shows.

## Known limitations

- **Demand shifts.** The prediction assumes 2027 demand looks like the average of the last five years. A viral post, a road closure, or a quota change would break that, and the site has no way to see it coming.
- **Small samples.** Odd-looking single days, especially a lone red day inside a green week in late October or early May, are usually a date with very few applicants. Trust the surrounding days more than the outlier.
- **Nights are ignored.** The lottery asks how many nights you want. The site treats every application as a single entry date and does not model how longer stays consume quota.
- **The Core group-size scaling** is a median ratio applied to a specific date. It is a reasonable approximation and no more.
- **Nothing here is official.** The Forest Service and recreation.gov do not publish odds and have no connection to this site.

## How the data is built

The lottery-simulation step and the per-year odds tables are built offline from Recreation.gov's published lottery data; the site serves the results. Questions about the method are welcome at [contact@permitodds.com](mailto:contact@permitodds.com).
