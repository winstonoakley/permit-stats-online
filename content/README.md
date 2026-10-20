# content/ — the site's guide and zone pages

Each Markdown file here is published by `app/pages.py` at the slug below. Edit the Markdown and redeploy; pages are rendered once per process. Titles and meta descriptions come from the table in `seo-meta.md`. `README.md` and `seo-meta.md` themselves are not published.

| File | Page |
|---|---|
| `how-the-lottery-works.md` | /lottery |
| `faq.md` | /faq |
| `methodology.md` | /methodology |
| `zones.md` | /zones/core, /zones/colchuck, /zones/snow, /zones/eightmile, /zones/stuart (intros; the odds calendar and month table below each are generated) |
| `about.md` | /about |
| `privacy.md` | /privacy |
| `terms.md` | /terms |
| — | /zones (index, generated in `app/pages.py`) |

## Rules the renderer enforces

- HTML comments (`<!-- ... -->`) are stripped before rendering and never reach the page or its source. The ads and affiliate text in `privacy.md`, `terms.md` and `about.md` is parked in comments until an ad network is chosen.
- `tests/test_pages.py` fails if any published page contains a `[EDIT ...]`, `[CONFIRM ...]` or `[NUMBER]` placeholder, or a slug with no row in `seo-meta.md`.

## Keeping the numbers current

The FAQ, the lottery page and the zone intros quote medians, monthly and weekday averages from the same predictor the calculator uses. They were last regenerated on 2026-10-19 (2027 predictions from the 2022–2026 lotteries, `sim_version` v1.1.1). After adding a data year or changing the predictor, run

    venv/Scripts/python.exe scripts/content_stats.py

and update the figures in those three files. The zone page calendars and tables update by themselves.

## Facts to re-check each winter

Lottery dates, fees and rules were checked against the Forest Service and Recreation.gov pages on 2026-10-19 (sources are linked at the bottom of `how-the-lottery-works.md`). When the 2027 lottery is announced, re-check the application window, results date, the April 1 release, and the daily lottery rules, and update "Last reviewed" on the lottery page. Trail distances and elevations in `zones.md` are rounded from general knowledge and have not been checked against a map.

## Tone

Plain, second person, no hype. Each page answers a question a person planning a trip actually has, and every number is one the site can back with its own data. If a paragraph does not tell the reader something they can act on, cut it.
