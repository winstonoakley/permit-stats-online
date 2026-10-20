# Titles, descriptions, slugs, navigation

Titles are under 60 characters; descriptions under 155. "Enchantments" appears in every title because it is the term people actually search. Site name: **Permit Odds**; domain: **permitodds.com** (chosen 2026-10-18).

| Slug | `<title>` | Meta description |
|---|---|---|
| `/` | Enchantments Lottery Odds Calculator | Estimate your odds of winning an Enchantments overnight permit. Enter up to three choices and see predicted and historical odds for every zone and date. |
| `/lottery` | How the Enchantments Lottery Works | Dates, fees, zones, group size rules, how applications are drawn, and what to do if you lose. Plain-language guide to the Enchantments permit lottery. |
| `/zones` | Enchantments Permit Zones and Odds | The five Enchantments permit zones compared: predicted 2027 odds for the median day, July and September, with a page per zone. |
| `/zones/core` | Core Zone Permit Odds by Date | Predicted 2027 odds of winning a Core Enchantment Zone permit for every date in the season, plus five years of history. |
| `/zones/colchuck` | Colchuck Zone Permit Odds by Date | Predicted 2027 odds for a Colchuck Lake overnight permit on every date from May 15 to October 31, with historical trends. |
| `/zones/snow` | Snow Zone Permit Odds by Date | Predicted odds for a Snow Lakes zone permit on every date of the 2027 season, with five years of lottery history. |
| `/zones/eightmile` | Eightmile / Caroline Permit Odds by Date | The easiest Enchantments permit to win. Predicted 2027 odds for Eightmile Lake and Lake Caroline on every date, with history. |
| `/zones/stuart` | Stuart Zone Permit Odds by Date | Predicted 2027 odds for a Stuart Lake zone permit on every date of the season, plus five years of lottery history. |
| `/faq` | Enchantments Lottery FAQ: Odds, Zones, Dates | Which zone is easiest, whether weekdays help, which month to pick, what alternates to list, and what the walk-up lottery is. Answers backed by data. |
| `/methodology` | How the Enchantments Odds Are Estimated | How this site simulates past lotteries, matches comparable dates, handles Core group size, and forecasts 2027. Limitations included. |
| `/about` | About Permit Odds | Who built the Enchantments odds calculator, where the data comes from, and how to get in touch. |
| `/privacy` | Privacy Policy | What Permit Odds records when you use the calculator, what it never collects, and how to have data removed. |
| `/terms` | Terms of Use | Permit Odds is unofficial and its odds are estimates. Read this before relying on them. |

## Navigation

Header, left to right: **Calculator** · **Zones** (the `/zones` index) · **How the lottery works** · **FAQ**

Footer: About · Methodology · Privacy · Terms · "Not affiliated with the U.S. Forest Service or recreation.gov" · consent-settings link (once ads are live)

## Other head tags for every page

- `<meta name="viewport" content="width=device-width, initial-scale=1">`
- `<link rel="canonical" href="https://permitodds.com/[slug]">`
- Open Graph title, description, and a single 1200×630 image (a photo you own of one of the lakes is ideal; a rendered heatmap works as a fallback)
- A favicon; a 32×32 larch or lake icon is plenty

## Routes the server also needs

`/robots.txt`, `/sitemap.xml` listing the thirteen URLs above, `/ads.txt` once a network is approved, `/favicon.ico`. All but `/ads.txt` are served by `app/main.py`; the pages themselves are rendered by `app/pages.py`, which reads the titles and descriptions from the table above.

## Internal links

Every page links to the calculator. Each zone page links to its neighbours and to the FAQ questions that mention it. The lottery page links to every zone page in the zone list. The FAQ links to the lottery and methodology pages. This is enough for a thirteen-page site; no need for a blog section unless you want to write.
