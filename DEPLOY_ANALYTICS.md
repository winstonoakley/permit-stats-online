# Deploying permitodds.com on Railway (with analytics v2)

Production URL: **https://permitodds.com** (registered 2026-10-18).
Hosting: Railway, Hobby plan, deploying `main` from GitHub
(`winstonoakley/permit-stats-online`). Analytics keeps SQLite on a Railway
volume (ANALYTICS_NOTES.md section 7).

Do these in order. The step that matters most is step 7: prove an event
survives a redeploy.

## 0. Before you touch Railway

- [x] Analytics merged to `main` and pushed (commit `63f7b79`, 2026-10-16).
- [ ] Pull `main` and commit the domain edits from 2026-10-18 (this file,
      `app/main.py`, `scripts/verify_persistence.py`,
      `.github/workflows/analytics-report.yml`, `ANALYTICS_NOTES.md`), then
      push. Railway deploys whatever is on `main`.
- [ ] `python -m pytest tests -q` passes on `main`.
- [ ] The analytics token. GitHub already holds one as the
      `ANALYTICS_TOKEN` secret. GitHub will not show it again, so either
      use the copy in your password manager, or generate a new one and
      replace the GitHub secret too:
      `python -c "import secrets; print(secrets.token_urlsafe(32))"`.
      Railway and GitHub must hold the same value.

## 1. Create the Railway service

- [ ] railway.com > sign in > **New Project** > **Deploy from GitHub repo**
      > `winstonoakley/permit-stats-online`. (If the repo is not listed,
      use "Configure GitHub App" to give Railway access to it.)
- [ ] The old project `zooming-benevolence` may still exist. Delete it or
      leave it stopped, so two services are not billing.
- [ ] Service > **Settings** > **Source**: branch `main`, auto-deploy on.
- [ ] Hobby plan ($5/month including $5 of usage). Under
      Workspace > Usage, set a usage limit (for example $10) so a bot
      flood in the lottery window cannot run up the bill.

The first deploy **will fail** with "no start command": Railpack detects
Python but only looks for a root `main.py`/`app.py`, and this app lives in
`app/main.py`. That is expected. Do steps 2 to 4 (volume, variables,
start command), then press Redeploy.

## 2. Volume

- [ ] On the project canvas, right-click the service > **Attach volume**
      (or New > Volume), attached to **this service**, mount path
      **`/data`**.
- [ ] Hobby volumes hold 5 GB; at a few hundred bytes per event that is
      millions of events.
- [ ] Optional, cheap insurance: turn on scheduled backups for the volume.
- [ ] Trade-off: a service with a volume has a few seconds of downtime on
      every deploy. Avoid deploying during Feb 15 - Mar 1.

## 3. Environment variables (service > Variables)

| Variable | Value | Why |
|---|---|---|
| `ANALYTICS_DB_PATH` | `/data/analytics.db` | Puts the database on the volume. Without it the file is wiped on every deploy, which is what happened to v1. |
| `ANALYTICS_TOKEN` | the token from step 0 | Unlocks `/analytics/export`, `/analytics/summary`, `/debug/analytics`. Without it they answer 503. |
| `ALLOWED_ORIGIN` | `https://permitodds.com` | CORS. No trailing slash. Defaults to `*` if unset. |
| `ANALYTICS_ENABLED` | leave unset | Set to `0` only to switch collection off without a code change. |
| `RAILPACK_START_CMD` | the uvicorn command from step 4 | Sets the start command; Railpack cannot find `app/main.py` on its own. |
| `RAILWAY_RUN_UID` | `0`, **only if** the logs show "Permission denied" on `/data` | Images that run as a non-root user cannot write to a fresh volume. |

## 4. Start command (rate limiting)

- [ ] Easiest: add a service variable **`RAILPACK_START_CMD`** with the
      command below (no quotes around the whole thing), then Redeploy.
      Railpack reads it at build time; it outranks auto-detection. The
      dashboard field (Settings > Deploy > Start Command) works too, but
      its location moves between Railway UI versions:

      uvicorn app.main:app --host 0.0.0.0 --port $PORT --forwarded-allow-ips="*"

      Without `--forwarded-allow-ips`, uvicorn ignores `X-Forwarded-For`
      from Railway's proxy (and Cloudflare's), so the rate limiter sees one
      address for everybody: 30 queries and 15 heatmaps per minute **for
      the whole site**. The cost of `*` is that a script can dodge the
      limit by forging the header; the analysis pipeline still drops it as
      `bot_burst`.
- [ ] Settings > **Networking** > **Generate Domain**. This gives a
      `*.up.railway.app` URL to test with before DNS is ready.

## 5. Connect permitodds.com

Railway's Hobby plan allows 2 custom domains per service. Use one for
`permitodds.com`; `www` is redirected to it at the DNS provider, so it does
not need a Railway slot.

- [ ] Service > Settings > Networking > **+ Custom Domain** >
      `permitodds.com`. Railway shows two records: a **CNAME** (target ends
      in `.up.railway.app`) and a **TXT** for verification. Both are
      required; without the TXT the domain answers 404.
- [ ] If Railway asks for a port, choose the one uvicorn listens on (the
      `$PORT` value it shows).
- [ ] At the DNS provider (instructions below assume Cloudflare; see the
      note for other registrars):

      | Type  | Name             | Target                     | Proxy   |
      |-------|------------------|----------------------------|---------|
      | CNAME | `@`              | Railway's CNAME target     | Proxied |
      | TXT   | the name Railway shows | the value Railway shows | n/a |
      | CNAME | `www`            | `permitodds.com`           | Proxied |

- [ ] Cloudflare > SSL/TLS > Overview: mode **Full**. Not Full (strict),
      which Railway says does not work as intended, and not Flexible, which
      causes redirect loops.
- [ ] Cloudflare > Rules > **Redirect Rules** (or a Bulk Redirect): send
      `www.permitodds.com/*` to `https://permitodds.com/${1}` with a
      **301**, preserving path and query string. One canonical host is
      better for SEO.
- [ ] Wait for the green check next to the domain in Railway. DNS usually
      takes minutes, Railway allows up to 72 hours; the certificate is
      issued within about an hour of DNS resolving.

Not on Cloudflare? A plain CNAME at the root (`@`) only works where the
DNS host supports CNAME flattening or ALIAS/ANAME records (Cloudflare,
Namecheap, DNSimple, bunny.net; Porkbun offers ALIAS). GoDaddy, Route 53,
Squarespace, NameSilo and Hostinger do not. The easy fix is to point the
domain's nameservers at a free Cloudflare account and follow the table
above.

Cloudflare also adds the visitor's country header the analytics code reads
(ANALYTICS_NOTES.md section 7.4), so country and region data comes free.

## 6. Deploy and look at it

- [ ] Deploy (or let the variable change redeploy). The logs must show
      **no** `Analytics init failed` line. (`Analytics disabled` means
      `ANALYTICS_ENABLED` is set.)
- [ ] Open https://permitodds.com, pick a date, press Fetch Results.
- [ ] https://www.permitodds.com redirects to https://permitodds.com.
- [ ] `curl -H "Authorization: Bearer <token>" https://permitodds.com/analytics/summary`
      shows `"db_path": "/data/analytics.db"`, `"schema_version": 2` and
      your page_view, heatmap_view and get_table.
- [ ] The same URL without the header answers **401**.

## 7. Prove events survive a redeploy (do not skip)

This is the check v1 never had. From the repo on your computer, with the
Railway CLI installed, logged in and linked to the new service
(`npm i -g @railway/cli`, `railway login`, `railway link`):

    $env:ANALYTICS_TOKEN = "<token>"        # PowerShell; bash: export ANALYTICS_TOKEN=...
    python scripts/verify_persistence.py --url https://permitodds.com --redeploy

Without the CLI, leave out `--redeploy`; the script pauses while you press
Redeploy in the dashboard. What it does:

1. Reads `started_at` from `/health`.
2. Writes a marker `page_view` through `/log_event` (user agent contains
   "bot", so reports ignore it).
3. Finds it in `/analytics/export` and checks the database is under `/data/`.
4. Redeploys.
5. Waits until `/health` shows a new `started_at`.
6. Exports again: the same event id and timestamp must be there and the
   event total must not have dropped.

- [ ] It ends with **PASS: events survive a redeploy.** Anything else: the
      volume is missing, mounted somewhere other than `/data`, or attached
      to a different service. Fix it and run the script again before
      relying on the data.
- [ ] Run it again after any change to the volume, the service or the
      variables.

## 8. Reports

- [ ] GitHub repo > Settings > Secrets and variables > Actions > Variables:
      set `ANALYTICS_URL` = `https://permitodds.com`, and change
      `ANALYTICS_SINCE` from `2026-10-16` to the real go-live date.
- [ ] Re-enable the workflow (it was disabled while no URL existed):
      `gh workflow enable analytics-report.yml`, or Actions > Analytics
      report > Enable workflow.
- [ ] Run it once by hand: Actions > Analytics report > Run workflow.
      Download the report artifact and check it shows your test visit.

## If something goes wrong

- Turn analytics off without a deploy of new code: set
  `ANALYTICS_ENABLED=0`. The site keeps working; nothing is recorded.
- A broken or full volume never breaks the odds tool: logging fails
  quietly (`Analytics logging failed` in the logs) and the read endpoints
  answer 503.
- `ERR_TOO_MANY_REDIRECTS`: Cloudflare SSL mode is Flexible, or the record
  is DNS-only; set **Full** and proxy the record.
- Domain answers 404 from Railway: the TXT verification record is missing
  or wrong.
