# Deploying analytics v2 on Railway

Option chosen in ANALYTICS_NOTES.md section 7: keep SQLite and put the file
on a Railway volume. Do these in order; the step that matters most is the
last one (prove an event survives a redeploy).

## 0. Before you touch Railway

- [ ] Merge `worktree-data-collection` into `heatmap-calendar`, then
      `heatmap-calendar` into `main` (or whichever branch the Railway
      service deploys; check Settings > Source). Production is still on
      `b453c00` from 2026-03-16, so this deploy also ships the heatmap and
      the calibration fix.
- [ ] `python -m pytest tests -q` passes on the merged branch.
- [ ] Generate a token and keep it in your password manager:
      `python -c "import secrets; print(secrets.token_urlsafe(32))"`

## 1. Volume

- [ ] Add a volume to the web service (on the project canvas, right-click
      the service or use the New menu), attached to **this service**, mount
      path **`/data`**.
- [ ] Hobby plan volumes hold 5 GB; at a few hundred bytes per event that
      is millions of events, so size is not a concern.
- [ ] Optional but cheap insurance: turn on Railway's scheduled backups
      for the volume, in the service's backup settings.
- [ ] Know the trade-off: a service with a volume has a few seconds of
      downtime on every deploy, because Railway will not mount one volume
      into two deployments at once. Fine for this site; avoid deploying
      during the lottery window anyway.

## 2. Environment variables (service > Variables)

| Variable | Value | Why |
|---|---|---|
| `ANALYTICS_DB_PATH` | `/data/analytics.db` | Puts the database on the volume. Without it the file lands in the container and is wiped on every deploy, which is what happened to v1. |
| `ANALYTICS_TOKEN` | the token from step 0 | Unlocks `/analytics/export`, `/analytics/summary`, `/debug/analytics`. Without it they answer 503. |
| `ALLOWED_ORIGIN` | `https://<your domain>` | CORS. Defaults to `*` if unset. |
| `ANALYTICS_ENABLED` | leave unset | Set to `0` only to switch collection off without a code change. |
| `RAILWAY_RUN_UID` | `0`, **only if** the logs show "Permission denied" on `/data` | Images that run as a non-root user cannot write to a fresh volume. |

## 3. Start command (rate limiting)

- [ ] Settings > Deploy > Custom Start Command. If it is empty, find the
      command Railway runs in the deploy logs. Make it:

      uvicorn app.main:app --host 0.0.0.0 --port $PORT --forwarded-allow-ips="*"

      Without `--forwarded-allow-ips`, uvicorn ignores `X-Forwarded-For`
      from Railway's proxy, so the rate limiter sees one address for
      everybody: 30 queries and 15 heatmaps per minute **for the whole
      site**, which real visitors could hit in the lottery window. The
      cost of `*` is that a script can dodge the limit by forging the
      header; the analysis pipeline still drops it as `bot_burst`.

## 4. Deploy and look at it

- [ ] Deploy. In the logs there must be **no** `Analytics init failed`
      line. (`Analytics disabled` means `ANALYTICS_ENABLED` is set.)
- [ ] Open the site, pick a date, press Fetch Results.
- [ ] `curl -H "Authorization: Bearer <token>" https://<domain>/analytics/summary`
      shows `"db_path": "/data/analytics.db"`, `"schema_version": 2` and
      your page_view, heatmap_view and get_table.
- [ ] The same URL without the header answers **401**.

## 5. Prove events survive a redeploy (do not skip)

This is the check v1 never had. From the repo on your computer, with the
Railway CLI installed, logged in and linked to the service
(`npm i -g @railway/cli`, `railway login`, `railway link`):

    $env:ANALYTICS_TOKEN = "<token>"        # PowerShell; bash: export ANALYTICS_TOKEN=...
    python scripts/verify_persistence.py --url https://<domain> --redeploy

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

## 6. Reports

- [ ] Set up the scheduled report (`.github/workflows/analytics-report.yml`;
      setup is in the comment at the top of that file). Set the
      `ANALYTICS_SINCE` variable to the date of this deploy.
- [ ] Run it once by hand: Actions > Analytics report > Run workflow.
      Download the report artifact and check it shows your test visit.

## If something goes wrong

- Turn analytics off without a deploy of new code: set
  `ANALYTICS_ENABLED=0`. The site keeps working; nothing is recorded.
- A broken or full volume never breaks the odds tool: logging fails
  quietly (`Analytics logging failed` in the logs) and the read endpoints
  answer 503.
