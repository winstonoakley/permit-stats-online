# Analytics notes: what the query logger records, and whether any of it exists

Audit of the visitor-logging path (`log_query_event()` in `app/main.py`,
`query_events` in `odds_database/analytics.db`, the metadata `index.html`
sends, and the three analytics endpoints). Written 2026-09-07 against the
`worktree-data-collection` branch at `f08773f`. No code was changed.

Where the branch being audited differs from `heatmap-calendar` (`343c36d`)
or from what GitHub says is deployed, the difference is called out.

## Summary

- The logger works mechanically: every successful `POST /estimate_odds`
  from the page produces one row with the choices, the full result, a
  browser-lifetime id, a lifetime query counter, and a user-agent string.
- Nothing else is logged: no page loads, no `/heatmap_odds`, no errors, no
  IP or country, no timing. `status` is always `'success'` and `latency_ms`
  is always NULL.
- Every row on this machine (121 across four `analytics.db` files) is local
  development traffic: one Firefox profile, headless Chrome test runs, and
  scripted API calls, all from `127.0.0.1`. There is no real visitor data.
- Production is Railway via the GitHub integration (51 deploy records since
  2025-12-02). The repo contains no Railway config and no volume
  declaration, the analytics path is hard-coded relative to the app
  directory, and the Railway CLI is not installed here, so I could not
  confirm a volume. If there is none, every deploy started with an empty
  database and no production data has ever survived. The last code deploy
  GitHub records is `b453c00` (2026-03-16), which has the logger but none
  of the heatmap or calibration work.
- Three endpoints read or write the analytics database with no
  authentication: `/debug/analytics` dumps the whole table,
  `/analytics/summary` reads it all into memory per request, and
  `/debug/log_test` inserts rows.

## 1. Code path

### 1.1 What the page sends

All of this is in the `Fetch Results` submit handler in `index.html`
(around lines 995 to 1212).

- `getSessionId()` reads `localStorage.sessionId`; if absent it stores
  `crypto.randomUUID()` (fallback `'sess-' + Math.random()`). There is no
  expiry and nothing ever clears it. `localStorage` is per origin, so the
  id identifies **one browser profile on one origin for life**. It is not a
  visit. The same person on a phone and a laptop is two ids; the same
  person on `127.0.0.1:8000` and `127.0.0.1:8765` is two ids (both cases
  are visible in the local data).
- `getNextQueryIndex()` reads `localStorage.queryIndex`, adds 1, writes it
  back, and returns it. It is incremented **before** the fetch, on every
  submit that passes client-side validation, and never resets. So it is a
  lifetime counter per browser, not a position within a visit, and it
  advances even when the request fails or lands on a different server.
  The local data shows both effects: one session runs 1 to 83 across ten
  weeks, with 50 to 52 missing from one database and present in another
  worktree's database, because the same browser talked to two local
  servers on the same port.
- `getDeviceType()` classifies the user agent as `mobile`
  (`/Mobi|Android/`), `tablet` (`/Tablet|iPad/`) or `desktop`. iPads in
  desktop mode report as `desktop`.
- `getOsName()` checks the user agent for `Windows`, then `Mac OS` or
  `Macintosh`, then `Linux`, then `Android`, then iPhone/iPad/iPod. The
  order makes the last two unreachable: every Android UA contains `Linux`
  and every iPhone UA contains `like Mac OS X`, so phones are recorded as
  `Linux` or `macOS`. `device_type` still says `mobile` for them, so the
  mobile/desktop split survives; the OS column does not.
- `browser` is the **raw `navigator.userAgent` string**, not a browser
  name.
- The payload is `permit_year`, `data_years`, `choices[]` (zone, month,
  day, group_size, in priority order), `session_id`, `query_index`,
  `device_type`, `browser`, `os_name`. `latency_ms` is commented out
  ("can be added later") and is never sent.
- The payload is also `console.log`ged on every submit (line 1200).

### 1.2 What the server does with it

`estimate_odds()` in `app/main.py` (line 253):

1. `@limiter.limit("30/minute")` keyed on the client address.
2. Builds `inputs` = `{permit_year, data_years, choices[]}`.
3. Runs `estimate_odds_for_choice_set()`. If this raises, the exception
   propagates: FastAPI returns a 500 and **no row is written**, because the
   logging call comes after it.
4. Copies `session_id`, `query_index`, `device_type`, `browser`, `os_name`,
   `latency_ms` straight from the payload (no validation beyond Pydantic's
   `str | None` / `int | None`), sets `sim_version` to a hard-coded string
   (`"v1.0.0"` on this branch; `"v1.1.1"` on `heatmap-calendar`), and reads
   `referrer` from the HTTP `Referer` header.
5. Calls `log_query_event()` inside `try/except Exception`, printing
   `"Analytics logging failed: ..."` to stdout on failure.

`log_query_event()` (line 60) substitutes `"anonymous"` for a missing
`session_id`, stamps `event_time_utc = datetime.now(timezone.utc)`, and
inserts one row with `event_type = 'get_table'`, `status = 'success'`,
`user_id = country = region = NULL`, `inputs_json` = the inputs dict, and
`results_json` = **the entire engine result** (every choice's
`odds_by_year`, comparable dates, and on `heatmap-calendar` also
`predicted` and `obs_by_year`). Rows are about 2.3 KB each locally.

`init_logging_db()` runs at import: `mkdir odds_database`, then
`executescript(schema.sql)`. The path is `BASE_DIR / "odds_database" /
"analytics.db"`, fixed relative to the code; `ODDS_DB_DIR` only redirects
the odds databases, and nothing redirects the analytics file. One
consequence locally: every git worktree has its own `analytics.db`, and
the development data is split across four of them.

On `heatmap-calendar` only: `OddsRequest` gains `log_event: bool = True`,
and the heatmap's priority-conditioning lookups call `/estimate_odds` with
`log_event: false` so they are not logged. The flag is client-controlled.

### 1.3 What is never logged

| Event | Why not |
|---|---|
| Page loads | `GET /` is a plain `FileResponse`; nothing records it. Visitors who never click Fetch Results are invisible. |
| `/heatmap_odds` | No logging call in the handler. The heatmap's conditioning calls to `/estimate_odds` are explicitly excluded on `heatmap-calendar`. |
| Errors | Engine exceptions raise before the log call (500, no row). Pydantic validation failures (422) never enter the handler. Client-side fetch failures and non-OK responses are shown in the page and not reported. `status` is therefore always `'success'`. |
| Timing | `latency_ms` is never sent by the page and never measured on the server. |
| Where the visitor came from | No IP address is stored, `country`/`region` are never derived. The `Referer` header of a same-origin `fetch` is the page's own URL, so `referrer` is `http://127.0.0.1:8000/` on 100% of local rows and would be the site's own URL in production. `document.referrer` (the external referrer) is never captured. No UTM handling. |
| Anything from `/health`, `/stats`, `/debug/*`, `/analytics/summary` | No logging. |

## 2. Schema

`schema.sql` contains two `CREATE TABLE IF NOT EXISTS query_events`
blocks. They have the same columns; the first declares `session_id TEXT
NOT NULL` and creates `idx_query_events_session_time`, the second declares
`session_id TEXT` (nullable) and creates `idx_query_events_event_time`.
Both create `idx_query_events_event_type`.

`executescript()` runs the statements in order, so the first block wins:
the table is created with `NOT NULL`, and the second `CREATE TABLE` is a
no-op. The index statements are separate and all run. Confirmed on all
four local databases with `sqlite_master` (the stored `CREATE TABLE` text
is the first block, comments and all) and `PRAGMA table_info`:

| Column | notnull |
|---|---|
| `session_id` | 1 |
| `event_time_utc`, `event_type`, `inputs_json`, `results_json`, `status` | 1 |
| everything else | 0 |

Indexes present: `idx_query_events_session_time`,
`idx_query_events_event_type`, `idx_query_events_event_time`.

Does it matter? Not for inserts: `log_query_event()` replaces a missing
`session_id` with `"anonymous"`, so the constraint is never hit. It does
matter for analysis: `"anonymous"` is a real value that collapses every
metadata-less call (direct API calls, scripts, anything that is not the
page) into one fake session, which `/analytics/summary` then counts as one
user in `unique_sessions` and `avg_queries_per_session`. The duplicated
block is also a maintenance trap: a column added to only one block will
silently not exist, and because everything is `IF NOT EXISTS`, there is no
migration path at all; any schema change requires deleting the database.

## 3. The local data

Opened read-only (`?mode=ro`). Four files exist; none is tracked by git.

| File | Rows | Time range (UTC) | Sessions | Notes |
|---|---|---|---|---|
| `permit-stats-online/odds_database/analytics.db` | 69 | 2026-06-29 to 2026-09-07 | 2 | main checkout |
| `.claude/worktrees/site-appearance/…` | 47 | 2026-09-07 only | 3 | |
| `.claude/worktrees/ads/…` | 3 | 2026-09-07 only | 1 | |
| `.claude/worktrees/fix-calibration/…` | 2 | 2026-09-07 only | 1 | |

Details for the main checkout's file, which is the only one with any
history:

- **Event types:** 69 `get_table` / `success`. No `test_debug` rows, so
  `/debug/log_test` has never written to this file. No `error` rows (the
  code cannot produce any).
- **Rows per month:** 2026-06: 32, 2026-07: 14, 2026-08: 0, 2026-09: 23.
  By day: 06-29: 24, 06-30: 8, 07-03: 9, 07-08: 5, 09-06: 1, 09-07: 22.
  Six days of activity in ten weeks, clustered in evening sessions.
- **Null rate per column:** `user_id`, `country`, `region`, `latency_ms`:
  100%. `device_type`, `browser`, `os`, `referrer`,
  `query_index_in_session`: 3 of 69 (the `anonymous` rows). Everything
  else 0%.
- **Sessions:** 2 distinct `session_id` values. One UUID
  (`28c5819f-…`) with 66 rows, `query_index` 1 to 71, first seen
  2026-06-29 and still going on 2026-09-07. `"anonymous"` with 3 rows.
- **Queries per session:** 66 and 3; the summary endpoint would report
  34.5 per session.
- **Device / OS / browser:** every non-anonymous row is `desktop` /
  `Windows` / Firefox on Windows 10 x64; the Firefox version climbs 152 to
  153 to 155 over the ten weeks, i.e. one browser being updated.
- **Referrer:** `http://127.0.0.1:8000/` on all 66 page rows, NULL on the
  3 anonymous rows.
- **`sim_version`:** `v1.0.0` 47, `v1.1.0` 2, `v1.1.1` 20, which just
  tracks which branch the local server was running that day.
- **Duplicates:** 10 distinct input sets were submitted more than once by
  the same session, e.g. Core/Colchuck/Stuart all on Oct 31, group of 4,
  six times; the same three zones on Jul 8 twice; several near-identical
  sets differing by one date. These are repeated clicks while testing.
- **Non-page rows:** the 3 `anonymous` rows were inserted within one
  second of each other (21:56:32Z on 09-07) with no session, device, UA,
  referrer or query index, and `sim_version v1.1.1`. The page always
  sends those fields, so these came from a script or curl.
- **Query index gaps:** 3, 4, 50, 51, 52 are missing. 50 to 52 are in the
  site-appearance worktree's database (same session id, same evening),
  so the browser was pointed at a server running from that worktree.
- **What was queried:** 190 choices across 69 rows. Zones: Core 79,
  Colchuck 45, Stuart 30, Eightmile 24, Snow 14. Group size 4 in 107 of
  190 choices. Three choices filled in 60 of 69 rows. `permit_year` is
  2026 on the 14 rows from June (before `PERMIT_YEAR` was bumped) and
  2027 after.

The other three files add: 28 more `anonymous` rows in site-appearance
(bursts between 21:36Z and 22:51Z on 09-07, including one request with
**eight** choices and three with the server default `permit_year = 2025`,
neither of which the page can send); 3 rows from `HeadlessChrome/152` on
`127.0.0.1:8765` in the ads worktree (an automated browser run, one input
set submitted three times in nine seconds); 4 rows from Chrome on `:8765`
and 15 more from the same Firefox session (`query_index` 50 to 52 and 72
to 83) in site-appearance; and 2 rows from Chrome on `:8000` in
fix-calibration.

**Verdict:** this is local development noise, all of it. Every row is
from `127.0.0.1`, from one human browser profile, headless test browsers,
or direct API calls. There is not one row that could be a visitor. The
earliest row anywhere is 2026-06-29, six months after the logger was
deployed (2025-12-13), so whatever was in earlier local databases was
deleted, and no production export has ever been brought to this machine.

## 4. Production reality

### 4.1 What is in the repo

- `git ls-files` does not include `odds_database/` or `analytics.db`. The
  seven `odds_databases/odds_*.db` files and `schema.sql` are tracked.
- There is **no `.gitignore`** and never has been (nothing in history).
  `git add -A` in any checkout would commit the local `analytics.db`.
- There is no `railway.json`, `railway.toml`, `Procfile`, `Dockerfile`,
  `nixpacks.toml`, start script, `.github/`, or `.env` anywhere in the
  history. `requirements.txt` is `fastapi`, `uvicorn[standard]`,
  `slowapi`. So Railway is building with its buildpack auto-detection and
  the start command lives in the Railway dashboard, not in the repo.
- Environment variables the code reads: `ALLOWED_ORIGIN` (CORS, default
  `*`) and `ODDS_DB_DIR` (odds databases only). Nothing lets you move
  `analytics.db`, which sits at `<app dir>/odds_database/analytics.db`.

### 4.2 What GitHub knows about the deploys

The Railway CLI is not installed on this machine (`railway: command not
found`), so I could not check the service directly. The GitHub API does
record deployments created by `railway-app[bot]` (repo
`winstonoakley/permit-stats-online`, public):

- 51 deployment records, Railway project `zooming-benevolence` (two early
  ones also went to a `satisfied-gratitude` project on 2025-12-05).
- First: 2025-12-02 (`19f65e4`, the initial odds-database upload). Then
  bursts through December 2025, including the four analytics commits on
  2025-12-13 (`8a6f6af`, `9f0a75b`, `6b89151`, `9f8b54d`), two on
  2026-03-16 (`ed89eda`, `b453c00`), and one on **2026-05-24 that
  references `19f65e4` again**, a commit with no app code at all. That
  last record and the `b453c00` one are both marked `inactive` as of
  2026-05-24T21:59:33Z. That could be a rollback, a redeploy from the
  dashboard, or the service being paused or re-created; GitHub cannot
  say, and neither can I. The Railway project page is
  `https://railway.com/project/cfc9d85b-19b2-4c2c-abaf-d5664b15cc9e`.
- `b453c00` is the head of `main` and the newest commit GitHub has seen
  deployed. It has `log_query_event()`, the same `schema.sql`, and the
  page's metadata code, but `data_years` defaults to 2020 to 2024, there
  is no heatmap, and `sim_version` is `v1.0.0`. None of
  `heatmap-calendar` (the 2025/2026 data, the calibration fix, `coregs`)
  is in production according to this record.

### 4.3 Has any production data survived?

Unknown, but the evidence points to no:

- Railway's container filesystem is ephemeral unless a volume is mounted.
  Nothing in the repo declares one, and the code writes to a path
  relative to the app directory, so a volume would have to be mounted at
  exactly `<app root>/odds_database` (typically `/app/odds_database`) by
  hand in the dashboard.
- Without a volume, each of the 51 deploys, plus every restart, started
  from `DB_DIR.mkdir()` and an empty `query_events`.
- The analytics endpoints are public, so a populated database would be
  visible to anyone at `/analytics/summary`, and nothing on this machine
  or in the repo suggests that was ever checked or exported.

To settle it, from the Railway dashboard or a machine with the CLI logged
in (all read-only):

1. `railway volume list` (or the service's Volumes tab): is there a
   volume, and is its mount path `<app root>/odds_database`?
2. `railway variables`: is `ALLOWED_ORIGIN` set? Is there a custom start
   command? Which branch is the service connected to, and is it live?
3. `railway ssh` then `ls -la odds_database/` and
   `sqlite3 odds_database/analytics.db "select count(*), min(event_time_utc), max(event_time_utc), count(distinct session_id) from query_events"`.
4. Or simply `GET https://<production domain>/analytics/summary`, which
   answers the question without credentials (see section 5 for why that
   is also a problem).

## 5. Risk: unauthenticated analytics endpoints

None of these check any credential, and only `/estimate_odds` and
`/heatmap_odds` are rate-limited.

| Endpoint | Method | What it does | Exposure |
|---|---|---|---|
| `/debug/analytics?limit=N` | GET | Returns the newest `N` rows with every column except `user_id`, `country`, `region`. No cap on `limit`, no rate limit. | Anyone can dump the whole table: every visitor's lifetime id, full user-agent string, referrer, every query and every result. Not names or IPs, but `session_id` + UA + timestamps is a persistent browser fingerprint, and `results_json` is the product's output in bulk. |
| `/analytics/summary` | GET | Loads **every** `inputs_json` into memory and parses it on each call. No rate limit. | Discloses usage volume and popular zones/dates. Cost is linear in table size per request, so it is the cheapest way to load the server once the table is large. |
| `/debug/log_test` | POST | Inserts one `test_debug` row. No rate limit. | Anyone can write unlimited rows: pollutes the data and fills the disk (or the volume). |
| `/estimate_odds` | POST | Logs client-supplied `session_id`, `device_type`, `browser`, `os_name`, `query_index` (and `log_event` on `heatmap-calendar`) verbatim. No length limits, no maximum on the `choices` list (an 8-choice request is in the local data). | Anyone can write arbitrary strings into the analytics table and opt out of being logged. The 30/minute limit is per client address; if Railway's proxy address is what `get_remote_address` sees, that is one shared bucket for everyone (verify `X-Forwarded-For` handling). |
| CORS | | `ALLOWED_ORIGIN` defaults to `*` together with `allow_credentials=True`. | Harmless today (no cookies or auth), but it should be set in production. |

`/stats` reads a `stats.db` that does not exist and always answers "No
matching data"; it is dead code, not a risk. `/health` is fine.

## 6. Can the current data answer the questions?

Assuming production data is eventually retained; today none of these can
be answered because there is no production data at all.

| Question | Verdict | Why |
|---|---|---|
| How many people use the tool each lottery season | **Partially** | Only visitors who click Fetch Results are counted (no page-load event). `session_id` is one browser profile for life, so it undercounts people with several devices and overcounts people who clear storage. Distinct `session_id` per season is a usable lower bound on active browsers, nothing more. |
| Which zones, dates and group sizes people consider | **Yes** | `inputs_json` has zone, month, day, group size and priority order for every submitted choice set; `/analytics/summary` already tallies zones, sizes and months. Needs the `anonymous` and developer rows filtered out. |
| Whether people iterate (queries per visit) | **Partially** | `query_index_in_session` is a lifetime counter, not per visit, and there is no visit boundary. Visits can be reconstructed after the fact by sessionising each `session_id`'s timestamps (e.g. a 30-minute gap starts a new visit), which is enough to get queries per visit and what changed between them. |
| Mobile vs desktop | **Partially** | `device_type` is stored and its mobile/desktop split is roughly right; tablets and iPads in desktop mode are misclassified, and `os` is wrong for every phone (recorded as Linux or macOS). The raw UA is stored, so all of this can be re-derived server-side later. |
| Where visitors come from | **No** | No IP, `country`/`region` never set, `referrer` is the same-origin page URL on every row, `document.referrer` and UTM parameters are never captured. |
| How the heatmap is used | **No** | `/heatmap_odds` is not logged, the heatmap's conditioning calls are explicitly excluded, and there are no UI events. (The heatmap is also not in production yet.) |
| Whether the tool is slow | **No** | `latency_ms` is never sent and no server-side timing is recorded. |
