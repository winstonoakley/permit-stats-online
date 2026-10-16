"""
Prove that analytics events survive a redeploy.

    python scripts/verify_persistence.py --url https://<your-domain> [--redeploy]

Needs ANALYTICS_TOKEN in the environment (or --token). Standard library only,
so it runs from any Python 3.9+ without installing anything.

Steps:
  1. GET /health and remember started_at (changes when the process restarts).
  2. POST /log_event with a unique visitor id. The user agent says "bot", so
     the event is flagged is_bot and never appears in reports.
  3. Find it through /analytics/export, check the database path is on the
     volume, and note the event id and total event count.
  4. Redeploy: with --redeploy, runs `railway redeploy -y` (Railway CLI,
     linked to the service); otherwise asks you to press Redeploy in the
     dashboard.
  5. Wait until /health reports a new started_at.
  6. Export again: the same event id must be there, with the same
     timestamp, and the total must not have gone down.

Exit code 0 = PASS, 1 = FAIL, 2 = could not run.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid

UA = "deploy-check-bot/1.0 (persistence test)"


def http(method, url, token=None, body=None, timeout=30):
    req = urllib.request.Request(url, method=method, data=body)
    req.add_header("User-Agent", UA)
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    if body is not None:
        req.add_header("Content-Type", "text/plain")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read().decode("utf-8")


def health(base):
    try:
        _, text = http("GET", base + "/health", timeout=10)
        return json.loads(text)
    except Exception:
        return None


def export_since(base, token, since):
    _, text = http("GET", f"{base}/analytics/export?since={since}&limit=100000", token=token, timeout=120)
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def summary(base, token):
    _, text = http("GET", base + "/analytics/summary", token=token)
    return json.loads(text)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", required=True, help="site base URL, e.g. https://enchantmentsodds.com")
    ap.add_argument("--token", default=os.getenv("ANALYTICS_TOKEN"), help="default: $ANALYTICS_TOKEN")
    ap.add_argument("--redeploy", action="store_true", help="run `railway redeploy -y` instead of asking you to")
    ap.add_argument("--service", help="Railway service name, passed to `railway redeploy -s`")
    ap.add_argument("--expect-db-prefix", default="/data/", help="the database must live under this path (default /data/)")
    ap.add_argument("--wait", type=int, default=600, help="seconds to wait for the new deployment (default 600)")
    args = ap.parse_args(argv)
    base = args.url.rstrip("/")
    if not args.token:
        print("ANALYTICS_TOKEN is not set (or pass --token).")
        return 2

    # 1. current process
    h0 = health(base)
    if not h0:
        print(f"{base}/health does not answer.")
        return 2
    if "started_at" not in h0:
        print("/health has no started_at: the deployed code predates this script. Deploy the analytics branch first.")
        return 2
    print(f"[1] live, started_at {h0['started_at']}, app_version {h0.get('app_version')}")

    # 2. write a marker event
    marker = f"deploy-check-{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:6]}"
    since = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1)).strftime("%Y-%m-%d")
    body = json.dumps({"event_type": "page_view", "visitor_id": marker, "visit_id": marker,
                       "seq_in_visit": 1, "page": {"path": "/deploy-check"}}).encode()
    status, _ = http("POST", base + "/log_event", body=body)
    print(f"[2] POST /log_event -> {status}, visitor_id {marker}")

    # 3. read it back
    try:
        s0 = summary(base, args.token)
    except urllib.error.HTTPError as e:
        print(f"/analytics/summary -> {e.code}: check ANALYTICS_TOKEN (401), that it is set on Railway (503), "
              f"or ANALYTICS_ENABLED.")
        return 2
    found = [e for e in export_since(base, args.token, since) if e.get("visitor_id") == marker]
    if not found:
        print("FAIL: the marker event is not in the export right after writing it. Check the server logs for "
              "'Analytics logging failed' / 'Analytics init failed'.")
        return 1
    ev = found[0]
    db = s0.get("db_path", "")
    print(f"[3] stored: event id {ev['id']} at {ev['event_time_utc']}; db_path {db}; events_total {s0['events_total']}")
    if not db.startswith(args.expect_db_prefix):
        print(f"FAIL: db_path {db!r} is not under {args.expect_db_prefix!r}. ANALYTICS_DB_PATH is not pointing at the "
              f"volume, so this data would be lost on the next deploy. Fix it before going on.")
        return 1

    # 4. redeploy
    if args.redeploy:
        exe = shutil.which("railway")
        if not exe:
            print("Railway CLI not found on PATH (npm i -g @railway/cli, then `railway login` and `railway link`).")
            return 2
        cmd = [exe, "redeploy", "-y"] + (["-s", args.service] if args.service else [])
        print(f"[4] running: {' '.join(cmd)}")
        r = subprocess.run(cmd)
        if r.returncode != 0:
            print("railway redeploy failed.")
            return 2
    else:
        input("[4] Now redeploy the service (Railway dashboard > the service > Deployments > ... > Redeploy). "
              "Press Enter once you have clicked it... ")

    # 5. wait for a new process
    deadline = time.time() + args.wait
    print("[5] waiting for a new started_at", end="", flush=True)
    h1 = None
    while time.time() < deadline:
        h1 = health(base)
        if h1 and h1.get("started_at") != h0["started_at"]:
            break
        print(".", end="", flush=True)
        time.sleep(5)
    print()
    if not h1 or h1.get("started_at") == h0["started_at"]:
        print(f"Gave up after {args.wait}s: the site is still the old process. Re-run once the redeploy is live.")
        return 2
    print(f"    new process started_at {h1['started_at']}")

    # 6. still there?
    s1 = summary(base, args.token)
    after = [e for e in export_since(base, args.token, since) if e.get("visitor_id") == marker]
    same = bool(after) and after[0]["id"] == ev["id"] and after[0]["event_time_utc"] == ev["event_time_utc"]
    print(f"[6] after redeploy: marker {'found' if after else 'MISSING'}"
          + (f" (id {after[0]['id']})" if after else "")
          + f"; events_total {s0['events_total']} -> {s1['events_total']}; db_path {s1.get('db_path')}")
    if same and s1["events_total"] >= s0["events_total"]:
        print("\nPASS: events survive a redeploy.")
        return 0
    print("\nFAIL: the database did not survive the redeploy. The volume is missing, mounted at a different path "
          "than ANALYTICS_DB_PATH, or attached to another service.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
