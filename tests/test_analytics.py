"""
Tests for the analytics event store (app/analytics.py) and its wiring in
app/main.py: schema migration, every event type, endpoint auth, and that a
failing analytics write never breaks an odds response.

Run:  python -m pytest tests -q
Set LEGACY_ANALYTICS_DB=<path to a v1 analytics.db> to also test the
migration on a copy of a real file (defaults to odds_database/analytics.db).
"""

import json
import os
import shutil
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import app.analytics as analytics
import app.main as main

ROOT = Path(__file__).resolve().parent.parent

FIREFOX_WIN = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:155.0) Gecko/20100101 Firefox/155.0"
IPHONE = ("Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 "
          "(KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1")
ANDROID_PHONE = ("Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 "
                 "(KHTML, like Gecko) Chrome/125.0.0.0 Mobile Safari/537.36")
ANDROID_TABLET = ("Mozilla/5.0 (Linux; Android 13; SM-X710) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
HEADLESS = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) HeadlessChrome/152.0.0.0 Safari/537.36")

# The original schema (first block of the old schema.sql), used to build a
# synthetic v1 file so the migration test never depends on a local file.
V1_SCHEMA = """
CREATE TABLE IF NOT EXISTS query_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    user_id   TEXT,
    event_time_utc TEXT NOT NULL,
    event_type     TEXT NOT NULL DEFAULT 'get_table',
    country   TEXT,
    region    TEXT,
    device_type TEXT,
    browser   TEXT,
    os        TEXT,
    referrer  TEXT,
    sim_version            TEXT,
    query_index_in_session INTEGER,
    inputs_json  TEXT NOT NULL,
    results_json TEXT NOT NULL,
    latency_ms INTEGER,
    status     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_query_events_session_time ON query_events (session_id, event_time_utc);
CREATE INDEX IF NOT EXISTS idx_query_events_event_type ON query_events (event_type);
"""

QUERY = {
    "permit_year": 2027,
    "data_years": [2022, 2023, 2024, 2025, 2026],
    "choices": [
        {"zone": "Core", "month": 7, "day": 15, "group_size": 4},
        {"zone": "Colchuck", "month": 8, "day": 6, "group_size": 2},
    ],
}
AUTH = {"Authorization": "Bearer test-token"}
IDS = {"visitor_id": "v-1111", "visit_id": "s-2222", "seq_in_visit": 3, "is_new_visitor": False}


# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def db_path(tmp_path, monkeypatch):
    p = tmp_path / "analytics.db"
    monkeypatch.setenv("ANALYTICS_DB_PATH", str(p))
    analytics.init_db()
    return p


@pytest.fixture
def client(db_path, monkeypatch):
    monkeypatch.setenv("ANALYTICS_TOKEN", "test-token")
    main.limiter.enabled = False
    with TestClient(main.app, raise_server_exceptions=False) as c:
        yield c
    main.limiter.enabled = True


def rows(path, sql, *params):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in conn.execute(sql, params)]
    finally:
        conn.close()


def one(path, sql, *params):
    return rows(path, sql, *params)[0]


def user_version(path):
    conn = sqlite3.connect(path)
    try:
        return conn.execute("PRAGMA user_version").fetchone()[0]
    finally:
        conn.close()


def make_v1_db(path, records):
    conn = sqlite3.connect(path)
    conn.executescript(V1_SCHEMA)
    conn.executemany(
        """INSERT INTO query_events (session_id, event_time_utc, event_type, browser, device_type, os,
                                     referrer, sim_version, query_index_in_session, inputs_json,
                                     results_json, latency_ms, status)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        records,
    )
    conn.commit()
    conn.close()


def v1_record(session, when, inputs, results=None, event_type="get_table", qidx=None, ua=FIREFOX_WIN):
    return (session, when, event_type, ua, "desktop", "Windows", "http://127.0.0.1:8000/",
            "v1.1.1", qidx, json.dumps(inputs), json.dumps(results or {}), None, "success")


# ---------------------------------------------------------------------------
# Schema and migration
# ---------------------------------------------------------------------------

def test_fresh_db_is_v2(db_path):
    assert user_version(db_path) == analytics.SCHEMA_VERSION
    names = {r["name"] for r in rows(db_path, "SELECT name FROM sqlite_master WHERE type = 'table'")}
    assert {"events", "query_choices"} <= names
    assert "query_events" not in names
    # a second init is a no-op
    analytics.init_db()
    assert user_version(db_path) == analytics.SCHEMA_VERSION


def test_migrates_synthetic_v1(tmp_path, monkeypatch):
    p = tmp_path / "legacy.db"
    inputs = {"permit_year": 2027, "data_years": [2022, 2023], "choices": QUERY["choices"]}
    results = {"years": [2022, 2023], "choices": [
        {"index": 1, "zone": "Core", "predicted": 0.25},
        {"index": 2, "zone": "Colchuck", "predicted": None},
    ]}
    make_v1_db(p, [
        v1_record("sess-A", "2026-06-29T00:18:51+00:00", inputs, results, qidx=1),
        v1_record("sess-A", "2026-06-29T00:28:51+00:00", inputs, results, qidx=2),   # same visit (10 min)
        v1_record("sess-A", "2026-06-29T03:00:00+00:00", inputs, results, qidx=3),   # new visit (>30 min)
        v1_record("anonymous", "2026-09-07T21:56:32+00:00", inputs, {}, ua=None),
        v1_record("anonymous", "2026-09-07T22:00:00+00:00", {"test": True}, {"message": "x"},
                  event_type="test_debug", ua=None),
    ])
    monkeypatch.setenv("ANALYTICS_DB_PATH", str(p))
    analytics.init_db()

    assert user_version(p) == 2
    names = {r["name"] for r in rows(p, "SELECT name FROM sqlite_master WHERE type = 'table'")}
    assert "query_events_v1" in names and "query_events" not in names
    assert one(p, "SELECT COUNT(*) AS n FROM query_events_v1")["n"] == 5

    ev = rows(p, "SELECT * FROM events ORDER BY id")
    assert len(ev) == 5
    assert all(e["schema_version"] == 1 for e in ev)

    a = [e for e in ev if e["visitor_id"] == "sess-A"]
    assert [e["source"] for e in a] == ["page"] * 3
    assert [e["seq_in_visit"] for e in a] == [1, 2, 1]
    assert a[0]["visit_id"] == a[1]["visit_id"] != a[2]["visit_id"]
    assert [e["is_new_visitor"] for e in a] == [1, 0, 0]
    assert a[0]["device_type"] == "desktop" and a[0]["os_family"] == "Windows"
    assert a[0]["browser_family"] == "Firefox" and a[0]["browser_major"] == 155
    assert json.loads(a[1]["detail_json"])["legacy_query_index"] == 2

    anon = [e for e in ev if e["visitor_id"] is None]
    assert {e["source"] for e in anon} == {"api", "test"}
    assert [e["event_type"] for e in anon] == ["get_table", "test_debug"]
    assert all(e["visit_id"] is None and e["seq_in_visit"] is None for e in anon)

    choices = rows(p, "SELECT * FROM query_choices ORDER BY event_id, choice_rank")
    assert len(choices) == 4 * 2   # four get_table rows with two choices each; test_debug has none
    first = choices[0]
    assert (first["zone"], first["permit_date"], first["group_size"]) == ("Core", "2027-07-15", 4)
    assert first["predicted"] == 0.25 and choices[1]["predicted"] is None

    # migration is idempotent
    analytics.init_db()
    assert one(p, "SELECT COUNT(*) AS n FROM events")["n"] == 5


LEGACY_DB = os.getenv("LEGACY_ANALYTICS_DB") or str(ROOT / "odds_database" / "analytics.db")


def _is_v1_file(path):
    if not os.path.exists(path):
        return False
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        return analytics.current_version(conn) == 1
    finally:
        conn.close()


@pytest.mark.skipif(not _is_v1_file(LEGACY_DB), reason="no v1 analytics.db to migrate (set LEGACY_ANALYTICS_DB)")
def test_migrates_real_local_db(tmp_path, monkeypatch):
    src = Path(LEGACY_DB)
    dst = tmp_path / "real.db"
    shutil.copy(src, dst)   # the original is never opened for writing

    legacy = rows(dst, "SELECT * FROM query_events ORDER BY id")
    expected_choices = sum(len(json.loads(r["inputs_json"]).get("choices", [])) for r in legacy)

    monkeypatch.setenv("ANALYTICS_DB_PATH", str(dst))
    analytics.init_db()

    assert user_version(dst) == 2
    ev = rows(dst, "SELECT * FROM events ORDER BY id")
    assert len(ev) == len(legacy)
    assert one(dst, "SELECT COUNT(*) AS n FROM query_events_v1")["n"] == len(legacy)
    assert {json.loads(e["detail_json"])["legacy_id"] for e in ev} == {r["id"] for r in legacy}
    assert one(dst, "SELECT COUNT(*) AS n FROM query_choices")["n"] == expected_choices
    assert all(e["event_time_utc"] for e in ev)
    assert all(e["schema_version"] == 1 for e in ev)
    # nothing that looks like a raw user agent survived
    assert not any("Mozilla/" in json.dumps(e) for e in ev)
    # anonymous rows became source='api' with no identity
    anon = [e for e in ev if e["source"] == "api"]
    assert all(e["visitor_id"] is None for e in anon)
    assert len(anon) == sum(1 for r in legacy if r["session_id"] == "anonymous")
    # visits are contiguous: seq restarts at 1 whenever the visit id changes
    for visitor in {e["visitor_id"] for e in ev if e["visitor_id"]}:
        seq, last_visit = [], None
        for e in [x for x in ev if x["visitor_id"] == visitor]:
            if e["visit_id"] != last_visit:
                assert e["seq_in_visit"] == 1
                last_visit = e["visit_id"]
            elif seq:
                assert e["seq_in_visit"] == seq[-1] + 1
            seq.append(e["seq_in_visit"])


# ---------------------------------------------------------------------------
# Enrichment
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("ua, expected", [
    (FIREFOX_WIN, ("desktop", "Windows", "Firefox", 155, 0)),
    (IPHONE, ("mobile", "iOS", "Safari", 17, 0)),
    (ANDROID_PHONE, ("mobile", "Android", "Chrome", 125, 0)),
    (ANDROID_TABLET, ("tablet", "Android", "Chrome", 124, 0)),
    (HEADLESS, ("bot", "Windows", "Headless Chrome", 152, 1)),
    ("curl/8.4.0", ("bot", None, None, None, 1)),
    (None, (None, None, None, None, 0)),
])
def test_parse_user_agent(ua, expected):
    d = analytics.parse_user_agent(ua)
    assert (d["device_type"], d["os_family"], d["browser_family"], d["browser_major"], d["is_bot"]) == expected


def test_geo_from_headers(monkeypatch):
    assert analytics.geo_from_headers({"CF-IPCountry": "us", "CF-Region-Code": "WA"}) == \
        {"country": "US", "region": "WA", "geo_source": "cf-ipcountry"}
    assert analytics.geo_from_headers({"CF-IPCountry": "XX"})["country"] is None
    assert analytics.geo_from_headers({"X-Forwarded-For": "203.0.113.9"}) == \
        {"country": None, "region": None, "geo_source": None}
    monkeypatch.setenv("GEO_COUNTRY_HEADER", "X-My-Country")
    assert analytics.geo_from_headers({"x-my-country": "CA"})["country"] == "CA"


def test_clean_referrer():
    assert analytics.clean_referrer("https://www.google.com/search?q=enchantments#x") == "https://www.google.com/search"
    assert analytics.clean_referrer("https://example.org") == "https://example.org/"
    assert analytics.clean_referrer("not a url") is None
    assert analytics.clean_referrer("") is None


# ---------------------------------------------------------------------------
# Event types through the API
# ---------------------------------------------------------------------------

def test_page_view_via_beacon(client, db_path):
    body = {
        "event_type": "page_view",
        "page": {"path": "/", "referrer": "https://www.reddit.com/r/hiking?utm=x",
                 "utm_source": "reddit", "viewport_w": 390},
        **IDS, "is_new_visitor": True,
    }
    r = client.post("/log_event", content=json.dumps(body),
                    headers={"Content-Type": "text/plain", "User-Agent": IPHONE,
                             "CF-IPCountry": "US", "CF-Region-Code": "WA",
                             "X-Forwarded-For": "203.0.113.9"})
    assert r.status_code == 204
    e = one(db_path, "SELECT * FROM events WHERE event_type = 'page_view'")
    assert e["source"] == "page" and e["status"] == "success"
    assert (e["visitor_id"], e["visit_id"], e["seq_in_visit"], e["is_new_visitor"]) == ("v-1111", "s-2222", 3, 1)
    assert e["page_referrer"] == "https://www.reddit.com/r/hiking"
    assert e["utm_source"] == "reddit" and e["viewport_w"] == 390
    assert (e["country"], e["region"], e["geo_source"]) == ("US", "WA", "cf-ipcountry")
    assert (e["device_type"], e["os_family"], e["browser_family"]) == ("mobile", "iOS", "Safari")
    assert "203.0.113.9" not in json.dumps(e)


def test_get_table_logged_with_choices_and_timing(client, db_path):
    r = client.post("/estimate_odds", json={**QUERY, **IDS}, headers={"User-Agent": FIREFOX_WIN})
    assert r.status_code == 200
    data = r.json()
    assert set(data) == {"years", "choices", "event_id"}      # shape unchanged apart from event_id
    assert data["years"] == QUERY["data_years"] and len(data["choices"]) == 2
    assert data["choices"][0]["predicted"] is not None

    e = one(db_path, "SELECT * FROM events WHERE id = ?", data["event_id"])
    assert e["event_type"] == "get_table" and e["source"] == "page"
    assert e["visitor_id"] == "v-1111" and e["seq_in_visit"] == 3
    assert e["sim_version"] == main.SIM_VERSION
    assert e["server_ms"] is not None and e["client_ms"] is None
    assert e["browser_family"] == "Firefox"
    detail = json.loads(e["detail_json"])
    assert detail["n_choices"] == 2 and detail["permit_year"] == 2027
    assert 0 < detail["total_predicted"] <= 1

    ch = rows(db_path, "SELECT * FROM query_choices WHERE event_id = ? ORDER BY choice_rank", data["event_id"])
    assert [(c["choice_rank"], c["zone"], c["permit_date"], c["group_size"]) for c in ch] == \
        [(1, "Core", "2027-07-15", 4), (2, "Colchuck", "2027-08-06", 2)]
    assert ch[0]["predicted"] == pytest.approx(data["choices"][0]["predicted"])
    assert ch[0]["conditioned"] == pytest.approx(ch[0]["predicted"])
    assert ch[1]["conditioned"] == pytest.approx((1 - ch[0]["predicted"]) * ch[1]["predicted"])

    # the timing beacon fills client_ms, but only for the owning visitor
    r = client.post("/log_event", content=json.dumps(
        {"event_type": "timing", "event_id": data["event_id"], "visitor_id": "someone-else", "client_ms": 999}))
    assert r.status_code == 204
    assert one(db_path, "SELECT client_ms FROM events WHERE id = ?", data["event_id"])["client_ms"] is None
    r = client.post("/log_event", content=json.dumps(
        {"event_type": "timing", "event_id": data["event_id"], "visitor_id": "v-1111", "client_ms": 412}))
    assert r.status_code == 204
    assert one(db_path, "SELECT client_ms FROM events WHERE id = ?", data["event_id"])["client_ms"] == 412


def test_get_table_without_identity_is_api_and_log_event_false_is_silent(client, db_path):
    r = client.post("/estimate_odds", json=QUERY)
    assert r.status_code == 200 and r.json()["event_id"] is not None
    assert one(db_path, "SELECT source, visitor_id FROM events WHERE id = ?", r.json()["event_id"]) == \
        {"source": "api", "visitor_id": None}

    r = client.post("/estimate_odds", json={**QUERY, **IDS, "log_event": False})
    assert r.status_code == 200 and r.json()["event_id"] is None
    assert one(db_path, "SELECT COUNT(*) AS n FROM events")["n"] == 1


def test_v1_page_fields_still_accepted(client, db_path):
    legacy = {**QUERY, "session_id": "old-session", "query_index": 57, "device_type": "desktop",
              "browser": FIREFOX_WIN, "os_name": "Windows", "latency_ms": 250}
    r = client.post("/estimate_odds", json=legacy, headers={"User-Agent": ""})
    assert r.status_code == 200
    e = one(db_path, "SELECT * FROM events WHERE id = ?", r.json()["event_id"])
    assert e["visitor_id"] == "old-session" and e["source"] == "page"
    assert e["client_ms"] == 250 and e["browser_family"] == "Firefox"   # UA fallback from the payload
    assert json.loads(e["detail_json"])["legacy_query_index"] == 57


def test_heatmap_view_logged(client, db_path):
    body = {"zone": "Snow", "group_size": 2, "permit_year": 2027, "from_choice": 2, "month": 8, **IDS}
    r = client.post("/heatmap_odds", json=body)
    assert r.status_code == 200
    assert {"zone", "group_size", "permit_year", "data_years", "odds_by_date", "predicted_by_date"} <= set(r.json())
    e = one(db_path, "SELECT * FROM events WHERE event_type = 'heatmap_view'")
    assert e["visitor_id"] == "v-1111" and e["server_ms"] is not None and e["sim_version"] == main.SIM_VERSION
    assert json.loads(e["detail_json"]) == \
        {"zone": "Snow", "group_size": 2, "permit_year": 2027, "month": 8, "from_choice": 2}


def test_heatmap_preload_logs_view_from_page(client, db_path):
    # the page preloads with log_event false, then logs the view when shown
    body = {"zone": "Snow", "group_size": 1, "permit_year": 2027, "log_event": False}
    r = client.post("/heatmap_odds", json=body)
    assert r.status_code == 200
    server_ms = r.json()["server_ms"]
    assert isinstance(server_ms, int)
    assert rows(db_path, "SELECT * FROM events WHERE event_type = 'heatmap_view'") == []

    detail = {"zone": "Snow", "group_size": 3, "permit_year": 2027, "month": 7, "from_choice": 1}
    r = client.post("/log_event", content=json.dumps(
        {"event_type": "heatmap_view", "server_ms": server_ms, "detail": detail, **IDS}))
    assert r.status_code == 204
    e = one(db_path, "SELECT * FROM events WHERE event_type = 'heatmap_view'")
    assert e["visitor_id"] == "v-1111" and e["source"] == "page"
    assert e["server_ms"] == server_ms and e["sim_version"] == main.SIM_VERSION
    assert json.loads(e["detail_json"]) == detail


def test_error_events(client, db_path, monkeypatch):
    # client-side error report
    r = client.post("/log_event", content=json.dumps(
        {"event_type": "error", "detail": {"endpoint": "/estimate_odds", "http_status": None, "message": "fetch_failed"}, **IDS}))
    assert r.status_code == 204

    # validation failure on the API
    r = client.post("/estimate_odds", json={"choices": [{"zone": "Core", "month": 13, "day": 1, "group_size": 4}]})
    assert r.status_code == 422

    # unhandled exception inside the engine
    def boom(**kwargs):
        raise RuntimeError("engine exploded")
    monkeypatch.setattr(main, "estimate_odds_for_choice_set", boom)
    r = client.post("/estimate_odds", json=QUERY)
    assert r.status_code == 500

    errs = rows(db_path, "SELECT * FROM events WHERE event_type = 'error' ORDER BY id")
    assert [e["status"] for e in errs] == ["error"] * 3
    details = [json.loads(e["detail_json"]) for e in errs]
    assert details[0]["message"] == "fetch_failed" and errs[0]["visitor_id"] == "v-1111"
    assert (details[1]["endpoint"], details[1]["http_status"]) == ("/estimate_odds", 422)
    assert (details[2]["endpoint"], details[2]["http_status"], details[2]["message"]) == ("/estimate_odds", 500, "RuntimeError")
    assert all("engine exploded" not in e["detail_json"] for e in errs[1:])


def test_ad_view_accepted_and_bad_beacons_rejected(client, db_path):
    r = client.post("/log_event", content=json.dumps({"event_type": "ad_view", "detail": {"slot": "top"}, **IDS}))
    assert r.status_code == 204
    assert one(db_path, "SELECT event_type FROM events WHERE id = 1")["event_type"] == "ad_view"

    assert client.post("/log_event", content="not json").status_code == 422
    assert client.post("/log_event", content=json.dumps({"event_type": "get_table"})).status_code == 422
    assert client.post("/log_event", content="x" * 9000).status_code == 413
    assert one(db_path, "SELECT COUNT(*) AS n FROM events WHERE event_type = 'ad_view'")["n"] == 1


# ---------------------------------------------------------------------------
# Protected endpoints and export
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("path", ["/debug/analytics", "/analytics/summary", "/analytics/export"])
def test_protected_endpoints_require_token(client, path, monkeypatch):
    assert client.get(path).status_code == 401
    assert client.get(path, headers={"Authorization": "Bearer wrong"}).status_code == 401
    assert client.get(path, headers={"Authorization": "Basic dGVzdC10b2tlbg=="}).status_code == 401
    assert client.get(path, headers={"Authorization": "Bearer test-token"}).status_code == 200
    monkeypatch.delenv("ANALYTICS_TOKEN")
    assert client.get(path, headers={"Authorization": "Bearer test-token"}).status_code == 503


def test_log_test_endpoint_is_gone(client):
    assert client.post("/debug/log_test").status_code in (404, 405)


def test_export_jsonl_and_since_filter(client, db_path):
    auth = {"Authorization": "Bearer test-token"}
    client.post("/estimate_odds", json={**QUERY, **IDS})
    client.post("/log_event", content=json.dumps({"event_type": "page_view", "page": {"path": "/"}, **IDS}))

    r = client.get("/analytics/export", headers=auth)
    assert r.status_code == 200 and r.headers["content-type"].startswith("application/x-ndjson")
    lines = [json.loads(ln) for ln in r.text.strip().splitlines()]
    assert [ln["event_type"] for ln in lines] == ["get_table", "page_view"]
    assert len(lines[0]["choices"]) == 2 and lines[0]["choices"][0]["zone"] == "Core"
    assert lines[0]["detail"]["n_choices"] == 2 and "detail_json" not in lines[0]

    r = client.get("/analytics/export", params={"since": "2999-01-01"}, headers=auth)
    assert r.status_code == 200 and r.text.strip() == ""
    assert client.get("/analytics/export", params={"since": "yesterday"}, headers=auth).status_code == 400

    s = client.get("/analytics/summary", headers=auth).json()
    assert s["events_by_type"] == {"get_table": 1, "page_view": 1}
    assert s["visitors"] == 1 and s["queries"] == 1 and s["top_zones"][0][0] in ("Core", "Colchuck")

    d = client.get("/debug/analytics", params={"limit": 1}, headers=auth).json()
    assert d["row_count"] == 1 and d["rows"][0]["event_type"] == "page_view"


# ---------------------------------------------------------------------------
# Analytics failures never break the site
# ---------------------------------------------------------------------------

def test_failing_analytics_write_does_not_break_responses(client, db_path, monkeypatch, capsys):
    def broken_connect(path=None):
        raise RuntimeError("disk on fire")
    monkeypatch.setattr(analytics, "connect", broken_connect)

    r = client.post("/estimate_odds", json={**QUERY, **IDS})
    assert r.status_code == 200
    assert r.json()["event_id"] is None and len(r.json()["choices"]) == 2

    assert client.post("/heatmap_odds", json={"zone": "Core", "group_size": 4, **IDS}).status_code == 200
    assert client.post("/log_event", content=json.dumps({"event_type": "page_view", **IDS})).status_code == 204
    assert client.post("/log_event", content=json.dumps(
        {"event_type": "timing", "event_id": 1, "visitor_id": "v-1111", "client_ms": 5})).status_code == 204
    assert "Analytics logging failed" in capsys.readouterr().err


def test_analytics_disabled_is_a_no_op(tmp_path, monkeypatch):
    p = tmp_path / "never" / "analytics.db"
    monkeypatch.setenv("ANALYTICS_DB_PATH", str(p))
    monkeypatch.setenv("ANALYTICS_ENABLED", "0")
    monkeypatch.setenv("ANALYTICS_TOKEN", "test-token")
    main.limiter.enabled = False
    try:
        with TestClient(main.app, raise_server_exceptions=False) as c:
            r = c.post("/estimate_odds", json={**QUERY, **IDS})
            assert r.status_code == 200 and r.json()["event_id"] is None and len(r.json()["choices"]) == 2
            assert c.post("/heatmap_odds", json={"zone": "Core", "group_size": 4, **IDS}).status_code == 200
            assert c.post("/log_event", content=json.dumps({"event_type": "page_view", **IDS})).status_code == 204
            assert c.get("/").status_code == 200
            assert c.get("/analytics/summary").status_code == 401   # token still checked first
            assert c.get("/analytics/summary", headers=AUTH).status_code == 503
    finally:
        main.limiter.enabled = True
    assert not p.parent.exists()   # nothing was created on disk


def test_unwritable_db_path_keeps_the_tool_working(tmp_path, monkeypatch):
    blocker = tmp_path / "a_file"
    blocker.write_text("not a directory")
    monkeypatch.setenv("ANALYTICS_DB_PATH", str(blocker / "sub" / "analytics.db"))
    monkeypatch.setenv("ANALYTICS_TOKEN", "test-token")
    main.limiter.enabled = False
    try:
        with TestClient(main.app, raise_server_exceptions=False) as c:
            r = c.post("/estimate_odds", json={**QUERY, **IDS})
            assert r.status_code == 200 and r.json()["event_id"] is None
            assert c.post("/heatmap_odds", json={"zone": "Core", "group_size": 4, **IDS}).status_code == 200
            assert c.post("/log_event", content=json.dumps({"event_type": "page_view", **IDS})).status_code == 204
            for path in ("/analytics/summary", "/analytics/export", "/debug/analytics"):
                assert c.get(path, headers=AUTH).status_code == 503, path
    finally:
        main.limiter.enabled = True


def test_health_reports_process_start(client):
    body = client.get("/health").json()
    assert body["status"] == "ok" and body["started_at"] == main.STARTED_AT
    assert "app_version" in body
