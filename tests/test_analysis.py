"""
Tests for the analysis pipeline (analysis/) against a synthetic fixture
database with known answers. Nothing here touches production.

Fixture (all times 18:00 UTC = 10:00 Pacific, so local day == UTC day):
  F   filler visitor: one page_view per day, 2026-02-15 .. 2026-03-01 (15 visits)
  A   desktop/Firefox, US-WA, from google:
        a1 2026-02-16: page_view, heatmap_view (Core), get_table x2 (10 min span)
        a2 2026-02-20: page_view only
  B   mobile/iOS, CA, from reddit (utm_source=reddit):
        b1 2026-03-10: page_view, get_table, ad_view x2
  removed: a bot page_view, an api get_table, a test row, a legacy (v1) row
  kept without a visit: one server-side 422 error event
"""

import json
import os
import shutil
import sqlite3
from pathlib import Path

import pandas as pd
import pytest

import app.analytics as analytics
from analysis import DataQualityError, checks, metrics, pull, report, transform

SINCE, UNTIL = "2026-01-01", "2026-04-01"
TODAY = pd.Timestamp("2026-09-07").date()

DESKTOP = {"country": "US", "region": "WA", "geo_source": "cf-ipcountry", "device_type": "desktop",
           "os_family": "Windows", "browser_family": "Firefox", "browser_major": 155, "is_bot": 0}
MOBILE = {"country": "CA", "region": None, "geo_source": "cf-ipcountry", "device_type": "mobile",
          "os_family": "iOS", "browser_family": "Safari", "browser_major": 17, "is_bot": 0}
BOT = {"device_type": "bot", "os_family": "Windows", "browser_family": "Headless Chrome", "browser_major": 152, "is_bot": 1}

A_CHOICES_1 = [
    {"zone": "Core", "month": 7, "day": 15, "group_size": 4, "predicted": 0.01, "conditioned": 0.01},
    {"zone": "Snow", "month": 6, "day": 16, "group_size": 2, "predicted": 0.32, "conditioned": 0.3168},
]
A_CHOICES_2 = [{"zone": "Core", "month": 8, "day": 6, "group_size": 4, "predicted": 0.02, "conditioned": 0.02}]
B_CHOICES = [{"zone": "Colchuck", "month": 9, "day": 12, "group_size": 2, "predicted": 0.15, "conditioned": 0.15}]

EXPECTED_PULLED = 29
EXPECTED_KEPT = 25
EXPECTED_VISITS = 18


def T(day, hh=18, mm=0):
    return f"{day}T{hh:02d}:{mm:02d}:00+00:00"


def build_fixture(db: Path):
    analytics.init_db(db)

    def ev(event_type, **kw):
        eid = analytics.log_event(event_type, path=db, **kw)
        assert eid is not None, f"fixture insert failed for {event_type}"
        return eid

    days = pd.date_range("2026-02-15", "2026-03-01", freq="D")
    for i, d in enumerate(days):
        ds = d.strftime("%Y-%m-%d")
        ev("page_view", visitor={"visitor_id": "F", "visit_id": f"f-{ds}", "seq_in_visit": 1, "is_new_visitor": i == 0},
           page={"path": "/"}, context=DESKTOP, event_time_utc=T(ds))

    a1 = {"visitor_id": "A", "visit_id": "a1"}
    ev("page_view", visitor={**a1, "seq_in_visit": 1, "is_new_visitor": True},
       page={"path": "/", "referrer": "https://www.google.com/", "viewport_w": 1440}, context=DESKTOP,
       event_time_utc=T("2026-02-16", 18, 0))
    ev("heatmap_view", visitor={**a1, "seq_in_visit": 2}, context=DESKTOP, sim_version="v1.1.1", server_ms=2000,
       detail={"zone": "Core", "group_size": 4, "permit_year": 2027, "month": 7, "from_choice": 1},
       event_time_utc=T("2026-02-16", 18, 2))
    ev("get_table", visitor={**a1, "seq_in_visit": 3}, context=DESKTOP, sim_version="v1.1.1", server_ms=50, client_ms=60,
       detail={"permit_year": 2027, "n_choices": 2}, choices=A_CHOICES_1, permit_year=2027,
       event_time_utc=T("2026-02-16", 18, 4))
    ev("get_table", visitor={**a1, "seq_in_visit": 4}, context=DESKTOP, sim_version="v1.1.1", server_ms=40, client_ms=80,
       detail={"permit_year": 2027, "n_choices": 1}, choices=A_CHOICES_2, permit_year=2027,
       event_time_utc=T("2026-02-16", 18, 10))
    ev("page_view", visitor={"visitor_id": "A", "visit_id": "a2", "seq_in_visit": 1, "is_new_visitor": False},
       page={"path": "/"}, context=DESKTOP, event_time_utc=T("2026-02-20"))

    b1 = {"visitor_id": "B", "visit_id": "b1"}
    ev("page_view", visitor={**b1, "seq_in_visit": 1, "is_new_visitor": True},
       page={"path": "/", "referrer": "https://www.reddit.com/r/hiking", "utm_source": "reddit", "viewport_w": 390},
       context=MOBILE, event_time_utc=T("2026-03-10", 18, 0))
    ev("get_table", visitor={**b1, "seq_in_visit": 2}, context=MOBILE, sim_version="v1.1.1", server_ms=70, client_ms=120,
       detail={"permit_year": 2027, "n_choices": 1}, choices=B_CHOICES, permit_year=2027,
       event_time_utc=T("2026-03-10", 18, 3))
    ev("ad_view", visitor={**b1, "seq_in_visit": 3}, context=MOBILE, detail={"slot": "top"}, event_time_utc=T("2026-03-10", 18, 4))
    ev("ad_view", visitor={**b1, "seq_in_visit": 4}, context=MOBILE, detail={"slot": "bottom"}, event_time_utc=T("2026-03-10", 18, 5))

    # rows the transform must remove
    ev("page_view", visitor={"visitor_id": "BOT", "visit_id": "bot-1", "seq_in_visit": 1}, page={"path": "/"},
       context=BOT, event_time_utc=T("2026-02-18", 18, 0))
    ev("get_table", source="api", context={"device_type": "bot", "is_bot": 1}, server_ms=30,
       choices=B_CHOICES, permit_year=2027, event_time_utc=T("2026-02-18", 18, 1))
    ev("test_debug", source="test", detail={"test": True}, event_time_utc=T("2026-02-18", 18, 2))
    legacy_id = ev("page_view", visitor={"visitor_id": "L", "visit_id": "l-1", "seq_in_visit": 1},
                   page={"path": "/"}, context=DESKTOP, event_time_utc=T("2026-02-18", 18, 3))
    conn = sqlite3.connect(db)
    conn.execute("UPDATE events SET schema_version = 1 WHERE id = ?", (legacy_id,))
    conn.commit()
    conn.close()

    # kept, but not part of any visit
    ev("error", source="page", status="error", context=DESKTOP,
       detail={"endpoint": "/estimate_odds", "method": "POST", "http_status": 422, "message": "validation_error"},
       event_time_utc=T("2026-02-18", 18, 5))
    return db


@pytest.fixture(scope="module")
def fixture_db(tmp_path_factory):
    return build_fixture(tmp_path_factory.mktemp("fixture") / "analytics.db")


@pytest.fixture
def rows(fixture_db):
    return list(pull.fetch_local(fixture_db, SINCE, UNTIL))


@pytest.fixture
def tables(rows):
    return transform.build_tables(rows)


# ---------------------------------------------------------------------------
# pull
# ---------------------------------------------------------------------------

def test_pull_local_caches_and_reuses(fixture_db, tmp_path):
    db = tmp_path / "copy.db"
    shutil.copy(fixture_db, db)
    raw_dir = tmp_path / "raw"
    p = pull.pull("local", SINCE, UNTIL, db_path=db, raw_dir=raw_dir, pull_date=pd.Timestamp("2026-09-07").date())
    assert p == raw_dir / "2026-09-07" / "events_local_20260101_20260401.jsonl"
    rows = pull.read_jsonl(p)
    assert len(rows) == EXPECTED_PULLED
    assert pull.read_meta(p)["rows"] == EXPECTED_PULLED
    assert [r["event_time_utc"] for r in rows] == sorted(r["event_time_utc"] for r in rows)

    db.unlink()   # a second pull must come from the cache, not the database
    assert pull.pull("local", SINCE, UNTIL, db_path=db, raw_dir=raw_dir, pull_date=pd.Timestamp("2026-09-07").date()) == p
    assert pull.find_cached(raw_dir, "local", SINCE, UNTIL) == p
    with pytest.raises(FileNotFoundError):
        pull.pull("local", SINCE, UNTIL, db_path=db, raw_dir=raw_dir, refresh=True)


def test_pull_prod_pages_through_export(fixture_db, rows, tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    import app.main as main
    monkeypatch.setenv("ANALYTICS_DB_PATH", str(fixture_db))
    monkeypatch.setenv("ANALYTICS_TOKEN", "t0k")
    main.limiter.enabled = False
    client = TestClient(main.app)
    calls = []

    def fake_get(url, token, timeout=300):
        calls.append(url)
        r = client.get(url.replace("http://site.test", ""), headers={"Authorization": f"Bearer {token}"})
        assert r.status_code == 200, r.text
        return r.content

    monkeypatch.setattr(pull, "_http_get", fake_get)
    got = list(pull.fetch_prod("http://site.test", "t0k", SINCE, UNTIL, page_size=7))
    assert [r["id"] for r in got] == [r["id"] for r in rows]
    assert len(calls) >= 4  # 29 rows in pages of 7, restarting at the last timestamp

    p = pull.pull("prod", SINCE, UNTIL, base_url="http://site.test", token="t0k", raw_dir=tmp_path)
    assert p.name == "events_prod_20260101_20260401.jsonl" and len(pull.read_jsonl(p)) == EXPECTED_PULLED
    main.limiter.enabled = True


def test_pull_rejects_bad_dates(fixture_db):
    with pytest.raises(ValueError):
        list(pull.fetch_local(fixture_db, "yesterday"))


# ---------------------------------------------------------------------------
# transform
# ---------------------------------------------------------------------------

def test_transform_known_answers(tables):
    assert tables.n_pulled == EXPECTED_PULLED and tables.n_kept == EXPECTED_KEPT
    assert tables.removed == {"legacy_schema": 1, "test_source": 1, "api_source": 1, "bot": 1,
                              "bot_burst": 0, "rate_limited": 0}
    assert tables.all_events["removed_reason"].notna().sum() == 4

    v = tables.visits.set_index("visit_id")
    assert len(v) == EXPECTED_VISITS and tables.visitors["visitor_id"].tolist() == ["F", "A", "B"]
    a1 = v.loc["a1"]
    assert (a1["n_events"], a1["n_page_views"], a1["n_queries"], a1["n_heatmaps"], a1["n_ad_views"]) == (4, 1, 2, 1, 0)
    assert a1["duration_s"] == 600 and a1["is_new_visitor"] == 1 and a1["referrer"] == "https://www.google.com/"
    assert a1["device_type"] == "desktop" and a1["country"] == "US" and bool(a1["in_lottery_window"]) is True
    assert str(a1["day"].date()) == "2026-02-16" and a1["season"] == 2026
    b1 = v.loc["b1"]
    assert (b1["n_queries"], b1["n_ad_views"], b1["device_type"], b1["utm_source"]) == (1, 2, "mobile", "reddit")
    assert bool(b1["in_lottery_window"]) is False
    assert v.loc["a2", "is_new_visitor"] == 0 and v["n_events"].sum() == EXPECTED_KEPT - 1   # the error row has no visit

    vis = tables.visitors.set_index("visitor_id")
    assert vis.loc["A", "n_visits"] == 2 and vis.loc["A", "n_queries"] == 2 and vis.loc["F", "n_visits_in_window"] == 15

    c = tables.choices
    assert len(c) == 4 and c["zone"].tolist() == ["Core", "Snow", "Core", "Colchuck"]
    assert c["permit_date"].dt.strftime("%Y-%m-%d").tolist() == ["2027-07-15", "2027-06-16", "2027-08-06", "2027-09-12"]
    assert c["choice_rank"].tolist() == [1, 2, 1, 1] and c["n_choices"].tolist() == [2, 2, 1, 1]

    nr = tables.null_rates.set_index(["scope", "column"])["null_share"]
    assert nr[("get_table", "client_ms")] == 0 and nr[("all", "country")] == 0
    assert nr[("all", "region")] == pytest.approx(4 / EXPECTED_KEPT)   # the four mobile rows carry no region


def test_transform_handles_empty():
    t = transform.build_tables([])
    assert t.n_pulled == 0 and t.visits.empty and t.choices.empty and t.visitors.empty
    m = metrics.compute_all(t, SINCE, UNTIL)
    assert m["totals"]["visits"] == 0 and m["ads"]["available"] is False and m["demand"]["n_choices"] == 0


# ---------------------------------------------------------------------------
# checks
# ---------------------------------------------------------------------------

def _names(results, ok):
    return {r.name for r in results if r.ok == ok}


def test_checks_pass_on_fixture(tables):
    results = checks.run_checks(tables, SINCE, UNTIL, today=TODAY)
    assert not checks.failures(results), [r.message for r in checks.failures(results)]
    assert "lottery_window_coverage" in _names(results, True)
    assert "legacy_rows" in _names(results, False)     # warn only
    checks.assert_ok(results)


def test_checks_fail_on_missing_window_day(rows):
    kept = [r for r in rows if not r["event_time_utc"].startswith("2026-02-22")]
    results = checks.run_checks(transform.build_tables(kept), SINCE, UNTIL, today=TODAY)
    bad = {r.name: r.message for r in checks.failures(results)}
    assert "lottery_window_coverage" in bad and "2026-02-22" in bad["lottery_window_coverage"]
    with pytest.raises(DataQualityError):
        checks.assert_ok(results)


def test_checks_fail_on_disorder_and_schema(rows):
    shuffled = rows[:5] + rows[10:15] + rows[5:10] + rows[15:]
    results = checks.run_checks(transform.build_tables(shuffled), SINCE, UNTIL, today=TODAY)
    assert "timestamps_ordered" in {r.name for r in checks.failures(results)}

    newer = [dict(r) for r in rows]
    newer[3]["schema_version"] = 3
    results = checks.run_checks(transform.build_tables(newer), SINCE, UNTIL, today=TODAY)
    assert "schema_version" in {r.name for r in checks.failures(results)}

    dup = [dict(r) for r in rows]
    dup[1]["id"] = dup[0]["id"]
    results = checks.run_checks(transform.build_tables(dup), SINCE, UNTIL, today=TODAY)
    assert "ids_unique" in {r.name for r in checks.failures(results)}

    out_of_range = checks.run_checks(transform.build_tables(rows), "2026-02-20", UNTIL, today=TODAY)
    assert "rows_in_range" in {r.name for r in checks.failures(out_of_range)}


def test_checks_skip_window_when_range_has_none(tables):
    results = checks.run_checks(tables, "2026-03-05", UNTIL, today=TODAY)
    cov = next(r for r in results if r.name == "lottery_window_coverage")
    assert cov.ok and cov.severity == "warn"


# ---------------------------------------------------------------------------
# metrics
# ---------------------------------------------------------------------------

def test_metrics_known_answers(tables):
    m = metrics.compute_all(tables, SINCE, UNTIL)
    assert m["totals"] == {"visits": 18, "visitors": 3, "new_visitors": 3, "queries": 3, "page_views": 18, "errors": 1}

    per_day = m["per_day"]
    assert len(per_day) == 90 and per_day.index[0] == pd.Timestamp("2026-01-01")
    assert per_day.loc["2026-02-16", "visits"] == 2 and per_day.loc["2026-02-16", "queries"] == 2
    assert per_day.loc["2026-02-15", "in_lottery_window"] and not per_day.loc["2026-03-02", "in_lottery_window"]
    assert per_day["visits"].sum() == 18 and m["per_season"].loc[2026, "visitors"] == 3
    assert m["per_week"]["visits"].sum() == 18

    w = m["window"]
    assert (w["visits_in_window"], w["visits_total"]) == (17, 18) and w["share_queries"] == pytest.approx(2 / 3)
    assert w["per_season"].loc[2026, "queries_in_window"] == 2

    q = m["queries_per_visit"]
    assert q["mean"] == pytest.approx(3 / 18) and q["max"] == 2
    assert q["share_zero"] == pytest.approx(16 / 18) and q["share_two_plus"] == pytest.approx(1 / 18)
    assert q["histogram"].to_dict() == {"0": 16, "1": 1, "2": 1}
    assert q["median_duration_s"] == pytest.approx(450)   # a1 lasts 600 s, b1 300 s

    d = m["demand"]
    assert d["n_choices"] == 4 and d["n_queries"] == 3 and d["permit_year"] == 2027
    assert d["by_zone"]["count"].to_dict() == {"Core": 2, "Snow": 1, "Colchuck": 1}
    assert d["by_zone"].loc["Core", "share"] == 0.5
    assert d["by_zone_first"]["count"].to_dict() == {"Core": 2, "Colchuck": 1}
    assert d["by_group_size"]["count"].to_dict() == {2: 2, 4: 2}
    assert d["by_month"]["count"].to_dict() == {6: 1, 7: 1, 8: 1, 9: 1}
    assert d["by_zone_date"].shape == (3, 170)
    assert d["by_zone_date"].loc["Core", pd.Timestamp("2027-07-15")] == 1 and d["by_zone_date"].values.sum() == 4
    assert d["top_zone_dates"].iloc[0]["choices"] == 1 and len(d["top_zone_dates"]) == 4
    assert d["visitors_by_zone"].to_dict() == {"Core": 1, "Colchuck": 1, "Snow": 1}

    dv = m["devices"]
    assert dv["device"]["count"].to_dict() == {"desktop": 17, "mobile": 1}
    assert dv["os"]["count"].to_dict() == {"Windows": 17, "iOS": 1}

    r = m["referrers"]
    assert r["share_direct"] == pytest.approx(16 / 18)
    assert r["by_host"]["count"].to_dict() == {"www.google.com": 1, "www.reddit.com": 1}
    assert r["by_utm_source"]["count"].to_dict() == {"reddit": 1}
    assert r["by_country"]["count"].to_dict() == {"US": 17, "CA": 1}

    lat = m["latency"]
    assert lat.loc["query: engine time (server)", ["n", "p50_ms"]].tolist() == [3, 50]
    assert lat.loc["query: round trip seen by the page", "p50_ms"] == 80
    assert lat.loc["heatmap: engine time (server)", ["n", "p50_ms", "p95_ms"]].tolist() == [1, 2000, 2000]

    h = m["heatmap"]
    assert h["n_opens"] == 1 and h["share_visits_with_heatmap"] == pytest.approx(1 / 18)
    assert h["opens_per_using_visit"] == 1 and h["share_using_visits_that_queried"] == 1
    assert h["share_queries_after_heatmap"] == pytest.approx(2 / 3)
    assert h["by_zone"]["count"].to_dict() == {"Core": 1} and h["by_from_choice"]["count"].to_dict() == {1: 1}

    a = m["ads"]
    assert a["available"] and a["n_ad_views"] == 2
    assert a["share_visits_exposed"] == pytest.approx(1 / 18) and a["views_per_exposed_visit"] == 2
    assert a["views_per_query"] == pytest.approx(2 / 3) and a["by_slot"]["count"].to_dict() == {"top": 1, "bottom": 1}


def test_ads_absent_is_handled(rows):
    no_ads = [r for r in rows if r["event_type"] != "ad_view"]
    t = transform.build_tables(no_ads)
    m = metrics.compute_all(t, SINCE, UNTIL)
    assert m["ads"] == {"available": False, "n_ad_views": 0}
    assert t.visits.set_index("visit_id").loc["b1", "n_ad_views"] == 0


# ---------------------------------------------------------------------------
# report and CLI
# ---------------------------------------------------------------------------

def test_report_renders(tables, tmp_path):
    results = checks.run_checks(tables, SINCE, UNTIL, today=TODAY)
    m = metrics.compute_all(tables, SINCE, UNTIL)
    path = report.render_report(tables, results, m, since=SINCE, until=UNTIL, source="local", out_dir=tmp_path,
                                meta={"pulled_at": "2026-09-07T10:00:00+00:00", "origin": "fixture.db", "rows": 29},
                                generated=pd.Timestamp("2026-09-07 12:00").to_pydatetime())
    assert path == tmp_path / "2026-09-07_report_since_2026-01-01.html"
    html = path.read_text(encoding="utf-8")
    assert html.index("Data quality") < html.index("How many people use the tool")   # provenance first
    assert "<b>29</b>rows pulled" in html and "<b>25</b>rows kept" in html
    assert "18 visits from 3 distinct browsers" in html
    assert "Colchuck" in html and "Ad exposure" in html and "2 ad views" in html
    assert "Data-quality checks failed" not in html
    assert html.count("data:image/png;base64,") >= 4

    # ads absent -> graceful section; failed checks -> banner
    no_ads = transform.build_tables([r for r in pull.read_jsonl(_dump(tables, tmp_path)) if r["event_type"] != "ad_view"])
    m2 = metrics.compute_all(no_ads, SINCE, UNTIL)
    bad = checks.run_checks(no_ads, "2026-02-20", UNTIL, today=TODAY)
    p2 = report.render_report(no_ads, bad, m2, since="2026-02-20", until=UNTIL, source="local", out_dir=tmp_path)
    html2 = p2.read_text(encoding="utf-8")
    assert "No <code>ad_view</code> events" in html2 and "Data-quality checks failed" in html2


def _dump(tables, tmp_path):
    p = tmp_path / "rows.jsonl"
    with p.open("w", encoding="utf-8") as f:
        for _, r in tables.all_events.iterrows():
            row = {c: r[c] for c in transform.EVENT_COLUMNS}
            f.write(json.dumps(row, default=str) + "\n")
    return p


def test_cli_end_to_end(fixture_db, tmp_path, capsys):
    from analysis.__main__ import main
    args = ["report", "--since", SINCE, "--until", UNTIL, "--source", "local", "--db", str(fixture_db),
            "--raw-dir", str(tmp_path / "raw"), "--out", str(tmp_path / "reports")]
    assert main(args) == 0
    out = capsys.readouterr().out
    assert "kept 25 of 29 rows" in out and "visits 18, visitors 3, queries 3" in out
    reports = list((tmp_path / "reports").glob("*_report_since_2026-01-01.html"))
    assert len(reports) == 1

    # --no-pull reuses the cache
    assert main(args + ["--no-pull"]) == 0

    # a corrupted pull (rows out of time order) fails loudly and writes nothing
    raw = pull.find_cached(tmp_path / "raw", "local", SINCE, UNTIL)
    rows = pull.read_jsonl(raw)
    raw.write_text("".join(json.dumps(r) + "\n" for r in rows[::-1]), encoding="utf-8")
    bad = args[:-1] + [str(tmp_path / "reports2"), "--no-pull"]
    assert main(bad) == 2
    assert "timestamps_ordered" in capsys.readouterr().err and not (tmp_path / "reports2").exists()
    assert main(bad + ["--allow-failed-checks"]) == 0
    assert "Data-quality checks failed" in next((tmp_path / "reports2").glob("*.html")).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# behavioural bot rule, server 429 rows, legacy rows in the report
# (added 2026-10-16 after the end-to-end run in ANALYTICS_NOTES section 9)
# ---------------------------------------------------------------------------

def _synthetic_rows(n_burst, spacing_s, extra=()):
    """One normal visit plus one visit making n_burst queries spacing_s apart,
    with a desktop Chrome user agent (so the UA rule cannot catch it)."""
    base = pd.Timestamp("2026-05-01T18:00:00Z")
    chrome = {"device_type": "desktop", "os_family": "Windows", "browser_family": "Chrome", "is_bot": 0}
    rows, i = [], 0

    def add(t, etype, visitor, visit, **kw):
        nonlocal i
        i += 1
        rows.append({"id": i, "schema_version": 2, "event_type": etype, "event_time_utc": t.isoformat(),
                     "source": "page", "status": kw.pop("status", "success"), "visitor_id": visitor,
                     "visit_id": visit, "seq_in_visit": kw.pop("seq", None), **chrome,
                     "detail": kw.pop("detail", {}), "choices": kw.pop("choices", [])})

    add(base, "page_view", "H", "h1", seq=1)
    for k in range(3):
        add(base + pd.Timedelta(seconds=40 * (k + 1)), "get_table", "H", "h1", seq=k + 2,
            choices=[{"zone": "Core", "permit_date": "2027-07-15", "group_size": 4 - k, "choice_rank": 1}])
    for k in range(n_burst):
        add(base + pd.Timedelta(seconds=300 + spacing_s * k), "get_table", "S", "s1", seq=k + 1,
            choices=[{"zone": "Core", "permit_date": "2027-07-01", "group_size": 1, "choice_rank": 1}])
    for t, etype, kw in extra:
        add(base + pd.Timedelta(seconds=t), etype, None, None, **kw)
    return rows


def test_burst_visit_removed_even_with_browser_ua():
    t = transform.build_tables(_synthetic_rows(n_burst=30, spacing_s=0.2))
    assert t.removed["bot_burst"] == 30
    assert set(t.visits["visit_id"]) == {"h1"} and len(t.visitors) == 1
    assert int(t.visits["n_queries"].sum()) == 3


def test_paced_iteration_is_not_a_burst():
    # 9 queries in 60 s is below the threshold; 10 spread over 10 minutes is too.
    for n, spacing in ((9, 6.0), (10, 60.0)):
        t = transform.build_tables(_synthetic_rows(n_burst=n, spacing_s=spacing))
        assert t.removed["bot_burst"] == 0, (n, spacing)
        assert set(t.visits["visit_id"]) == {"h1", "s1"}


def test_server_429_rows_removed_but_page_errors_kept():
    rl = {"endpoint": "/estimate_odds", "method": "POST", "http_status": 429, "message": "rate_limited"}
    v422 = {"endpoint": "/estimate_odds", "method": "POST", "http_status": 422, "message": "validation_error"}
    t = transform.build_tables(_synthetic_rows(0, 1, extra=[
        (500, "error", {"status": "error", "detail": rl}),
        (501, "error", {"status": "error", "detail": rl}),
        (502, "error", {"status": "error", "detail": v422}),
    ]))
    assert t.removed["rate_limited"] == 2
    assert int((t.events["event_type"] == "error").sum()) == 1


def test_legacy_rows_listed_in_report_with_original_timestamps(tables, tmp_path):
    results = checks.run_checks(tables, SINCE, UNTIL, today=TODAY)
    m = metrics.compute_all(tables, SINCE, UNTIL)
    html = report.render_report(tables, results, m, since=SINCE, until=UNTIL, source="local",
                                out_dir=tmp_path).read_text(encoding="utf-8")
    leg = tables.all_events[tables.all_events["removed_reason"] == "legacy_schema"]
    assert len(leg) == 1
    assert "Legacy rows (schema v1)" in html
    assert leg["event_time_utc"].iloc[0] in html
