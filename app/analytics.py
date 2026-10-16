"""
Analytics event store for the permit odds tool.

One SQLite file (ANALYTICS_DB_PATH, default <repo>/odds_database/analytics.db)
holding the two tables defined in schema.sql: `events`, one row per event of
any type, and `query_choices`, one row per submitted choice of a get_table
event. The schema is versioned with PRAGMA user_version; init_db() migrates
an older file forward (v1 = the original query_events table, which is kept
as query_events_v1) and is safe to call on every start.

Every write is best-effort: log_event() and set_client_ms() catch all
exceptions, print them, and return None, so analytics can never break an
odds response.

Privacy: no IP address and no raw user-agent string are ever stored.
Country/region come from the proxy's geolocation headers when present, and
the user agent is reduced here to coarse device, OS and browser families.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import re
import sqlite3
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Iterator, List, Mapping, Optional
from urllib.parse import urlsplit, urlunsplit

BASE_DIR = Path(__file__).resolve().parent.parent
SCHEMA_PATH = BASE_DIR / "schema.sql"
DEFAULT_DB_PATH = BASE_DIR / "odds_database" / "analytics.db"

SCHEMA_VERSION = 2
KNOWN_EVENT_TYPES = ("page_view", "get_table", "heatmap_view", "error", "ad_view")
VISIT_GAP = dt.timedelta(minutes=30)   # the page uses the same gap for its visit id
ERROR_EVENTS_PER_MINUTE = 120          # cap on stored error rows so bad requests cannot fill the disk

EVENT_COLUMNS = (
    "schema_version", "event_type", "event_time_utc", "source", "status",
    "visitor_id", "visit_id", "seq_in_visit", "is_new_visitor",
    "page_path", "page_referrer", "utm_source", "utm_medium", "utm_campaign", "viewport_w",
    "country", "region", "geo_source",
    "device_type", "os_family", "browser_family", "browser_major", "is_bot",
    "app_version", "sim_version", "server_ms", "client_ms",
    "detail_json",
)
CHOICE_COLUMNS = ("choice_rank", "zone", "permit_date", "month", "day", "group_size", "predicted", "conditioned")

# Proxy headers that carry the visitor's country / region. Railway's own
# proxy sets none of these; Cloudflare (CF-*), Vercel and CloudFront do.
# GEO_COUNTRY_HEADER / GEO_REGION_HEADER add one more name each.
COUNTRY_HEADERS = ("cf-ipcountry", "x-vercel-ip-country", "cloudfront-viewer-country", "x-country-code")
REGION_HEADERS = ("cf-region-code", "x-vercel-ip-country-region", "cloudfront-viewer-country-region", "x-region-code")


# ---------------------------------------------------------------------------
# Connection and schema
# ---------------------------------------------------------------------------

def enabled() -> bool:
    """ANALYTICS_ENABLED=0/false/off turns every write into a no-op and stops
    init_db() from touching the disk. Read on every call, like db_path()."""
    return os.getenv("ANALYTICS_ENABLED", "1").strip().lower() not in ("0", "false", "no", "off")


def db_path() -> Path:
    """Where the database lives; read from the environment on every call so
    tests and deployments can point it elsewhere without an import cycle."""
    return Path(os.getenv("ANALYTICS_DB_PATH") or DEFAULT_DB_PATH)


def connect(path: Optional[os.PathLike | str] = None) -> sqlite3.Connection:
    p = Path(path) if path else db_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    # Autocommit mode; multi-statement writes use explicit BEGIN/COMMIT.
    conn = sqlite3.connect(p, timeout=5, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _schema_statements() -> List[str]:
    text = SCHEMA_PATH.read_text(encoding="utf-8")
    lines = [ln.split("--", 1)[0] for ln in text.splitlines()]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (name,)
    ).fetchone() is not None


def current_version(conn: sqlite3.Connection) -> int:
    """0 = empty file, 1 = legacy query_events table, 2+ = PRAGMA user_version."""
    v = conn.execute("PRAGMA user_version").fetchone()[0]
    if v:
        return int(v)
    return 1 if _table_exists(conn, "query_events") else 0


def init_db(path: Optional[os.PathLike | str] = None) -> Path:
    """Create or migrate the database. Idempotent; one transaction."""
    conn = connect(path)
    try:
        version = current_version(conn)
        conn.execute("BEGIN IMMEDIATE")
        try:
            for stmt in _schema_statements():
                conn.execute(stmt)
            for target, step in MIGRATIONS:
                if version < target:
                    step(conn)
                    version = target
            conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
    finally:
        conn.close()
    return Path(path) if path else db_path()


# ---------------------------------------------------------------------------
# Migration v1 -> v2: import query_events rows, keep the table as query_events_v1
# ---------------------------------------------------------------------------

def _import_v1_rows(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "query_events"):
        return
    rows = conn.execute("SELECT * FROM query_events ORDER BY event_time_utc, id").fetchall()
    visits: Dict[str, Dict[str, Any]] = {}
    seen: set = set()
    for raw in rows:
        r = dict(raw)
        sid = r.get("session_id")
        visitor = sid if sid and sid != "anonymous" else None
        etype = r.get("event_type") or "get_table"
        source = "test" if etype == "test_debug" else ("page" if visitor else "api")
        when = _parse_iso(r.get("event_time_utc"))

        visit_id = seq = is_new = None
        if visitor:
            is_new = 0 if visitor in seen else 1
            seen.add(visitor)
            if when is not None:
                st = visits.get(visitor)
                if st is None or when - st["last"] > VISIT_GAP:
                    st = {"id": str(uuid.uuid4()), "last": when, "seq": 0}
                    visits[visitor] = st
                st["seq"] += 1
                st["last"] = when
                visit_id, seq = st["id"], st["seq"]

        inputs = _loads(r.get("inputs_json")) or {}
        results = _loads(r.get("results_json")) or {}
        choices_in = inputs.get("choices") if isinstance(inputs.get("choices"), list) else []
        predicted_by_index: Dict[Any, Any] = {}
        for c in results.get("choices") or []:
            if isinstance(c, dict) and c.get("index") is not None:
                predicted_by_index[c["index"]] = c.get("predicted")
        choices = []
        for i, c in enumerate(choices_in, start=1):
            if isinstance(c, dict):
                choices.append({
                    "zone": c.get("zone"), "month": c.get("month"), "day": c.get("day"),
                    "group_size": c.get("group_size"),
                    "predicted": predicted_by_index.get(i), "conditioned": None,
                })

        detail: Dict[str, Any] = {
            "permit_year": inputs.get("permit_year"),
            "data_years": inputs.get("data_years"),
            "n_choices": len(choices),
            "legacy_id": r.get("id"),
            "legacy_query_index": r.get("query_index_in_session"),
        }
        if source == "test":
            detail["inputs"] = inputs

        row = {
            "schema_version": 1,
            "event_type": etype,
            "event_time_utc": r.get("event_time_utc") or _now_iso(),
            "source": source,
            "status": r.get("status") if r.get("status") in ("success", "error") else "success",
            "visitor_id": visitor,
            "visit_id": visit_id,
            "seq_in_visit": seq,
            "is_new_visitor": is_new,
            "sim_version": r.get("sim_version"),
            "client_ms": r.get("latency_ms"),
            "detail_json": _dumps(detail),
            **parse_user_agent(r.get("browser")),
        }
        event_id = _insert_event(conn, row)
        _insert_choices(conn, event_id, inputs.get("permit_year"), choices)

    conn.execute("ALTER TABLE query_events RENAME TO query_events_v1")


MIGRATIONS = [(2, _import_v1_rows)]


# ---------------------------------------------------------------------------
# Request enrichment: geography from headers, device from the user agent
# ---------------------------------------------------------------------------

_COUNTRY_RE = re.compile(r"^[A-Z]{2}$")
_REGION_RE = re.compile(r"^[A-Za-z0-9 _.\-]{1,32}$")
_UNKNOWN_COUNTRIES = {"XX", "T1", "ZZ"}


def _lower_headers(headers: Mapping[str, str]) -> Dict[str, str]:
    return {str(k).lower(): v for k, v in headers.items()}


def geo_from_headers(headers: Mapping[str, str]) -> Dict[str, Any]:
    """Country/region from a proxy geolocation header. Never looks at the
    forwarded IP address."""
    h = _lower_headers(headers)
    country_names = tuple(filter(None, (os.getenv("GEO_COUNTRY_HEADER", "").lower(),))) + COUNTRY_HEADERS
    region_names = tuple(filter(None, (os.getenv("GEO_REGION_HEADER", "").lower(),))) + REGION_HEADERS

    country = region = source = None
    for name in country_names:
        val = (h.get(name) or "").strip().upper()
        if _COUNTRY_RE.match(val) and val not in _UNKNOWN_COUNTRIES:
            country, source = val, name
            break
    for name in region_names:
        val = (h.get(name) or "").strip()
        if _REGION_RE.match(val):
            region = val
            source = source or name
            break
    return {"country": country, "region": region, "geo_source": source}


_BOT_RE = re.compile(
    r"bot|crawl|spider|slurp|headless|lighthouse|pagespeed|curl/|wget/|python-requests|"
    r"python-urllib|httpx/|go-http-client|java/|okhttp|scrapy|facebookexternalhit|preview",
    re.I,
)
_BROWSERS = (
    ("Edge", re.compile(r"Edg(?:e|A|iOS)?/(\d+)")),
    ("Opera", re.compile(r"(?:OPR|Opera)/(\d+)")),
    ("Samsung Internet", re.compile(r"SamsungBrowser/(\d+)")),
    ("Firefox", re.compile(r"(?:Firefox|FxiOS)/(\d+)")),
    ("Chrome", re.compile(r"(?:HeadlessChrome|Chrome|CriOS)/(\d+)")),
    ("Safari", re.compile(r"Version/(\d+)[^ ]* .*Safari/")),
)


def parse_user_agent(ua: Optional[str]) -> Dict[str, Any]:
    """Reduce a user-agent string to device_type, os_family, browser_family,
    browser_major and is_bot. The string itself is never stored."""
    out: Dict[str, Any] = {
        "device_type": None, "os_family": None, "browser_family": None,
        "browser_major": None, "is_bot": 0,
    }
    if not ua or not isinstance(ua, str):
        return out
    ua = ua[:512]
    is_bot = bool(_BOT_RE.search(ua))

    # Order matters: iPhone UAs contain "like Mac OS X", Android UAs contain "Linux".
    if "Windows" in ua:
        os_family = "Windows"
    elif re.search(r"iPhone|iPad|iPod", ua):
        os_family = "iOS"
    elif "Android" in ua:
        os_family = "Android"
    elif "Mac OS X" in ua or "Macintosh" in ua:
        os_family = "macOS"
    elif "CrOS" in ua:
        os_family = "ChromeOS"
    elif "Linux" in ua:
        os_family = "Linux"
    else:
        os_family = None

    browser_family = browser_major = None
    for family, pattern in _BROWSERS:
        m = pattern.search(ua)
        if m:
            browser_family, browser_major = family, int(m.group(1))
            break
    if "HeadlessChrome" in ua:
        browser_family = "Headless Chrome"

    if is_bot:
        device = "bot"
    elif re.search(r"iPad|Tablet", ua) or ("Android" in ua and "Mobile" not in ua):
        device = "tablet"
    elif re.search(r"Mobi|iPhone|iPod|Android", ua):
        device = "mobile"
    elif os_family in ("Windows", "macOS", "Linux", "ChromeOS"):
        device = "desktop"
    else:
        device = "other"

    out.update(device_type=device, os_family=os_family, browser_family=browser_family,
               browser_major=browser_major, is_bot=int(is_bot))
    return out


def context_from_headers(headers: Mapping[str, str], ua_fallback: Optional[str] = None) -> Dict[str, Any]:
    """Everything log_event() wants from a request: geo + parsed device."""
    h = _lower_headers(headers)
    ctx = geo_from_headers(h)
    ctx.update(parse_user_agent(h.get("user-agent") or ua_fallback))
    return ctx


def clean_referrer(url: Optional[str]) -> Optional[str]:
    """Origin + path only; the query string and fragment are dropped."""
    if not url or not isinstance(url, str):
        return None
    try:
        parts = urlsplit(url.strip()[:1024])
    except ValueError:
        return None
    if not parts.scheme or not parts.netloc:
        return None
    return urlunsplit((parts.scheme, parts.netloc, parts.path or "/", "", ""))[:512]


# ---------------------------------------------------------------------------
# Writing events
# ---------------------------------------------------------------------------

_error_budget = {"minute": -1, "count": 0}
_error_lock = threading.Lock()


def _error_budget_ok() -> bool:
    minute = int(time.time() // 60)
    with _error_lock:
        if _error_budget["minute"] != minute:
            _error_budget["minute"], _error_budget["count"] = minute, 0
        _error_budget["count"] += 1
        return _error_budget["count"] <= ERROR_EVENTS_PER_MINUTE


def log_event(
    event_type: str,
    *,
    source: str = "page",
    status: str = "success",
    visitor: Optional[Mapping[str, Any]] = None,
    page: Optional[Mapping[str, Any]] = None,
    context: Optional[Mapping[str, Any]] = None,
    app_version: Optional[str] = None,
    sim_version: Optional[str] = None,
    server_ms: Optional[int] = None,
    client_ms: Optional[int] = None,
    detail: Optional[Mapping[str, Any]] = None,
    choices: Optional[List[Mapping[str, Any]]] = None,
    permit_year: Optional[int] = None,
    event_time_utc: Optional[str] = None,
    path: Optional[os.PathLike | str] = None,
) -> Optional[int]:
    """
    Insert one event (and its query_choices rows, if any). Returns the new
    event id, or None if anything went wrong; never raises.

    visitor: visitor_id, visit_id, seq_in_visit, is_new_visitor
    page:    path, referrer, utm_source, utm_medium, utm_campaign, viewport_w
    context: output of context_from_headers()
    choices: dicts with zone, month, day, group_size, predicted, conditioned
    """
    if not enabled():
        return None
    try:
        if event_type == "error" and not _error_budget_ok():
            return None
        visitor = visitor or {}
        page = page or {}
        context = context or {}
        row: Dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "event_type": _clip(event_type, 32),
            "event_time_utc": event_time_utc or _now_iso(),
            "source": source if source in ("page", "api", "test") else "api",
            "status": status if status in ("success", "error") else "success",
            "visitor_id": _clip(visitor.get("visitor_id"), 64),
            "visit_id": _clip(visitor.get("visit_id"), 64),
            "seq_in_visit": _int(visitor.get("seq_in_visit")),
            "is_new_visitor": _bool01(visitor.get("is_new_visitor")),
            "page_path": _clip(page.get("path"), 256),
            "page_referrer": clean_referrer(page.get("referrer")),
            "utm_source": _clip(page.get("utm_source"), 128),
            "utm_medium": _clip(page.get("utm_medium"), 128),
            "utm_campaign": _clip(page.get("utm_campaign"), 128),
            "viewport_w": _int(page.get("viewport_w")),
            "country": context.get("country"),
            "region": context.get("region"),
            "geo_source": context.get("geo_source"),
            "device_type": context.get("device_type"),
            "os_family": context.get("os_family"),
            "browser_family": context.get("browser_family"),
            "browser_major": _int(context.get("browser_major")),
            "is_bot": _bool01(context.get("is_bot")) or 0,
            "app_version": _clip(app_version, 64),
            "sim_version": _clip(sim_version, 32),
            "server_ms": _int(server_ms),
            "client_ms": _int(client_ms),
            "detail_json": _dumps(dict(detail)) if detail else None,
        }
        conn = connect(path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                event_id = _insert_event(conn, row)
                if choices:
                    _insert_choices(conn, event_id, permit_year, choices)
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()
        return event_id
    except Exception as e:  # analytics must never break the caller
        print(f"Analytics logging failed ({event_type}): {e}", file=sys.stderr)
        return None


def set_client_ms(event_id: int, visitor_id: str, client_ms: int,
                  path: Optional[os.PathLike | str] = None) -> bool:
    """Fill in the page-measured latency for an event this visitor owns.
    Best-effort; returns True if a row was updated."""
    if not enabled():
        return False
    try:
        if not visitor_id or _int(event_id) is None or _int(client_ms) is None:
            return False
        conn = connect(path)
        try:
            cur = conn.execute(
                "UPDATE events SET client_ms = ? WHERE id = ? AND visitor_id = ? AND client_ms IS NULL",
                (int(client_ms), int(event_id), visitor_id),
            )
            return cur.rowcount > 0
        finally:
            conn.close()
    except Exception as e:
        print(f"Analytics logging failed (timing): {e}", file=sys.stderr)
        return False


def _insert_event(conn: sqlite3.Connection, row: Mapping[str, Any]) -> int:
    cols = [c for c in EVENT_COLUMNS if c in row]
    sql = f"INSERT INTO events ({', '.join(cols)}) VALUES ({', '.join('?' for _ in cols)})"
    cur = conn.execute(sql, [row[c] for c in cols])
    return int(cur.lastrowid)


def _insert_choices(conn: sqlite3.Connection, event_id: int, permit_year: Optional[int],
                    choices: List[Mapping[str, Any]]) -> None:
    rank = 0
    for c in choices:
        zone = _clip(c.get("zone"), 64)
        month, day, group_size = _int(c.get("month")), _int(c.get("day")), _int(c.get("group_size"))
        if not zone or month is None or day is None or group_size is None:
            continue
        rank += 1
        permit_date = None
        if _int(permit_year) is not None:
            try:
                permit_date = dt.date(int(permit_year), month, day).isoformat()
            except ValueError:
                permit_date = None
        conn.execute(
            f"INSERT INTO query_choices ({', '.join(CHOICE_COLUMNS)}, event_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (rank, zone, permit_date, month, day, group_size,
             _float(c.get("predicted")), _float(c.get("conditioned")), event_id),
        )


# ---------------------------------------------------------------------------
# Reading events
# ---------------------------------------------------------------------------

def iter_events(
    since: Optional[str] = None,
    until: Optional[str] = None,
    limit: int = 10000,
    order: str = "asc",
    path: Optional[os.PathLike | str] = None,
) -> Iterator[Dict[str, Any]]:
    """Events as dicts with `detail` parsed and `choices` attached, oldest
    first by default. `since` is inclusive, `until` exclusive, both ISO 8601."""
    conn = connect(path)
    try:
        sql = "SELECT * FROM events WHERE 1 = 1"
        params: List[Any] = []
        if since:
            sql += " AND event_time_utc >= ?"
            params.append(since)
        if until:
            sql += " AND event_time_utc < ?"
            params.append(until)
        direction = "DESC" if order == "desc" else "ASC"
        sql += f" ORDER BY event_time_utc {direction}, id {direction} LIMIT ?"
        params.append(int(limit))
        for r in conn.execute(sql, params):
            ev = dict(r)
            ev["detail"] = _loads(ev.pop("detail_json"))
            ev["choices"] = [
                dict(c) for c in conn.execute(
                    f"SELECT {', '.join(CHOICE_COLUMNS)} FROM query_choices WHERE event_id = ? ORDER BY choice_rank",
                    (ev["id"],),
                )
            ]
            yield ev
    finally:
        conn.close()


def summary(path: Optional[os.PathLike | str] = None) -> Dict[str, Any]:
    """High-level counts for the protected summary endpoint."""
    conn = connect(path)
    try:
        q = lambda sql, *p: conn.execute(sql, p).fetchall()
        one = lambda sql, *p: conn.execute(sql, p).fetchone()[0]
        page = "source = 'page' AND is_bot = 0"
        return {
            "db_path": str(Path(path) if path else db_path()),
            "schema_version": current_version(conn),
            "events_total": one("SELECT COUNT(*) FROM events"),
            "events_by_type": {r[0]: r[1] for r in q("SELECT event_type, COUNT(*) FROM events GROUP BY 1")},
            "events_by_source": {r[0]: r[1] for r in q("SELECT source, COUNT(*) FROM events GROUP BY 1")},
            "first_event": one("SELECT MIN(event_time_utc) FROM events"),
            "last_event": one("SELECT MAX(event_time_utc) FROM events"),
            "visitors": one(f"SELECT COUNT(DISTINCT visitor_id) FROM events WHERE {page}"),
            "visits": one(f"SELECT COUNT(DISTINCT visit_id) FROM events WHERE {page}"),
            "queries": one(f"SELECT COUNT(*) FROM events WHERE event_type = 'get_table' AND {page}"),
            "page_views": one(f"SELECT COUNT(*) FROM events WHERE event_type = 'page_view' AND {page}"),
            "device_types": {r[0]: r[1] for r in q(f"SELECT device_type, COUNT(*) FROM events WHERE {page} GROUP BY 1")},
            "countries": {r[0]: r[1] for r in q(
                f"SELECT country, COUNT(*) FROM events WHERE {page} GROUP BY 1 ORDER BY 2 DESC LIMIT 20")},
            "top_zones": [[r[0], r[1]] for r in q(
                f"SELECT c.zone, COUNT(*) FROM query_choices c JOIN events e ON e.id = c.event_id "
                f"WHERE e.{page} GROUP BY 1 ORDER BY 2 DESC")],
            "group_sizes": {r[0]: r[1] for r in q(
                f"SELECT c.group_size, COUNT(*) FROM query_choices c JOIN events e ON e.id = c.event_id "
                f"WHERE e.{page} GROUP BY 1 ORDER BY 1")},
            "months": {r[0]: r[1] for r in q(
                f"SELECT c.month, COUNT(*) FROM query_choices c JOIN events e ON e.id = c.event_id "
                f"WHERE e.{page} GROUP BY 1 ORDER BY 1")},
            "median_server_ms": one(
                "SELECT server_ms FROM events WHERE server_ms IS NOT NULL ORDER BY server_ms "
                "LIMIT 1 OFFSET (SELECT COUNT(*) FROM events WHERE server_ms IS NOT NULL) / 2"
            ) if one("SELECT COUNT(*) FROM events WHERE server_ms IS NOT NULL") else None,
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _parse_iso(s: Any) -> Optional[dt.datetime]:
    if not s or not isinstance(s, str):
        return None
    try:
        d = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None
    return d if d.tzinfo else d.replace(tzinfo=dt.timezone.utc)


def _loads(s: Any) -> Any:
    if not s:
        return None
    try:
        return json.loads(s)
    except (TypeError, ValueError):
        return None


def _dumps(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"), default=str)


def _clip(s: Any, n: int) -> Optional[str]:
    if s is None:
        return None
    s = str(s).strip()
    return s[:n] if s else None


def _int(v: Any) -> Optional[int]:
    if v is None or isinstance(v, bool):
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _float(v: Any) -> Optional[float]:
    if v is None or isinstance(v, bool):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f == f else None  # drop NaN


def _bool01(v: Any) -> Optional[int]:
    if v is None:
        return None
    return 1 if v in (True, 1, "1", "true", "True") else 0
