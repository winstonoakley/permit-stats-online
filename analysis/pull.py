"""
Stage 1: fetch events and cache them as raw JSON Lines.

    prod   GET {ANALYTICS_URL}/analytics/export?since=...  with Bearer ANALYTICS_TOKEN
    local  read a SQLite analytics.db directly (same row shape as the export)

Raw files are cached under data/raw/<pull date>/ and named by source and
range, so a report can be regenerated from the same pull without touching
the server: pull() returns the cached path when it already exists.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Dict, Iterator, List, Optional

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_RAW_DIR = ROOT / "data" / "raw"
DEFAULT_LOCAL_DB = ROOT / "odds_database" / "analytics.db"
PAGE_SIZE = 50000          # the export endpoint caps limit at 100000
_ISO_PREFIX = re.compile(r"^\d{4}-\d{2}-\d{2}")


def _check_iso(name: str, value: Optional[str]) -> None:
    if value is not None and not _ISO_PREFIX.match(value):
        raise ValueError(f"{name} must be ISO 8601 (YYYY-MM-DD...), got {value!r}")


def raw_path(raw_dir: Path, source: str, since: str, until: Optional[str], pull_date: dt.date) -> Path:
    tag = lambda s: re.sub(r"[^0-9A-Za-z]+", "", s)[:14]
    name = f"events_{source}_{tag(since)}_{tag(until) if until else 'open'}.jsonl"
    return Path(raw_dir) / pull_date.isoformat() / name


# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------

def _http_get(url: str, token: str, timeout: int = 300) -> bytes:
    """Kept as a module-level function so tests can substitute it."""
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}", "Accept": "application/x-ndjson"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def fetch_prod(base_url: str, token: str, since: str, until: Optional[str] = None,
               page_size: int = PAGE_SIZE) -> Iterator[Dict]:
    """Page through /analytics/export. The endpoint has no cursor, so each
    page restarts at the last timestamp seen (inclusive) and rows are
    de-duplicated by id."""
    _check_iso("since", since)
    _check_iso("until", until)
    if not base_url or not token:
        raise ValueError("prod source needs --url/ANALYTICS_URL and ANALYTICS_TOKEN")
    seen: set = set()
    cursor = since
    while True:
        params = {"since": cursor, "limit": page_size}
        if until:
            params["until"] = until
        url = base_url.rstrip("/") + "/analytics/export?" + urllib.parse.urlencode(params)
        try:
            body = _http_get(url, token)
        except urllib.error.HTTPError as e:
            raise RuntimeError(f"export request failed: HTTP {e.code} {e.reason} for {url}") from e
        page = [json.loads(line) for line in body.decode("utf-8").splitlines() if line.strip()]
        new = [r for r in page if r.get("id") not in seen]
        for r in new:
            seen.add(r.get("id"))
            yield r
        if len(page) < page_size:
            return
        last_ts = page[-1].get("event_time_utc")
        if not last_ts or last_ts == cursor and not new:
            raise RuntimeError(f"export pagination stalled at {cursor!r}; more than {page_size} rows share one timestamp")
        cursor = last_ts


def fetch_local(db_path: Path, since: str, until: Optional[str] = None) -> Iterator[Dict]:
    """Read a local analytics.db through the web app's own reader so the
    rows have exactly the export's shape."""
    _check_iso("since", since)
    _check_iso("until", until)
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"local analytics database not found: {db_path}")
    sys.path.insert(0, str(ROOT))
    from app import analytics  # noqa: WPS433  (lazy: the web app is not an analysis dependency otherwise)
    yield from analytics.iter_events(since=since, until=until, limit=10_000_000, path=db_path)


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

def pull(
    source: str,
    since: str,
    until: Optional[str] = None,
    *,
    base_url: Optional[str] = None,
    token: Optional[str] = None,
    db_path: Optional[Path] = None,
    raw_dir: Optional[Path] = None,
    pull_date: Optional[dt.date] = None,
    refresh: bool = False,
) -> Path:
    """Fetch (or reuse) the raw JSONL for a range. Returns its path."""
    if source not in ("prod", "local"):
        raise ValueError("source must be 'prod' or 'local'")
    raw_dir = Path(raw_dir or DEFAULT_RAW_DIR)
    pull_date = pull_date or dt.date.today()
    path = raw_path(raw_dir, source, since, until, pull_date)
    if path.exists() and not refresh:
        return path

    if source == "prod":
        rows = fetch_prod(base_url or os.getenv("ANALYTICS_URL", ""), token or os.getenv("ANALYTICS_TOKEN", ""), since, until)
        origin = base_url or os.getenv("ANALYTICS_URL", "")
    else:
        db = Path(db_path or os.getenv("ANALYTICS_DB_PATH") or DEFAULT_LOCAL_DB)
        rows = fetch_local(db, since, until)
        origin = str(db)

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".jsonl.part")
    n = 0
    with tmp.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, default=str) + "\n")
            n += 1
    tmp.replace(path)
    meta = {
        "source": source, "origin": origin, "since": since, "until": until,
        "pulled_at": dt.datetime.now(dt.timezone.utc).isoformat(), "rows": n,
    }
    path.with_suffix(".meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    return path


def read_meta(path: Path) -> Dict:
    p = Path(path).with_suffix(".meta.json")
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}


def read_jsonl(path: Path) -> List[Dict]:
    with Path(path).open("r", encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def find_cached(raw_dir: Path, source: str, since: str, until: Optional[str]) -> Optional[Path]:
    """Most recent cached pull for this source and range, if any."""
    raw_dir = Path(raw_dir)
    if not raw_dir.exists():
        return None
    wanted = raw_path(raw_dir, source, since, until, dt.date(2000, 1, 1)).name
    candidates = sorted(raw_dir.glob(f"*/{wanted}"))
    return candidates[-1] if candidates else None
