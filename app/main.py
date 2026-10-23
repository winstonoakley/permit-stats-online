from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, PlainTextResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
import hmac
import json
import os
import re
import sqlite3
import sys
import threading
import time
from datetime import date
from pathlib import Path
from pydantic import BaseModel, Field, ValidationError, field_validator
from typing import Any, Dict, List, Literal, Optional
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from . import analytics
from . import pages
from .odds_engine import ENGINE_LOCK, Choice, estimate_odds_for_choice_set, estimate_season_odds


limiter = Limiter(key_func=get_remote_address)
app = FastAPI()
app.state.limiter = limiter

# Restrict CORS to the deployment origin; fall back to * for local dev.
# Set ALLOWED_ORIGIN env var in production ("https://permitodds.com").
_allowed_origin = os.getenv("ALLOWED_ORIGIN", "*")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[_allowed_origin],
    allow_credentials=True,
    allow_methods=["POST", "GET"],
    allow_headers=["Content-Type", "Authorization"],
)

DB_PATH = Path("stats.db")

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent   # project root (where index.html lives)
INDEX_PATH = BASE_DIR / "index.html"

# Versions recorded with each analytics event.
SIM_VERSION = "v1.1.1"  # bump this when you change the algorithm (v1.1.0: server-side smoothed predictor; v1.1.1: data-derived coregs)
APP_VERSION = (os.getenv("RAILWAY_GIT_COMMIT_SHA") or os.getenv("APP_VERSION") or "")[:12] or None

# Create or migrate the analytics database once at startup. Failure must not
# stop the site; events are simply not recorded until it is fixed.
if analytics.enabled():
    try:
        analytics.init_db()
    except Exception as e:
        print("Analytics init failed:", e, file=sys.stderr)
else:
    print("Analytics disabled (ANALYTICS_ENABLED); no events are recorded.", file=sys.stderr)


# --- Analytics helpers -------------------------------------------------------

def _visitor_from(m: Any) -> Dict[str, Any]:
    """Identity fields from a request model; falls back to the v1 session_id
    an older cached page might still send."""
    return {
        "visitor_id": getattr(m, "visitor_id", None) or getattr(m, "session_id", None),
        "visit_id": getattr(m, "visit_id", None),
        "seq_in_visit": getattr(m, "seq_in_visit", None),
        "is_new_visitor": getattr(m, "is_new_visitor", None),
    }


def _log_request_error(request: Request, http_status: int, message: str) -> None:
    """Record a failed request. Never includes the request body."""
    analytics.log_event(
        "error",
        source="page" if request.headers.get("referer") else "api",
        status="error",
        context=analytics.context_from_headers(request.headers),
        app_version=APP_VERSION,
        detail={
            "endpoint": request.url.path,
            "method": request.method,
            "http_status": http_status,
            "message": message[:200],
        },
    )


def _choices_for_log(result_choices: List[Dict[str, Any]]):
    """Per-choice predicted and priority-conditioned probabilities, mirroring
    the page: P2 is scaled by (1 - P1), P3 by (1 - P1 - P2)."""
    remaining = 1.0
    total = 0.0
    out = []
    for c in result_choices:
        p = c.get("predicted")
        conditioned = None
        if isinstance(p, (int, float)) and p == p:
            p = max(0.0, min(1.0, float(p)))
            conditioned = remaining * p
            remaining = max(0.0, remaining - conditioned)
            total += conditioned
        else:
            p = None
        out.append({
            "zone": c.get("zone"), "month": c.get("month"), "day": c.get("day"),
            "group_size": c.get("group_size"), "predicted": p, "conditioned": conditioned,
        })
    return out, round(min(total, 1.0), 6)


def require_analytics_token(request: Request) -> None:
    """Bearer-token guard for the analytics read endpoints. Fails closed when
    ANALYTICS_TOKEN is not configured."""
    expected = os.getenv("ANALYTICS_TOKEN")
    if not expected:
        raise HTTPException(status_code=503, detail="ANALYTICS_TOKEN is not configured")
    header = request.headers.get("authorization", "")
    supplied = header[7:].strip() if header[:7].lower() == "bearer " else ""
    if not supplied or not hmac.compare_digest(supplied, expected):
        raise HTTPException(
            status_code=401,
            detail="Invalid or missing analytics token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    # Only after the token checks out: say plainly when there is nothing to read.
    if not analytics.enabled():
        raise HTTPException(status_code=503, detail="analytics is disabled (ANALYTICS_ENABLED)")


def _db_unavailable(e: Exception) -> HTTPException:
    print(f"Analytics read failed: {e}", file=sys.stderr)
    return HTTPException(status_code=503, detail="analytics database unavailable")


# --- Error handlers (each also records an `error` event, best-effort) -------

@app.exception_handler(RequestValidationError)
async def _on_validation_error(request: Request, exc: RequestValidationError):
    _log_request_error(request, 422, "validation_error")
    return await request_validation_exception_handler(request, exc)


@app.exception_handler(RateLimitExceeded)
async def _on_rate_limited(request: Request, exc: RateLimitExceeded):
    _log_request_error(request, 429, "rate_limited")
    return _rate_limit_exceeded_handler(request, exc)


@app.exception_handler(Exception)
async def _on_unhandled(request: Request, exc: Exception):
    _log_request_error(request, 500, type(exc).__name__)
    return JSONResponse({"detail": "Internal Server Error"}, status_code=500)


@app.api_route("/", methods=["GET", "HEAD"], response_class=HTMLResponse)
async def serve_index():
    return FileResponse(INDEX_PATH)


# --- Content pages (app/pages.py renders content/*.md) ---

app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")

_PAGE_HEADERS = {"Cache-Control": "public, max-age=600"}


def _guide_route(slug: str):
    def serve() -> HTMLResponse:
        return HTMLResponse(pages.render_guide(slug), headers=_PAGE_HEADERS)
    return serve


def _zone_route(slug: str):
    # Sync, so the first (uncached, ~2-3 s) render runs in the threadpool.
    def serve(size: int = Query(pages.DEFAULT_GROUP_SIZE, ge=1, le=8)) -> HTMLResponse:
        return HTMLResponse(pages.render_zone(slug, size), headers=_PAGE_HEADERS)
    return serve


for _slug, _ in pages.GUIDE_PAGES:
    app.add_api_route(_slug, _guide_route(_slug), methods=["GET", "HEAD"], response_class=HTMLResponse)
for _slug, _, _ in pages.ZONES:
    app.add_api_route(_slug, _zone_route(_slug), methods=["GET", "HEAD"], response_class=HTMLResponse)

threading.Thread(target=pages.warm_cache, name="warm-content-pages", daemon=True).start()


@app.api_route("/zones", methods=["GET", "HEAD"], response_class=HTMLResponse)
def zones_index():
    return HTMLResponse(pages.render_zones_index(), headers=_PAGE_HEADERS)


@app.api_route("/robots.txt", methods=["GET", "HEAD"], response_class=PlainTextResponse)
def robots_txt():
    return PlainTextResponse(pages.robots_txt())


@app.api_route("/sitemap.xml", methods=["GET", "HEAD"])
def sitemap_xml():
    return Response(pages.sitemap_xml(), media_type="application/xml")


@app.api_route("/favicon.ico", methods=["GET", "HEAD"])
def favicon():
    return FileResponse(BASE_DIR / "static" / "favicon.svg", media_type="image/svg+xml")

# --- Models (shapes of data we send/receive) ---

class StatsRequest(BaseModel):
    zone: str       # e.g. "Zone A"
    date: date      # e.g. "2025-08-12"
    group_size: int # e.g. 4


class StatsResponse(BaseModel):
    zone: str
    date: date
    group_size: int
    estimated_success_rate: float
    note: str

class ChoiceInput(BaseModel):
    zone: str
    month: int = Field(ge=1, le=12)
    day: int = Field(ge=1, le=31)
    group_size: int = Field(ge=1, le=8)


class VisitorMeta(BaseModel):
    """Optional analytics identity the page attaches to its requests."""
    visitor_id: Optional[str] = Field(None, max_length=64)
    visit_id: Optional[str] = Field(None, max_length=64)
    seq_in_visit: Optional[int] = Field(None, ge=0, le=1_000_000)
    is_new_visitor: Optional[bool] = None
    # False for helper lookups (e.g. the heatmap calendar conditioning on
    # higher-priority choices) so they are not logged as user "Fetch Results".
    log_event: bool = True


class OddsRequest(VisitorMeta):
    permit_year: int = 2025
    data_years: List[int] = [2022, 2023, 2024, 2025, 2026]
    choices: List[ChoiceInput]  # up to 3

    # v1 metadata, still accepted from older cached pages. session_id stands
    # in for visitor_id; browser (a raw UA string) is only a fallback when the
    # User-Agent header is missing; the rest is ignored.
    session_id: Optional[str] = None
    query_index: Optional[int] = None
    device_type: Optional[str] = None
    browser: Optional[str] = None
    os_name: Optional[str] = None
    latency_ms: Optional[int] = None

class OddsResponse(BaseModel):
    years: List[int]
    choices: List[dict]  # keep loose for now; can tighten later
    event_id: Optional[int] = None  # analytics row for this query, for the timing beacon

class HeatmapRequest(VisitorMeta):
    zone: str
    group_size: int = Field(ge=1, le=8)
    permit_year: int = 2027
    data_years: List[int] = [2022, 2023, 2024, 2025, 2026]
    from_choice: Optional[int] = Field(None, ge=1, le=3)   # which choice's calendar was opened
    month: Optional[int] = Field(None, ge=1, le=12)        # month shown first


class ClientPage(BaseModel):
    path: Optional[str] = Field(None, max_length=256)
    referrer: Optional[str] = Field(None, max_length=1024)
    utm_source: Optional[str] = Field(None, max_length=128)
    utm_medium: Optional[str] = Field(None, max_length=128)
    utm_campaign: Optional[str] = Field(None, max_length=128)
    viewport_w: Optional[int] = Field(None, ge=0, le=20000)


class ClientEvent(VisitorMeta):
    """Body of POST /log_event. `timing` is not an event: it fills client_ms
    on an existing get_table row."""
    event_type: Literal["page_view", "error", "ad_view", "timing", "heatmap_view"]
    page: Optional[ClientPage] = None
    client_ms: Optional[int] = Field(None, ge=0, le=600_000)
    # heatmap_view only: the engine time /heatmap_odds reported. The page
    # preloads calendars with log_event false and logs the view when shown.
    server_ms: Optional[int] = Field(None, ge=0, le=600_000)
    event_id: Optional[int] = Field(None, ge=1)
    detail: Optional[Dict[str, Any]] = None

    @field_validator("detail")
    @classmethod
    def _detail_is_small(cls, v):
        if v is not None and len(json.dumps(v, default=str)) > 2000:
            raise ValueError("detail too large")
        return v


# --- Helper function to query the database ---

def get_success_rate_from_db(zone: str, date_str: str, group_size: int) -> float | None:
    """
    Look up success_rate in the stats.db file.

    Returns:
        float between 0 and 1 if found, or None if no matching row.
    """
    if not DB_PATH.exists():
        return None

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(
        """
        SELECT success_rate
        FROM stats
        WHERE zone = ?
          AND date = ?
          AND group_size = ?
        """,
        (zone, date_str, group_size),
    )
    row = cur.fetchone()
    conn.close()

    if row is None:
        return None

    return float(row[0])


# --- API endpoints ---

# When this process started. Changes on every deploy, restart or redeploy,
# which is how scripts/verify_persistence.py knows a redeploy has happened.
STARTED_AT = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


@app.get("/health")
def health():
    return {"status": "ok", "started_at": STARTED_AT, "app_version": APP_VERSION}


@app.post("/stats", response_model=StatsResponse)
def get_stats(request: StatsRequest):
    """
    This version looks for a matching row in stats.db.
    If it finds one, it returns that success_rate.
    If not, it returns a default message.
    """

    # Convert date object to text like "2025-08-12"
    date_str = request.date.isoformat()

    rate = get_success_rate_from_db(
        zone=request.zone,
        date_str=date_str,
        group_size=request.group_size,
    )

    if rate is None:
        # Nothing in the database for this combination
        return StatsResponse(
            zone=request.zone,
            date=request.date,
            group_size=request.group_size,
            estimated_success_rate=0.0,
            note="No matching data found in the database for these inputs.",
        )

    # Found a match
    return StatsResponse(
        zone=request.zone,
        date=request.date,
        group_size=request.group_size,
        estimated_success_rate=round(rate, 2),
        note="Result retrieved from local stats.db file.",
    )

@app.post("/estimate_odds", response_model=OddsResponse)
@limiter.limit("30/minute")
def estimate_odds(payload: OddsRequest, request: Request):
    # Convert Pydantic models to dataclass Choices
    choices = [
        Choice(
            zone=c.zone,
            month=c.month,
            day=c.day,
            group_size=c.group_size
        )
        for c in payload.choices
    ]

    # Folder where your odds_YYYY.db files live
    # Use environment variable if set, otherwise fall back to local folder
    db_dir = os.getenv("ODDS_DB_DIR") or os.path.join(str(BASE_DIR), "odds_databases")

    # ---- Run the odds engine (your existing logic) ----
    t0 = time.perf_counter()
    with ENGINE_LOCK:
        result = estimate_odds_for_choice_set(
            permit_year=payload.permit_year,
            choices=choices,
            data_years=payload.data_years,
            db_dir=db_dir,
        )
    server_ms = int((time.perf_counter() - t0) * 1000)

    # ---- Log this "Fetch Results" event (best-effort; log_event never raises) ----
    event_id = None
    if payload.log_event:
        visitor = _visitor_from(payload)
        log_choices, total_predicted = _choices_for_log(result["choices"])
        event_id = analytics.log_event(
            "get_table",
            source="page" if visitor["visitor_id"] else "api",
            visitor=visitor,
            context=analytics.context_from_headers(request.headers, ua_fallback=payload.browser),
            app_version=APP_VERSION,
            sim_version=SIM_VERSION,
            server_ms=server_ms,
            client_ms=payload.latency_ms,
            detail={
                "permit_year": payload.permit_year,
                "data_years": payload.data_years,
                "n_choices": len(payload.choices),
                "total_predicted": total_predicted,
                "legacy_query_index": payload.query_index,
            },
            choices=log_choices,
            permit_year=payload.permit_year,
        )

    # ---- Return the response as before, plus the analytics row id ----
    return OddsResponse(**result, event_id=event_id)


@app.post("/heatmap_odds")
@limiter.limit("15/minute")
def heatmap_odds(payload: HeatmapRequest, request: Request):
    """
    Returns first-choice odds for a single zone/group size for every date in
    the lottery season, keyed by permit date, across all data years.
    Used by the frontend heatmap calendar.
    """
    db_dir = os.getenv("ODDS_DB_DIR") or os.path.join(str(BASE_DIR), "odds_databases")

    t0 = time.perf_counter()
    with ENGINE_LOCK:
        result = estimate_season_odds(
            zone=payload.zone,
            group_size=payload.group_size,
            permit_year=payload.permit_year,
            data_years=payload.data_years,
            db_dir=db_dir,
        )
    server_ms = int((time.perf_counter() - t0) * 1000)

    if payload.log_event:
        visitor = _visitor_from(payload)
        analytics.log_event(
            "heatmap_view",
            source="page" if visitor["visitor_id"] else "api",
            visitor=visitor,
            context=analytics.context_from_headers(request.headers),
            app_version=APP_VERSION,
            sim_version=SIM_VERSION,
            server_ms=server_ms,
            detail={
                "zone": payload.zone,
                "group_size": payload.group_size,
                "permit_year": payload.permit_year,
                "month": payload.month,
                "from_choice": payload.from_choice,
            },
        )
    # The page logs its own heatmap_view (see ClientEvent) and passes this on.
    result["server_ms"] = server_ms
    return result


@app.post("/log_event", status_code=204)
@limiter.limit("60/minute")
async def log_client_event(request: Request):
    """
    Page-originated events: page_view, error, ad_view, and the `timing`
    follow-up that records how long a query took from the visitor's side.
    Sent with navigator.sendBeacon, which posts text/plain, so the body is
    parsed by hand. Always answers 204 once the body is well-formed.
    """
    raw = await request.body()
    if len(raw) > 8192:
        return Response(status_code=413)
    try:
        event = ClientEvent.model_validate(json.loads(raw or b"{}"))
    except (ValueError, ValidationError):
        _log_request_error(request, 422, "validation_error")
        return Response(status_code=422)

    visitor = _visitor_from(event)
    if event.event_type == "timing":
        if event.event_id and event.client_ms is not None and visitor["visitor_id"]:
            analytics.set_client_ms(event.event_id, visitor["visitor_id"], event.client_ms)
        return Response(status_code=204)

    is_heatmap = event.event_type == "heatmap_view"
    analytics.log_event(
        event.event_type,
        source="page",
        status="error" if event.event_type == "error" else "success",
        visitor=visitor,
        page=event.page.model_dump() if event.page else None,
        context=analytics.context_from_headers(request.headers),
        app_version=APP_VERSION,
        sim_version=SIM_VERSION if is_heatmap else None,
        server_ms=event.server_ms if is_heatmap else None,
        client_ms=event.client_ms,
        detail=event.detail,
    )
    return Response(status_code=204)


# --- Analytics read endpoints (Bearer ANALYTICS_TOKEN) ---

_ISO_PREFIX = re.compile(r"^\d{4}-\d{2}-\d{2}")


@app.get("/analytics/export", dependencies=[Depends(require_analytics_token)])
def analytics_export(
    since: Optional[str] = Query(None, description="ISO 8601, inclusive"),
    until: Optional[str] = Query(None, description="ISO 8601, exclusive"),
    limit: int = Query(10000, ge=1, le=100000),
):
    """
    Events as JSON Lines, oldest first, each with its query choices attached.
    This is what the analysis pipeline pulls from.
    """
    for name, value in (("since", since), ("until", until)):
        if value is not None and not _ISO_PREFIX.match(value):
            raise HTTPException(status_code=400, detail=f"{name} must be ISO 8601 (YYYY-MM-DD...)")

    # Open the database before streaming starts so a missing or unwritable
    # file is a clean 503 rather than a broken stream.
    try:
        events = analytics.iter_events(since=since, until=until, limit=limit)
        first = next(events, None)
    except (sqlite3.Error, OSError) as e:
        raise _db_unavailable(e)

    def lines():
        if first is None:
            return
        yield json.dumps(first, default=str) + "\n"
        for ev in events:
            yield json.dumps(ev, default=str) + "\n"

    return StreamingResponse(
        lines(),
        media_type="application/x-ndjson",
        headers={"Content-Disposition": 'attachment; filename="events.jsonl"'},
    )


@app.get("/debug/analytics", dependencies=[Depends(require_analytics_token)])
def debug_analytics(limit: int = Query(10, ge=1, le=500)):
    """The most recent events, newest first."""
    try:
        rows = list(analytics.iter_events(limit=limit, order="desc"))
    except (sqlite3.Error, OSError) as e:
        raise _db_unavailable(e)
    return {"db_path": str(analytics.db_path()), "row_count": len(rows), "rows": rows}


@app.get("/analytics/summary", dependencies=[Depends(require_analytics_token)])
def analytics_summary():
    """High-level usage statistics for the tool."""
    try:
        return analytics.summary()
    except (sqlite3.Error, OSError) as e:
        raise _db_unavailable(e)
