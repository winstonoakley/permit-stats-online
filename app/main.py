from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
import os
from pydantic import BaseModel
from datetime import date, datetime, timezone
import sqlite3
import json
from pathlib import Path
from pydantic import BaseModel, Field
from typing import List
from .odds_engine import Choice, estimate_odds_for_choice_set


app = FastAPI()

# Allow our web page to make requests to this backend (for now, allow everything)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # later we can tighten this
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_PATH = Path("stats.db")

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent   # project root (where index.html & schema.sql live)
DB_DIR = BASE_DIR / "odds_database"
DB_DIR.mkdir(exist_ok=True)

ANALYTICS_DB_PATH = DB_DIR / "analytics.db"
SCHEMA_PATH = BASE_DIR / "schema.sql"


def init_logging_db():
    """Create the analytics.db file and ensure query_events table exists."""
    conn = sqlite3.connect(ANALYTICS_DB_PATH)
    try:
        with SCHEMA_PATH.open("r", encoding="utf-8") as f:
            schema_sql = f.read()
        conn.executescript(schema_sql)
        conn.commit()
    finally:
        conn.close()


# Run this once when the app starts
init_logging_db()

def log_query_event(
    inputs: dict,
    results: dict,
    status: str = "success",
    event_type: str = "get_table",
    session_id: str | None = None,
    user_id: str | None = None,
    sim_version: str | None = None,
    query_index_in_session: int | None = None,
    device_type: str | None = None,
    browser: str | None = None,
    os_name: str | None = None,
    country: str | None = None,
    region: str | None = None,
    referrer: str | None = None,
    latency_ms: int | None = None,
) -> None:
    """Insert a single query log row into the analytics database."""
    event_time_utc = datetime.now(timezone.utc).isoformat()

    conn = sqlite3.connect(ANALYTICS_DB_PATH)
    try:
        conn.execute(
            """
            INSERT INTO query_events (
                session_id, user_id,
                event_time_utc, event_type,
                country, region, device_type, browser, os, referrer,
                sim_version, query_index_in_session,
                inputs_json, results_json,
                latency_ms, status
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            (
                session_id,
                user_id,
                event_time_utc,
                event_type,
                country,
                region,
                device_type,
                browser,
                os_name,
                referrer,
                sim_version,
                query_index_in_session,
                json.dumps(inputs),
                json.dumps(results),
                latency_ms,
                status,
            ),
        )
        conn.commit()
    finally:
        conn.close()


# Path to the repo root (where index.html lives)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INDEX_PATH = os.path.join(BASE_DIR, "index.html")

@app.get("/", response_class=HTMLResponse)
async def serve_index():
    return FileResponse(INDEX_PATH)

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

class OddsRequest(BaseModel):
    permit_year: int = 2025
    data_years: List[int] = [2020, 2021, 2022, 2023, 2024]  # or your four years
    choices: List[ChoiceInput]  # up to 3

class OddsResponse(BaseModel):
    years: List[int]
    choices: List[dict]  # keep loose for now; can tighten later


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

@app.get("/health")
def health():
    return {"status": "ok"}


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
def estimate_odds(payload: OddsRequest):
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
    db_dir = os.getenv("ODDS_DB_DIR") or r"C:\permit-stats-backend-starter\odds_databases"

    # ---- Build "inputs" dict for logging ----
    # This captures exactly what the user asked for.
    inputs = {
        "permit_year": payload.permit_year,
        "data_years": payload.data_years,
        "choices": [
            {
                "zone": c.zone,
                "month": c.month,
                "day": c.day,
                "group_size": c.group_size,
            }
            for c in payload.choices
        ],
    }

    # ---- Run the odds engine (your existing logic) ----
    result = estimate_odds_for_choice_set(
        permit_year=payload.permit_year,
        choices=choices,
        data_years=payload.data_years,
        db_dir=db_dir,
    )

    # ---- Log this "Get Table" event into analytics.db (best-effort) ----
    try:
        log_query_event(
            inputs=inputs,
            results=result,
            status="success",
            event_type="get_table",
            # The rest of the fields can stay None for now:
            session_id=None,
            user_id=None,
            sim_version="v1.0.0",
            query_index_in_session=None,
            device_type=None,
            browser=None,
            os_name=None,
            country=None,
            region=None,
            referrer=None,
            latency_ms=None,
        )
    except Exception as e:
        # Do not break the main functionality if analytics fails
        print("Analytics logging failed:", e)

    # ---- Return the response as before ----
    return OddsResponse(**result)


@app.get("/debug/analytics")
def debug_analytics(limit: int = 10):
    """
    Debug endpoint to inspect analytics.db.
    Returns the most recent rows from query_events.
    """
    # 1. Check that the DB file exists
    if not ANALYTICS_DB_PATH.exists():
        return {
            "error": "analytics.db not found",
            "path": str(ANALYTICS_DB_PATH),
        }

    conn = sqlite3.connect(ANALYTICS_DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    try:
        # 2. Try to read from query_events
        cur.execute(
            """
            SELECT
                id,
                event_time_utc,
                event_type,
                status,
                inputs_json,
                results_json
            FROM query_events
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows = [dict(row) for row in cur.fetchall()]
    except sqlite3.OperationalError as e:
        # Covers cases like "no such table: query_events"
        return {
            "error": "SQLite error while querying query_events",
            "details": str(e),
            "db_path": str(ANALYTICS_DB_PATH),
        }
    finally:
        conn.close()

    return {
        "db_path": str(ANALYTICS_DB_PATH),
        "row_count": len(rows),
        "rows": rows,
    }

@app.post("/debug/log_test")
def debug_log_test():
    """
    Test endpoint: tries to write a single row into query_events.
    This lets us see if log_query_event works in isolation.
    """
    sample_inputs = {"test": True, "source": "debug_log_test"}
    sample_results = {"message": "This is a test row"}

    try:
        log_query_event(
            inputs=sample_inputs,
            results=sample_results,
            status="success",
            event_type="test_debug",
            session_id=None,
            user_id=None,
            sim_version="v1.0.0",
            query_index_in_session=None,
            device_type=None,
            browser=None,
            os_name=None,
            country=None,
            region=None,
            referrer=None,
            latency_ms=None,
        )
        return {"ok": True, "message": "Row inserted into query_events"}
    except Exception as e:
        # Here we *don't* swallow the error; we return it so we can see what's wrong.
        return {"ok": False, "error": str(e)}





