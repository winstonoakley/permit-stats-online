from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from datetime import date
import sqlite3
from pathlib import Path
from pydantic import BaseModel, Field
from typing import List
from .odds_engine import Choice, estimate_odds_for_choice_set
import os


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


    result = estimate_odds_for_choice_set(
        permit_year=payload.permit_year,
        choices=choices,
        data_years=payload.data_years,
        db_dir=db_dir,
    )

    return OddsResponse(**result)
