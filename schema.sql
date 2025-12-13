CREATE TABLE IF NOT EXISTS query_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    -- Identity / grouping
    session_id TEXT NOT NULL,
    user_id   TEXT,                          -- null for now if you don’t have logins

    -- Event metadata
    event_time_utc TEXT NOT NULL,            -- ISO8601 timestamp (e.g., 2025-12-12T03:21:45Z)
    event_type     TEXT NOT NULL DEFAULT 'get_table',

    -- Context
    country   TEXT,                          -- optional, can be null
    region    TEXT,                          -- optional, can be null
    device_type TEXT,                        -- 'desktop' | 'tablet' | 'mobile' | etc.
    browser   TEXT,
    os        TEXT,
    referrer  TEXT,

    -- Simulation / app versioning
    sim_version            TEXT,             -- e.g., 'v1.0.3'
    query_index_in_session INTEGER,          -- 1, 2, 3… for this session

    -- Inputs / results (JSON blobs)
    inputs_json  TEXT NOT NULL,              -- JSON string of all inputs
    results_json TEXT NOT NULL,              -- JSON string of odds table & total odds

    -- Diagnostics
    latency_ms INTEGER,                      -- time from click to response
    status     TEXT NOT NULL                 -- 'success' | 'error' | etc.
);

CREATE INDEX IF NOT EXISTS idx_query_events_session_time
    ON query_events (session_id, event_time_utc);

CREATE INDEX IF NOT EXISTS idx_query_events_event_type
    ON query_events (event_type);

-- Table to log each "Get Table" query event
CREATE TABLE IF NOT EXISTS query_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    -- Identity / grouping (can be NULL for now if you don't wire them yet)
    session_id TEXT,
    user_id    TEXT,

    -- Event metadata
    event_time_utc TEXT NOT NULL,
    event_type     TEXT NOT NULL DEFAULT 'get_table',

    -- Context (optional)
    country     TEXT,
    region      TEXT,
    device_type TEXT,
    browser     TEXT,
    os          TEXT,
    referrer    TEXT,

    -- Simulation / app versioning
    sim_version            TEXT,
    query_index_in_session INTEGER,

    -- Inputs / results (JSON blobs)
    inputs_json  TEXT NOT NULL,
    results_json TEXT NOT NULL,

    -- Diagnostics
    latency_ms INTEGER,
    status     TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_query_events_event_time
    ON query_events (event_time_utc);

CREATE INDEX IF NOT EXISTS idx_query_events_event_type
    ON query_events (event_type);
