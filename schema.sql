-- Analytics event store, schema version 2.
--
-- Applied by app/analytics.py on every start (every statement is
-- idempotent) and versioned with PRAGMA user_version. A v1 file (the old
-- query_events table) is migrated forward by app/analytics.py, which keeps
-- the original table as query_events_v1.
--
-- Privacy: no IP address and no raw user-agent string are stored anywhere.
--
-- Known event types (validated in code, not here, so a new type is a code
-- change only): page_view, get_table, heatmap_view, error, ad_view.

CREATE TABLE IF NOT EXISTS events (
    id              INTEGER PRIMARY KEY,
    schema_version  INTEGER NOT NULL DEFAULT 2,       -- 1 for rows imported from query_events
    event_type      TEXT    NOT NULL,
    event_time_utc  TEXT    NOT NULL,                 -- ISO 8601, server clock
    source          TEXT    NOT NULL DEFAULT 'page'
                    CHECK (source IN ('page', 'api', 'test')),
    status          TEXT    NOT NULL DEFAULT 'success'
                    CHECK (status IN ('success', 'error')),

    -- identity: NULL for api/test rows
    visitor_id      TEXT,                             -- random id, lives in the browser for up to ~13 months
    visit_id        TEXT,                             -- random id, new after 30 minutes of inactivity
    seq_in_visit    INTEGER,                          -- 1, 2, 3 ... within the visit
    is_new_visitor  INTEGER,                          -- 1 on the first event of a new visitor_id

    -- page context (page_view only)
    page_path       TEXT,
    page_referrer   TEXT,                             -- origin + path, query string removed
    utm_source      TEXT,
    utm_medium      TEXT,
    utm_campaign    TEXT,
    viewport_w      INTEGER,

    -- coarse geography from the proxy's headers
    country         TEXT,                             -- ISO 3166-1 alpha-2
    region          TEXT,
    geo_source      TEXT,                             -- header the values came from

    -- parsed device
    device_type     TEXT,                             -- mobile | tablet | desktop | bot | other
    os_family       TEXT,
    browser_family  TEXT,
    browser_major   INTEGER,
    is_bot          INTEGER NOT NULL DEFAULT 0,

    -- versions and timing
    app_version     TEXT,                             -- git commit of the deploy
    sim_version     TEXT,                             -- odds engine version (get_table, heatmap_view)
    server_ms       INTEGER,                          -- engine time measured on the server
    client_ms       INTEGER,                          -- click-to-response measured by the page

    -- small type-specific payload, never the full engine result
    detail_json     TEXT                              -- get_table: permit_year, data_years, n_choices, total_predicted
                                                      -- heatmap_view: zone, group_size, month, from_choice
                                                      -- error: endpoint, method, http_status, message
                                                      -- ad_view: whatever the ads workflow defines
);

CREATE INDEX IF NOT EXISTS ix_events_time    ON events (event_time_utc);
CREATE INDEX IF NOT EXISTS ix_events_type    ON events (event_type, event_time_utc);
CREATE INDEX IF NOT EXISTS ix_events_visitor ON events (visitor_id, event_time_utc);
CREATE INDEX IF NOT EXISTS ix_events_visit   ON events (visit_id, seq_in_visit);

-- One row per submitted choice of a get_table event, in priority order.
CREATE TABLE IF NOT EXISTS query_choices (
    id            INTEGER PRIMARY KEY,
    event_id      INTEGER NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    choice_rank   INTEGER NOT NULL,                   -- 1 = first choice
    zone          TEXT    NOT NULL,
    permit_date   TEXT,                               -- YYYY-MM-DD in the permit year; NULL if month/day is not a date
    month         INTEGER NOT NULL,
    day           INTEGER NOT NULL,
    group_size    INTEGER NOT NULL,
    predicted     REAL,                               -- engine's first-choice probability for this choice
    conditioned   REAL,                               -- after priority conditioning: what the user was shown
    UNIQUE (event_id, choice_rank)
);

CREATE INDEX IF NOT EXISTS ix_choices_zone_date ON query_choices (zone, permit_date);
