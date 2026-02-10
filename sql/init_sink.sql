CREATE TABLE IF NOT EXISTS earthquake_events (
    event_id     TEXT PRIMARY KEY,
    event_time   TIMESTAMPTZ,
    magnitude    NUMERIC(5, 2),
    depth        NUMERIC(8, 3),
    latitude     NUMERIC(10, 6),
    longitude    NUMERIC(10, 6),
    place        TEXT,
    url          TEXT,
    status       TEXT,
    tsunami      SMALLINT,
    sig          INTEGER,
    updated_at   TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_sink_earthquake_events_event_time ON earthquake_events (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_sink_earthquake_events_magnitude  ON earthquake_events (magnitude DESC);