CREATE TABLE IF NOT EXISTS earthquake_events (
    event_id     TEXT PRIMARY KEY,
    event_time   TIMESTAMPTZ NOT NULL,
    magnitude    NUMERIC(5, 2),
    depth        NUMERIC(8, 3),
    latitude     NUMERIC(10, 6),
    longitude    NUMERIC(10, 6),
    place        TEXT,
    url          TEXT,
    status       TEXT,
    tsunami      SMALLINT,
    sig          INTEGER,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_earthquake_events_event_time ON earthquake_events (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_earthquake_events_magnitude  ON earthquake_events (magnitude DESC);

CREATE USER debezium WITH REPLICATION LOGIN PASSWORD 'dbz_password';

GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;

ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;

SELECT pg_create_logical_replication_slot('debezium_slot', 'pgoutput')
WHERE NOT EXISTS (
    SELECT 1 FROM pg_replication_slots WHERE slot_name = 'debezium_slot'
);

CREATE PUBLICATION dbz_pub FOR TABLE earthquake_events;