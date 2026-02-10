import os
import time
import logging
import sys
from datetime import datetime, timezone, timedelta

import requests
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

USGS_BASE_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"

DB_CONFIG = {
    "host":     os.environ.get("DB_HOST", "localhost"),
    "port":     int(os.environ.get("DB_PORT", 5432)),
    "dbname":   os.environ.get("DB_NAME", "earthquakes"),
    "user":     os.environ.get("DB_USER", "postgres"),
    "password": os.environ.get("DB_PASSWORD", "postgres"),
}

POLL_INTERVAL   = int(os.environ.get("POLL_INTERVAL_SECONDS", 60))
LOOKBACK_SECS   = int(os.environ.get("LOOKBACK_SECONDS", 90))

UPSERT_SQL = """
INSERT INTO earthquake_events
    (event_id, event_time, magnitude, depth, latitude, longitude,
     place, url, status, tsunami, sig, updated_at)
VALUES %s
ON CONFLICT (event_id) DO UPDATE SET
    event_time  = EXCLUDED.event_time,
    magnitude   = EXCLUDED.magnitude,
    depth       = EXCLUDED.depth,
    latitude    = EXCLUDED.latitude,
    longitude   = EXCLUDED.longitude,
    place       = EXCLUDED.place,
    url         = EXCLUDED.url,
    status      = EXCLUDED.status,
    tsunami     = EXCLUDED.tsunami,
    sig         = EXCLUDED.sig,
    updated_at  = EXCLUDED.updated_at
"""


def get_connection(retries: int = 10, delay: int = 5) -> psycopg2.extensions.connection:
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.autocommit = False
            log.info("Connected to PostgreSQL at %s:%s/%s", DB_CONFIG["host"], DB_CONFIG["port"], DB_CONFIG["dbname"])
            return conn
        except psycopg2.OperationalError as exc:
            log.warning("Database connection attempt %d/%d failed: %s", attempt, retries, exc)
            if attempt < retries:
                time.sleep(delay)
    log.error("Exhausted all database connection retries. Exiting.")
    sys.exit(1)


def fetch_earthquakes(start: datetime, end: datetime) -> list[dict]:
    params = {
        "format":    "geojson",
        "starttime": start.strftime("%Y-%m-%dT%H:%M:%S"),
        "endtime":   end.strftime("%Y-%m-%dT%H:%M:%S"),
        "orderby":   "time-asc",
    }
    try:
        resp = requests.get(USGS_BASE_URL, params=params, timeout=20)
        resp.raise_for_status()
        geojson = resp.json()
        return geojson.get("features", [])
    except requests.RequestException as exc:
        log.error("USGS API request failed: %s", exc)
        return []


def parse_feature(feature: dict) -> tuple | None:
    try:
        props = feature.get("properties", {})
        coords = feature.get("geometry", {}).get("coordinates", [None, None, None])

        event_id   = feature["id"]
        event_time = datetime.fromtimestamp(props["time"] / 1000, tz=timezone.utc)
        magnitude  = props.get("mag")
        depth      = coords[2]
        longitude  = coords[0]
        latitude   = coords[1]
        place      = props.get("place")
        url        = props.get("url")
        status     = props.get("status")
        tsunami    = props.get("tsunami", 0)
        sig        = props.get("sig")
        updated_at = datetime.fromtimestamp(props["updated"] / 1000, tz=timezone.utc)

        return (event_id, event_time, magnitude, depth, latitude, longitude,
                place, url, status, tsunami, sig, updated_at)
    except (KeyError, TypeError, ValueError) as exc:
        log.debug("Skipping malformed feature %s: %s", feature.get("id"), exc)
        return None


def upsert_events(conn: psycopg2.extensions.connection, rows: list[tuple]) -> int:
    if not rows:
        return 0
    with conn.cursor() as cur:
        execute_values(cur, UPSERT_SQL, rows, page_size=500)
    conn.commit()
    return len(rows)


def run_poll_cycle(conn: psycopg2.extensions.connection) -> None:
    now   = datetime.now(tz=timezone.utc)
    start = now - timedelta(seconds=LOOKBACK_SECS)

    log.info("Polling USGS for events between %s and %s", start.isoformat(), now.isoformat())
    features = fetch_earthquakes(start, now)
    log.info("Received %d feature(s) from USGS API", len(features))

    rows = [parsed for f in features if (parsed := parse_feature(f)) is not None]
    count = upsert_events(conn, rows)
    log.info("Upserted %d earthquake event(s) into source database", count)


def main() -> None:
    log.info("Earthquake CDC Poller starting — poll_interval=%ds lookback=%ds", POLL_INTERVAL, LOOKBACK_SECS)
    conn = get_connection()

    while True:
        try:
            run_poll_cycle(conn)
        except psycopg2.OperationalError as exc:
            log.warning("Lost database connection, reconnecting: %s", exc)
            try:
                conn.close()
            except Exception:
                pass
            conn = get_connection()
        except Exception as exc:
            log.error("Unhandled error in poll cycle: %s", exc, exc_info=True)

        log.info("Sleeping %d seconds until next poll cycle", POLL_INTERVAL)
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()