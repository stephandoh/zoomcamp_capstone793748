import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
import requests
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger(__name__)


# Load env
BASE_DIR = Path(__file__).resolve().parents[1]
load_dotenv(BASE_DIR / ".env")

API_KEY = os.getenv("WEATHER_API_KEY")
S3_BUCKET_BRONZE = os.getenv("S3_BUCKET_BRONZE", "voltstream-bronze")
S3_BUCKET_SILVER = os.getenv("S3_BUCKET_SILVER", "voltstream-silver")

# Watermark key — separate from EV watermark
WATERMARK_KEY = "state/weather_last_ingestion.json"

s3 = boto3.client("s3")


# Watermark — read from S3
def get_last_ingestion_time() -> datetime:
    """
    Read the last successful weather ingestion timestamp from S3.
    Falls back to 7 days ago on first run.
    """
    try:
        obj = s3.get_object(Bucket=S3_BUCKET_BRONZE, Key=WATERMARK_KEY)
        state = json.loads(obj["Body"].read().decode("utf-8"))
        last_run = datetime.fromisoformat(state["last_ingestion_utc"])
        logger.info(f"Watermark found — last weather ingestion at {last_run.isoformat()}")
        return last_run
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            fallback = datetime.now(timezone.utc) - timedelta(days=7)
            logger.info(f"No watermark found — first run, defaulting to {fallback.isoformat()}")
            return fallback
        raise


# Watermark — write to S3
def save_ingestion_time(run_time: datetime) -> None:
    """
    Persist the current run timestamp to S3.
    """
    state = {"last_ingestion_utc": run_time.isoformat()}
    s3.put_object(
        Bucket=S3_BUCKET_BRONZE,
        Key=WATERMARK_KEY,
        Body=json.dumps(state),
        ContentType="application/json",
    )
    logger.info(f"Watermark saved — {run_time.isoformat()}")


# Load weather zones from Silver S3
def load_weather_zones() -> list[dict]:
    """
    Read distinct lat_round/lon_round pairs from the Silver EV stations
    NDJSON files in S3. This ensures weather coverage always matches
    the actual station footprint — no hardcoded coordinates.

    Returns a list of dicts: [{"lat_round": 5.6, "lon_round": -0.2}, ...]
    """
    logger.info(f"Loading weather zones from s3://{S3_BUCKET_SILVER}/ev/")

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=S3_BUCKET_SILVER, Prefix="ev/")

    zones = set()

    for page in pages:
        if "Contents" not in page:
            continue

        for obj in page["Contents"]:
            key = obj["Key"]

            if not key.endswith(".json"):
                continue

            try:
                file_obj = s3.get_object(Bucket=S3_BUCKET_SILVER, Key=key)
                content = file_obj["Body"].read().decode("utf-8")

                for line in content.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                        lat = record.get("lat_round") or record.get("latitude")
                        lon = record.get("lon_round") or record.get("longitude")

                        if lat is None or lon is None:
                            continue

                        # Round to 1 decimal — weather zone grouping strategy
                        # Groups geographically close stations into shared zones
                        # reducing API calls from 500+ (per station) to ~20-30 (per zone)
                        lat_round = round(float(lat), 1)
                        lon_round = round(float(lon), 1)
                        zones.add((lat_round, lon_round))

                    except (json.JSONDecodeError, ValueError, TypeError):
                        continue

            except ClientError as e:
                logger.warning(f"Could not read {key}: {e}")
                continue

    zone_list = [{"lat_round": lat, "lon_round": lon} for lat, lon in sorted(zones)]
    logger.info(f"Found {len(zone_list)} distinct weather zones from Silver EV data")
    return zone_list


# Fetch weather for a single zone
def fetch_weather(lat: float, lon: float) -> dict | None:
    """
    Fetch current weather from OpenWeatherMap for a given coordinate.
    Returns None on failure so the caller can skip and continue.
    Units are metric — temperature in Celsius.
    Conversion to Fahrenheit happens in the Silver processing layer.
    """
    params = {
        "lat": lat,
        "lon": lon,
        "appid": API_KEY,
        "units": "metric",
    }

    try:
        response = requests.get(
            "https://api.openweathermap.org/data/2.5/weather",
            params=params,
            timeout=10,
        )
        response.raise_for_status()
        return response.json()

    except requests.exceptions.RequestException as e:
        logger.warning(f"Weather fetch failed for ({lat}, {lon}): {e}")
        return None


# Upload raw NDJSON to S3 Bronze
def upload_to_s3(data: list, run_time: datetime) -> str:
    """
    Write raw weather records to S3 Bronze as NDJSON.
    Partitioned by year/month/day for efficient downstream reads.
    Raw response is preserved — cleaning happens in bronze_to_silver_weather.py.
    Returns the full S3 key for logging.
    """
    key = (
        f"weather/{run_time.year}/{run_time.month:02d}/{run_time.day:02d}/"
        f"weather_{run_time.strftime('%Y%m%dT%H%M%SZ')}.json"
    )

    body = "\n".join(json.dumps(record) for record in data)

    s3.put_object(
        Bucket=S3_BUCKET_BRONZE,
        Key=key,
        Body=body,
        ContentType="application/x-ndjson",
    )

    logger.info(f"Uploaded {len(data)} weather records → s3://{S3_BUCKET_BRONZE}/{key}")
    return key


# Main
def main():
    logger.info("=== Weather ingestion starting ===")

    if not API_KEY:
        logger.error("WEATHER_API_KEY is not set — aborting")
        raise EnvironmentError("WEATHER_API_KEY missing from environment")

    run_time = datetime.now(timezone.utc)

    # Load zones dynamically from Silver — always reflects current station footprint
    zones = load_weather_zones()

    if not zones:
        logger.warning("No weather zones found in Silver — has ev_ingest + bronze_to_silver_ev run yet?")
        return

    weather_records = []
    failed_zones = []

    for zone in zones:
        lat = zone["lat_round"]
        lon = zone["lon_round"]

        raw = fetch_weather(lat, lon)

        if raw is None:
            failed_zones.append((lat, lon))
            continue

        # Attach zone keys and ingestion metadata to raw response
        # so the Silver processing script can join back to stations
        raw["_lat_round"] = lat
        raw["_lon_round"] = lon
        raw["_ingested_at"] = run_time.isoformat()

        weather_records.append(raw)

        # Polite rate limiting — free tier allows 60 calls/min
        time.sleep(1)

    logger.info(f"Successfully fetched {len(weather_records)} zones")

    if failed_zones:
        logger.warning(f"Failed zones ({len(failed_zones)}): {failed_zones}")

    if not weather_records:
        logger.error("No weather records collected — nothing to upload")
        return

    upload_to_s3(weather_records, run_time)
    save_ingestion_time(run_time)

    logger.info(f"=== Weather ingestion complete — {len(weather_records)}/{len(zones)} zones ===")


if __name__ == "__main__":
    main()