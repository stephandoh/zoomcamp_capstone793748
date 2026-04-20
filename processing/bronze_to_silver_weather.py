import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pandas as pd
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# -------------------------
# Logging
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger(__name__)

# -------------------------
# Load env
# -------------------------
BASE_DIR = Path(__file__).resolve().parents[1]
load_dotenv(BASE_DIR / ".env")

S3_BUCKET_BRONZE = os.getenv("S3_BUCKET_BRONZE", "voltstream-bronze")
S3_BUCKET_SILVER = os.getenv("S3_BUCKET_SILVER", "voltstream-silver")

s3 = boto3.client("s3")


# -------------------------
# Read Bronze weather files from S3
# -------------------------
def read_bronze_data(prefix="weather/") -> list:
    """
    Read all NDJSON weather files from the Bronze prefix in S3.
    Uses paginator to handle any number of files without truncation.
    """
    logger.info(f"Reading bronze weather data from s3://{S3_BUCKET_BRONZE}/{prefix}")

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=S3_BUCKET_BRONZE, Prefix=prefix)

    records = []
    files_read = 0

    for page in pages:
        if "Contents" not in page:
            continue

        for obj in page["Contents"]:
            key = obj["Key"]

            if not key.endswith(".json"):
                continue

            try:
                file_obj = s3.get_object(Bucket=S3_BUCKET_BRONZE, Key=key)
                content = file_obj["Body"].read().decode("utf-8")

                for line in content.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue

                files_read += 1

            except ClientError as e:
                logger.warning(f"Could not read {key}: {e}")
                continue

    logger.info(f"Loaded {len(records)} raw weather records from {files_read} file(s)")
    return records


# -------------------------
# Transform Bronze → Silver
# -------------------------
def transform_weather(records: list) -> pd.DataFrame:
    """
    Transform raw OpenWeatherMap API responses into a clean Silver DataFrame.

    The raw Bronze records contain the full API response with nested fields
    plus the three metadata fields we attached during ingestion:
      _lat_round, _lon_round, _ingested_at

    We extract only what we need for the pipeline:
      - Zone join keys (lat_round, lon_round)
      - Core meteorological fields (temperature, condition, wind_speed)
      - Fahrenheit conversion for temperature
        (raw API returns Celsius — brief requires Fahrenheit for risk scoring)
      - ingested_at for lineage

    Dedup key is lat_round + lon_round only — we want exactly one
    weather reading per zone, keeping the most recently ingested.
    """
    logger.info(f"Transforming {len(records)} raw weather records...")

    cleaned = []

    for record in records:
        if not isinstance(record, dict):
            continue

        try:
            lat_round = record.get("_lat_round")
            lon_round = record.get("_lon_round")
            ingested_at = record.get("_ingested_at")

            # Extract from nested OpenWeatherMap response structure
            main = record.get("main") or {}
            wind = record.get("wind") or {}
            weather_list = record.get("weather") or [{}]
            weather = weather_list[0] if weather_list else {}

            temp_celsius = main.get("temp")

            # Celsius → Fahrenheit conversion
            # Required for risk score thresholds defined in the brief:
            # >95°F (35°C) extreme heat, <20°F (-6.7°C) extreme cold
            temp_fahrenheit = (
                round((temp_celsius * 9 / 5) + 32, 2)
                if temp_celsius is not None
                else None
            )

            cleaned.append({
                "lat_round":        lat_round,
                "lon_round":        lon_round,
                "temperature_c":    round(float(temp_celsius), 2) if temp_celsius is not None else None,
                "temperature_f":    temp_fahrenheit,
                "condition":        weather.get("main"),
                "condition_description": weather.get("description"),
                "wind_speed":       wind.get("speed"),
                "humidity":         main.get("humidity"),
                "ingested_at":      ingested_at,
            })

        except (TypeError, ValueError, KeyError) as e:
            logger.warning(f"Skipping malformed record: {e}")
            continue

    if not cleaned:
        logger.warning("No records produced after transformation")
        return pd.DataFrame()

    df = pd.DataFrame(cleaned)

    # Safe numeric conversion
    df["lat_round"]      = pd.to_numeric(df["lat_round"], errors="coerce")
    df["lon_round"]      = pd.to_numeric(df["lon_round"], errors="coerce")
    df["temperature_c"]  = pd.to_numeric(df["temperature_c"], errors="coerce")
    df["temperature_f"]  = pd.to_numeric(df["temperature_f"], errors="coerce")
    df["wind_speed"]     = pd.to_numeric(df["wind_speed"], errors="coerce")
    df["humidity"]       = pd.to_numeric(df["humidity"], errors="coerce")

    # Drop records missing zone keys — unusable for joining to stations
    before = len(df)
    df = df.dropna(subset=["lat_round", "lon_round"])
    dropped = before - len(df)
    if dropped > 0:
        logger.warning(f"Dropped {dropped} records missing lat_round/lon_round")

    # Dedup — one row per weather zone, keep most recently ingested
    # Using lat_round + lon_round only (not condition/temperature)
    # which was the original bug — different readings at the same zone
    # would not deduplicate correctly with the broader key
    before_dedup = len(df)
    df = (
        df.sort_values("ingested_at", ascending=False, na_position="last")
          .drop_duplicates(subset=["lat_round", "lon_round"])
          .reset_index(drop=True)
    )
    after_dedup = len(df)

    if before_dedup != after_dedup:
        logger.info(
            f"Deduplication removed {before_dedup - after_dedup} rows "
            f"— kept most recent reading per zone"
        )

    # Add processing timestamp
    df["processed_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    logger.info(f"Silver weather transformation complete — {len(df)} clean rows")
    return df


# -------------------------
# Upload to Silver S3
# -------------------------
def upload_to_s3(df: pd.DataFrame, run_time: datetime) -> str:
    """
    Write Silver weather records to S3 as NDJSON, partitioned by date.
    Returns the full S3 key for logging.
    """
    key = (
        f"weather/{run_time.year}/{run_time.month:02d}/{run_time.day:02d}/"
        f"silver_weather_{run_time.strftime('%Y%m%dT%H%M%SZ')}.json"
    )

    s3.put_object(
        Bucket=S3_BUCKET_SILVER,
        Key=key,
        Body=df.to_json(orient="records", lines=True),
        ContentType="application/x-ndjson",
    )

    logger.info(f"Uploaded {len(df)} rows → s3://{S3_BUCKET_SILVER}/{key}")
    return key


# -------------------------
# Main
# -------------------------
def main():
    logger.info("=== Bronze → Silver weather transformation starting ===")

    run_time = datetime.now(timezone.utc)

    bronze_records = read_bronze_data()

    if not bronze_records:
        logger.error("No bronze weather data found — has weather_ingest.py run yet?")
        return

    silver_df = transform_weather(bronze_records)

    if silver_df.empty:
        logger.error("Transformation produced no rows — check bronze weather data")
        return

    upload_to_s3(silver_df, run_time)

    logger.info(
        f"=== Bronze → Silver weather complete — "
        f"{len(silver_df)} zones processed ==="
    )


if __name__ == "__main__":
    main()