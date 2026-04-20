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
# Read all Bronze EV files from S3
# -------------------------
def read_bronze_data(prefix="ev/") -> list:
    """
    Read all NDJSON files from the Bronze EV prefix in S3.
    Uses a paginator to handle buckets with more than 1000 objects —
    the original code used list_objects_v2 directly which silently
    truncates at 1000 files.
    """
    logger.info(f"Reading bronze data from s3://{S3_BUCKET_BRONZE}/{prefix}")

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

    logger.info(f"Loaded {len(records)} raw records from {files_read} file(s)")
    return records


# -------------------------
# Flatten a single station record
# into one row per connection
# -------------------------
def flatten_station(row: dict) -> list[dict]:
    """
    Flatten a single raw station record into one row per connection.

    This is the core transformation — a station with N connections
    produces N rows, each carrying the station-level fields plus
    the connection-level fields. This is the correct grain for the
    Silver layer and matches what the instructor described as
    "one row per Connection/Plug."

    The original code took connections[0] only, silently discarding
    all other connectors. A station with 9 connections would appear
    as having 1. This is fixed here by iterating over all connections.

    Stations with no connections produce one row with null connection
    fields — we preserve them rather than dropping the station entirely.
    """
    if not isinstance(row, dict):
        return []

    address = row.get("AddressInfo") or {}
    status = row.get("StatusType") or {}
    operator = row.get("OperatorInfo") or {}
    connections = row.get("Connections") or []

    # Station-level fields — same across all connection rows
    station_base = {
        "station_id":           row.get("ID"),
        "uuid":                 row.get("UUID"),
        "operator_id":          row.get("OperatorID"),
        "operator_name":        operator.get("Title"),
        "usage_type_id":        row.get("UsageTypeID"),
        "status_type_id":       row.get("StatusTypeID"),
        "number_of_points":     row.get("NumberOfPoints"),
        "is_recently_verified": row.get("IsRecentlyVerified"),
        "date_created":         row.get("DateCreated"),
        "date_last_verified":   row.get("DateLastVerified"),
        "date_last_status_update": row.get("DateLastStatusUpdate"),

        # Address fields — flattened from nested AddressInfo object
        "title":                address.get("Title"),
        "address_line1":        address.get("AddressLine1"),
        "address_line2":        address.get("AddressLine2"),
        "town":                 address.get("Town"),
        "state_or_province":    address.get("StateOrProvince"),
        "postcode":             address.get("Postcode"),
        "country_id":           address.get("CountryID"),
        "country_iso_code":     (address.get("Country") or {}).get("ISOCode"),
        "latitude":             address.get("Latitude"),
        "longitude":            address.get("Longitude"),
        "related_url":          address.get("RelatedURL"),
    }

    # If no connections, return one row with null connection fields
    if not connections:
        return [{
            **station_base,
            "connection_id":        None,
            "connection_type_id":   None,
            "connection_type_name": None,
            "connection_status_type_id": None,
            "level_id":             None,
            "level_description":    None,
            "amps":                 None,
            "voltage":              None,
            "power_kw":             None,
            "current_type_id":      None,
            "current_type_description": None,
            "quantity":             None,
            "is_operational":       None,
        }]

    # One row per connection — this is the fix
    rows = []
    for conn in connections:
        if not isinstance(conn, dict):
            continue

        conn_status_type_id = conn.get("StatusTypeID")

        rows.append({
            **station_base,
            "connection_id":        conn.get("ID"),
            "connection_type_id":   conn.get("ConnectionTypeID"),
            "connection_type_name": (conn.get("ConnectionType") or {}).get("FormalName"),
            "connection_status_type_id": conn_status_type_id,
            "level_id":             conn.get("LevelID"),
            "level_description":    (conn.get("Level") or {}).get("Description"),
            "amps":                 conn.get("Amps"),
            "voltage":              conn.get("Voltage"),
            "power_kw":             conn.get("PowerKW"),
            "current_type_id":      conn.get("CurrentTypeID"),
            "current_type_description": (conn.get("CurrentType") or {}).get("Description"),
            "quantity":             conn.get("Quantity"),

            # StatusTypeID 50 = Operational in Open Charge Map reference data
            # This is the single source of truth for operational status
            "is_operational": conn_status_type_id == 50,
        })

    return rows


# -------------------------
# Transform Bronze → Silver
# -------------------------
def transform_to_silver(records: list) -> pd.DataFrame:
    """
    Transform raw Bronze records into a clean Silver DataFrame.

    Steps:
    1. Flatten all records — one row per connection
    2. Cast timestamps to UTC-aware strings
    3. Add geo rounding columns for weather zone joining
    4. Add data quality flag for records missing critical fields
    5. Deduplicate on station_id + connection_id keeping
       the most recent date_last_status_update
    """
    logger.info(f"Transforming {len(records)} raw records...")

    # Flatten all stations — one row per connection
    all_rows = []
    for record in records:
        all_rows.extend(flatten_station(record))

    if not all_rows:
        logger.warning("No rows produced after flattening")
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    logger.info(f"Produced {len(df)} rows after flattening connections")

    # Timestamp normalization to UTC
    for col in ["date_created", "date_last_verified", "date_last_status_update"]:
        df[col] = pd.to_datetime(df[col], errors="coerce", utc=True) \
                    .dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Geo rounding — weather zone join keys
    # Rounding to 1 decimal groups geographically close stations
    # into shared weather zones, reducing API calls from 500+ to ~20-30
    df["lat_round"] = pd.to_numeric(df["latitude"], errors="coerce").round(1)
    df["lon_round"] = pd.to_numeric(df["longitude"], errors="coerce").round(1)

    # Numeric casting
    for col in ["amps", "voltage", "power_kw"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Data quality flag — records missing critical fields are flagged
    # but NOT dropped. They stay in Silver for auditability.
    # The BI layer filters on this flag.
    df["data_quality_error"] = (
        df["latitude"].isna() |
        df["longitude"].isna() |
        df["station_id"].isna() |
        (df["power_kw"] < 0).fillna(False)
    )

    dq_count = df["data_quality_error"].sum()
    if dq_count > 0:
        logger.warning(f"{dq_count} records flagged with data quality errors")

    # Deduplication — keep most recent record per station + connection
    # This handles multiple Bronze files containing the same station
    # (e.g. if ingestion ran twice before Silver processing)
    before_dedup = len(df)
    df = (
        df.sort_values("date_last_status_update", ascending=False, na_position="last")
          .drop_duplicates(subset=["station_id", "connection_id"])
          .reset_index(drop=True)
    )
    after_dedup = len(df)

    if before_dedup != after_dedup:
        logger.info(f"Deduplication removed {before_dedup - after_dedup} duplicate rows")

    # SCD Type 2 columns
    # valid_from / valid_to / is_current track when a record was current
    # On initial load all records are current (is_current=True, valid_to=None)
    # The upsert/merge logic in the serving layer will close these out
    # when a newer version of the same station arrives
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    df["valid_from"] = now_utc
    df["valid_to"] = None
    df["is_current"] = True

    logger.info(f"Silver transformation complete — {len(df)} clean rows")
    return df


# -------------------------
# Upload to Silver S3
# -------------------------
def upload_to_s3(df: pd.DataFrame, run_time: datetime) -> str:
    """
    Write Silver records to S3 as NDJSON, partitioned by date.
    Returns the full S3 key for logging.
    """
    key = (
        f"ev/{run_time.year}/{run_time.month:02d}/{run_time.day:02d}/"
        f"silver_ev_{run_time.strftime('%Y%m%dT%H%M%SZ')}.json"
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
    logger.info("=== Bronze → Silver EV transformation starting ===")

    run_time = datetime.now(timezone.utc)

    bronze_records = read_bronze_data()

    if not bronze_records:
        logger.error("No bronze data found — has ev_ingest.py run yet?")
        return

    silver_df = transform_to_silver(bronze_records)

    if silver_df.empty:
        logger.error("Transformation produced no rows — check bronze data quality")
        return

    upload_to_s3(silver_df, run_time)

    logger.info(
        f"=== Bronze → Silver EV complete — "
        f"{len(silver_df)} rows from {len(bronze_records)} stations ==="
    )


if __name__ == "__main__":
    main()