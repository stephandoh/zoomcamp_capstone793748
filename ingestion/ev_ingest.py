import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
import requests
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

API_KEY = os.getenv("OPENCHARGE_API_KEY")
COUNTRY_CODE = os.getenv("COUNTRY_CODE", "US")
S3_BUCKET_BRONZE = os.getenv("S3_BUCKET_BRONZE", "voltstream-bronze")

WATERMARK_KEY = "state/ev_stations_last_ingestion.json"

s3 = boto3.client("s3")


# -------------------------
# Watermark — read from S3
# -------------------------
def get_last_ingestion_time() -> datetime | None:
    """
    Read the last successful ingestion timestamp from S3.

    Returns None on first run — signals to the caller that no
    modifiedsince filter should be applied, giving a full historical
    load to establish the baseline dataset.

    Subsequent runs return the actual last run timestamp, enabling
    true incremental ingestion.
    """
    try:
        obj = s3.get_object(Bucket=S3_BUCKET_BRONZE, Key=WATERMARK_KEY)
        state = json.loads(obj["Body"].read().decode("utf-8"))
        last_run = datetime.fromisoformat(state["last_ingestion_utc"])
        logger.info(f"Watermark found — fetching records modified since {last_run.isoformat()}")
        return last_run
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            logger.info("No watermark found — first run, performing full historical load")
            return None
        raise


# -------------------------
# Watermark — write to S3
# -------------------------
def save_ingestion_time(run_time: datetime) -> None:
    """
    Persist the current run timestamp to S3 so the next run
    knows where to pick up from.
    """
    state = {"last_ingestion_utc": run_time.isoformat()}
    s3.put_object(
        Bucket=S3_BUCKET_BRONZE,
        Key=WATERMARK_KEY,
        Body=json.dumps(state),
        ContentType="application/json",
    )
    logger.info(f"Watermark saved — {run_time.isoformat()}")


# -------------------------
# Fetch EV data with correct pagination
# -------------------------
def fetch_ev_data(
    api_key: str,
    country_code: str,
    modifiedsince: str | None = None,
    page_size: int = 100,
    max_results: int = 5000,
) -> list:
    """
    Fetch EV station data from Open Charge Map API.

    On first run modifiedsince is None — no date filter is applied
    and the API returns the full historical dataset for the country.

    On subsequent runs modifiedsince is set to the last watermark —
    only stations updated since then are returned (incremental load).

    Pagination works by passing an offset parameter that advances
    by page_size on each iteration. The loop stops when:
      - the API returns fewer records than page_size (last page), or
      - we hit max_results as a safety cap.
    """
    results = []
    offset = 0

    while True:
        params = {
            "output": "json",
            "countrycode": country_code,
            "maxresults": page_size,
            "offset": offset,
            "compact": True,
            "verbose": False,
            "key": api_key,
        }

        if modifiedsince:
            params["modifiedsince"] = modifiedsince
            logger.info(f"Incremental run — modified since {modifiedsince} (offset {offset})")
        else:
            logger.info(f"Full historical load — no date filter (offset {offset})")

        try:
            response = requests.get(
                "https://api.openchargemap.io/v3/poi/",
                params=params,
                timeout=10,
            )
            response.raise_for_status()
            page_data = response.json()

        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed at offset {offset}: {e}")
            break

        if not page_data:
            logger.info(f"Empty page at offset {offset} — pagination complete")
            break

        results.extend(page_data)
        fetched = len(results)
        logger.info(
            f"Fetched {len(page_data)} records at offset {offset} "
            f"— total so far: {fetched}"
        )

        # Last page — API returned fewer records than page_size
        if len(page_data) < page_size:
            logger.info("Received partial page — end of results")
            break

        # Safety cap
        if fetched >= max_results:
            logger.warning(f"Hit max_results cap of {max_results} — stopping early")
            break

        offset += page_size
        time.sleep(0.5)

    return results


# -------------------------
# Upload raw NDJSON to S3 Bronze
# -------------------------
def upload_to_s3(data: list, run_time: datetime) -> str:
    """
    Write raw records to S3 as NDJSON (one JSON object per line).
    Partitioned by year/month/day for efficient downstream reads.
    Returns the full S3 key for logging.
    """
    key = (
        f"ev/{run_time.year}/{run_time.month:02d}/{run_time.day:02d}/"
        f"ev_stations_{run_time.strftime('%Y%m%dT%H%M%SZ')}.json"
    )

    body = "\n".join(json.dumps(record) for record in data)

    s3.put_object(
        Bucket=S3_BUCKET_BRONZE,
        Key=key,
        Body=body,
        ContentType="application/x-ndjson",
    )

    logger.info(f"Uploaded {len(data)} records → s3://{S3_BUCKET_BRONZE}/{key}")
    return key


# -------------------------
# Main
# -------------------------
def main():
    logger.info("=== EV station ingestion starting ===")

    if not API_KEY:
        logger.error("OPENCHARGE_API_KEY is not set — aborting")
        raise EnvironmentError("OPENCHARGE_API_KEY missing from environment")

    run_time = datetime.now(timezone.utc)

    # None on first run → full historical load (no modifiedsince filter)
    # datetime on subsequent runs → incremental load
    last_ingestion = get_last_ingestion_time()
    modifiedsince_param = (
        last_ingestion.strftime("%Y-%m-%dT%H:%M:%SZ")
        if last_ingestion
        else None
    )

    ev_data = fetch_ev_data(
        api_key=API_KEY,
        country_code=COUNTRY_CODE,
        modifiedsince=modifiedsince_param,
    )

    if not ev_data:
        logger.info("No records returned — nothing to upload")
    else:
        upload_to_s3(ev_data, run_time)
        save_ingestion_time(run_time)

    logger.info(f"=== EV station ingestion complete — {len(ev_data)} records ===")


if __name__ == "__main__":
    main()