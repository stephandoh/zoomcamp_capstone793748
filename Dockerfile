# -------------------------
# VoltStream Ingestion & Processing Container
# -------------------------
# Runs all four pipeline scripts in sequence:
#   1. ev_ingest.py              — fetch EV stations → S3 Bronze
#   2. bronze_to_silver_ev.py    — transform EV → S3 Silver
#   3. weather_ingest.py         — fetch weather zones → S3 Bronze
#   4. bronze_to_silver_weather.py — transform weather → S3 Silver
#
# Redshift COPY and dbt run are handled by Airflow operators
# outside this container — this container owns Bronze and Silver only.
# -------------------------

FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install dependencies first (separate layer for Docker cache efficiency)
# If requirements.txt hasn't changed, this layer is cached and skipped
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY ingestion/ ./ingestion/
COPY processing/ ./processing/

# Environment variables are injected at runtime by Airflow or docker run
# Never bake secrets into the image
# Required vars:
#   OPENCHARGE_API_KEY
#   WEATHER_API_KEY
#   S3_BUCKET_BRONZE
#   S3_BUCKET_SILVER
#   AWS_ACCESS_KEY_ID
#   AWS_SECRET_ACCESS_KEY
#   COUNTRY_CODE (optional, defaults to US)

# Default command runs the full Bronze + Silver pipeline in order
# Can be overridden at runtime to run individual scripts:
#   docker run voltstream-pipeline python ingestion/ev_ingest.py
CMD ["sh", "-c", "\
    python ingestion/ev_ingest.py && \
    python processing/bronze_to_silver_ev.py && \
    python ingestion/weather_ingest.py && \
    python processing/bronze_to_silver_weather.py \
"]