# Redshift SQL Reference

This document contains every SQL statement used in the VoltStream pipeline. Use it as a reference when setting up the project from scratch, debugging issues, or verifying data at any layer.

All statements are designed to run in the Redshift Query Editor v2 or any SQL client connected to your Redshift Serverless workgroup.

## Schema Setup

Run these once when setting up the project for the first time:

```sql
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS gold;
```

## Permission Grants

### Granting permissions to the admin user

```sql
GRANT ALL ON DATABASE dev TO admin;
GRANT ALL ON SCHEMA public TO admin;
GRANT ALL ON SCHEMA staging TO admin;
GRANT ALL ON SCHEMA gold TO admin;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT ALL ON TABLES TO admin;

ALTER DEFAULT PRIVILEGES IN SCHEMA staging
GRANT ALL ON TABLES TO admin;

ALTER DEFAULT PRIVILEGES IN SCHEMA gold
GRANT ALL ON TABLES TO admin;
```

### Granting permissions to an IAM user

When Airflow connects via IAM credentials, Redshift maps the session to a different user. Find the mapped username and grant permissions to it:

```sql
SELECT current_user;
```

Then replace `IAM:your-username` with the value returned:

```sql
GRANT ALL ON DATABASE dev TO "IAM:your-username";
GRANT ALL ON SCHEMA public TO "IAM:your-username";
GRANT ALL ON SCHEMA staging TO "IAM:your-username";
GRANT ALL ON SCHEMA gold TO "IAM:your-username";

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT ALL ON TABLES TO "IAM:your-username";

ALTER DEFAULT PRIVILEGES IN SCHEMA staging
GRANT ALL ON TABLES TO "IAM:your-username";

ALTER DEFAULT PRIVILEGES IN SCHEMA gold
GRANT ALL ON TABLES TO "IAM:your-username";
```

### Granting table-level permissions

If a user gets a `permission denied for relation` error on a specific table:

```sql
GRANT ALL ON TABLE public.ev_stations TO "IAM:your-username";
GRANT ALL ON TABLE public.weather TO "IAM:your-username";
```

## Source Table DDL

### public.ev_stations

Receives Silver EV station data loaded via COPY command. One row per station + connection combination.

```sql
CREATE TABLE IF NOT EXISTS public.ev_stations (
    station_id                INTEGER,
    uuid                      VARCHAR(100),
    operator_id               INTEGER,
    operator_name             VARCHAR(255),
    usage_type_id             INTEGER,
    status_type_id            INTEGER,
    number_of_points          INTEGER,
    is_recently_verified      BOOLEAN,
    date_created              VARCHAR(30),
    date_last_verified        VARCHAR(30),
    date_last_status_update   VARCHAR(30),
    title                     VARCHAR(255),
    address_line1             VARCHAR(255),
    address_line2             VARCHAR(255),
    town                      VARCHAR(100),
    state_or_province         VARCHAR(100),
    postcode                  VARCHAR(20),
    country_id                INTEGER,
    country_iso_code          VARCHAR(10),
    latitude                  FLOAT,
    longitude                 FLOAT,
    related_url               VARCHAR(500),
    connection_id             INTEGER,
    connection_type_id        INTEGER,
    connection_type_name      VARCHAR(255),
    connection_status_type_id INTEGER,
    level_id                  INTEGER,
    level_description         VARCHAR(255),
    amps                      FLOAT,
    voltage                   FLOAT,
    power_kw                  FLOAT,
    current_type_id           INTEGER,
    current_type_description  VARCHAR(100),
    quantity                  INTEGER,
    is_operational            BOOLEAN,
    lat_round                 FLOAT,
    lon_round                 FLOAT,
    data_quality_error        BOOLEAN,
    valid_from                VARCHAR(30),
    valid_to                  VARCHAR(30),
    is_current                BOOLEAN
);
```

### public.weather

Receives Silver weather data loaded via COPY command. One row per weather zone per pipeline run.

```sql
CREATE TABLE IF NOT EXISTS public.weather (
    lat_round             FLOAT,
    lon_round             FLOAT,
    temperature_c         FLOAT,
    temperature_f         FLOAT,
    condition             VARCHAR(100),
    condition_description VARCHAR(255),
    wind_speed            FLOAT,
    humidity              FLOAT,
    ingested_at           VARCHAR(50),
    processed_at          VARCHAR(50)
);
```

## COPY Commands

These load Silver data from S3 into Redshift. Replace `YYYY/MM/DD` with the actual date partition and `YOUR_ACCOUNT_ID` with your AWS account ID.

### Load EV stations

```sql
TRUNCATE TABLE public.ev_stations;

COPY public.ev_stations
FROM 's3://voltstream-silver/ev/YYYY/MM/DD/'
IAM_ROLE 'arn:aws:iam::YOUR_ACCOUNT_ID:role/AmazonRedshift-CommandsAccessRole'
FORMAT AS JSON 'auto'
REGION 'us-east-1';
```

### Load weather

```sql
TRUNCATE TABLE public.weather;

COPY public.weather
FROM 's3://voltstream-silver/weather/YYYY/MM/DD/'
IAM_ROLE 'arn:aws:iam::YOUR_ACCOUNT_ID:role/AmazonRedshift-CommandsAccessRole'
FORMAT AS JSON 'auto'
REGION 'us-east-1';
```

> Always run TRUNCATE before COPY. Without it each COPY appends rows and duplicates accumulate across runs. The Airflow DAG handles this automatically in the correct order.

## Verification Queries

### Verify source tables loaded correctly

```sql
SELECT COUNT(*) AS ev_row_count FROM public.ev_stations;
SELECT COUNT(*) AS weather_row_count FROM public.weather;
```

Expected: 150 rows in `ev_stations`, 89 rows in `weather` for the initial US dataset.

### Verify no duplicates in source tables

```sql
SELECT lat_round, lon_round, COUNT(*)
FROM public.weather
GROUP BY lat_round, lon_round
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC
LIMIT 10;

SELECT station_id, connection_id, COUNT(*)
FROM public.ev_stations
GROUP BY station_id, connection_id
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC
LIMIT 10;
```

Both queries should return zero rows if the TRUNCATE + COPY sequence ran correctly.

### Verify COPY errors

If a COPY command fails, inspect the error details:

```sql
SELECT *
FROM sys_load_error_detail
ORDER BY start_time DESC
LIMIT 10;
```

Common error causes are VARCHAR columns that are too short for the actual data values — increase the column width in the DDL and retry.

### Verify schemas exist

```sql
SELECT schema_name
FROM information_schema.schemata
ORDER BY schema_name;
```

You should see `gold`, `public`, and `staging` in the results.

### Verify all tables exist

```sql
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema IN ('public', 'staging', 'gold')
ORDER BY table_schema, table_name;
```

Expected tables after a full pipeline run:

| Schema | Table |
|---|---|
| public | ev_stations |
| public | weather |
| staging | stg_ev |
| staging | stg_weather |
| gold | dim_station |
| gold | dim_weather |
| gold | fact_station_status |
| gold | fct_risk |

## Gold Layer Queries

### Risk summary by band

```sql
SELECT
    risk_band,
    COUNT(*) AS stations,
    ROUND(AVG(risk_score), 1) AS avg_risk_score,
    ROUND(SUM(est_revenue_at_risk), 2) AS total_revenue_at_risk
FROM gold.fct_risk
GROUP BY risk_band
ORDER BY avg_risk_score DESC;
```

### Risk by weather condition

```sql
SELECT
    condition,
    COUNT(*) AS station_connections,
    ROUND(AVG(risk_score), 1) AS avg_risk_score,
    ROUND(AVG(temperature_f), 1) AS avg_temp_f,
    ROUND(AVG(wind_speed), 1) AS avg_wind_speed
FROM gold.fct_risk
GROUP BY condition
ORDER BY avg_risk_score DESC;
```

### Top 10 highest risk stations

```sql
SELECT
    title,
    town,
    state_or_province,
    condition,
    temperature_f,
    wind_speed,
    risk_score,
    risk_band,
    est_revenue_at_risk
FROM gold.fct_risk
ORDER BY risk_score DESC
LIMIT 10;
```

### Offline stations with revenue at risk

```sql
SELECT
    title,
    town,
    state_or_province,
    power_kw,
    condition,
    risk_score,
    est_revenue_at_risk
FROM gold.fct_risk
WHERE is_operational = false
   OR is_operational IS NULL
ORDER BY est_revenue_at_risk DESC;
```

### Total revenue at risk

```sql
SELECT
    ROUND(SUM(est_revenue_at_risk), 2) AS total_revenue_at_risk,
    COUNT(*) AS total_station_connections,
    SUM(CASE WHEN is_operational = false OR is_operational IS NULL THEN 1 ELSE 0 END) AS offline_connections
FROM gold.fct_risk;
```

### Weather zone coverage

```sql
SELECT
    weather_zone_id,
    lat_round,
    lon_round,
    condition,
    temperature_f,
    wind_speed,
    humidity
FROM gold.dim_weather
ORDER BY lat_round, lon_round;
```

### Station history over time

```sql
SELECT
    station_id,
    scored_at,
    risk_score,
    risk_band,
    condition,
    temperature_f,
    is_operational,
    est_revenue_at_risk
FROM gold.fact_station_status
ORDER BY station_id, scored_at DESC
LIMIT 50;
```

## Deduplication Queries

If duplicate rows accumulate in source tables due to multiple COPY runs without TRUNCATE, use these to clean them up.

### Deduplicate weather

```sql
CREATE TABLE public.weather_dedup AS
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY lat_round, lon_round
               ORDER BY ingested_at DESC
           ) AS rn
    FROM public.weather
) t
WHERE rn = 1;

DROP TABLE public.weather CASCADE;
ALTER TABLE public.weather_dedup RENAME TO weather;

SELECT COUNT(*) FROM public.weather;
```

### Deduplicate ev_stations

```sql
CREATE TABLE public.ev_stations_dedup AS
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY station_id, connection_id
               ORDER BY date_last_status_update DESC
           ) AS rn
    FROM public.ev_stations
) t
WHERE rn = 1;

DROP TABLE public.ev_stations CASCADE;
ALTER TABLE public.ev_stations_dedup RENAME TO ev_stations;

SELECT COUNT(*) FROM public.ev_stations;
```

> After deduplicating source tables always re-run `dbt run` and `dbt test` to rebuild the Gold models from the clean data.

## Dropping and Resetting Everything

If you need to start completely fresh:

```sql
DROP TABLE IF EXISTS public.ev_stations CASCADE;
DROP TABLE IF EXISTS public.weather CASCADE;
DROP SCHEMA IF EXISTS staging CASCADE;
DROP SCHEMA IF EXISTS gold CASCADE;
```

Then recreate schemas, grant permissions, recreate tables, reload data, and run dbt again from scratch following the steps in the [Local Setup Guide](02_local_setup.md).
