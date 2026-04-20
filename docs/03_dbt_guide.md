# dbt Guide

This guide covers everything you need to know about the dbt setup in the VoltStream pipeline — installation, project structure, running commands, and how to debug common errors.

dbt is the transformation layer. It reads clean data from the `public` schema in Redshift (loaded via COPY commands) and builds the analytical Gold models in the `staging` and `gold` schemas.

## Why dbt is Not in the Main Project Structure

You will notice that dbt lives under `dbt/voltstream_dbt/` rather than at the project root. This is intentional.

dbt has its own project conventions — `models/`, `macros/`, `tests/`, `target/`, `dbt_packages/` — and generates a lot of build artifacts. Keeping it in its own subfolder keeps the root clean and makes it clear that dbt is one layer of the pipeline, not the whole thing.

The `profiles.yml` inside `dbt/voltstream_dbt/` uses environment variables rather than hardcoded credentials. This means the file is safe to commit to git — it contains no actual secrets. The real credentials live in your `.env` file and are injected at runtime.

## Installation

Install dbt Core and the Redshift adapter using pip:

```bash
pip install dbt-core==1.8.0
pip install dbt-redshift==1.8.0
```

Verify the installation:

```bash
dbt --version
```

You should see output similar to:

```
Core:
  - installed: 1.8.0
  - latest:    1.8.0 - Up to date!

Plugins:
  - redshift: 1.8.0 - Up to date!
```

## Project Structure

```
dbt/voltstream_dbt/
├── dbt_project.yml             # Project config, model materialisation settings
├── profiles.yml                # Connection config using env vars
├── macros/
│   └── generate_schema_name.sql  # Overrides dbt default schema naming
└── models/
    ├── sources.yml             # Defines source tables in Redshift public schema
    ├── staging/
    │   ├── stg_ev.sql          # Cleans and filters ev_stations source
    │   ├── stg_weather.sql     # Cleans weather source
    │   └── schema.yml          # Tests for staging models
    └── marts/
        ├── dim_station.sql     # Station dimension — one row per station
        ├── dim_weather.sql     # Weather dimension — one row per zone
        ├── fct_risk.sql        # Risk scoring model — joins EV + weather
        ├── fact_station_status.sql  # Incremental fact table — history over time
        └── schema.yml          # Tests for Gold models
```

## What is in dbt_project.yml

The `dbt_project.yml` configures how dbt builds your models. The key sections are:

```yaml
models:
  voltstream_dbt:
    staging:
      +materialized: view       # Staging models are views — lightweight, always fresh
      +schema: staging          # Built in the staging schema in Redshift
    marts:
      +materialized: table      # Gold models are tables — pre-computed, fast to query
      +schema: gold             # Built in the gold schema in Redshift

vars:
  energy_rate: 0.35             # Default $/kWh used in revenue at risk calculation
```

The `energy_rate` variable drives the `est_revenue_at_risk` calculation in `fct_risk.sql`. You can override it at run time without changing any code:

```bash
dbt run --vars '{"energy_rate": 0.42}'
```

## What is in profiles.yml

`profiles.yml` tells dbt how to connect to Redshift. The version inside `dbt/voltstream_dbt/` uses environment variables so it is safe to commit:

```yaml
voltstream_dbt:
  outputs:
    dev:
      type: redshift
      method: database
      host: "{{ env_var('DBT_HOST') }}"
      port: 5439
      dbname: "{{ env_var('DBT_DATABASE') }}"
      user: "{{ env_var('DBT_USER') }}"
      password: "{{ env_var('DBT_PASSWORD') }}"
      schema: staging
      threads: 4
      connect_timeout: 30
      is_serverless: true
      serverless_work_group: default-workgroup
  target: dev
```

When running dbt locally your machine reads credentials from `C:\Users\your-username\.dbt\profiles.yml` (Windows) or `~/.dbt/profiles.yml` (Mac/Linux) by default — this file contains hardcoded values and is never committed. The env var version inside the project is used exclusively by the Airflow container.

## dbt Commands

All dbt commands should be run from inside the `dbt/voltstream_dbt/` directory:

```bash
cd dbt/voltstream_dbt
```

### dbt debug

Tests the connection between dbt and Redshift. Run this first whenever you set up a new environment or change credentials:

```bash
dbt debug
```

A successful output ends with `All checks passed!`. If the connection fails check your `profiles.yml` values and make sure Redshift is publicly accessible on port 5439.

### dbt run

Builds all models in dependency order:

```bash
dbt run
```

dbt resolves dependencies automatically using `{{ ref() }}` — staging models run before marts, `fct_risk` runs before `fact_station_status`. You do not need to specify an order.

To run a specific model only:

```bash
dbt run -s dim_station
```

To run a model and all models that depend on it:

```bash
dbt run -s fct_risk+
```

### dbt test

Runs all 25 data quality tests defined in `schema.yml` files:

```bash
dbt test
```

Tests validate things like `not_null`, `unique`, and `accepted_values`. If a test fails dbt reports how many rows violated the constraint and where to find the compiled SQL.

To run tests for a specific model:

```bash
dbt test -s dim_weather
```

### dbt clean

Removes the `target/` and `dbt_packages/` directories — generated artifacts that should not be committed to git:

```bash
dbt clean
```

Run this if you are getting unexpected behaviour from cached compiled SQL or if you want to start fresh.

### dbt docs

Generates documentation for all models, sources, and tests:

```bash
dbt docs generate
dbt docs serve
```

This opens a browser with an interactive lineage graph showing how every model relates to every other model. Useful for understanding the pipeline at a glance.

## Model Descriptions

### stg_ev

Reads from `public.ev_stations` and applies two filters — only current records (`is_current = true`) and only quality-passed records (`data_quality_error = false`). Selects all columns needed by downstream Gold models. Materialised as a view.

### stg_weather

Reads from `public.weather`. Passes through all columns including both Celsius and Fahrenheit temperature fields. Materialised as a view.

### dim_station

One row per charging station. Built from `stg_ev` by deduplicating on `station_id` and keeping the most recently verified record. Contains location, address, operator, and geo fields. Does not contain connection-level detail — that lives in the fact table.

### dim_weather

One row per weather zone defined by `lat_round` and `lon_round`. Generates a `weather_zone_id` surrogate key using `md5(lat_round || ',' || lon_round)`. Contains current weather conditions at time of pipeline run. Deduplicates at the model level so it is robust to duplicate source rows.

### fct_risk

The core business logic model. Joins `stg_ev` to `stg_weather` on `lat_round` and `lon_round`. Calculates a risk score (0-100) based on weather condition, temperature, wind speed, and station power output. Calculates `est_revenue_at_risk` for offline stations using the `energy_rate` variable.

Risk scoring logic:

| Factor | Condition | Score |
|---|---|---|
| Weather | Thunderstorm or Snow | +40 |
| Weather | Rain | +20 |
| Weather | Other | +10 |
| Temperature | >95°F or <20°F | +20 |
| Wind | >20 mph | +20 |
| Power | >150kW | +30 |
| Power | >50kW | +20 |

Maximum possible score is 100 (capped). Risk bands: High ≥ 40, Medium ≥ 20, Low < 20.

### fact_station_status

Incremental fact table. Each pipeline run appends new rows — one per station + connection combination. This builds up history over time enabling trend analysis of how risk scores change as weather conditions change. The `scored_at` timestamp records when each row was generated.

## generate_schema_name Macro

Without this macro dbt prepends the target schema to any custom schema you define — producing names like `staging_staging` and `staging_gold` instead of `staging` and `gold`. The macro in `macros/generate_schema_name.sql` overrides this behaviour:

```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

This ensures models materialise into exactly the schema name specified in `dbt_project.yml`.

## Common Errors and How to Fix Them

### Could not find profile named 'voltstream_dbt'

dbt cannot find your `profiles.yml`. This usually means you are running dbt from the wrong directory, or your local `~/.dbt/profiles.yml` does not have a `voltstream_dbt` profile.

Fix: make sure you are in `dbt/voltstream_dbt/` and that your local `profiles.yml` at `~/.dbt/profiles.yml` contains the `voltstream_dbt` profile with hardcoded credentials.

### Env var required but not provided: 'DBT_HOST'

dbt is reading the `profiles.yml` inside the project folder (the env var version) but the environment variables are not set in your shell session.

Fix: either set the environment variables in your shell, or run dbt without `--profiles-dir` so it reads from `~/.dbt/profiles.yml` instead:

```bash
dbt run   # reads from ~/.dbt/profiles.yml automatically
```

### Permission denied for schema

Your Redshift user does not have permission to create tables in the target schema.

Fix: run the grant statements in Redshift Query Editor v2:

```sql
GRANT ALL ON SCHEMA staging TO admin;
GRANT ALL ON SCHEMA gold TO admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT ALL ON TABLES TO admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT ALL ON TABLES TO admin;
```

### Syntax error at or near "qualify"

Redshift does not support the `QUALIFY` clause. If you see this error a model is using `QUALIFY row_number()` which needs to be replaced with a standard CTE pattern:

```sql
-- Instead of this (not supported in Redshift)
SELECT * FROM my_table
QUALIFY row_number() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

-- Use this
WITH ranked AS (
  SELECT *, row_number() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rn
  FROM my_table
)
SELECT * FROM ranked WHERE rn = 1
```

### unique test failing with N results

The uniqueness test on a column found duplicate values. This usually means the source table (`public.ev_stations` or `public.weather`) has duplicate rows from multiple COPY runs without a preceding TRUNCATE.

Fix: truncate the source table and reload:

```sql
TRUNCATE TABLE public.weather;
COPY public.weather FROM 's3://voltstream-silver/weather/YYYY/MM/DD/' ...;
```

Then re-run dbt:

```bash
dbt run
dbt test
```

### Relation does not exist

dbt cannot find a source table or a model it depends on.

Fix: check that `public.ev_stations` and `public.weather` exist in Redshift and contain data. Run `SELECT COUNT(*) FROM public.ev_stations;` to verify. If the tables do not exist, run the CREATE TABLE statements from the [AWS Setup Guide](01_aws_setup.md) and reload data.

Next: [Redshift SQL Reference](04_redshift_sql_reference.md)
