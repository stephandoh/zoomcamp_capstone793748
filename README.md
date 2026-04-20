# VoltStream — EV Grid Resilience Pipeline

A production-grade end-to-end data engineering pipeline that ingests real-time EV charging station data and weather conditions, correlates them through a Medallion architecture, and surfaces grid risk and revenue exposure through a live dashboard.

Built for VoltStream Energy Solutions, a municipal EV charging network operator managing 500+ stations across the US.

---

<img width="1274" height="428" alt="image" src="https://github.com/user-attachments/assets/44b026f8-f86a-4c23-9bd5-fd5a03cebb11" />

---

## The Problem

VoltStream operates a large network of EV charging stations but has no visibility into how weather conditions affect station availability. Maintenance is reactive. Failures are discovered after the fact, and there is no way to anticipate grid strain events before they cause downtime and lost revenue.

## The Solution

An automated data pipeline that runs daily, correlating EV station operational status with local weather conditions across geographic zones. The result is a risk-scored dataset that identifies which stations are most vulnerable before adverse weather hits; enabling proactive maintenance scheduling and quantifying financial exposure from station downtime.

---

## Key Findings

<img width="824" height="643" alt="final_dashboard" src="https://github.com/user-attachments/assets/b5ea988b-fe39-4ac8-a202-8dd036f55cb0" />

- **$231,875** estimated hourly revenue at risk across the monitored station network
- **Rain** produces the highest average risk score (~27/100) across all weather conditions
- **DC fast chargers** (>50kW) carry elevated baseline grid risk regardless of weather due to high power draw
- **89 distinct weather zones** derived from station coordinates, reducing API calls from 500+ (per station) to 89 (per zone)
- **25/25 data quality tests** passing across all pipeline layers on every run

---

## Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Ingestion | Python + Docker | Fetch EV + weather data from APIs |
| Bronze storage | AWS S3 | Raw NDJSON landing zone |
| Processing | Python (Pandas) | Flatten, clean, deduplicate |
| Silver storage | AWS S3 | Structured, quality-checked data |
| Orchestration | Apache Airflow | Daily scheduling, task coordination |
| Warehouse | Amazon Redshift Serverless | Analytical query layer |
| Transformation | dbt | Staging + Gold models, data tests |
| Dashboard | Metabase | Grid resilience visualisation |

---

## Pipeline Overview

The pipeline runs daily at midnight UTC via Airflow and executes 7 tasks in sequence:

```
APIs → ev_ingest.py → S3 Bronze → bronze_to_silver_ev.py → S3 Silver
     → weather_ingest.py → S3 Bronze → bronze_to_silver_weather.py → S3 Silver
     → Redshift COPY → dbt run (6 models) → dbt test (25 tests)
```

**Medallion architecture:**
- **Bronze**: raw API responses stored as NDJSON, partitioned by date
- **Silver**: flattened, deduplicated, quality-flagged data with SCD Type 2 columns
- **Gold**: `dim_station`, `dim_weather`, `fct_risk`, `fact_station_status` — business-ready Star Schema

**Weather zone strategy**: rather than making one API call per station (500+), station coordinates are rounded to 1 decimal place, grouping geographically close stations into shared weather zones. This reduces API calls by ~85% while maintaining regional accuracy.

---

## Dashboard

The Metabase dashboard surfaces four key business views:

- **Revenue at Risk** : total estimated hourly revenue exposure from offline stations
- **Weather Correlation** : average risk score by weather condition (proves weather drives grid risk)
- **Maintenance Priority List** : stations ranked by risk score with weather context
- **Weather Condition filter** : filters all visuals simultaneously by condition

---

## Project Structure

```
voltstream/
├── Dockerfile                    # Pipeline container image
├── docker-compose.yml            # Airflow local environment
├── requirements.txt              # Pipeline dependencies
├── pyrightconfig.json            # VS Code type checking config
├── .env                          # Secrets (not committed)
├── .gitignore
├── README.md
│
├── ingestion/
│   ├── ev_ingest.py              # Open Charge Map API → S3 Bronze
│   └── weather_ingest.py         # OpenWeatherMap API → S3 Bronze
│
├── processing/
│   ├── bronze_to_silver_ev.py    # EV Bronze → Silver transformation
│   └── bronze_to_silver_weather.py # Weather Bronze → Silver transformation
│
├── airflow/
│   ├── Dockerfile                # Airflow image with providers + dbt
│   ├── requirements-airflow.txt  # Airflow dependencies
│   └── dags/
│       └── voltstream_dag.py     # 7-task orchestration DAG
│
├── dbt/
│   └── voltstream_dbt/
│       ├── dbt_project.yml
│       ├── profiles.yml          # Uses env vars, no hardcoded credentials
│       ├── macros/
│       │   └── generate_schema_name.sql
│       └── models/
│           ├── sources.yml
│           ├── staging/
│           │   ├── stg_ev.sql
│           │   ├── stg_weather.sql
│           │   └── schema.yml
│           └── marts/
│               ├── dim_station.sql
│               ├── dim_weather.sql
│               ├── fct_risk.sql
│               ├── fact_station_status.sql
│               └── schema.yml
│
└── docs/
    ├── architecture.html         # Interactive architecture diagram
    ├── architecture.png          # Static architecture diagram
    ├── 01_aws_setup.md           # S3, IAM, Redshift setup guide
    ├── 02_local_setup.md         # Local reproduction guide
    ├── 03_dbt_guide.md           # dbt setup and commands
    └── 04_redshift_sql_reference.md # All SQL used in the project
```

---

## Quick Start

**Prerequisites:**
- Python 3.12+
- Docker Desktop
- AWS account with S3 and Redshift Serverless access
- API keys from [Open Charge Map](https://openchargemap.org/develop/api) and [OpenWeatherMap](https://openweathermap.org/api)

**1 — Clone the repo:**
```bash
git clone https://github.com/your-username/voltstream.git
cd voltstream
```

**2 — Set up environment variables:**
```bash
cp .env.example .env
# Fill in your actual keys — see docs/01_aws_setup.md
```

**3 — Build the pipeline image:**
```bash
docker build -t voltstream-pipeline .
```

**4 — Run the pipeline manually:**
```bash
docker run --rm --env-file .env voltstream-pipeline
```

**5 — Start Airflow:**
```bash
docker-compose up airflow-init
docker-compose up -d
# Open http://localhost:8080 (admin / admin)
```

**6 — Run dbt:**
```bash
cd dbt/voltstream_dbt
dbt run
dbt test
```

See the full reproduction guides in [`docs/`](docs/) for detailed AWS setup, Redshift configuration, and dbt instructions.

---

## Data Sources

- **[Open Charge Map API](https://openchargemap.org/develop/api)**: global EV charging station locations, connector types, operational status. Free tier, requires API key.
- **[OpenWeatherMap API](https://openweathermap.org/api)**: current weather conditions by coordinate. Free tier (60 calls/minute), requires API key.

---

## Documentation

| Document | Description |
|---|---|
| [AWS Setup](docs/01_aws_setup.md) | S3 buckets, IAM user, permissions, Redshift Serverless |
| [Local Setup](docs/02_local_setup.md) | Full reproduction guide with Docker and Airflow |
| [dbt Guide](docs/03_dbt_guide.md) | dbt installation, commands, common errors |
| [Redshift SQL Reference](docs/04_redshift_sql_reference.md) | All SQL used in the project |

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on how to contribute to this project.

---

## Author

Stephanie Dawsonn-Andoh  
Data Engineer
