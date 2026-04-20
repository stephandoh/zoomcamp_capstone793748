# Local Setup Guide

This guide walks you through reproducing the VoltStream pipeline on your local machine. By the end you will have the full pipeline running — ingestion scripts, Airflow orchestration, dbt transformations, and the Metabase dashboard.

Before starting, make sure you have completed the [AWS Setup Guide](01_aws_setup.md) and have all your API keys and AWS credentials ready.

## Prerequisites

Make sure the following are installed on your machine:

- **Python 3.12+** — [https://www.python.org/downloads](https://www.python.org/downloads)
- **Docker Desktop** — [https://www.docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop)
- **Git** — [https://git-scm.com/downloads](https://git-scm.com/downloads)
- **dbt Core** — installed via pip (covered in the [dbt Guide](03_dbt_guide.md))

Verify your installations before proceeding:

```bash
python --version
docker --version
docker-compose --version
git --version
```

## Cloning the Repository

```bash
git clone https://github.com/your-username/voltstream.git
cd voltstream
```

## Setting Up Environment Variables

The pipeline reads all secrets from a `.env` file in the project root. This file is never committed to git.

Copy the example file and fill in your values:

```bash
cp .env.example .env
```

Open `.env` and fill in every value. Refer to the [AWS Setup Guide](01_aws_setup.md) for where to find each key.

```dotenv
OPENCHARGE_API_KEY=your_open_charge_map_key
WEATHER_API_KEY=your_openweathermap_key
COUNTRY_CODE=US
S3_BUCKET_BRONZE=voltstream-bronze
S3_BUCKET_SILVER=voltstream-silver
AWS_ACCESS_KEY_ID=your_iam_access_key
AWS_SECRET_ACCESS_KEY=your_iam_secret_key
REDSHIFT_PASSWORD=your_redshift_admin_password
DBT_HOST=default-workgroup.ACCOUNTID.us-east-1.redshift-serverless.amazonaws.com
DBT_DATABASE=dev
DBT_USER=admin
DBT_PASSWORD=your_redshift_admin_password
```

> **Never commit your `.env` file.** It is already listed in `.gitignore`. Double check with `git status` before pushing.

## Installing Python Dependencies

Install the pipeline dependencies in your local Python environment:

```bash
pip install -r requirements.txt
```

These are the dependencies for the four ingestion and processing scripts only. Airflow dependencies are handled separately inside Docker.

## Running Without Docker

If you want to run the pipeline scripts directly without Docker — for example during development or debugging — you can run each script individually from the project root:

```bash
# Step 1 — ingest EV stations into S3 Bronze
python ingestion/ev_ingest.py

# Step 2 — transform EV Bronze → Silver
python processing/bronze_to_silver_ev.py

# Step 3 — ingest weather data into S3 Bronze
# (runs after Silver EV exists — reads zones from Silver)
python ingestion/weather_ingest.py

# Step 4 — transform weather Bronze → Silver
python processing/bronze_to_silver_weather.py
```

Run them in this exact order. `weather_ingest.py` depends on Silver EV data existing because it reads weather zones from `s3://voltstream-silver/ev/`.

On the first run `ev_ingest.py` performs a full historical load — no watermark exists yet so it fetches all available records for your `COUNTRY_CODE`. Subsequent runs are incremental — only records modified since the last run are fetched.

## Running With Docker

Docker packages all four scripts into a single container image that runs them in sequence. This is the same container that Airflow uses in production.

### Building the pipeline image

From the project root:

```bash
docker build -t voltstream-pipeline .
```

This builds the `voltstream-pipeline` image using the `Dockerfile` at the project root. The build installs all Python dependencies from `requirements.txt` and copies the `ingestion/` and `processing/` folders into the image.

The first build takes 2-3 minutes. Subsequent builds are fast because Docker caches the dependency layer — only the scripts themselves are rebuilt if you change them.

### Running the pipeline container

```bash
docker run --rm \
  -e OPENCHARGE_API_KEY="your_key" \
  -e WEATHER_API_KEY="your_key" \
  -e S3_BUCKET_BRONZE="voltstream-bronze" \
  -e S3_BUCKET_SILVER="voltstream-silver" \
  -e AWS_ACCESS_KEY_ID="your_key" \
  -e AWS_SECRET_ACCESS_KEY="your_key" \
  -e COUNTRY_CODE="US" \
  voltstream-pipeline
```

On Windows PowerShell use backticks for line continuation:

```powershell
docker run --rm `
  -e OPENCHARGE_API_KEY="your_key" `
  -e WEATHER_API_KEY="your_key" `
  -e S3_BUCKET_BRONZE="voltstream-bronze" `
  -e S3_BUCKET_SILVER="voltstream-silver" `
  -e AWS_ACCESS_KEY_ID="your_key" `
  -e AWS_SECRET_ACCESS_KEY="your_key" `
  -e COUNTRY_CODE="US" `
  voltstream-pipeline
```

You can also run a single script inside the container by overriding the default command:

```bash
docker run --rm --env-file .env voltstream-pipeline python ingestion/ev_ingest.py
```

### Verifying the container works

Before running the full pipeline, confirm all modules import correctly:

```bash
docker run --rm voltstream-pipeline python -c "
import ingestion.ev_ingest as ev
import ingestion.weather_ingest as w
import processing.bronze_to_silver_ev as bse
import processing.bronze_to_silver_weather as bsw
print('All modules import cleanly')
"
```

## Setting Up Airflow

Airflow orchestrates the full pipeline on a daily schedule. It runs inside Docker Compose alongside a Postgres metadata database.

<img width="1273" height="691" alt="image" src="https://github.com/user-attachments/assets/929403d6-9b2f-4f24-8615-534a741940c6" />

### Project structure for Airflow

```
voltstream/
├── docker-compose.yml
└── airflow/
    ├── Dockerfile              # Airflow image with providers + dbt
    ├── requirements-airflow.txt
    └── dags/
        └── voltstream_dag.py
```

### First-time initialisation

Run this once to set up the Airflow database and create the admin user:

```bash
docker-compose up airflow-init
```

Wait for the output to show `Airflow initialised successfully` before proceeding.

### Starting Airflow

```bash
docker-compose up -d
```

This starts three containers in the background:

- `voltstream-postgres-1` — Airflow metadata database
- `voltstream-webserver-1` — Airflow UI at `http://localhost:8080`
- `voltstream-scheduler-1` — monitors DAGs and triggers runs

Check all containers are healthy:

```bash
docker-compose ps
```

All three containers should show `healthy` or `running` status. The webserver takes about 30 seconds to become healthy after starting.

### Accessing the Airflow UI

Open `http://localhost:8080` in your browser and log in with:

- **Username:** `admin`
- **Password:** `admin`

### Configuring Airflow Variables

The DAG reads API keys from Airflow Variables rather than environment variables. Go to `Admin → Variables` in the UI and add these four variables one by one using the **+** button:

| Key | Value |
|---|---|
| `opencharge_api_key` | your Open Charge Map API key |
| `weather_api_key` | your OpenWeatherMap API key |
| `aws_access_key_id` | your AWS access key ID |
| `aws_secret_access_key` | your AWS secret access key |

### Configuring the AWS connection

Go to `Admin → Connections` and click **+** to add a new connection:

| Field | Value |
|---|---|
| Connection Id | `aws_default` |
| Connection Type | `Amazon Web Services` |
| AWS Access Key ID | your AWS access key ID |
| AWS Secret Access Key | your AWS secret access key |
| Extra | `{"region_name": "us-east-1"}` |

Click **Save**.

### Triggering a DAG run

1. Navigate to **DAGs** in the top menu
2. Find `voltstream_pipeline` — it will be paused by default
3. Toggle the pause switch on the left to enable it
4. Click the play button (▶) on the right and select **Trigger DAG**
5. Click on `voltstream_pipeline` to open it
6. Click **Graph** to watch the tasks run in real time

The full pipeline takes approximately 4-6 minutes to complete depending on API response times and Redshift startup time.

### Stopping Airflow

```bash
docker-compose down
```

This stops and removes the containers but preserves the Postgres volume — your DAG run history is retained. To remove everything including history:

```bash
docker-compose down -v
```

## Loading Data into Redshift

After the pipeline container runs, Silver data sits in S3. The Airflow DAG handles loading automatically via COPY commands. If you want to load manually:

```sql
TRUNCATE TABLE public.ev_stations;
COPY public.ev_stations
FROM 's3://voltstream-silver/ev/YYYY/MM/DD/'
IAM_ROLE 'arn:aws:iam::YOUR_ACCOUNT:role/AmazonRedshift-CommandsAccessRole'
FORMAT AS JSON 'auto'
REGION 'us-east-1';

TRUNCATE TABLE public.weather;
COPY public.weather
FROM 's3://voltstream-silver/weather/YYYY/MM/DD/'
IAM_ROLE 'arn:aws:iam::YOUR_ACCOUNT:role/AmazonRedshift-CommandsAccessRole'
FORMAT AS JSON 'auto'
REGION 'us-east-1';
```

Replace `YYYY/MM/DD` with today's date and `YOUR_ACCOUNT` with your AWS account ID.

> **Always TRUNCATE before COPY.** Running COPY without TRUNCATE appends data and creates duplicates. The Airflow DAG handles this automatically — TRUNCATE always runs before COPY in the task sequence.

## Setting Up Metabase

Metabase is the dashboard layer. It runs as a separate Docker container independent of Airflow.

### Starting Metabase

```bash
docker run -d -p 3000:3000 --name metabase metabase/metabase
```

Wait about 60 seconds for Metabase to initialise, then open `http://localhost:3000`.

### Connecting Metabase to Redshift

During the setup wizard, when prompted to add your data, fill in:

| Field | Value |
|---|---|
| Database type | Amazon Redshift |
| Display name | VoltStream |
| Host | your Redshift endpoint |
| Port | 5439 |
| Database name | dev |
| Username | admin |
| Password | your Redshift password |

Click **Connect database**.

### Building the dashboard

The dashboard cannot be automated — it must be built manually in the Metabase UI. See the dashboard section in the main [README](../README.md) for the list of visuals to build and the queries behind each one.

The four required visuals are:

**Total Estimated Revenue at Risk** — `New Question → fct_risk → Sum of est_revenue_at_risk → Number visualization`

**Avg Risk Score by Weather Condition** — `New Question → fct_risk → Group by condition → Average of risk_score → Bar chart`

**Maintenance Priority List** — `New Question → fct_risk → Select columns → Sort by risk_score descending → Table visualization`

**Weather Condition filter** — `Edit dashboard → Filter icon → Text or Category → connect to condition field on all cards`

## Verifying the Full Pipeline

After running the pipeline end to end, verify data at each layer:

```bash
# Check Bronze files landed in S3
aws s3 ls s3://voltstream-bronze/ev/ --recursive
aws s3 ls s3://voltstream-bronze/weather/ --recursive

# Check Silver files landed in S3
aws s3 ls s3://voltstream-silver/ev/ --recursive
aws s3 ls s3://voltstream-silver/weather/ --recursive
```

In Redshift Query Editor v2:

```sql
-- Verify source tables
SELECT COUNT(*) AS ev_count FROM public.ev_stations;
SELECT COUNT(*) AS weather_count FROM public.weather;

-- Verify Gold models
SELECT COUNT(*) FROM gold.dim_station;
SELECT COUNT(*) FROM gold.dim_weather;
SELECT COUNT(*) FROM gold.fct_risk;
SELECT COUNT(*) FROM gold.fact_station_status;

-- Spot check risk scores
SELECT risk_band, COUNT(*) as stations,
       ROUND(AVG(risk_score), 1) as avg_risk,
       ROUND(SUM(est_revenue_at_risk), 2) as total_revenue_at_risk
FROM gold.fct_risk
GROUP BY risk_band
ORDER BY avg_risk DESC;
```

If all counts are non-zero and the risk query returns results you have successfully reproduced the full pipeline.

Next: [dbt Guide](03_dbt_guide.md)
