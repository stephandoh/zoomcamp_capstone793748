"""
VoltStream EV Grid Resilience Pipeline
=======================================
Orchestrates the full pipeline on a daily schedule:

  1. run_pipeline     — Docker container: Bronze + Silver ingestion
  2. truncate_ev      — Redshift: clear stale EV data
  3. truncate_weather — Redshift: clear stale weather data
  4. copy_ev          — Redshift: COPY Silver EV → public.ev_stations
  5. copy_weather     — Redshift: COPY Silver weather → public.weather
  6. dbt_run          — dbt: build all staging + gold models
  7. dbt_test         — dbt: run all 25 data tests
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator

default_args = {
    "owner": "voltstream",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

RUN_DATE = "{{ macros.ds_format(ds, '%Y-%m-%d', '%Y/%m/%d') }}"
IAM_ROLE = "arn:aws:iam::885180542073:role/service-role/AmazonRedshift-CommandsAccessRole-20260414T152746"
REDSHIFT_WORKGROUP = "default-workgroup"
REDSHIFT_DATABASE = "dev"

with DAG(
    dag_id="voltstream_pipeline",
    description="VoltStream EV Grid Resilience — daily pipeline",
    default_args=default_args,
    start_date=datetime(2026, 4, 19),
    schedule_interval="0 0 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["voltstream", "ev", "weather", "dbt"],
) as dag:

    run_pipeline = DockerOperator(
        task_id="run_pipeline",
        image="voltstream-pipeline:latest",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        environment={
            "OPENCHARGE_API_KEY": "{{ var.value.opencharge_api_key }}",
            "WEATHER_API_KEY": "{{ var.value.weather_api_key }}",
            "S3_BUCKET_BRONZE": "voltstream-bronze",
            "S3_BUCKET_SILVER": "voltstream-silver",
            "AWS_ACCESS_KEY_ID": "{{ var.value.aws_access_key_id }}",
            "AWS_SECRET_ACCESS_KEY": "{{ var.value.aws_secret_access_key }}",
            "COUNTRY_CODE": "US",
        },
    )

    truncate_ev = RedshiftDataOperator(
        task_id="truncate_ev",
        database=REDSHIFT_DATABASE,
        workgroup_name=REDSHIFT_WORKGROUP,
        sql="TRUNCATE TABLE public.ev_stations;",
        wait_for_completion=True,
        aws_conn_id="aws_default",
    )

    truncate_weather = RedshiftDataOperator(
        task_id="truncate_weather",
        database=REDSHIFT_DATABASE,
        workgroup_name=REDSHIFT_WORKGROUP,
        sql="TRUNCATE TABLE public.weather;",
        wait_for_completion=True,
        aws_conn_id="aws_default",
    )

    copy_ev = RedshiftDataOperator(
        task_id="copy_ev",
        database=REDSHIFT_DATABASE,
        workgroup_name=REDSHIFT_WORKGROUP,
        sql=f"""
            COPY public.ev_stations
            FROM 's3://voltstream-silver/ev/{RUN_DATE}/'
            IAM_ROLE '{IAM_ROLE}'
            FORMAT AS JSON 'auto'
            REGION 'us-east-1';
        """,
        wait_for_completion=True,
        aws_conn_id="aws_default",
    )

    copy_weather = RedshiftDataOperator(
        task_id="copy_weather",
        database=REDSHIFT_DATABASE,
        workgroup_name=REDSHIFT_WORKGROUP,
        sql=f"""
            COPY public.weather
            FROM 's3://voltstream-silver/weather/{RUN_DATE}/'
            IAM_ROLE '{IAM_ROLE}'
            FORMAT AS JSON 'auto'
            REGION 'us-east-1';
        """,
        wait_for_completion=True,
        aws_conn_id="aws_default",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd /opt/airflow/dbt/voltstream_dbt && "
            "dbt run --profiles-dir /opt/airflow/dbt/voltstream_dbt"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/airflow/dbt/voltstream_dbt && "
            "dbt test --profiles-dir /opt/airflow/dbt/voltstream_dbt"
        ),
    )

    run_pipeline >> [truncate_ev, truncate_weather]
    truncate_ev >> copy_ev
    truncate_weather >> copy_weather
    [copy_ev, copy_weather] >> dbt_run
    dbt_run >> dbt_test