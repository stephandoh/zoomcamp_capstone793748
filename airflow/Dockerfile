FROM apache/airflow:2.9.1-python3.12

# Install required Airflow providers and dbt
RUN pip install --no-cache-dir \
    apache-airflow-providers-amazon==8.7.1 \
    apache-airflow-providers-docker==3.10.0 \
    dbt-core==1.8.0 \
    dbt-redshift==1.8.0