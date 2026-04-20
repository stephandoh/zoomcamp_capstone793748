try:
    from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
    print("RedshiftSQLOperator found at: airflow.providers.amazon.aws.operators.redshift_sql")
except ImportError as e:
    print(f"Not found: {e}")

try:
    from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
    print("RedshiftDataOperator found")
except ImportError as e:
    print(f"RedshiftDataOperator not found: {e}")

import subprocess
result = subprocess.run(
    ["pip", "show", "apache-airflow-providers-amazon"],
    capture_output=True, text=True
)
print(result.stdout)