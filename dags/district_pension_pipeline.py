from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

PROJECT_DIR = "/Users/sohamdutta/Documents/Projects/district-pension-pipeline"

default_args = {
    "owner": "soham",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

def run_script(script_name):
    return f"""
    cd {PROJECT_DIR}
    source venv/bin/activate
    export JAVA_HOME=$(/usr/libexec/java_home -v 17)
    export PATH="$JAVA_HOME/bin:$PATH"
    python scripts/{script_name}
    """

with DAG(
    dag_id="district_pension_pipeline",
    default_args=default_args,
    description="Jharkhand district pension data pipeline",
    start_date=datetime(2026, 11, 13),
    schedule=None,
    catchup=False,
    tags=["Jharkhand", "PySpark", "Pension"],
) as dag:

    ingest_bronze = BashOperator(
        task_id="ingest_bronze",
        bash_command=run_script("01_ingest_bronze.py"),
    )

    validate_bronze = BashOperator(
        task_id="validate_bronze",
        bash_command=run_script("03_validate_bronze.py"),
    )

    build_silver = BashOperator(
        task_id="build_silver",
        bash_command=run_script("04_build_silver.py"),
    )

    build_gold = BashOperator(
        task_id="build_gold",
        bash_command=run_script("05_build_gold.py"),
    )

    build_ml_features = BashOperator(
        task_id="build_ml_features",
        bash_command=run_script("06_build_ml_features.py"),
    )

    ingest_bronze >> validate_bronze >> build_silver >> build_gold >> build_ml_features