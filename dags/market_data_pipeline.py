from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "rita-kaniska",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="market_data_pipeline",
    description="Ingest raw market data into MinIO and run Spark transformations",
    default_args=default_args,
    start_date=datetime(2025, 5, 18),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    tags=["market-data", "minio", "spark"],
) as dag:
    ingest_raw_data = BashOperator(
        task_id="ingest_raw_data",
        bash_command=(
            "set -euo pipefail; "
            "cd /opt/airflow && "
            "python src/ingestion/upload_raw_to_minio.py"
        ),
        execution_timeout=timedelta(minutes=45),
    )

    transform_market_data = BashOperator(
        task_id="transform_market_data",
        bash_command=(
            "set -euo pipefail; "
            "cd /opt/airflow && "
            "python src/jobs/transform_data.py"
        ),
        execution_timeout=timedelta(minutes=60),
    )

    run_data_quality_checks = BashOperator(
        task_id="run_data_quality_checks",
        bash_command=(
            "set -euo pipefail; "
            "cd /opt/airflow && "
            "python src/quality/check_curated_data.py"
        ),
        execution_timeout=timedelta(minutes=30),
    )

    ingest_raw_data >> transform_market_data >> run_data_quality_checks
