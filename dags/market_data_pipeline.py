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
    start_date=datetime(2026, 5, 18),
    schedule=None,
    catchup=False,
    tags=["market-data", "minio", "spark"],
) as dag:
    ingest_raw_data = BashOperator(
        task_id="ingest_raw_data",
        bash_command="cd /opt/airflow && python src/ingestion/upload_raw_to_minio.py",
    )

    transform_market_data = BashOperator(
        task_id="transform_market_data",
        bash_command="cd /opt/airflow && python src/jobs/transform_data.py",
    )

    run_data_quality_checks = BashOperator(
        task_id="run_data_quality_checks",
        bash_command="cd /opt/airflow && python src/quality/check_curated_data.py",
    )

    ingest_raw_data >> transform_market_data >> run_data_quality_checks
