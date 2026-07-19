from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from datetime import timedelta

with DAG(
    dag_id="trafiklab_static_refresh_dag",
    schedule=timedelta(days=1),  # Samtrafiken republishes daily; trip_ids rotate on each publish
    start_date=datetime(2026, 7, 12),
    catchup=False,
) as dag:
        run_ingestion = BashOperator(
        task_id="run_ingestion",
        bash_command="python /opt/airflow/project/ingestion/ingest_static_gtfs.py" 
    )

