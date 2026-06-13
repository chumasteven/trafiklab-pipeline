from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="trafiklab_realtime_dag",
    schedule="*/5 * * * *",  # Run every 5 minutes
    start_date=datetime(2026, 6, 13),
    catchup=False,
) as dag:
    
    run_producer = BashOperator(
        task_id="run_producer",
        bash_command="python /opt/airflow/project/kafka/producer.py"
    )

    run_consumer = BashOperator(
        task_id="run_consumer",
        bash_command="python /opt/airflow/project/kafka/consumer.py"
    )

    run_producer >> run_consumer # Set the producer to run before the consumer