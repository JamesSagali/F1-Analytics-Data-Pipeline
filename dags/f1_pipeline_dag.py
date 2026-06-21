from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'james'
}

with DAG(
    dag_id='f1_weekly_pipeline',
    default_args=default_args,
    start_date=datetime(2026, 5, 24),
    schedule="14 6 * * 1",
    catchup=False
) as dag:

    run_pipeline = BashOperator(
        task_id='run_f1_pipeline',
        bash_command='python /opt/airflow/scripts/ingest_pipeline.py'
    )
    
    run_lap_times = BashOperator(
        task_id='run_lap_times_pipeline',
        bash_command='python /opt/airflow/scripts/ingest_lap_times.py'
    )
    
    run_pipeline >> run_lap_times