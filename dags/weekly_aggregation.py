from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess


def run_spark_aggregation():
    target_date = datetime.now().strftime('%Y-%m-%d')
    subprocess.run(['python', '/opt/airflow/scripts/script.py', target_date])


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'weekly_log_aggregation',
    default_args=default_args,
    schedule_interval='0 7 * * *',
    catchup=False,
) as dag:
    aggregate_task = PythonOperator(
        task_id='run_spark_aggregation',
        python_callable=run_spark_aggregation,
    )
