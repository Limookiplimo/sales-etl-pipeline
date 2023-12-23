import sys
sys.path.append('~/Documents/Projects/piper/orchestrations')

from job_scripts.generator import load_to_database

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import tomli

default_args = {
    "owner": "limoo",
    "start_date": datetime(2023,12,11),
    "retry_delay": timedelta(minutes=1),
}
dag = DAG(
    "generate_data",
    default_args=default_args,
    description='Orchestration of data generation, processing and persistence into a data warehouse',
    schedule_interval="5 * * * *"
)

task_1 = PythonOperator(
    task_id='generate_data',
    provide_context=True,
    python_callable=load_to_database,
    dag=dag
)

task_1
