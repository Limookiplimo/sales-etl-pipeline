from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from generator import generate_order_products, load_to_database


default_args = {
    'owner': 'limoo',
    'start_date': datetime(2023,12,11),
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='piper_dags',
    default_args=default_args,
    description='Orchestration of data generation, processing and persistence into a data warehouse',
    schedule='*/3 * * * *',
)

def generate_data():
    generate_order_products()

def populate_transaction_database():
    load_to_database()

with dag:
    t1 = PythonOperator(
        task_id='generate_data',
        provide_context=True,
        python_callable=generate_data,
    )
    t2 = PythonOperator(
        task_id='persist_data',
        provide_context=True,
        python_callable=populate_transaction_database,
    )
t1 >> t2
