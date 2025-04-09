from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def say_hello():
    print("Hello from Airflow!")

with DAG(
    dag_id="hello_airflow",  # make sure this matches what you try to trigger
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id="say_hello",
        python_callable=say_hello
    )
