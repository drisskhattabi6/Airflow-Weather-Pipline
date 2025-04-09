from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import time

# Function to print a message
def print_hello():
    print("Hello, Airflow!")

# Function to wait
def wait_function():
    time.sleep(5)

# Default arguments for DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
}

# Create DAG
dag = DAG(
    "simple_dag",
    default_args=default_args,
    schedule_interval="@daily",  # Runs daily
    catchup=False,
)

# Task 1: Print Hello
task1 = PythonOperator(
    task_id="print_hello",
    python_callable=print_hello,
    dag=dag,
)

# Task 2: Wait for 5 seconds
task2 = PythonOperator(
    task_id="wait_task",
    python_callable=wait_function,
    dag=dag,
)

# Task 3: Print Completion Message
task3 = BashOperator(
    task_id="print_complete",
    bash_command="echo 'Workflow Complete!'",
    dag=dag,
)

# Define task dependencies
task1 >> task2 >> task3
