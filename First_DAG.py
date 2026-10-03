from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define your Python function (task logic)
def my_task():
    print("Hello, this is my first Airflow task!")

# Define the DAG
with DAG(
    dag_id="basic_dag_example",
    start_date=datetime(2026, 10, 3),   # DAG start date
    schedule_interval="@daily",         # Run frequency
    catchup=False,                      # Skip past runs
    tags=["example"],                   # Optional tags
) as dag:

    # Define a task
    task1 = PythonOperator(
        task_id="print_hello",
        python_callable=my_task
    )

    

    # If you had multiple tasks, you could set dependencies like:
    task1
