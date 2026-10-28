from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def extract_data():
    print("Extracting data...")


def transform_data():
    print("Transforming data...")


def load_data():
    print("Loading data...")


with DAG(
    dag_id="my_first_dag",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    task2 = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    task3 = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
    )

    task1 >> task2 >> task3