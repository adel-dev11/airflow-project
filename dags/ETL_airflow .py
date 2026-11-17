from datetime import datetime, timedelta

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook


# Configuration
LOCAL_TZ = "Africa/Cairo"
POSTGRES_CONN_ID = "postgres_orders_db"

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


# 1. Extract
def extract_orders():

    context = get_current_context()

    start_date = context["data_interval_start"].in_timezone(LOCAL_TZ)
    end_date = context["data_interval_end"].in_timezone(LOCAL_TZ)

    report_date = start_date.date()

    hook = PostgresHook(
        postgres_conn_id=POSTGRES_CONN_ID
    )

    query = """
        SELECT COUNT(*)
        FROM public."Orders"
        WHERE "OrderDate" >= %s
          AND "OrderDate" < %s;
    """

    total_orders = hook.get_first(
        query,
        parameters=(start_date, end_date)
    )[0]

    print(f"Report date: {report_date}")
    print(f"Total orders extracted: {total_orders}")

    return {
        "report_date": report_date.isoformat(),
        "total_orders": total_orders
    }


# 2. Transform
def transform_orders():

    context = get_current_context()

    data = context["ti"].xcom_pull(
        task_ids="extract"
    )

    total_orders = data["total_orders"]

    if total_orders < 0:
        raise ValueError(
            "Total orders cannot be negative!"
        )

    transformed_data = {
        "report_date": data["report_date"],
        "total_orders": int(total_orders)
    }

    print(f"Transformed data: {transformed_data}")

    return transformed_data


# 3. Load
def load_orders():

    context = get_current_context()

    data = context["ti"].xcom_pull(
        task_ids="transform"
    )

    hook = PostgresHook(
        postgres_conn_id=POSTGRES_CONN_ID
    )

    query = """
        INSERT INTO public.daily_orders_summary
        (
            report_date,
            total_orders,
            loaded_at
        )
        VALUES (
            %s,
            %s,
            timezone('Africa/Cairo', now())
        )

        ON CONFLICT (report_date)

        DO UPDATE SET
            total_orders = EXCLUDED.total_orders,
            loaded_at = timezone('Africa/Cairo', now());
    """

    hook.run(
        query,
        parameters=(
            data["report_date"],
            data["total_orders"]
        )
    )

    print(
        f"Successfully loaded report for "
        f"{data['report_date']}"
    )


# DAG Definition
with DAG(
    dag_id="daily_orders_report_byadel",

    description="Daily orders ETL pipeline",

    start_date=pendulum.datetime(
        2026, 10, 1,
        tz=LOCAL_TZ
    ),

    schedule="@daily",

    catchup=False,

    max_active_runs=1,

    tags=[
        "postgres",
        "orders",
        "etl",
        "report"
    ],

    default_args=default_args,

) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_orders,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_orders,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_orders,
    )

    # Task Dependencies
    extract >> transform >> load
