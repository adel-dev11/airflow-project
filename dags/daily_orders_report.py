from datetime import datetime,timedelta
import psycopg2
from airflow import DAG 
from airflow.operators.python import PythonOperator 
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def extract_orders(ds):                           #ds-> is the date of dag run in airflow
    hook = PostgresHook(postgres_conn_id="postgres_orders_db")
    query = """
        select count(*)
        from "Orders"
        where "OrderDate" >= %s                                 -- start of the day
          and "OrderDate" <  %s::date + INTERVAL '1 day';       -- end of the day
    """
    total_orders = hook.get_first(query,parameters=(ds,ds))[0]
    return{
        "report_date":ds,
        "total_orders": total_orders
    }

def transform_orders(ti):
    data = ti.xcom_pull(task_ids='extract')
    total_orders=data["total_orders"]
    if total_orders<0:
        raise ValueError("Total orders cannot be negative")
    return{
        "report_date":data["report_date"],
        "total_orders": total_orders
    }

def load_orders(ti):
    data = ti.xcom_pull(task_ids='transform')
    hook = PostgresHook(postgres_conn_id="postgres_orders_db")

    query = """
        INSERT INTO public.daily_orders_summary
            (report_date, total_orders, loaded_at)
        VALUES (%s, %s, NOW() AT TIME ZONE 'Africa/Cairo')
        ON CONFLICT (report_date)
        DO UPDATE SET
            total_orders = EXCLUDED.total_orders,
            loaded_at = NOW() AT TIME ZONE 'Africa/Cairo';
    """

    hook.run(
        query,
        parameters=(data["report_date"], data["total_orders"])
    )

with DAG(
    dag_id="daily_orders_report_adel",
    description="Daily report for yesterday's orders",
    start_date=datetime(2026,10,1),
    schedule="@daily",
    catchup=False,
    tags=["postgres","orders","report"],
    default_args=default_args,

) as dag:
    extract=PythonOperator(
        task_id="extract",
        python_callable=extract_orders,
    )


    transform=PythonOperator(
        task_id="transform",
        python_callable=transform_orders,
    )


    load=PythonOperator(
        task_id="load",
        python_callable=load_orders,
    )
    extract>>transform>>load
