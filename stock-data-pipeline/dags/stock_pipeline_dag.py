# dags/stock_pipeline_dag.py

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from stock_etl import run_stock_etl

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}


with DAG(
    dag_id="stock_market_etl",
    default_args=default_args,
    description="Fetch stock data from Alpha Vantage and store in PostgreSQL",
    schedule_interval="@daily",  # change to "0 * * * *" for hourly
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["stocks", "etl", "demo"],
) as dag:

    fetch_and_store = PythonOperator(
        task_id="fetch_and_store_stock_data",
        python_callable=run_stock_etl,
        provide_context=True,
    )

    fetch_and_store
