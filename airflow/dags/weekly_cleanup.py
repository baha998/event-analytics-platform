from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from analytics_tasks import call_api

with DAG("weekly_data_cleanup", schedule="0 3 * * 0", start_date=datetime(2025, 1, 1), catchup=False, is_paused_upon_creation=True, default_args={"retries": 2, "retry_delay": timedelta(minutes=5)}) as dag:
    cleanup_events = PythonOperator(task_id="cleanup_events", python_callable=call_api, op_args=["/api/cleanup/events?days=30"])
    cleanup_minio = PythonOperator(task_id="cleanup_minio", python_callable=call_api, op_args=["/api/cleanup/minio?days=30"])
    optimize = PythonOperator(task_id="optimize", python_callable=call_api, op_args=["/api/optimize/clickhouse"])
    cleanup_events >> cleanup_minio >> optimize
