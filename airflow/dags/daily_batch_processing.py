from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from analytics_tasks import call_api

with DAG("daily_event_export", schedule="0 2 * * *", start_date=datetime(2025, 1, 1), catchup=False, is_paused_upon_creation=True, default_args={"retries": 2, "retry_delay": timedelta(minutes=5)}) as dag:
    export = PythonOperator(task_id="export", python_callable=call_api, op_args=["/api/batch/export"])
    aggregate = PythonOperator(task_id="aggregate", python_callable=call_api, op_args=["/api/batch/aggregate"])
    cleanup = PythonOperator(task_id="cleanup", python_callable=call_api, op_args=["/api/batch/cleanup"])
    export >> aggregate >> cleanup
