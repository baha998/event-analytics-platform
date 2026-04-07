"""
Daily Event Export and Batch Processing DAG

This DAG orchestrates daily batch processing tasks:
1. Export events to MinIO storage
2. Aggregate batch data
3. Cleanup temporary files

Schedule: Daily at 2:00 AM
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Default arguments for all tasks
default_args = {
    'owner': 'analytics-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),  # 30 minutes timeout per task
}

# Define the DAG
with DAG(
    'daily_event_export',
    default_args=default_args,
    description='Daily batch export, aggregation, and cleanup of event data',
    schedule_interval='0 2 * * *',  # Daily at 2:00 AM
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['batch', 'export', 'daily'],
) as dag:

    # Task 1: Export events to MinIO
    export_to_minio = BashOperator(
        task_id='export_to_minio',
        bash_command='''
            curl -X POST http://go-api:8080/api/batch/export \
                -H "Authorization: Bearer ${JWT_TOKEN}" \
                -H "Content-Type: application/json" \
                -w "\\nHTTP Status: %{http_code}\\n" \
                -f
        ''',
        retries=3,
        retry_delay=timedelta(minutes=5),
    )

    # Task 2: Aggregate batch data
    aggregate_batch_data = BashOperator(
        task_id='aggregate_batch_data',
        bash_command='''
            curl -X POST http://go-api:8080/api/batch/aggregate \
                -H "Authorization: Bearer ${JWT_TOKEN}" \
                -H "Content-Type: application/json" \
                -w "\\nHTTP Status: %{http_code}\\n" \
                -f
        ''',
        retries=2,
        retry_delay=timedelta(minutes=5),
    )

    # Task 3: Cleanup temporary files
    cleanup_temp_files = BashOperator(
        task_id='cleanup_temp_files',
        bash_command='''
            curl -X POST http://go-api:8080/api/batch/cleanup \
                -H "Content-Type: application/json" \
                -w "\\nHTTP Status: %{http_code}\\n" \
                -f
        ''',
    )

    # Define task dependencies
    export_to_minio >> aggregate_batch_data >> cleanup_temp_files
