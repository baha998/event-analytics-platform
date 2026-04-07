"""
Weekly Data Cleanup DAG

This DAG orchestrates weekly cleanup and optimization tasks:
1. Cleanup old events from ClickHouse (30+ days)
2. Cleanup old Parquet files from MinIO (30+ days)
3. Optimize ClickHouse tables

Schedule: Weekly on Sunday at 3:00 AM
Tasks run in parallel for efficiency
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
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),  # 1 hour timeout per task
}

# Define the DAG
with DAG(
    'weekly_data_cleanup',
    default_args=default_args,
    description='Weekly cleanup of old events, Parquet files, and ClickHouse optimization',
    schedule_interval='0 3 * * 0',  # Weekly on Sunday at 3:00 AM
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['cleanup', 'maintenance', 'weekly'],
) as dag:

    # Task 1: Cleanup old events (30+ days)
    cleanup_old_events = BashOperator(
        task_id='cleanup_old_events',
        bash_command='''
            curl -X POST "http://go-api:8080/api/cleanup/events?days=30" \
                -H "Authorization: Bearer ${JWT_TOKEN}" \
                -H "Content-Type: application/json" \
                -w "\\nHTTP Status: %{http_code}\\n" \
                -f
        ''',
    )

    # Task 2: Cleanup old Parquet files from MinIO (30+ days)
    cleanup_old_parquet = BashOperator(
        task_id='cleanup_old_parquet',
        bash_command='''
            curl -X POST "http://go-api:8080/api/cleanup/minio?days=30" \
                -H "Authorization: Bearer ${JWT_TOKEN}" \
                -H "Content-Type: application/json" \
                -w "\\nHTTP Status: %{http_code}\\n" \
                -f
        ''',
    )

    # Task 3: Optimize ClickHouse tables
    optimize_clickhouse = BashOperator(
        task_id='optimize_clickhouse',
        bash_command='''
            curl -X POST "http://go-api:8080/api/optimize/clickhouse" \
                -H "Authorization: Bearer ${JWT_TOKEN}" \
                -H "Content-Type: application/json" \
                -w "\\nHTTP Status: %{http_code}\\n" \
                -f
        ''',
    )

    # No task dependencies - all tasks run in parallel
    # This allows cleanup operations to execute simultaneously for better performance
