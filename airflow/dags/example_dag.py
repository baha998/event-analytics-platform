"""
Example Airflow DAG for Event Analytics Pipeline

This DAG demonstrates how to orchestrate data processing tasks
in your event analytics system.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Default arguments for all tasks
default_args = {
    'owner': 'analytics-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def check_kafka_health():
    """Check if Kafka is healthy and receiving events"""
    import requests
    try:
        # This would connect to your Go API health endpoint
        response = requests.get('http://go-api:8080/health', timeout=5)
        if response.status_code == 200:
            print("✓ Kafka and Go API are healthy")
            return True
        else:
            print(f"✗ Health check failed with status: {response.status_code}")
            return False
    except Exception as e:
        print(f"✗ Health check failed: {str(e)}")
        return False


def check_clickhouse_health():
    """Check if ClickHouse is healthy"""
    import requests
    try:
        response = requests.get('http://clickhouse-server:8123/ping', timeout=5)
        if response.status_code == 200:
            print("✓ ClickHouse is healthy")
            return True
        else:
            print(f"✗ ClickHouse health check failed")
            return False
    except Exception as e:
        print(f"✗ ClickHouse health check failed: {str(e)}")
        return False


# Define the DAG
with DAG(
    'event_analytics_example',
    default_args=default_args,
    description='Example DAG for event analytics pipeline health checks',
    schedule_interval='@hourly',  # Run every hour
    start_date=datetime(2024, 1, 1),
    catchup=False,  # Don't backfill
    tags=['analytics', 'example'],
) as dag:

    # Task 1: Check Kafka/Go API health
    check_kafka = PythonOperator(
        task_id='check_kafka_health',
        python_callable=check_kafka_health,
    )

    # Task 2: Check ClickHouse health
    check_clickhouse = PythonOperator(
        task_id='check_clickhouse_health',
        python_callable=check_clickhouse_health,
    )

    # Task 3: Log success
    log_success = BashOperator(
        task_id='log_success',
        bash_command='echo "All systems healthy at $(date)"',
    )

    # Define task dependencies
    [check_kafka, check_clickhouse] >> log_success


# Example: More advanced DAG for batch processing
with DAG(
    'daily_batch_export',
    default_args=default_args,
    description='Daily batch export of events to MinIO/Parquet',
    schedule_interval='0 2 * * *',  # Run at 2 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['analytics', 'batch', 'export'],
) as batch_dag:

    # This is a placeholder - you would implement actual export logic
    export_to_parquet = BashOperator(
        task_id='export_events_to_parquet',
        bash_command='''
            echo "Starting daily export..."
            # In production, this would trigger your Go batch export service
            # curl -X POST http://go-api:8080/api/batch/export?date={{ ds }}
            echo "Export completed for partition date: {{ ds }}"
        ''',
    )

    verify_export = BashOperator(
        task_id='verify_export',
        bash_command='echo "Verifying export integrity..."',
    )

    export_to_parquet >> verify_export
