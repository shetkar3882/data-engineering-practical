from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# Practical 6 (Question 12): Apache Airflow DAG Configuration
# Default execution arguments
default_args = {
    "owner": "data_engineering_lab",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the Directed Acyclic Graph (DAG) scheduled daily
with DAG(
    "daily_data_engineering_etl",
    default_args=default_args,
    description="Practical 6 - Sequential Airflow DAG: start notifier -> extraction -> success logger",
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=["data_engineering", "practical_6", "etl"]
) as dag:

    # Task 1: Start Notifier (Anchor)
    start_notifier = EmptyOperator(
        task_id="start_notifier"
    )

    # Task 2: Data Extraction Script
    extraction_script = BashOperator(
        task_id="run_extraction_script",
        bash_command="python3 scripts/data_extraction.py",
    )

    # Task 3: Pipeline Success Logger
    pipeline_success_logger = BashOperator(
        task_id="pipeline_success_logger",
        bash_command='echo "ETL Execution completed successfully at $(date)"',
    )

    # Sequential Dependency Chain
    start_notifier >> extraction_script >> pipeline_success_logger