"""
Practical 6: Apache Airflow DAG Sequential Dependency Orchestrator & Local Simulator
Question 12: Configure a Directed Acyclic Graph (DAG) in Apache Airflow to run daily,
executing sequential dependency tasks: a start notifier, an extraction script,
and a pipeline success logger.
"""

import os
import sys
import subprocess
from datetime import datetime


def simulate_local_dag_execution():
    """
    Simulates sequential execution of the 3 DAG tasks locally:
    1. start_notifier (EmptyOperator anchor)
    2. run_extraction_script (BashOperator script)
    3. pipeline_success_logger (BashOperator logger)
    """
    print("=" * 65)
    print("Practical 6: Apache Airflow DAG Sequential Dependency Simulation")
    print("=" * 65)
    print(f"Execution Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Schedule Interval: Daily (schedule_interval=timedelta(days=1))")
    print("-" * 65)

    # Task 1: Start Notifier
    print("[TASK 1/3] Executing 'start_notifier' (EmptyOperator)...")
    print("  -> Status: SUCCESS (Workflow boundary initialized)")

    # Task 2: Extraction Script
    print("\n[TASK 2/3] Executing 'run_extraction_script' (BashOperator)...")
    base_dir = os.path.dirname(os.path.abspath(__file__))
    extraction_script = os.path.join(base_dir, "scripts", "data_extraction.py")
    res = subprocess.run(
        [sys.executable, extraction_script],
        cwd=base_dir,
        capture_output=True,
        text=True
    )
    if res.returncode == 0:
        for line in res.stdout.strip().splitlines():
            print(f"  -> {line}")
        print("  -> Status: SUCCESS")
    else:
        print(f"  -> Error: {res.stderr}")
        print("  -> Status: FAILED")
        return False

    # Task 3: Success Logger
    print("\n[TASK 3/3] Executing 'pipeline_success_logger' (BashOperator)...")
    log_msg = f"ETL Execution completed successfully at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    print(f"  -> Output: {log_msg}")
    print("  -> Status: SUCCESS")

    print("-" * 65)
    print("DAG Execution Result: ALL 3 TASKS COMPLETED (Upstream -> Downstream Succeeded)")
    print("Linear Chain: start_notifier >> run_extraction_script >> pipeline_success_logger")
    print("=" * 65)
    return True


if __name__ == "__main__":
    simulate_local_dag_execution()