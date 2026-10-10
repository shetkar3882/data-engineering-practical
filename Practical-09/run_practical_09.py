"""
Practical 9: Enterprise Data Pipelines & Reporting Master Runner
Executes Questions 18 through 20 in sequence.
"""

import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from task_18_csv_cleaning_sql_report import run_pipeline as run_q18
from task_19_ecommerce_star_schema_etl import run_ecommerce_etl as run_q19
from task_20_historical_and_incremental_pipeline import run as run_q20


def main():
    print("#" * 70)
    print("DATA ENGINEERING - PRACTICAL 9: ENTERPRISE PIPELINES & ARCHITECTURES")
    print("Questions 18 through 20")
    print("#" * 70)
    print()

    print("\n>>> EXECUTING QUESTION 18 (End-to-End Pipeline: CSV -> SQL -> Report) <<<")
    run_q18()

    print("\n>>> EXECUTING QUESTION 19 (E-Commerce Multi-Table Star Schema ETL) <<<")
    run_q19()

    print("\n>>> EXECUTING QUESTION 20 (Historical & Newly Arriving Data Pipeline) <<<")
    run_q20()

    print("\n" + "#" * 70)
    print("ALL PRACTICAL 9 TASKS COMPLETED SUCCESSFULLY WITH 0 ERRORS!")
    print("#" * 70)


if __name__ == "__main__":
    main()
