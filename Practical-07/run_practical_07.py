"""
Practical 7: Advanced ETL & Data Quality Engineering Master Runner
Executes Questions 6 through 11 in sequence.
"""

import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from task_06_csv_etl import run as run_q6
from task_07_multi_csv_combine import run as run_q7
from task_08_json_relational_etl import run as run_q8
from task_09_10_11_validation_incremental_etl import run as run_q9_10_11


def main():
    print("#" * 70)
    print("DATA ENGINEERING - PRACTICAL 7: ADVANCED ETL & DATA QUALITY SUITE")
    print("Questions 6 through 11")
    print("#" * 70)
    print()

    print("\n>>> EXECUTING QUESTION 6 (CSV -> Clean -> Transform -> SQL DB) <<<")
    run_q6()

    print("\n>>> EXECUTING QUESTION 7 (Multi-CSV Extraction & Consolidation) <<<")
    run_q7()

    print("\n>>> EXECUTING QUESTION 8 (JSON Extraction -> NoSQL & Relational Load) <<<")
    run_q8()

    print("\n>>> EXECUTING QUESTIONS 9, 10, 11 (Quarantine, Validation, Watermark) <<<")
    run_q9_10_11()

    print("\n" + "#" * 70)
    print("ALL PRACTICAL 7 TASKS COMPLETED SUCCESSFULLY WITH 0 ERRORS!")
    print("#" * 70)


if __name__ == "__main__":
    main()
