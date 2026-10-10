# 🛡️ Data Engineering – Comprehensive University Lab Practicals Suite

[![Python](https://img.shields.io/badge/Python-3.13-blue.svg?logo=python)](https://www.python.org/)
[![Pandas](https://img.shields.io/badge/Pandas-2.0+-orange.svg?logo=pandas)](https://pandas.pydata.org/)
[![PySpark](https://img.shields.io/badge/PySpark-4.x-red.svg?logo=apache-spark)](https://spark.apache.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8+-teal.svg?logo=apache-airflow)](https://airflow.apache.org/)
[![SQLite](https://img.shields.io/badge/SQLite-Relational%20Store-003B57.svg?logo=sqlite)](https://sqlite.org/)

Production-grade, modular, and thoroughly tested solutions for the university **Data Engineering Lab Curriculum** covering **Questions 1 through 20** organized systematically into **Practicals 1 through 9**.

---

## 📌 Curriculum Overview & Practical Mapping

| Practical | Questions | Focus Area | Status | Location |
|:---|:---:|:---|:---:|:---|
| **Practical 1** | **Q1 – Q4** | Data Parsing (TXT, CSV, HTML, XML, JSON), Anomaly Check, Binary I/O, RegEx, SQL CRUD | ✅ Done | [`exercise-01-data-parsing/`](./exercise-01-data-parsing/) |
| **Practical 2** | *Pending* | Syllabus Question Placeholder | ⏳ Reserved | [`practical-02-placeholder/`](./practical-02-placeholder/) |
| **Practical 3** | *Pending* | Syllabus Question Placeholder | ⏳ Reserved | [`practical-03-placeholder/`](./practical-03-placeholder/) |
| **Practical 4** | *Pending* | Syllabus Question Placeholder | ⏳ Reserved | [`practical-04-placeholder/`](./practical-04-placeholder/) |
| **Practical 5** | **Q5** | REST API Data Ingestion, JSON Normalization & Flat CSV Merging | ✅ Done | [`practical-05-api-csv-pipeline/`](./practical-05-api-csv-pipeline/) |
| **Practical 6** | **Q12** | Apache Airflow Directed Acyclic Graph (DAG) Sequential Dependency Orchestration | ✅ Done | [`practical-06-airflow-dag-orchestration/`](./practical-06-airflow-dag-orchestration/) |
| **Practical 7** | **Q6 – Q11** | CSV ETL, Multi-CSV Consolidation, JSON-to-Relational ETL, Quarantine Log, Pre-Load Validation & High-Watermark Incremental Delta Loading | ✅ Done | [`practical-07-advanced-etl-and-validation/`](./practical-07-advanced-etl-and-validation/) |
| **Practical 8** | **Q13 – Q17** | Big Data Processing with PySpark (Schema Display, Filter/Group/Agg, Deduplication, Joins & Category Sales Aggregation) | ✅ Done | [`practical-08-pyspark-data-processing/`](./practical-08-pyspark-data-processing/) |
| **Practical 9** | **Q18 – Q20** | End-to-End CSV-to-Report Pipeline, E-Commerce Multi-Table Star Schema ETL & Dual-Layer Historical + Streaming Delta Ingestion Architecture | ✅ Done | [`practical-09-enterprise-pipelines-and-reporting/`](./practical-09-enterprise-pipelines-and-reporting/) |

---

## 🏗️ Repository Architecture

```
python-data-processing-practicals/
├── COLLEGE_ASSIGNMENTS_COMPLETE_REFERENCE.md       # Master syllabus reference & execution manual
├── exercise-01-data-parsing/                       # Practical 1: Questions 1-4
│   ├── task_01_parse_and_validate.py              # Q1: Multi-format parsing & anomaly detection
│   ├── task_02_binary_files.py                    # Q2: Binary I/O (pickle, struct, raw bytes)
│   ├── task_03_regex_patterns.py                  # Q3: Regex searching, splitting, replacing
│   ├── task_04_database_crud.py                   # Q4: Relational database design & SQL CRUD
│   └── data_samples/                              # TXT, CSV, HTML, XML, JSON sample datasets
├── practical-02-placeholder/                       # Practical 2 (Reserved Placeholder)
├── practical-03-placeholder/                       # Practical 3 (Reserved Placeholder)
├── practical-04-placeholder/                       # Practical 4 (Reserved Placeholder)
├── practical-05-api-csv-pipeline/                  # Practical 5: Question 5
│   ├── api_csv_etl_pipeline.py                    # REST API + JSON normalize + CSV merge
│   └── locations.csv                              # Flat geographic metadata
├── practical-06-airflow-dag-orchestration/         # Practical 6: Question 12
│   ├── dags/data_pipeline_dag.py                  # Airflow DAG definition (Daily schedule)
│   ├── scripts/data_extraction.py                 # Task 2 BashOperator extraction script
│   └── airflow_etl_orchestration.py               # Local sequential DAG simulator
├── practical-07-advanced-etl-and-validation/       # Practical 7: Questions 6-11
│   ├── task_06_csv_etl.py                         # Q6: CSV -> Clean -> Transform -> SQL DB
│   ├── task_07_multi_csv_combine.py               # Q7: Multi-CSV extraction & union
│   ├── task_08_json_relational_etl.py             # Q8: JSON extraction -> Relational DB
│   ├── task_09_10_11_validation_incremental_etl.py# Q9-11: Quarantine, Validation, Incremental load
│   ├── run_practical_07.py                        # Practical 7 master runner
│   └── data/                                      # Sample CSV, JSON, and delta batches
├── practical-08-pyspark-data-processing/           # Practical 8: Questions 13-17
│   ├── pyspark_data_processing.py                 # Q13-17: PySpark schema, agg, deduplication, join
│   └── data/                                      # Sales transactions & product catalog CSVs
├── practical-09-enterprise-pipelines-and-reporting/# Practical 9: Questions 18-20
│   ├── task_18_csv_cleaning_sql_report.py         # Q18: CSV -> Pandas -> SQL DB -> KPI Report
│   ├── task_19_ecommerce_star_schema_etl.py       # Q19: E-Commerce Star Schema (Dim + Fact tables)
│   ├── task_20_historical_and_incremental_pipeline.py # Q20: Dual-layer historical + delta pipeline
│   ├── run_practical_09.py                        # Practical 9 master runner
│   └── data/                                      # Raw streams, multi-table CSVs, archive files
├── requirements.txt                               # Global Python dependencies
└── README.md                                      # Repository documentation
```

---

## 🚀 Quick Start & Execution Guide

### 1. Prerequisites & Dependencies
```bash
python -m pip install -r requirements.txt
```

### 2. Running Individual Practicals

#### Practical 1: Data Parsing, Binary Files, RegEx & Relational CRUD (Q1 – Q4)
```bash
cd exercise-01-data-parsing
python task_01_parse_and_validate.py
python task_02_binary_files.py
python task_03_regex_patterns.py
python task_04_database_crud.py
cd ..
```

#### Practical 5: API Ingestion & Flat File Merging (Q5)
```bash
python practical-05-api-csv-pipeline/api_csv_etl_pipeline.py
```

#### Practical 6: Airflow DAG Sequential Dependency Orchestration (Q12)
```bash
python practical-06-airflow-dag-orchestration/airflow_etl_orchestration.py
```

#### Practical 7: Advanced ETL & Data Validation Suite (Q6 – Q11)
```bash
python practical-07-advanced-etl-and-validation/run_practical_07.py
```

#### Practical 8: Big Data Processing with PySpark (Q13 – Q17)
```bash
python practical-08-pyspark-data-processing/pyspark_data_processing.py
```

#### Practical 9: Enterprise Pipelines & Reporting Architectures (Q18 – Q20)
```bash
python practical-09-enterprise-pipelines-and-reporting/run_practical_09.py
```

---

## 📊 Key Highlights & Engineering Features

- **Strict Data Validation & Quarantine:** Isolates invalid or corrupted records into a timestamped quarantine audit log before database commit.
- **High-Watermark Delta Loading:** Guarantees zero duplicate insertions and avoids expensive full-table reloads by tracking timestamps (`last_processed_timestamp`).
- **Star Schema Dimensional Modeling:** Implements clean separation of dimensions (`dim_customers`, `dim_products`) and transactions (`fact_orders`).
