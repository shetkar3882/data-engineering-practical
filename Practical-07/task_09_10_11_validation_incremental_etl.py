"""
Practical 7 - Questions 9, 10, 11:
- Question 9: Design an ETL process to identify and remove invalid records.
- Question 10: Create a pipeline that performs data validation before loading data into the target database.
- Question 11: Implement incremental data loading instead of loading the entire dataset every time.
"""

import os
import sqlite3
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_FILE = os.path.join(BASE_DIR, "warehouse_etl.db")
QUARANTINE_FILE = os.path.join(BASE_DIR, "quarantine_invalid_records.csv")
BATCH1_FILE = os.path.join(DATA_DIR, "batch1_input.csv")
BATCH2_FILE = os.path.join(DATA_DIR, "batch2_input.csv")


def ensure_sample_batches():
    """Guarantees data directory and batch CSV input files exist."""
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(BATCH1_FILE):
        batch1_df = pd.DataFrame({
            "id": [1, 2, 3, 4],
            "customer_name": ["Alice Johnson", "Bob Corrupt", "Charlie OutOfBounds", "Diana Prince"],
            "account_balance": [1500.50, -50.00, 2300.00, 500.00],
            "credit_score": [750, 680, 200, 810],
            "updated_at": ["2026-09-01T10:00:00", "2026-09-01T10:05:00", "2026-09-01T10:10:00", "2026-09-01T10:15:00"]
        })
        batch1_df.to_csv(BATCH1_FILE, index=False)
        print(f"[SETUP] Initialized Batch 1 CSV: {BATCH1_FILE}")

    if not os.path.exists(BATCH2_FILE):
        batch2_df = pd.DataFrame({
            "id": [5, 6],
            "customer_name": ["Evan Wright", "Fiona Gallagher"],
            "account_balance": [3200.00, 1800.00],
            "credit_score": [720, 790],
            "updated_at": ["2026-09-02T11:00:00", "2026-09-02T11:30:00"]
        })
        batch2_df.to_csv(BATCH2_FILE, index=False)
        print(f"[SETUP] Initialized Batch 2 CSV: {BATCH2_FILE}")


def init_database(db_path: str = DB_FILE, reset: bool = False):
    """Initializes target relational tables and watermark metadata tracking store."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    if reset:
        cursor.execute("DROP TABLE IF EXISTS customer_metrics")
        cursor.execute("DROP TABLE IF EXISTS etl_watermark")
        if os.path.exists(QUARANTINE_FILE):
            try:
                os.remove(QUARANTINE_FILE)
            except OSError:
                pass
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customer_metrics (
            id INTEGER PRIMARY KEY,
            customer_name TEXT NOT NULL,
            account_balance REAL NOT NULL,
            credit_score INTEGER NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS etl_watermark (
            pipeline_name TEXT PRIMARY KEY,
            last_processed_timestamp TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()


def get_watermark(db_path: str = DB_FILE, pipeline_name: str = "customer_metrics_etl") -> str:
    """Retrieves the highest timestamp watermark processed so far."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT last_processed_timestamp FROM etl_watermark WHERE pipeline_name = ?", (pipeline_name,))
    row = cursor.fetchone()
    conn.close()
    return row[0] if row else "1970-01-01T00:00:00"


def update_watermark(ts: str, db_path: str = DB_FILE, pipeline_name: str = "customer_metrics_etl") -> None:
    """Atomically commits the new watermark after successful ingestion."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO etl_watermark (pipeline_name, last_processed_timestamp)
        VALUES (?, ?)
        ON CONFLICT(pipeline_name) DO UPDATE SET last_processed_timestamp=excluded.last_processed_timestamp
    """, (pipeline_name, ts))
    conn.commit()
    conn.close()


def validate_records(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    [Question 10: Pre-load Data Validation & Question 9: Identification & Quarantine]
    Evaluates records against integrity constraints:
    Rule 1: ID and customer_name cannot be null.
    Rule 2: account_balance must be non-negative (>= 0.0).
    Rule 3: credit_score must fall within valid range [300, 850].
    """
    rejection_reasons = []

    for idx, row in df.iterrows():
        reasons = []
        if pd.isna(row.get("id")) or pd.isna(row.get("customer_name")):
            reasons.append("Null identifier/name")
        if row.get("account_balance", -1) < 0.0:
            reasons.append("Negative account balance")
        score = row.get("credit_score", 0)
        if score < 300 or score > 850:
            reasons.append(f"Credit score {score} out of range [300, 850]")
        
        rejection_reasons.append("; ".join(reasons) if reasons else "VALID")

    df_evaluated = df.copy()
    df_evaluated["rejection_reason"] = rejection_reasons

    valid_mask = df_evaluated["rejection_reason"] == "VALID"
    valid_df = df_evaluated[valid_mask].drop(columns=["rejection_reason"])
    invalid_df = df_evaluated[~valid_mask]

    return valid_df, invalid_df


def run_pipeline_step(csv_file: str, batch_label: str):
    """
    Orchestrates validation, isolation of invalid records, and incremental delta load.
    """
    ensure_sample_batches()
    print(f"\n--- Ingesting {batch_label}: {os.path.basename(csv_file)} ---")
    raw_df = pd.read_csv(csv_file)
    print(f"[EXTRACT] Read {len(raw_df)} records.")

    # Validation (Q10) & Quarantine (Q9)
    valid_df, invalid_df = validate_records(raw_df)

    if not invalid_df.empty:
        print(f"[QUARANTINE - Q9] Isolated {len(invalid_df)} invalid record(s):")
        for _, inv_row in invalid_df.iterrows():
            print(f"  -> ID: {inv_row['id']}, Customer: {inv_row['customer_name']}, Reason: {inv_row['rejection_reason']}")
        
        # Append to quarantine log with reason
        file_exists = os.path.exists(QUARANTINE_FILE)
        invalid_df.to_csv(QUARANTINE_FILE, mode="a", index=False, header=not file_exists)
        print(f"  -> Appended to '{os.path.basename(QUARANTINE_FILE)}'")

    # Incremental Loading (Q11)
    watermark = get_watermark()
    print(f"[DELTA FILTER - Q11] Current High-Watermark: {watermark}")
    delta_df = valid_df[valid_df["updated_at"] > watermark]

    if not delta_df.empty:
        conn = sqlite3.connect(DB_FILE)
        delta_df.to_sql("customer_metrics", conn, if_exists="append", index=False)
        conn.close()

        new_watermark = delta_df["updated_at"].max()
        update_watermark(new_watermark)
        print(f"[LOAD - Q11] Successfully inserted {len(delta_df)} new record(s).")
        print(f"  -> Advanced High-Watermark to: {new_watermark}")
    else:
        print("[LOAD - Q11] No newly arriving delta records found past watermark. Target database unchanged.")


def run():
    print("=" * 65)
    print("Practical 7: Questions 9, 10, 11 - Validation, Quarantine & Delta Ingestion")
    print("=" * 65)
    ensure_sample_batches()
    init_database(reset=True)

    # Step 1: Initial Batch
    run_pipeline_step(BATCH1_FILE, "Batch 1 (Initial Load + Corrupted Records)")

    # Step 2: Incremental Delta Batch
    run_pipeline_step(BATCH2_FILE, "Batch 2 (Incremental Arrivals)")

    # Final DB Verification
    conn = sqlite3.connect(DB_FILE)
    final_count = pd.read_sql("SELECT COUNT(*) as total FROM customer_metrics", conn).iloc[0]["total"]
    watermark = get_watermark()
    conn.close()
    print(f"\nFinal Target Table Record Count: {final_count} | Active Watermark: {watermark}")
    print("=" * 65)


if __name__ == "__main__":
    run()
