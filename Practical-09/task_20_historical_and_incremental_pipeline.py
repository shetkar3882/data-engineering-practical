"""
Practical 9 - Question 20:
Design a data pipeline that handles both historical and newly arriving data.
"""

import os
import sqlite3
import pandas as pd
from datetime import datetime

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
DB_PATH = os.path.join(os.path.dirname(__file__), "unified_enterprise_store.db")
HISTORICAL_CSV = os.path.join(DATA_DIR, "historical_archive.csv")
INCOMING_CSV = os.path.join(DATA_DIR, "incoming_delta_stream.csv")


def init_storage(db_path: str = DB_PATH):
    """Initializes unified entity store and metadata audit table."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    # Main Entity Store
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_profiles (
            user_id TEXT PRIMARY KEY,
            username TEXT NOT NULL,
            activity_score INTEGER NOT NULL,
            tier TEXT NOT NULL,
            record_timestamp TEXT NOT NULL,
            last_ingested_at TEXT NOT NULL
        )
    """)
    # Ingestion Audit & Watermark Log
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_ingestion_log (
            batch_id INTEGER PRIMARY KEY AUTOINCREMENT,
            ingestion_mode TEXT NOT NULL,
            records_processed INTEGER NOT NULL,
            highest_watermark TEXT NOT NULL,
            executed_at TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()


def load_historical_backfill(historical_file: str = HISTORICAL_CSV):
    """
    Cold Layer / Historical Backfill:
    Loads baseline historical partition into the unified store.
    """
    print(f"\n[PHASE 1: HISTORICAL ARCHIVE BACKFILL]")
    print(f"Reading cold historical partition from '{os.path.basename(historical_file)}'...")
    df = pd.read_csv(historical_file)
    df["last_ingested_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    inserted = 0
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT OR REPLACE INTO user_profiles
            (user_id, username, activity_score, tier, record_timestamp, last_ingested_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (row["user_id"], row["username"], row["activity_score"], row["tier"], row["record_timestamp"], row["last_ingested_at"]))
        inserted += 1

    max_watermark = df["record_timestamp"].max()
    cursor.execute("""
        INSERT INTO pipeline_ingestion_log (ingestion_mode, records_processed, highest_watermark, executed_at)
        VALUES ('HISTORICAL_BACKFILL', ?, ?, ?)
    """, (inserted, max_watermark, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

    conn.commit()
    conn.close()
    print(f"  -> Committed {inserted} historical baseline records. Initial Watermark: {max_watermark}")


def process_incoming_delta(delta_file: str = INCOMING_CSV):
    """
    Hot Layer / Newly Arriving Data:
    Processes newly arriving records, applies incremental watermark filtering,
    and performs idempotent UPSERTs (updating existing records if timestamps are newer,
    or inserting new entities).
    """
    print(f"\n[PHASE 2: NEWLY ARRIVING DELTA INGESTION]")
    print(f"Reading hot incoming stream from '{os.path.basename(delta_file)}'...")
    df = pd.read_csv(delta_file)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Get current high-watermark
    cursor.execute("SELECT highest_watermark FROM pipeline_ingestion_log ORDER BY batch_id DESC LIMIT 1")
    row = cursor.fetchone()
    current_watermark = row[0] if row else "1970-01-01T00:00:00"
    print(f"  -> Active System High-Watermark: {current_watermark}")

    # Identify new or updated records
    delta_records = df[df["record_timestamp"] > current_watermark].copy()
    print(f"  -> Identified {len(delta_records)} newly arriving record(s) past watermark.")

    upserted = 0
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for _, item in delta_records.iterrows():
        cursor.execute("""
            INSERT INTO user_profiles (user_id, username, activity_score, tier, record_timestamp, last_ingested_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username = excluded.username,
                activity_score = excluded.activity_score,
                tier = excluded.tier,
                record_timestamp = excluded.record_timestamp,
                last_ingested_at = excluded.last_ingested_at
            WHERE excluded.record_timestamp > user_profiles.record_timestamp
        """, (item["user_id"], item["username"], item["activity_score"], item["tier"], item["record_timestamp"], now_str))
        upserted += 1

    new_watermark = delta_records["record_timestamp"].max() if not delta_records.empty else current_watermark
    cursor.execute("""
        INSERT INTO pipeline_ingestion_log (ingestion_mode, records_processed, highest_watermark, executed_at)
        VALUES ('STREAMING_DELTA', ?, ?, ?)
    """, (upserted, new_watermark, now_str))

    conn.commit()
    conn.close()
    print(f"  -> Idempotently merged {upserted} records into unified store. New Watermark: {new_watermark}")


def verify_unified_store():
    """Prints the final unified state of records and the audit log."""
    print(f"\n[PHASE 3: PIPELINE STATE AUDIT & VERIFICATION]")
    conn = sqlite3.connect(DB_PATH)
    profiles = pd.read_sql("SELECT * FROM user_profiles ORDER BY user_id ASC", conn)
    audit = pd.read_sql("SELECT * FROM pipeline_ingestion_log", conn)
    conn.close()

    print("\n--- Unified Entity Store (Merged Historical + Newly Arriving Data) ---")
    print(profiles.to_string(index=False))

    print("\n--- Pipeline Ingestion Audit Log ---")
    print(audit.to_string(index=False))


def run():
    print("=" * 65)
    print("Question 20: Dual-Layer Pipeline for Historical & Newly Arriving Data")
    print("=" * 65)
    init_storage()
    load_historical_backfill()
    process_incoming_delta()
    verify_unified_store()
    print("=" * 65)


if __name__ == "__main__":
    run()
