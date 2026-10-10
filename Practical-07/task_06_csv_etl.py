"""
Practical 7 - Question 6:
Design an ETL pipeline to extract data from a CSV file, clean it, transform it, and store it in a database.
"""

import os
import sqlite3
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
INPUT_CSV = os.path.join(DATA_DIR, "customer_orders.csv")
DB_FILE = os.path.join(BASE_DIR, "warehouse_etl.db")


def ensure_sample_data():
    """Guarantees data directory and input CSV file exist."""
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(INPUT_CSV):
        sample_df = pd.DataFrame({
            "order_id": ["ORD-101", "ORD-102", "ORD-103", "ORD-104", "ORD-105"],
            "customer_name": [" Alice Smith ", "Bob Jones", " Charlie Brown ", "Diana Prince", " Evan Wright "],
            "email": ["alice@example.com", "bob@example.com", "charlie@example.com", "diana@example.com", None],
            "order_amount": [1200.50, 450.00, 890.25, 3400.00, 650.00],
            "discount": [0.10, 0.00, 0.05, 0.15, 0.00],
            "order_status": ["SHIPPED", "PENDING", "DELIVERED", "DELIVERED", "CANCELLED"],
            "order_date": ["2026-09-01", "2026-09-02", "2026-09-03", "2026-09-04", "2026-09-05"]
        })
        sample_df.to_csv(INPUT_CSV, index=False)
        print(f"[SETUP] Initialized sample CSV dataset: {INPUT_CSV}")


def extract(file_path: str = INPUT_CSV) -> pd.DataFrame:
    """Extract raw data from CSV file."""
    ensure_sample_data()
    print(f"[EXTRACT] Reading data from {file_path}")
    df = pd.read_csv(file_path)
    print(f"  -> Extracted {len(df)} records.")
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and transform order records."""
    print("[TRANSFORM] Cleaning and enriching data...")
    cleaned = df.copy()

    # 1. Clean string fields (strip whitespaces)
    cleaned["customer_name"] = cleaned["customer_name"].astype(str).str.strip()
    cleaned["email"] = cleaned["email"].fillna("unknown@domain.com").astype(str).str.strip().str.lower()
    cleaned["order_status"] = cleaned["order_status"].astype(str).str.strip().str.upper()

    # 2. Add business transformations: Net amount and tax
    cleaned["net_amount"] = (cleaned["order_amount"] * (1 - cleaned["discount"])).round(2)
    cleaned["tax_amount"] = (cleaned["net_amount"] * 0.18).round(2)
    cleaned["total_billed"] = (cleaned["net_amount"] + cleaned["tax_amount"]).round(2)

    print(f"  -> Cleaned {len(cleaned)} rows. Added net_amount, tax_amount, and total_billed.")
    return cleaned


def load(df: pd.DataFrame, db_path: str = DB_FILE, table_name: str = "orders_clean") -> None:
    """Load transformed dataset into SQLite database."""
    print(f"[LOAD] Storing records into database table '{table_name}' in '{db_path}'...")
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    
    # Verify count
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    conn.close()
    print(f"  -> Successfully committed {count} rows into table '{table_name}'.")


def run():
    print("=" * 65)
    print("Question 6: CSV -> Clean -> Transform -> SQL Database ETL Pipeline")
    print("=" * 65)
    raw_data = extract(INPUT_CSV)
    transformed_data = transform(raw_data)
    load(transformed_data)
    print("=" * 65)


if __name__ == "__main__":
    run()
