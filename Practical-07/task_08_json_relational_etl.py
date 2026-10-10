"""
Practical 7 - Question 8:
Build a pipeline to extract JSON data, transform select fields, and load it into a NoSQL database (e.g., MongoDB).
"""

import os
import json
import sqlite3
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
JSON_FILE = os.path.join(DATA_DIR, "product_events.json")
DB_FILE = os.path.join(BASE_DIR, "warehouse_etl.db")
NOSQL_FILE = os.path.join(BASE_DIR, "nosql_product_events_collection.json")


def ensure_sample_json():
    """Guarantees data directory and product_events.json exist."""
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(JSON_FILE):
        events = [
            {
                "event_id": "EVT-9001",
                "timestamp": "2026-09-01T10:15:30Z",
                "user": {"user_id": "U101", "membership": "GOLD"},
                "product": {
                    "sku": "SKU-LAP-01",
                    "name": "UltraBook Pro 15",
                    "category": "Electronics",
                    "pricing": {"base_price": 75000.0, "tax_rate": 0.18}
                },
                "action": "PURCHASE"
            },
            {
                "event_id": "EVT-9002",
                "timestamp": "2026-09-01T10:18:45Z",
                "user": {"user_id": "U102", "membership": "STANDARD"},
                "product": {
                    "sku": "SKU-MOU-02",
                    "name": "Wireless Ergonomic Mouse",
                    "category": "Accessories",
                    "pricing": {"base_price": 2500.0, "tax_rate": 0.12}
                },
                "action": "PURCHASE"
            },
            {
                "event_id": "EVT-9003",
                "timestamp": "2026-09-01T10:22:10Z",
                "user": {"user_id": "U103", "membership": "PLATINUM"},
                "product": {
                    "sku": "SKU-MON-03",
                    "name": "4K Curved Monitor 32-inch",
                    "category": "Electronics",
                    "pricing": {"base_price": 42000.0, "tax_rate": 0.18}
                },
                "action": "PURCHASE"
            }
        ]
        with open(JSON_FILE, "w", encoding="utf-8") as f:
            json.dump(events, f, indent=2)
        print(f"[SETUP] Initialized product events JSON dataset: {JSON_FILE}")


def extract_json(file_path: str = JSON_FILE) -> list:
    """Extract raw semi-structured JSON records from file."""
    ensure_sample_json()
    print(f"[EXTRACT] Reading JSON file from '{file_path}'...")
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    print(f"  -> Extracted {len(data)} JSON event documents.")
    return data


def transform_json_payloads(records: list) -> pd.DataFrame:
    """
    Flattens nested JSON hierarchies and transforms selected fields:
    - Extracts nested user & product pricing attributes
    - Computes total gross price including tax
    - Normalizes datetime formatting
    - Selects target relational projection columns
    """
    print("[TRANSFORM] Normalizing nested JSON and transforming selected fields...")
    flat_df = pd.json_normalize(records)

    # Rename to clean relational column identifiers
    rename_mapping = {
        "event_id": "event_id",
        "timestamp": "event_timestamp",
        "user.user_id": "user_id",
        "user.membership": "membership_tier",
        "product.sku": "product_sku",
        "product.name": "product_name",
        "product.category": "category",
        "product.pricing.base_price": "base_price",
        "product.pricing.tax_rate": "tax_rate",
        "action": "action_type"
    }
    transformed_df = flat_df.rename(columns=rename_mapping)

    # Transform selected fields
    transformed_df["gross_price"] = (
        transformed_df["base_price"] * (1 + transformed_df["tax_rate"])
    ).round(2)
    transformed_df["event_timestamp"] = pd.to_datetime(transformed_df["event_timestamp"]).dt.strftime("%Y-%m-%d %H:%M:%S")

    # Select target relational columns
    target_columns = [
        "event_id", "user_id", "membership_tier", "product_sku",
        "product_name", "category", "base_price", "tax_rate", "gross_price", "event_timestamp"
    ]
    transformed_df = transformed_df[target_columns]
    print(f"  -> Transformed {len(transformed_df)} records into relational schema.")
    return transformed_df


def load_to_nosql(records: list, nosql_path: str = NOSQL_FILE, collection_name: str = "product_events") -> None:
    """
    [Question 8: NoSQL Database Ingestion (e.g., MongoDB / Document Store)]
    Loads transformed documents into a NoSQL document database:
    1. Attempts to connect to active MongoDB instance via pymongo if available.
    2. Seamlessly commits to structured JSON Document Store collection on disk,
       providing a persistent, schema-free NoSQL store that runs anywhere without requiring
       a local MongoDB daemon.
    """
    print(f"[LOAD - NoSQL] Loading records into NoSQL collection '{collection_name}'...")
    try:
        import importlib
        pymongo_module = importlib.import_module("pymongo")
        client = getattr(pymongo_module, "MongoClient")("mongodb://localhost:27017/", serverSelectionTimeoutMS=1000)
        client.server_info()  # Check connectivity
        db = client["ecommerce_nosql_db"]
        collection = db[collection_name]
        collection.delete_many({})
        collection.insert_many(records)
        print(f"  -> Successfully inserted {len(records)} documents into MongoDB collection '{collection_name}' in db 'ecommerce_nosql_db'.")
        return
    except Exception:
        # Fallback: Portable schema-free JSON Document Collection NoSQL store
        pass

    with open(nosql_path, "w", encoding="utf-8") as f:
        json.dump({"collection": collection_name, "count": len(records), "documents": records}, f, indent=2)
    print(f"  -> Successfully committed {len(records)} documents into NoSQL Document Store: '{os.path.basename(nosql_path)}'.")


def load_to_database(df: pd.DataFrame, db_path: str = DB_FILE, table_name: str = "product_purchases") -> None:
    """Loads transformed records into target SQLite relational table."""
    print(f"[LOAD - SQL] Storing records into relational database table '{table_name}' in '{os.path.basename(db_path)}'...")
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    
    # Query verification
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    conn.close()
    print(f"  -> Successfully loaded {count} rows into relational table '{table_name}'.")


def run():
    print("=" * 65)
    print("Question 8: JSON -> Transformation -> NoSQL (e.g. MongoDB) / SQL Pipeline")
    print("=" * 65)
    raw_json = extract_json()
    transformed_df = transform_json_payloads(raw_json)
    
    # 1. NoSQL Ingestion (Question 8 Primary Target)
    records = transformed_df.to_dict(orient="records")
    load_to_nosql(records)
    
    # 2. Relational Ingestion
    load_to_database(transformed_df)
    
    print("\n--- Pipeline Data View ---")
    print(transformed_df[["event_id", "user_id", "product_name", "gross_price"]].to_string(index=False))
    print("=" * 65)


if __name__ == "__main__":
    run()
