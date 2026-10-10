"""
Practical 9 - Question 19:
Build an ETL pipeline for an e-commerce dataset containing customers, orders, products, and payments into a Star Schema relational database.
"""

import os
import sqlite3
import pandas as pd

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
DB_PATH = os.path.join(os.path.dirname(__file__), "ecommerce_warehouse.db")


def run_ecommerce_etl():
    print("=" * 65)
    print("Question 19: E-Commerce Multi-Table Relational ETL (Star Schema)")
    print("=" * 65)

    # 1. Extraction
    print("[EXTRACT] Ingesting relational tables...")
    customers = pd.read_csv(os.path.join(DATA_DIR, "customers.csv"))
    products = pd.read_csv(os.path.join(DATA_DIR, "products.csv"))
    orders = pd.read_csv(os.path.join(DATA_DIR, "orders.csv"))
    payments = pd.read_csv(os.path.join(DATA_DIR, "payments.csv"))
    print(f"  -> Customers: {len(customers)}, Products: {len(products)}, Orders: {len(orders)}, Payments: {len(payments)}")

    # 2. Transformation & Cleaning
    print("[TRANSFORM] Cleaning entities and building Star Schema dimensional model...")
    # Normalize customer strings
    customers["email"] = customers["email"].astype(str).str.lower().str.strip()
    customers["name"] = customers["name"].astype(str).str.strip()

    # Filter completed transactions only
    settled = payments[payments["payment_status"] == "COMPLETED"].copy()
    print(f"  -> Settled Payments Filtered: {len(settled)} of {len(payments)} records.")

    # Construct Fact Table: join orders with products and settled payments
    fact_orders = orders.merge(products, on="product_id", how="inner").merge(settled, on="order_id", how="inner")
    fact_orders["gross_amount"] = (fact_orders["quantity"] * fact_orders["price"]).round(2)

    # Select Star Schema attributes
    dim_customers = customers[["customer_id", "name", "email", "signup_date"]].copy()
    dim_products = products[["product_id", "product_name", "category", "price"]].copy()
    fact_table = fact_orders[[
        "order_id", "customer_id", "product_id", "payment_id", "quantity", "gross_amount", "order_date"
    ]].copy()

    # 3. Loading into Star Schema Relational Store
    print(f"[LOAD] Storing Star Schema tables into '{os.path.basename(DB_PATH)}'...")
    conn = sqlite3.connect(DB_PATH)
    dim_customers.to_sql("dim_customers", conn, if_exists="replace", index=False)
    dim_products.to_sql("dim_products", conn, if_exists="replace", index=False)
    fact_table.to_sql("fact_orders", conn, if_exists="replace", index=False)

    # Dimensional Analytics Verification Query
    print("\n[ANALYTICS] Verifying Star Schema via Cross-Dimensional Join Query:")
    query_result = pd.read_sql("""
        SELECT
            f.order_id,
            c.name as customer_name,
            p.product_name,
            p.category,
            f.quantity,
            f.gross_amount,
            f.order_date
        FROM fact_orders f
        JOIN dim_customers c ON f.customer_id = c.customer_id
        JOIN dim_products p ON f.product_id = p.product_id
        ORDER BY f.order_date ASC;
    """, conn)
    conn.close()

    print(query_result.to_string(index=False))
    print(f"\n[COMPLETE] Successfully built dim_customers, dim_products, and fact_orders.")
    print("=" * 65)


if __name__ == "__main__":
    run_ecommerce_etl()
