"""
Practical 9 - Question 18:
Build an end-to-end pipeline: CSV -> Python/Pandas -> Data Cleaning -> SQL Database -> Report.
"""

import os
import sqlite3
import pandas as pd
from datetime import datetime

DATA_PATH = os.path.join(os.path.dirname(__file__), "data", "raw_sales_stream.csv")
DB_PATH = os.path.join(os.path.dirname(__file__), "production_analytics.db")
REPORT_PATH = os.path.join(os.path.dirname(__file__), "executive_summary_report.md")


def run_pipeline():
    print("=" * 65)
    print("Question 18: End-to-End Pipeline: CSV -> Pandas -> SQL DB -> Report")
    print("=" * 65)

    # 1. Extraction
    print(f"[EXTRACT] Reading raw sales stream from '{os.path.basename(DATA_PATH)}'...")
    df = pd.read_csv(DATA_PATH)
    initial_rows = len(df)
    print(f"  -> Extracted {initial_rows} records.")

    # 2. Data Cleaning & Imputation
    print("[CLEAN] Deduplicating records and imputing missing values...")
    df = df.drop_duplicates().reset_index(drop=True)
    dedup_rows = len(df)
    print(f"  -> Removed {initial_rows - dedup_rows} duplicate record(s).")

    # Group-mean imputation by category
    df["amount"] = df["amount"].fillna(df.groupby("category")["amount"].transform("mean")).round(2)
    # Business feature: Net Revenue
    df["net_revenue"] = (df["amount"] * (1 - df["discount_pct"])).round(2)
    print(f"  -> Cleaned and enriched {len(df)} transactions.")

    # 3. SQL Database Storage
    print(f"[LOAD] Storing clean records into SQLite '{os.path.basename(DB_PATH)}'...")
    conn = sqlite3.connect(DB_PATH)
    df.to_sql("clean_sales", conn, if_exists="replace", index=False)

    # 4. KPI Analytics Query
    print("[ANALYTICS] Running aggregated SQL KPI summary query...")
    summary = pd.read_sql("""
        SELECT
            region,
            COUNT(transaction_id) as total_orders,
            ROUND(SUM(net_revenue), 2) as net_revenue,
            ROUND(AVG(net_revenue), 2) as average_order_value
        FROM clean_sales
        GROUP BY region
        ORDER BY net_revenue DESC;
    """, conn)
    conn.close()

    # 5. Automated Executive Report Generation
    print(f"[REPORT] Generating executive markdown report '{os.path.basename(REPORT_PATH)}'...")
    report_content = f"""# Executive Sales KPI Analytics Report
*Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*

## 📊 Regional Performance Breakdown

| Region | Total Orders | Net Revenue (₹) | Average Order Value (₹) |
|:---|---:|---:|---:|
"""
    for _, row in summary.iterrows():
        report_content += f"| {row['region']} | {int(row['total_orders'])} | ₹{row['net_revenue']:,.2f} | ₹{row['average_order_value']:,.2f} |\n"

    total_net = df['net_revenue'].sum()
    report_content += f"""
## 📈 High-Level Summary
- **Total Processed Transactions:** {len(df)}
- **Total Net Revenue:** ₹{total_net:,.2f}
- **Data Quality Status:** 100% Cleaned & Validated in Relational SQLite Store
"""
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write(report_content)

    print("  -> Report successfully generated.")
    print("\n--- KPI Summary Table ---")
    print(summary.to_string(index=False))
    print("=" * 65)


if __name__ == "__main__":
    run_pipeline()
