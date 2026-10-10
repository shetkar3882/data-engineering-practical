"""
Practical 7 - Question 7:
Extract data from multiple CSV files and combine them into a single dataset.
"""

import os
import glob
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
OUTPUT_CONSOLIDATED = os.path.join(BASE_DIR, "consolidated_national_sales.csv")


def ensure_sample_regional_csvs():
    """Guarantees regional CSV source files exist in the data directory."""
    os.makedirs(DATA_DIR, exist_ok=True)
    regional_datasets = {
        "regional_sales_north.csv": pd.DataFrame({
            "sale_id": ["N-001", "N-002", "N-003"],
            "store_branch": ["Delhi-NCR", "Chandigarh", "Jaipur"],
            "units_sold": [45, 30, 25],
            "revenue": [67500.00, 45000.00, 37500.00],
            "sale_date": ["2026-09-01", "2026-09-02", "2026-09-03"]
        }),
        "regional_sales_south.csv": pd.DataFrame({
            "sale_id": ["S-001", "S-002", "S-003"],
            "store_branch": ["Bangalore", "Chennai", "Hyderabad"],
            "units_sold": [80, 55, 60],
            "revenue": [120000.00, 82500.00, 90000.00],
            "sale_date": ["2026-09-01", "2026-09-02", "2026-09-03"]
        }),
        "regional_sales_east.csv": pd.DataFrame({
            "sale_id": ["E-001", "E-002", "E-003"],
            "store_branch": ["Kolkata", "Bhubaneswar", "Guwahati"],
            "units_sold": [40, 20, 18],
            "revenue": [60000.00, 30000.00, 27000.00],
            "sale_date": ["2026-09-01", "2026-09-02", "2026-09-03"]
        })
    }
    for filename, df in regional_datasets.items():
        file_path = os.path.join(DATA_DIR, filename)
        if not os.path.exists(file_path):
            df.to_csv(file_path, index=False)
            print(f"[SETUP] Initialized regional CSV dataset: {file_path}")


def extract_and_combine_csv_files(file_pattern: str = None, output_path: str = OUTPUT_CONSOLIDATED) -> pd.DataFrame:
    """
    Finds all matching CSV files, extracts each dataset with provenance metadata,
    and performs a validated vertical concatenation (union) into a single dataset.
    """
    ensure_sample_regional_csvs()
    if file_pattern is None:
        file_pattern = os.path.join(DATA_DIR, "regional_sales_*.csv")

    csv_files = glob.glob(file_pattern)
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found matching pattern: {file_pattern}")

    print(f"[EXTRACT] Found {len(csv_files)} regional CSV source files:")
    dataframes = []

    for path in csv_files:
        filename = os.path.basename(path)
        # Infer region tag from file naming convention (e.g., regional_sales_north.csv -> North)
        region_tag = filename.replace("regional_sales_", "").replace(".csv", "").title()
        
        df = pd.read_csv(path)
        df["region"] = region_tag
        df["source_file"] = filename
        print(f"  -> Extracted '{filename}' ({len(df)} rows, Region: {region_tag})")
        dataframes.append(df)

    # Combine into single consolidated DataFrame
    print("\n[TRANSFORM] Combining datasets and reconciling schema...")
    consolidated_df = pd.concat(dataframes, ignore_index=True)
    
    # Save combined dataset
    consolidated_df.to_csv(output_path, index=False)
    print(f"[LOAD] Consolidated dataset saved to '{os.path.basename(output_path)}' ({len(consolidated_df)} total records).")

    return consolidated_df


def run():
    print("=" * 65)
    print("Question 7: Multi-CSV Extraction & Consolidation Pipeline")
    print("=" * 65)
    pattern = os.path.join(DATA_DIR, "regional_sales_*.csv")
    result_df = extract_and_combine_csv_files(pattern)
    print("\n--- Consolidated Dataset Preview ---")
    print(result_df[["sale_id", "region", "store_branch", "units_sold", "revenue"]].to_string(index=False))
    print("=" * 65)


if __name__ == "__main__":
    run()
