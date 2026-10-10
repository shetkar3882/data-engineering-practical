"""
Practical 5: API & Flat File ETL Ingestion Pipeline
Question 5: Write a Python script to fetch user data from a public REST API,
clean it, and merge it with a flat CSV file containing location details.
"""

import os
import pandas as pd
import requests


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOCATIONS_CSV = os.path.join(BASE_DIR, "locations.csv")
OUTPUT_CSV = os.path.join(BASE_DIR, "cleaned_warehouse_profiles.csv")


def extract_api_data(url: str = "https://jsonplaceholder.typicode.com/users") -> pd.DataFrame:
    """
    Fetches user records from a public REST API and normalizes JSON payloads.
    Falls back gracefully to embedded mock dataset if external API is unreachable.
    """
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        data = response.json()
        print(f"[EXTRACT] Successfully fetched {len(data)} user records from REST API: {url}")
        return pd.json_normalize(data)
    except Exception as e:
        print(f"[WARN] REST API unavailable ({e}). Using robust fallback user dataset.")
        fallback_users = [
            {"id": 1, "name": "Leanne Graham", "email": "Sincere@april.biz", "company": {"name": "Romaguera-Crona"}},
            {"id": 2, "name": "Ervin Howell", "email": "Shanna@melissa.tv", "company": {"name": "Deckow-Crist"}},
            {"id": 3, "name": "Clementine Bauch", "email": "Nathan@yesenia.net", "company": {"name": "Romaguera-Jacobson"}},
            {"id": 4, "name": "Patricia Lebsack", "email": "Julianne.OConner@kory.org", "company": {"name": "Robel-Corkery"}},
            {"id": 5, "name": "Chelsey Dietrich", "email": "Lucio_Hettinger@annie.ca", "company": {"name": "Keebler LLC"}},
            {"id": 6, "name": "Mrs. Dennis Schulist", "email": "Karley_Dach@jasper.info", "company": {"name": "Considine-Lockman"}},
            {"id": 7, "name": "Kurtis Weissnat", "email": "Telly.Hoeger@billy.biz", "company": {"name": "Johns Group"}},
            {"id": 8, "name": "Nicholas Runolfsdottir V", "email": "Sherwood@rosamond.me", "company": {"name": "Abernathy Group"}},
            {"id": 9, "name": "Glenna Reichert", "email": "Chaim_McDermott@dana.io", "company": {"name": "Yost and Sons"}},
            {"id": 10, "name": "Clementina DuBuque", "email": "Rey.Padberg@karina.biz", "company": {"name": "Hoeger LLC"}},
        ]
        return pd.json_normalize(fallback_users)


def extract_csv_data(file_path: str = LOCATIONS_CSV) -> pd.DataFrame:
    """Reads location records from a flat CSV file."""
    if not os.path.exists(file_path):
        # Generate default location dataset
        sample_locs = pd.DataFrame({
            "id": list(range(1, 11)),
            "city": [
                "New York", "London", "Paris", "Tokyo", "Berlin",
                "Delhi", "Sydney", "Moscow", "Cairo", "Beijing"
            ],
            "country": [
                "USA", "UK", "France", "Japan", "Germany",
                "India", "Australia", "Russia", "Egypt", "China"
            ]
        })
        sample_locs.to_csv(file_path, index=False)
        print(f"[EXTRACT] Initialized default locations dataset: {file_path}")

    df = pd.read_csv(file_path)
    print(f"[EXTRACT] Read {len(df)} location rows from '{file_path}'")
    return df


def clean_and_transform_api_data(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans and extracts target attributes from normalized API dataframe."""
    target_cols = [c for c in ["id", "name", "email", "company.name", "company"] if c in df.columns]
    cleaned = df[target_cols].copy()
    if "company.name" in cleaned.columns:
        cleaned.rename(columns={"company.name": "company"}, inplace=True)
    
    # Strip whitespace & lowercase emails
    cleaned["name"] = cleaned["name"].astype(str).str.strip()
    cleaned["email"] = cleaned["email"].astype(str).str.strip().str.lower()
    return cleaned


def merge_and_save(api_df: pd.DataFrame, csv_df: pd.DataFrame, output_path: str = OUTPUT_CSV) -> pd.DataFrame:
    """Joins API user profiles with geographic flat data on user 'id'."""
    merged = pd.merge(api_df, csv_df, on="id", how="inner")
    merged.to_csv(output_path, index=False)
    print(f"[LOAD] Staged {len(merged)} combined profiles into '{output_path}'")
    return merged


def run_pipeline():
    print("=" * 65)
    print("Practical 5: API User Data & Flat CSV Location Integration")
    print("=" * 65)
    raw_api_df = extract_api_data()
    cleaned_api_df = clean_and_transform_api_data(raw_api_df)
    csv_df = extract_csv_data(LOCATIONS_CSV)
    result = merge_and_save(cleaned_api_df, csv_df, OUTPUT_CSV)
    print("\n--- Pipeline Sample Output (First 5 Rows) ---")
    print(result.head())
    print("=" * 65)
    return result


if __name__ == "__main__":
    run_pipeline()
    