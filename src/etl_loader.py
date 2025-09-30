"""
ETL Loader for Blood Inventory ETL & Dashboard project.

- Reads CSVs from project root `data/` folder
- Cleans and standardizes data
- Loads into SQLite database at project root: blood_inventory.db
"""

import os
import pandas as pd
import sqlite3

# Project root and paths
current_dir = os.path.dirname(__file__) if "__file__" in globals() else os.getcwd()
project_root = os.path.abspath(os.path.join(current_dir, ".."))
data_dir = os.path.join(project_root, "data")
db_path = os.path.join(project_root, "blood_inventory.db")

# CSV files
donors_csv = os.path.join(data_dir, "donors.csv")
donations_csv = os.path.join(data_dir, "donations.csv")
requests_csv = os.path.join(data_dir, "hospital_requests.csv")
inventory_csv = os.path.join(data_dir, "inventory.csv")

# 1️⃣ Load CSVs
donors_df = pd.read_csv(donors_csv, parse_dates=["dob"])
donations_df = pd.read_csv(donations_csv, parse_dates=["donation_date", "expiry_date"])
requests_df = pd.read_csv(requests_csv, parse_dates=["request_date","fulfilled_date"], dayfirst=False)
inventory_df = pd.read_csv(inventory_csv, parse_dates=["last_updated"])

# 2️⃣ Data cleaning
# Example: ensure QC passed donations only
donations_df = donations_df[donations_df["qc_pass"] == True]

# Fill missing fulfilled_date with NULL
requests_df["fulfilled_date"] = requests_df["fulfilled_date"].where(pd.notnull(requests_df["fulfilled_date"]))

# Standardize string columns (lowercase)
for df in [donors_df, donations_df, requests_df, inventory_df]:
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda x: x.str.strip())

# 3️⃣ Connect to SQLite
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Optional: drop tables if they exist
cursor.executescript("""
DROP TABLE IF EXISTS donors;
DROP TABLE IF EXISTS donations;
DROP TABLE IF EXISTS hospital_requests;
DROP TABLE IF EXISTS inventory;
""")

# 4️⃣ Load into SQLite
donors_df.to_sql("donors", conn, index=False)
donations_df.to_sql("donations", conn, index=False)
requests_df.to_sql("hospital_requests", conn, index=False)
inventory_df.to_sql("inventory", conn, index=False)

conn.commit()
conn.close()

print(f"ETL complete. Data loaded into SQLite database at {db_path}")
