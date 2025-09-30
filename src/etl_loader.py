import pandas as pd
import sqlite3
import os

# --- Paths ---
current_dir = os.path.dirname(__file__) if "__file__" in globals() else os.getcwd()
project_root = os.path.abspath(os.path.join(current_dir, ".."))
data_dir = os.path.join(project_root, "data")
os.makedirs(data_dir, exist_ok=True)

db_path = os.path.join(project_root, "blood_inventory.db")

# Load CSVs
donors_df = pd.read_csv(os.path.join(data_dir, "donors.csv"), parse_dates=["dob"])
donations_df = pd.read_csv(os.path.join(data_dir, "donations.csv"), parse_dates=["donation_date","expiry_date"])
requests_df = pd.read_csv(os.path.join(data_dir, "hospital_requests.csv"), parse_dates=["request_date","fulfilled_date"])
inventory_df = pd.read_csv(os.path.join(data_dir, "inventory.csv"), parse_dates=["last_updated"])

# Connect to SQLite
conn = sqlite3.connect(db_path)

# Write tables
donors_df.to_sql("donors", conn, if_exists="replace", index=False)
donations_df.to_sql("donations", conn, if_exists="replace", index=False)
requests_df.to_sql("hospital_requests", conn, if_exists="replace", index=False)
inventory_df.to_sql("inventory", conn, if_exists="replace", index=False)

conn.close()
print(f"ETL complete. Data loaded into {db_path}")
