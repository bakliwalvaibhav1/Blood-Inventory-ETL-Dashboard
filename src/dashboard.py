"""
Streamlit Dashboard for Blood Inventory ETL & Dashboard

- Connects to SQLite database
- Shows key metrics and charts
"""

import os
import pandas as pd
import sqlite3
import streamlit as st

# Project root and database path
current_dir = os.path.dirname(__file__) if "__file__" in globals() else os.getcwd()
project_root = os.path.abspath(os.path.join(current_dir, ".."))
db_path = os.path.join(project_root, "blood_inventory.db")

# Connect to SQLite
conn = sqlite3.connect(db_path)

# Load tables
donors_df = pd.read_sql("SELECT * FROM donors", conn, parse_dates=["dob"])
donations_df = pd.read_sql("SELECT * FROM donations", conn, parse_dates=["donation_date", "expiry_date"])
requests_df = pd.read_sql("SELECT * FROM hospital_requests", conn, parse_dates=["request_date","fulfilled_date"])
inventory_df = pd.read_sql("SELECT * FROM inventory", conn, parse_dates=["last_updated"])

conn.close()

# --- Streamlit Layout ---
st.set_page_config(page_title="Blood Inventory Dashboard", layout="wide")
st.title("Blood Inventory Dashboard")

# 1️⃣ Key Metrics
st.header("Key Metrics")
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Donors", len(donors_df))
col2.metric("Total Donations", len(donations_df))
col3.metric("Active Inventory Units", inventory_df["units_available"].sum())
col4.metric("Hospital Requests", len(requests_df))

# 2️⃣ Blood Type Inventory
st.header("Inventory by Blood Type & Component")
inv_summary = inventory_df.groupby(["blood_type","component"])["units_available"].sum().reset_index()
st.bar_chart(inv_summary.pivot(index="blood_type", columns="component", values="units_available"))

# 3️⃣ Donations Over Time
st.header("Donations Over Time")
donations_time = donations_df.groupby("donation_date")["units"].sum().reset_index()
st.line_chart(donations_time.rename(columns={"donation_date":"index"}).set_index("index"))

# 4️⃣ Requests Status
st.header("Hospital Requests Status")
status_counts = requests_df["status"].value_counts()
st.bar_chart(status_counts)

# 5️⃣ Optional: Filter by Blood Type
st.header("Filter Inventory by Blood Type")
blood_type_filter = st.selectbox("Select Blood Type", ["All"] + sorted(inventory_df["blood_type"].unique().tolist()))
if blood_type_filter != "All":
    filtered_inv = inventory_df[inventory_df["blood_type"] == blood_type_filter]
else:
    filtered_inv = inventory_df
st.dataframe(filtered_inv)
