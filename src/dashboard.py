import streamlit as st
import pandas as pd
import sqlite3
from pathlib import Path
from datetime import datetime

DB_PATH = Path(__file__).parent.parent / "blood_inventory.db"

st.set_page_config(
    page_title="Blood Inventory Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

@st.cache_data
def load_data():
    conn = sqlite3.connect(DB_PATH)
    donors = pd.read_sql("SELECT * FROM donors", conn)
    donations = pd.read_sql("SELECT * FROM donations", conn, parse_dates=["donation_date","expiry_date"])
    requests = pd.read_sql("SELECT * FROM hospital_requests", conn, parse_dates=["request_date","fulfilled_date"])
    inventory = pd.read_sql("SELECT * FROM inventory", conn, parse_dates=["last_updated"])
    conn.close()
    return donors, donations, requests, inventory

donors_df, donations_df, requests_df, inventory_df = load_data()

st.title("Blood Inventory Dashboard")

# Sidebar filters
st.sidebar.header("Filters")
blood_types = sorted(donors_df["blood_type"].unique())
components = sorted(inventory_df["component"].unique())
locations = sorted(inventory_df["location_id"].unique())

blood_type = st.sidebar.selectbox("Blood type", ["All"]+blood_types)
component = st.sidebar.selectbox("Component", ["All"]+components)
location = st.sidebar.selectbox("Location", ["All"]+locations)

# Filter inventory
inv = inventory_df.copy()
if blood_type != "All":
    inv = inv[inv["blood_type"]==blood_type]
if component != "All":
    inv = inv[inv["component"]==component]
if location != "All":
    inv = inv[inv["location_id"]==location]

# KPIs
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Donors", donors_df.shape[0])
col2.metric("Total Donated Units", donations_df["units"].sum())
col3.metric("Total Inventory Units", inventory_df["units_available"].sum())
col4.metric("Total Requests", requests_df.shape[0])

# Layout
left, right = st.columns(2)

with left:
    st.subheader("Inventory by blood type & component")
    chart = inv.groupby(["blood_type","component"])["units_available"].sum().unstack()
    st.bar_chart(chart)

    st.subheader("Low stock blood types (<7 units)")
    low = inv.groupby("blood_type")["units_available"].sum()
    low = low[low<7]
    if not low.empty:
        st.write(low)

with right:
    st.subheader("Donations over time")
    if not donations_df.empty:
        daily = donations_df.groupby("donation_date")["units"].sum()
        st.line_chart(daily)

    st.subheader("Requests status")
    st.bar_chart(requests_df["status"].value_counts())
