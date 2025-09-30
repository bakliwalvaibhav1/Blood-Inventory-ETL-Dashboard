"""
Streamlit Dashboard for Blood Inventory ETL & Dashboard

- Loads from SQLite database (blood_inventory.db) if present, else CSVs from data/
- Top metrics, sidebar filters, two-column charts, tables, basic forecast, and alerts
"""

import os
from datetime import datetime, timedelta
import sqlite3
import pandas as pd
import streamlit as st

# ---------------------
# Helpers / config
# ---------------------
def project_root_from_src():
    # If running from src/, project root is parent directory
    current = os.path.dirname(__file__) if "__file__" in globals() else os.getcwd()
    return os.path.abspath(os.path.join(current, ".."))

ROOT = project_root_from_src()
DB_PATH = os.path.join(ROOT, "blood_inventory.db")
DATA_DIR = os.path.join(ROOT, "data")

@st.cache_data
def load_data():
    """Load tables from SQLite if available, otherwise from CSV files."""
    if os.path.exists(DB_PATH):
        conn = sqlite3.connect(DB_PATH)
        try:
            donors = pd.read_sql("SELECT * FROM donors", conn, parse_dates=["dob"])
        except Exception:
            donors = pd.DataFrame()
        try:
            donations = pd.read_sql("SELECT * FROM donations", conn, parse_dates=["donation_date", "expiry_date"])
        except Exception:
            donations = pd.DataFrame()
        try:
            requests = pd.read_sql("SELECT * FROM hospital_requests", conn, parse_dates=["request_date", "fulfilled_date"])
        except Exception:
            requests = pd.DataFrame()
        try:
            inventory = pd.read_sql("SELECT * FROM inventory", conn, parse_dates=["last_updated"])
        except Exception:
            inventory = pd.DataFrame()
        conn.close()
    else:
        # fallback to CSVs
        donors = pd.read_csv(os.path.join(DATA_DIR, "donors.csv"), parse_dates=["dob"]) if os.path.exists(os.path.join(DATA_DIR, "donors.csv")) else pd.DataFrame()
        donations = pd.read_csv(os.path.join(DATA_DIR, "donations.csv"), parse_dates=["donation_date", "expiry_date"]) if os.path.exists(os.path.join(DATA_DIR, "donations.csv")) else pd.DataFrame()
        requests = pd.read_csv(os.path.join(DATA_DIR, "hospital_requests.csv"), parse_dates=["request_date", "fulfilled_date"]) if os.path.exists(os.path.join(DATA_DIR, "hospital_requests.csv")) else pd.DataFrame()
        inventory = pd.read_csv(os.path.join(DATA_DIR, "inventory.csv"), parse_dates=["last_updated"]) if os.path.exists(os.path.join(DATA_DIR, "inventory.csv")) else pd.DataFrame()

    # Normalize columns & dtypes
    for df in [donors, donations, requests, inventory]:
        if not df.empty:
            obj_cols = df.select_dtypes(include="object").columns
            df[obj_cols] = df[obj_cols].apply(lambda s: s.str.strip() if s.dtype == "object" else s)

    # Ensure date cols are datetime.date or datetime64[ns]
    if not donations.empty:
        donations["donation_date"] = pd.to_datetime(donations["donation_date"], errors="coerce")
        donations["expiry_date"] = pd.to_datetime(donations["expiry_date"], errors="coerce")
    if not requests.empty:
        requests["request_date"] = pd.to_datetime(requests["request_date"], errors="coerce")
        requests["fulfilled_date"] = pd.to_datetime(requests["fulfilled_date"], errors="coerce")
    if not inventory.empty:
        inventory["last_updated"] = pd.to_datetime(inventory["last_updated"], errors="coerce")

    return donors, donations, requests, inventory

def simple_forecast(requests_df, blood_type=None, component=None, days_ahead=7):
    """Compute a simple 7-day moving average forecast for units_requested.
       Returns a DataFrame with historical daily totals and forecasted days."""
    if requests_df.empty:
        return pd.DataFrame()

    df = requests_df.copy()
    if blood_type:
        df = df[df["blood_type"] == blood_type]
    if component:
        df = df[df["component"] == component]

    # daily aggregation
    df = df.dropna(subset=["request_date"])
    daily = df.groupby(df["request_date"].dt.date)["units_requested"].sum().reset_index()
    daily.columns = ["date", "units_requested"]
    daily = daily.set_index("date").asfreq("D", fill_value=0)

    # 7-day moving average
    daily["ma7"] = daily["units_requested"].rolling(window=7, min_periods=1).mean()

    # Forecast next days using last ma7 as constant (simple)
    last_ma = daily["ma7"].iloc[-1] if not daily.empty else 0
    future_dates = [daily.index[-1] + timedelta(days=i) for i in range(1, days_ahead + 1)] if not daily.empty else []
    future = pd.DataFrame({"date": future_dates, "units_requested": [None]*len(future_dates), "ma7": [last_ma]*len(future_dates)})
    future = future.set_index("date")
    result = pd.concat([daily, future])
    result = result.reset_index()
    return result

# ---------------------
# App layout / load
# ---------------------
st.set_page_config(page_title="Blood Inventory Dashboard", layout="wide")
donors_df, donations_df, requests_df, inventory_df = load_data()

# Top-level metrics
total_donors = len(donors_df) if not donors_df.empty else 0
total_donated_units = int(donations_df["units"].sum()) if not donations_df.empty else 0
total_inventory_units = int(inventory_df["units_available"].sum()) if not inventory_df.empty else 0
total_requests = len(requests_df) if not requests_df.empty else 0

st.title("Blood Inventory Dashboard")
kpis = st.columns(4)
kpis[0].metric("Total Donors", f"{total_donors:,}")
kpis[1].metric("Total Donated Units", f"{total_donated_units:,}")
kpis[2].metric("Total Inventory Units", f"{total_inventory_units:,}")
kpis[3].metric("Total Requests", f"{total_requests:,}")

# Sidebar filters
st.sidebar.header("Filters")
blood_types = sorted(inventory_df["blood_type"].unique().tolist()) if not inventory_df.empty else []
components = sorted(inventory_df["component"].unique().tolist()) if not inventory_df.empty else []
locations = sorted(inventory_df["location_id"].unique().tolist()) if not inventory_df.empty else []

blood_type_filter = st.sidebar.selectbox("Blood Type", ["All"] + blood_types)
component_filter = st.sidebar.selectbox("Component", ["All"] + components)
location_filter = st.sidebar.selectbox("Location", ["All"] + locations)

# Date range filter for time series
min_date = None
max_date = None
if not donations_df.empty:
    min_date = donations_df["donation_date"].min().date()
    max_date = donations_df["donation_date"].max().date()
if min_date and max_date:
    date_range = st.sidebar.date_input("Donation date range", [min_date, max_date], min_value=min_date, max_value=max_date)
else:
    date_range = None

# Apply filters
def apply_filters(df):
    if df is None or df.empty:
        return df
    res = df.copy()
    if blood_type_filter != "All" and "blood_type" in res.columns:
        res = res[res["blood_type"] == blood_type_filter]
    if component_filter != "All" and "component" in res.columns:
        res = res[res["component"] == component_filter]
    if location_filter != "All" and "location_id" in res.columns:
        res = res[res["location_id"] == location_filter]
    if date_range is not None and "donation_date" in res.columns:
        start, end = date_range
        res = res[(res["donation_date"].dt.date >= start) & (res["donation_date"].dt.date <= end)]
    return res

filtered_inventory = apply_filters(inventory_df)
filtered_donations = apply_filters(donations_df)
filtered_requests = apply_filters(requests_df)

# ---------------------
# Main charts (two-column)
# ---------------------
col1, col2 = st.columns([1, 1])

with col1:
    st.subheader("Inventory by Blood Type & Component")
    if filtered_inventory is None or filtered_inventory.empty:
        st.info("No inventory data to show.")
    else:
        inv_summary = filtered_inventory.groupby(["blood_type", "component"])["units_available"].sum().reset_index()
        pivot = inv_summary.pivot(index="blood_type", columns="component", values="units_available").fillna(0)
        st.bar_chart(pivot)  # Streamlit handles axes nicely (no tilted x-axis)

    st.markdown("**Low stock alerts**")
    # Low stock: blood types whose total units across locations < threshold (e.g., 5)
    threshold = st.sidebar.number_input("Low stock threshold (units)", value=5, min_value=1)
    low_stock = inv_summary.groupby("blood_type")["units_available"].sum().reset_index() if not filtered_inventory.empty else pd.DataFrame()
    if not low_stock.empty:
        low = low_stock[low_stock["units_available"] < threshold]
        if not low.empty:
            st.warning(f"Low stock for: {', '.join(low['blood_type'].tolist())}")
            st.table(low.sort_values("units_available"))
        else:
            st.success("No blood types below threshold.")
    else:
        st.info("No inventory summary available for low stock check.")

with col2:
    st.subheader("Donations Over Time")
    if filtered_donations is None or filtered_donations.empty:
        st.info("No donation data to show.")
    else:
        donations_ts = filtered_donations.copy()
        donations_ts["date"] = donations_ts["donation_date"].dt.date
        daily = donations_ts.groupby("date")["units"].sum().reset_index().set_index("date")
        st.line_chart(daily)

    st.subheader("Requests Status & Urgency")
    if filtered_requests is None or filtered_requests.empty:
        st.info("No requests data to show.")
    else:
        status_counts = filtered_requests["status"].value_counts()
        st.bar_chart(status_counts)

        st.write("Urgency breakdown")
        urgency_counts = filtered_requests["urgency"].value_counts()
        st.bar_chart(urgency_counts)

# ---------------------
# Forecast panel
# ---------------------
st.subheader("Simple 7-day Demand Forecast (requests)")

selected_bt = st.selectbox("Select Blood Type for Forecast", ["All"] + blood_types)
selected_comp = st.selectbox("Select Component for Forecast", ["All"] + components)

bt_arg = None if selected_bt == "All" else selected_bt
comp_arg = None if selected_comp == "All" else selected_comp

forecast_df = simple_forecast(requests_df, blood_type=bt_arg, component=comp_arg, days_ahead=7)
if forecast_df.empty:
    st.info("Not enough request data to produce forecast.")
else:
    # show last 30 historical points + forecast
    to_show = forecast_df.tail(30 + 7).set_index("date")
    st.line_chart(to_show[["units_requested", "ma7"]])

    st.markdown("**Forecast (next 7 days, using last 7-day MA)**")
    future = to_show[to_show.index > (to_show.index.max() - timedelta(days=7))]
    st.table(future[["ma7"]].rename(columns={"ma7": "predicted_units"}).reset_index())

# ---------------------
# Location-wise inventory & near-expiry
# ---------------------
st.subheader("Location-wise Inventory")
if filtered_inventory is None or filtered_inventory.empty:
    st.info("No inventory data.")
else:
    loc_table = filtered_inventory.groupby(["location_id", "blood_type", "component"])["units_available"].sum().reset_index()
    st.dataframe(loc_table.sort_values(["location_id", "blood_type", "component"]))

st.subheader("Items Near Expiry (<= 7 days)")
if inventory_df is None or inventory_df.empty:
    st.info("No inventory expiry data.")
else:
    inv = inventory_df.copy()
    # inventory may not have expiry_date per-bag; if donations had expiry, we could join — attempt to find expiry in donations
    if "expiry_date" in donations_df.columns:
        # join donations expiry to inventory by blood_type & component (approximate)
        join_df = donations_df[["blood_type", "component", "expiry_date"]].copy()
        # find nearest expiry (min) per type-component
        min_expiry = join_df.groupby(["blood_type", "component"])["expiry_date"].min().reset_index()
        inv = inv.merge(min_expiry, on=["blood_type", "component"], how="left")
    else:
        inv["expiry_date"] = pd.NaT

    inv["days_to_expiry"] = (pd.to_datetime(inv["expiry_date"]) - pd.Timestamp(datetime.utcnow())).dt.days
    near = inv[inv["days_to_expiry"].notna() & (inv["days_to_expiry"] <= 7)]
    if not near.empty:
        st.table(near[["inventory_id", "blood_type", "component", "location_id", "units_available", "expiry_date", "days_to_expiry"]])
    else:
        st.write("No inventory items expiring within 7 days.")

# Footer / notes
st.markdown("---")
st.caption("Data source: local SQLite DB (blood_inventory.db) or CSVs under data/ . Forecast uses simple 7-day moving average as a quick baseline.")
