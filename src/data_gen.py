#!/usr/bin/env python3
"""
Mock data generator for the Blood Inventory ETL & Dashboard project.

Generates CSVs in the project root `data/` folder:
    - donors.csv
    - donations.csv
    - hospital_requests.csv
    - inventory.csv

Usage:
    python data_gen.py
"""
import os
import random
import uuid
from datetime import datetime, timedelta
import pandas as pd

random.seed(42)

# Determine project root (one level up from src/ if running inside src/)
current_dir = os.path.dirname(__file__) if "__file__" in globals() else os.getcwd()
project_root = os.path.abspath(os.path.join(current_dir, ".."))  # root folder
data_dir = os.path.join(project_root, "data")
os.makedirs(data_dir, exist_ok=True)

# Static lists
blood_types = ["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"]
components = ["whole_blood", "plasma", "platelets"]
locations = ["center_1", "center_2", "center_3", "mobile_drive_1"]
hospitals = [f"hospital_{i}" for i in range(1, 11)]
donor_count = 250
today = datetime.utcnow().date()

# 1) Donors table
donors = []
for i in range(1, donor_count + 1):
    donor_id = f"donor_{i}"
    dob = today - timedelta(days=random.randint(18*365, 70*365))
    donors.append({"donor_id": donor_id, "dob": dob.isoformat(), "blood_type": random.choice(blood_types)})
pd.DataFrame(donors).to_csv(os.path.join(data_dir, "donors.csv"), index=False)

# 2) Donations table
n_donations = 600
don_rows = []
for _ in range(n_donations):
    donation_id = str(uuid.uuid4())
    donor_id = f"donor_{random.randint(1, donor_count)}"
    bt = random.choice(blood_types)
    comp = random.choice(components)
    units = 1
    donation_date = today - timedelta(days=random.randint(0, 90))
    expiry_date = donation_date + timedelta(days=365 if comp=="plasma" else 5 if comp=="platelets" else 42)
    location = random.choice(locations)
    qc_pass = random.random() > 0.02
    notes = ""
    don_rows.append({
        "donation_id": donation_id,
        "donor_id": donor_id,
        "blood_type": bt,
        "component": comp,
        "units": units,
        "donation_date": donation_date.isoformat(),
        "expiry_date": expiry_date.isoformat(),
        "location_id": location,
        "qc_pass": qc_pass,
        "notes": notes
    })
pd.DataFrame(don_rows).to_csv(os.path.join(data_dir, "donations.csv"), index=False)

# 3) Hospital requests table
n_requests = 300
req_rows = []
for _ in range(n_requests):
    request_id = str(uuid.uuid4())
    hospital_id = random.choice(hospitals)
    bt = random.choice(blood_types)
    comp = random.choice(components)
    units_requested = random.randint(1, 6)
    request_date = today - timedelta(days=random.randint(0, 90))
    urgency_rand = random.random()
    urgency = "routine" if urgency_rand<0.7 else "urgent" if urgency_rand<0.95 else "emergency"
    fulfilled = random.random() < 0.82
    if fulfilled:
        fulfilled_date = request_date + timedelta(days=random.randint(0,3))
        if fulfilled_date > today:
            fulfilled_date = today
        status = "fulfilled"
    else:
        fulfilled_date = ""
        status = "partial" if random.random() < 0.5 else "cancelled"
    req_rows.append({
        "request_id": request_id,
        "hospital_id": hospital_id,
        "blood_type": bt,
        "component": comp,
        "units_requested": units_requested,
        "request_date": request_date.isoformat(),
        "urgency": urgency,
        "status": status,
        "fulfilled_date": fulfilled_date.isoformat() if fulfilled_date else ""
    })
pd.DataFrame(req_rows).to_csv(os.path.join(data_dir, "hospital_requests.csv"), index=False)

# 4) Inventory snapshot (simple aggregation)
donations_df = pd.read_csv(os.path.join(data_dir, "donations.csv"), parse_dates=["donation_date", "expiry_date"])
donations_df["donation_date"] = donations_df["donation_date"].dt.date
donations_df["expiry_date"] = donations_df["expiry_date"].dt.date
valid_don = donations_df[(donations_df["qc_pass"]==True) & (donations_df["expiry_date"]>=today)]
total_by_type = valid_don.groupby(["blood_type","component"])["units"].sum().reset_index().rename(columns={"units":"units_available_total"})

requests_df = pd.read_csv(os.path.join(data_dir, "hospital_requests.csv"), parse_dates=["request_date","fulfilled_date"])
requests_df["request_date"] = requests_df["request_date"].dt.date
requests_df["fulfilled_date"] = pd.to_datetime(requests_df["fulfilled_date"], errors="coerce").dt.date
fulfilled_reqs = requests_df[requests_df["status"]=="fulfilled"]
fulfilled_group = fulfilled_reqs.groupby(["blood_type","component"])["units_requested"].sum().reset_index().rename(columns={"units_requested":"units_fulfilled"})

inv_rows = []
for _, row in total_by_type.iterrows():
    bt = row["blood_type"]
    comp = row["component"]
    total_units = int(row["units_available_total"])
    fulfilled = int(fulfilled_group[(fulfilled_group["blood_type"]==bt)&(fulfilled_group["component"]==comp)]["units_fulfilled"].sum() or 0)
    net_units = max(total_units - fulfilled, 0)
    loc_shares = [random.random() for _ in locations]
    s = sum(loc_shares) if sum(loc_shares)>0 else 1
    loc_units = [int(net_units*(x/s)) for x in loc_shares]
    diff = net_units - sum(loc_units)
    i=0
    while diff>0:
        loc_units[i%len(loc_units)] +=1
        i+=1
        diff-=1
    for loc, units_here in zip(locations, loc_units):
        inv_rows.append({
            "inventory_id": str(uuid.uuid4()),
            "blood_type": bt,
            "component": comp,
            "units_available": units_here,
            "location_id": loc,
            "last_updated": today.isoformat(),
            "notes": ""
        })
pd.DataFrame(inv_rows).to_csv(os.path.join(data_dir, "inventory.csv"), index=False)

print("Mock data created in the 'data' folder at project root.")
print("Files: donors.csv, donations.csv, hospital_requests.csv, inventory.csv")
