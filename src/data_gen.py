"""
Better Mock data generator for Blood Inventory ETL & Dashboard
- Guarantees all blood types & components are represented
"""

import os
import random
import uuid
from datetime import datetime, timedelta
import pandas as pd

random.seed(123)

# --- Paths ---
current_dir = os.path.dirname(__file__) if "__file__" in globals() else os.getcwd()
project_root = os.path.abspath(os.path.join(current_dir, ".."))
data_dir = os.path.join(project_root, "data")
os.makedirs(data_dir, exist_ok=True)

# --- Constants ---
blood_types = ["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"]
components = ["whole_blood", "plasma", "platelets"]
locations = ["center_1", "center_2", "center_3", "mobile_drive_1"]
hospitals = [f"hospital_{i}" for i in range(1, 16)]
donor_count = 500
today = datetime.utcnow().date()

# --- 1) Donors ---
donors = []
for i, bt in enumerate(blood_types, 1):
    # Ensure at least one donor of each type
    donors.append({
        "donor_id": f"donor_{i}",
        "dob": (today - timedelta(days=random.randint(20*365, 60*365))).isoformat(),
        "blood_type": bt
    })

# Fill the rest randomly
for i in range(len(blood_types)+1, donor_count+1):
    donors.append({
        "donor_id": f"donor_{i}",
        "dob": (today - timedelta(days=random.randint(18*365, 70*365))).isoformat(),
        "blood_type": random.choice(blood_types)
    })
donors_df = pd.DataFrame(donors)
donors_df.to_csv(os.path.join(data_dir, "donors.csv"), index=False)

# --- 2) Donations ---
donations = []
n_donations = 1500

# Ensure every (blood_type, component) combination appears
for bt in blood_types:
    for comp in components:
        donations.append({
            "donation_id": str(uuid.uuid4()),
            "donor_id": f"donor_{random.randint(1, donor_count)}",
            "blood_type": bt,
            "component": comp,
            "units": random.randint(1, 3),
            "donation_date": (today - timedelta(days=random.randint(0, 180))).isoformat(),
            "expiry_date": (today + timedelta(days=30)).isoformat(),
            "location_id": random.choice(locations),
            "qc_pass": True,
            "notes": ""
        })

# Fill the rest randomly
for _ in range(n_donations - len(donations)):
    bt = random.choice(blood_types)
    comp = random.choice(components)
    donation_date = today - timedelta(days=random.randint(0, 180))
    expiry_date = donation_date + timedelta(days=365 if comp=="plasma" else 5 if comp=="platelets" else 42)
    donations.append({
        "donation_id": str(uuid.uuid4()),
        "donor_id": f"donor_{random.randint(1, donor_count)}",
        "blood_type": bt,
        "component": comp,
        "units": random.randint(1, 3),
        "donation_date": donation_date.isoformat(),
        "expiry_date": expiry_date.isoformat(),
        "location_id": random.choice(locations),
        "qc_pass": random.random() > 0.02,
        "notes": ""
    })
donations_df = pd.DataFrame(donations)
donations_df.to_csv(os.path.join(data_dir, "donations.csv"), index=False)

# --- 3) Hospital Requests ---
requests = []
n_requests = 600

# Ensure each blood type requested at least once
for bt in blood_types:
    requests.append({
        "request_id": str(uuid.uuid4()),
        "hospital_id": random.choice(hospitals),
        "blood_type": bt,
        "component": random.choice(components),
        "units_requested": random.randint(1, 5),
        "request_date": (today - timedelta(days=random.randint(0, 60))).isoformat(),
        "urgency": random.choice(["routine","urgent","emergency"]),
        "status": "fulfilled",
        "fulfilled_date": today.isoformat()
    })

# Fill rest randomly
for _ in range(n_requests - len(requests)):
    request_date = today - timedelta(days=random.randint(0, 180))
    fulfilled = random.random() < 0.85
    if fulfilled:
        fulfilled_date = request_date + timedelta(days=random.randint(0,2))
        if fulfilled_date > today: fulfilled_date = today
        status = "fulfilled"
    else:
        fulfilled_date = ""
        status = random.choice(["partial","cancelled"])
    requests.append({
        "request_id": str(uuid.uuid4()),
        "hospital_id": random.choice(hospitals),
        "blood_type": random.choice(blood_types),
        "component": random.choice(components),
        "units_requested": random.randint(1, 8),
        "request_date": request_date.isoformat(),
        "urgency": random.choice(["routine","urgent","emergency"]),
        "status": status,
        "fulfilled_date": fulfilled_date if fulfilled_date else ""
    })
requests_df = pd.DataFrame(requests)
requests_df.to_csv(os.path.join(data_dir, "hospital_requests.csv"), index=False)

# --- 4) Inventory snapshot ---
donations_valid = donations_df[donations_df["qc_pass"]==True]
total_by_type = donations_valid.groupby(["blood_type","component"])["units"].sum().reset_index()

requests_fulfilled = requests_df[requests_df["status"]=="fulfilled"]
fulfilled_by_type = requests_fulfilled.groupby(["blood_type","component"])["units_requested"].sum().reset_index()

inv_rows = []
for _, row in total_by_type.iterrows():
    bt, comp, total_units = row["blood_type"], row["component"], int(row["units"])
    fulfilled = int(fulfilled_by_type[
        (fulfilled_by_type["blood_type"]==bt)&(fulfilled_by_type["component"]==comp)
    ]["units_requested"].sum() or 0)
    net_units = max(total_units - fulfilled, 0)
    # spread across locations
    shares = [random.random() for _ in locations]
    s = sum(shares) or 1
    units_split = [int(net_units*(x/s)) for x in shares]
    while sum(units_split) < net_units:
        units_split[random.randrange(len(units_split))] += 1
    for loc, units_here in zip(locations, units_split):
        inv_rows.append({
            "inventory_id": str(uuid.uuid4()),
            "blood_type": bt,
            "component": comp,
            "units_available": units_here,
            "location_id": loc,
            "last_updated": today.isoformat(),
            "notes": ""
        })
inventory_df = pd.DataFrame(inv_rows)
inventory_df.to_csv(os.path.join(data_dir, "inventory.csv"), index=False)

print("✅ Mock data generated with all blood types & components represented.")
