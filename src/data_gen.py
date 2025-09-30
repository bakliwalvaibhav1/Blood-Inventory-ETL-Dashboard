import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import uuid
from pathlib import Path
import os

# DATA_DIR = Path(__file__).parent / "data"
# DATA_DIR.mkdir(exist_ok=True)

# --- Paths ---
current_dir = os.path.dirname(__file__) if "__file__" in globals() else os.getcwd()
project_root = os.path.abspath(os.path.join(current_dir, ".."))
data_dir = os.path.join(project_root, "data")
os.makedirs(data_dir, exist_ok=True)

# Config
NUM_DONORS = 500
NUM_DONATIONS = 5000
NUM_REQUESTS = 200
LOCATIONS = ["center_1", "center_2", "mobile_drive_1"]
BLOOD_TYPES = ["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"]
COMPONENTS = ["plasma", "platelets", "whole_blood"]

# Generate donors
donors = []
for i in range(NUM_DONORS):
    dob = datetime(1955,1,1) + timedelta(days=random.randint(0, 20000))
    donors.append({
        "donor_id": f"D{i+1}",
        "dob": dob.date(),
        "blood_type": random.choice(BLOOD_TYPES)
    })
donors_df = pd.DataFrame(donors)
donors_df.to_csv(os.path.join(data_dir, "donors.csv"), index=False)

# Generate donations
donations = []
for i in range(NUM_DONATIONS):
    donor = random.choice(donors)
    donation_date = datetime.today() - timedelta(days=random.randint(0, 90))
    component = random.choice(COMPONENTS)
    # expiry: plasma 42 days, platelets 5 days, whole_blood 35 days
    if component == "plasma":
        expiry_days = 42
    elif component == "platelets":
        expiry_days = 5
    else:
        expiry_days = 35
    expiry_date = donation_date + timedelta(days=expiry_days)
    donations.append({
        "donation_id": f"DN{i+1}",
        "donor_id": donor["donor_id"],
        "blood_type": donor["blood_type"],
        "component": component,
        "units": random.randint(1,3),
        "donation_date": donation_date.date(),
        "expiry_date": expiry_date.date(),
        "location_id": random.choice(LOCATIONS),
        "qc_pass": random.choice([True, True, True, False])  # ~75% pass
    })
donations_df = pd.DataFrame(donations)
donations_df.to_csv(os.path.join(data_dir, "donations.csv"), index=False)

# Generate requests
requests = []
for i in range(NUM_REQUESTS):
    bt = random.choice(BLOOD_TYPES)
    comp = random.choice(COMPONENTS)
    req_date = datetime.today() - timedelta(days=random.randint(0, 90))
    status = random.choices(["fulfilled","pending","cancelled"], weights=[0.6,0.3,0.1])[0]
    fulfilled_date = req_date + timedelta(days=random.randint(0,5)) if status=="fulfilled" else pd.NaT
    requests.append({
        "request_id": f"R{i+1}",
        "hospital_id": f"H{random.randint(1,10)}",
        "blood_type": bt,
        "component": comp,
        "units_requested": random.randint(1,5),
        "request_date": req_date.date(),
        "status": status,
        "urgency": random.choice(["high","medium","low"]),
        "fulfilled_date": fulfilled_date if pd.notna(fulfilled_date) else ""
    })
requests_df = pd.DataFrame(requests)
requests_df.to_csv(os.path.join(data_dir, "hospital_requests.csv"), index=False)

# Generate inventory as snapshot from donations
inventory_list = []
for bt in BLOOD_TYPES:
    for comp in COMPONENTS:
        units = donations_df[(donations_df["blood_type"]==bt) & 
                            (donations_df["component"]==comp) & 
                            (donations_df["qc_pass"]==True)]["units"].sum()
        inventory_list.append({
            "inventory_id": f"I_{bt}_{comp}",
            "blood_type": bt,
            "component": comp,
            "units_available": units,
            "location_id": random.choice(LOCATIONS),
            "last_updated": datetime.today().date(),
            "notes": ""
        })
inventory_df = pd.DataFrame(inventory_list)
inventory_df.to_csv(os.path.join(data_dir, "inventory.csv"), index=False)

print("Data generation complete.")
