"""
One-time migration script: CSV → Supabase
------------------------------------------
Reads existing CSV files and inserts them into the Supabase tables.
Run once after tables are created:
    cd C:\\Users\\BretElliott\\ramz-accounting
    pip install supabase
    python labor/seed_db.py
"""

import os
import sys
from pathlib import Path

import pandas as pd
from supabase import create_client

# ---------------------------------------------------------------------------
# Supabase credentials (read from .streamlit/secrets.toml manually)
# ---------------------------------------------------------------------------
_ROOT = Path(__file__).parent.parent
_SECRETS_PATH = _ROOT / ".streamlit" / "secrets.toml"

# Parse the secrets file
secrets = {}
with open(_SECRETS_PATH) as f:
    section = None
    for line in f:
        line = line.strip()
        if line.startswith("["):
            section = line.strip("[]")
            secrets[section] = {}
        elif "=" in line and section:
            key, val = line.split("=", 1)
            secrets[section][key.strip()] = val.strip().strip('"')

SUPABASE_URL = secrets["supabase"]["url"]
SUPABASE_KEY = secrets["supabase"]["key"]

sb = create_client(SUPABASE_URL, SUPABASE_KEY)

# ---------------------------------------------------------------------------
# CSV paths
# ---------------------------------------------------------------------------
_LABOR = Path(__file__).parent
_FZ = _LABOR.parent / "fz_fees"

LOCATIONS_CSV = _FZ / "locations.csv"
REFERENCE_CSV = _LABOR / "reference_data.csv"
BAND_GOALS_CSV = _LABOR / "band_goals.csv"
DM_LIST_CSV = _LABOR / "dm_list.csv"
ADMIN_USERS_CSV = _LABOR / "admin_users.csv"
WEEKLY_LOCK_CSV = _LABOR / "weekly_lock.csv"
CHANGE_LOG_CSV = _LABOR / "change_log.csv"


def seed_table(table_name, csv_path, sep="|", columns=None):
    """Read a CSV and upsert all rows into a Supabase table."""
    if not csv_path.exists():
        print(f"  SKIP {table_name} — {csv_path.name} not found")
        return

    df = pd.read_csv(csv_path, sep=sep, dtype=str)
    if columns:
        df = df[columns]

    # Clean up NaN
    df = df.fillna("")
    records = df.to_dict("records")

    if not records:
        print(f"  SKIP {table_name} — no data in {csv_path.name}")
        return

    print(f"  {table_name}: inserting {len(records)} rows...")
    sb.table(table_name).upsert(records).execute()
    print(f"  {table_name}: done")


def main():
    print("Seeding Supabase from CSV files...\n")

    # 1. Stores
    seed_table("stores", LOCATIONS_CSV, sep="|",
               columns=["location_id", "store_name"])

    # 2. Reference data
    seed_table("reference_data", REFERENCE_CSV, sep="|",
               columns=["location_id", "store_name", "dm", "revenue_band"])

    # 3. Band goals (numeric column)
    if BAND_GOALS_CSV.exists():
        df = pd.read_csv(BAND_GOALS_CSV, sep="|")
        records = [
            {"revenue_band": row["revenue_band"], "hourly_goal": float(row["hourly_goal"])}
            for _, row in df.iterrows()
        ]
        print(f"  band_goals: inserting {len(records)} rows...")
        sb.table("band_goals").upsert(records).execute()
        print(f"  band_goals: done")
    else:
        print("  SKIP band_goals — band_goals.csv not found")

    # 4. DM list
    if DM_LIST_CSV.exists():
        df = pd.read_csv(DM_LIST_CSV)
        records = [{"dm_name": name} for name in df["dm_name"].dropna().unique()]
        print(f"  dm_list: inserting {len(records)} rows...")
        sb.table("dm_list").upsert(records).execute()
        print(f"  dm_list: done")
    else:
        print("  SKIP dm_list — dm_list.csv not found")

    # 5. Admin users
    if ADMIN_USERS_CSV.exists():
        df = pd.read_csv(ADMIN_USERS_CSV, dtype=str)
        records = [{"email": e.strip().lower()} for e in df["email"].dropna()]
        print(f"  admin_users: inserting {len(records)} rows...")
        sb.table("admin_users").upsert(records).execute()
        print(f"  admin_users: done")
    else:
        # Default admin
        print("  admin_users: inserting default admin...")
        sb.table("admin_users").upsert([{"email": "elliottbae@gmail.com"}]).execute()
        print("  admin_users: done")

    # 6. Weekly locks (if any exist)
    if WEEKLY_LOCK_CSV.exists():
        df = pd.read_csv(WEEKLY_LOCK_CSV, sep="|", dtype=str)
        if not df.empty:
            df["hourly_goal"] = pd.to_numeric(df["hourly_goal"], errors="coerce").fillna(0)
            records = df[["week_start", "location_id", "store_name", "dm",
                          "revenue_band", "hourly_goal", "source"]].to_dict("records")
            print(f"  weekly_locks: inserting {len(records)} rows...")
            # Insert in batches of 100 to avoid payload limits
            for i in range(0, len(records), 100):
                batch = records[i:i+100]
                sb.table("weekly_locks").upsert(
                    batch, on_conflict="week_start,location_id"
                ).execute()
            print(f"  weekly_locks: done")
        else:
            print("  SKIP weekly_locks — no data")
    else:
        print("  SKIP weekly_locks — weekly_lock.csv not found")

    # 7. Change log (if any exist)
    if CHANGE_LOG_CSV.exists():
        df = pd.read_csv(CHANGE_LOG_CSV, dtype=str)
        if not df.empty:
            df = df.fillna("")
            records = df[["timestamp", "user_email", "week_start", "location_id",
                          "field_changed", "old_value", "new_value", "action"]].to_dict("records")
            print(f"  change_log: inserting {len(records)} rows...")
            for i in range(0, len(records), 100):
                batch = records[i:i+100]
                sb.table("change_log").insert(batch).execute()
            print(f"  change_log: done")
        else:
            print("  SKIP change_log — no data")
    else:
        print("  SKIP change_log — change_log.csv not found")

    print("\nSeed complete!")


if __name__ == "__main__":
    main()
