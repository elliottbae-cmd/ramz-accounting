"""
Daily Supabase backup script.
Exports all key tables to dated CSV files under backups/YYYY-MM-DD/
Run via GitHub Actions or locally.
"""

import os
import csv
import json
from datetime import date, datetime
from pathlib import Path
from supabase import create_client

# ── Supabase connection ────────────────────────────────────────────────────────
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── Tables to back up ─────────────────────────────────────────────────────────
TABLES = [
    "reference_data",
    "weekly_locks",
    "rev_band_submissions",
    "store_sales",
    "store_sos",
    "store_votg",
    "gm_contacts",
    "dm_list",
    "app_settings",
    "email_log",
    "admin_users",
]

# ── Output folder ─────────────────────────────────────────────────────────────
today = date.today().isoformat()
backup_dir = Path("backups") / today
backup_dir.mkdir(parents=True, exist_ok=True)

summary = []

for table in TABLES:
    try:
        # Fetch all rows (up to 10,000 — adjust range if tables grow huge)
        response = supabase.table(table).select("*").range(0, 9999).execute()
        rows = response.data or []

        if not rows:
            summary.append(f"  {table}: 0 rows (empty table)")
            # Still write an empty file so we know the table exists
            filepath = backup_dir / f"{table}.csv"
            with open(filepath, "w", newline="", encoding="utf-8") as f:
                f.write("")
            continue

        filepath = backup_dir / f"{table}.csv"
        fieldnames = list(rows[0].keys())

        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        summary.append(f"  {table}: {len(rows)} rows → {filepath}")

    except Exception as e:
        summary.append(f"  {table}: ERROR — {e}")

# ── Write manifest ────────────────────────────────────────────────────────────
manifest = {
    "backup_date": today,
    "backup_time_utc": datetime.utcnow().isoformat(),
    "tables": {},
}

for table in TABLES:
    fp = backup_dir / f"{table}.csv"
    if fp.exists():
        with open(fp, encoding="utf-8") as f:
            row_count = max(0, sum(1 for _ in f) - 1)  # subtract header
        manifest["tables"][table] = {"rows": row_count, "file": str(fp)}
    else:
        manifest["tables"][table] = {"rows": 0, "error": "not exported"}

with open(backup_dir / "manifest.json", "w", encoding="utf-8") as f:
    json.dump(manifest, f, indent=2)

# ── Print summary ─────────────────────────────────────────────────────────────
print(f"\nRam-Z DB Backup — {today}")
print("=" * 50)
for line in summary:
    print(line)
print(f"\nManifest written to {backup_dir / 'manifest.json'}")
print("Backup complete.")
