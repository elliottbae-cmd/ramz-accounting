"""
Daily Supabase backup script.
Exports all key tables to dated CSV files under backups/YYYY-MM-DD/
Run via GitHub Actions or locally.
"""

import os
import csv
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

from supabase import create_client

# ── Max rows per table (warn if hit — means table may be truncated) ───────────
MAX_BACKUP_ROWS = 10_000

# ── Supabase connection ────────────────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("ERROR: SUPABASE_URL and SUPABASE_KEY environment variables must be set.")
    sys.exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── Tables to back up ─────────────────────────────────────────────────────────
TABLES = [
    "reference_data",
    "weekly_locks",
    "weekly_actuals",
    "stores",
    "change_log",
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
truncated_tables = []

for table in TABLES:
    try:
        response = supabase.table(table).select("*").range(0, MAX_BACKUP_ROWS - 1).execute()
        rows = response.data or []

        if not rows:
            summary.append(f"  {table}: 0 rows (empty table)")
            # Write empty file with no rows — skipping header since schema unknown
            filepath = backup_dir / f"{table}.csv"
            filepath.write_text("", encoding="utf-8")
            continue

        # Warn if we may have hit the row limit
        if len(rows) >= MAX_BACKUP_ROWS:
            truncated_tables.append(table)
            summary.append(f"  {table}: ⚠ {len(rows)} rows (MAY BE TRUNCATED — table has >{MAX_BACKUP_ROWS} rows)")
        else:
            summary.append(f"  {table}: {len(rows)} rows → {backup_dir / f'{table}.csv'}")

        filepath = backup_dir / f"{table}.csv"
        fieldnames = list(rows[0].keys())

        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    except Exception as e:
        summary.append(f"  {table}: ERROR — {e}")

# ── Write manifest ────────────────────────────────────────────────────────────
manifest = {
    "backup_date": today,
    "backup_time_utc": datetime.now(timezone.utc).isoformat(),
    "truncated_tables": truncated_tables,
    "tables": {},
}

for table in TABLES:
    fp = backup_dir / f"{table}.csv"
    if fp.exists():
        with open(fp, encoding="utf-8") as f:
            lines = sum(1 for _ in f)
        # Subtract header row; empty files have 0 lines
        row_count = max(0, lines - 1) if lines > 0 else 0
        manifest["tables"][table] = {
            "rows": row_count,
            "file": str(fp),
            "truncated": table in truncated_tables,
        }
    else:
        manifest["tables"][table] = {"rows": 0, "error": "not exported"}

with open(backup_dir / "manifest.json", "w", encoding="utf-8") as f:
    json.dump(manifest, f, indent=2)

# ── Print summary ─────────────────────────────────────────────────────────────
print(f"\nRam-Z DB Backup — {today}")
print("=" * 50)
for line in summary:
    print(line)

if truncated_tables:
    print(f"\n⚠ WARNING: The following tables may have been truncated at {MAX_BACKUP_ROWS} rows:")
    for t in truncated_tables:
        print(f"  - {t}")

print(f"\nManifest written to {backup_dir / 'manifest.json'}")
print("Backup complete.")
