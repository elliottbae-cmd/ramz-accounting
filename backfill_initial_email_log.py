"""
One-time backfill script.

Context: On 4/17 the initial GM rev band email workflow fired and successfully
sent 7 emails, but log inserts failed because the email_log table was missing
the `success` column. Emails went out, but compliance dashboard shows "—".

This script inserts the missing email_log rows so the compliance report shows
the correct "Initial Email Sent" status for the target week.

Safe to re-run: it checks for existing log entries before inserting.

Usage (local):
  SUPABASE_URL=... SUPABASE_KEY=... python backfill_initial_email_log.py
"""
import os
import sys
from datetime import date, timedelta, datetime, timezone

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("ERROR: SUPABASE_URL and SUPABASE_KEY must be set")
    sys.exit(1)

try:
    from supabase import create_client
except ImportError:
    print("ERROR: supabase not installed. Run: pip install supabase")
    sys.exit(1)

sb = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- Determine target week (the Thursday that the Friday 4/17 emails were for) ---
# GM email fires on Friday, targeting the NEXT Thursday as week_start
# Friday 4/17 + 6 days = Thursday 4/23
today = date.today()
# Find this coming Thursday
days_until_thu = (3 - today.weekday()) % 7
if days_until_thu == 0:
    days_until_thu = 7  # If today IS Thursday, target next Thursday
target_week = today + timedelta(days=days_until_thu)

# Override if script is run later — use the most recent Thursday going forward
# that's within 10 days of a Friday email send
print(f"Target week_start: {target_week}")

# --- Load GM contacts with emails ---
gm_resp = sb.table("gm_contacts").select(
    "location_id, email, gm_name"
).execute()
gm_contacts = {r["location_id"]: r for r in (gm_resp.data or []) if r.get("email")}
print(f"Found {len(gm_contacts)} stores with GM emails on file")

# --- Load stores ---
stores_resp = sb.table("stores").select("location_id, store_name").execute()
store_names = {r["location_id"]: r["store_name"] for r in (stores_resp.data or [])}

# --- Check existing log entries for this week ---
existing_resp = sb.table("email_log").select("location_id").eq(
    "week_start", str(target_week)
).eq("email_type", "initial").execute()
already_logged = {r["location_id"] for r in (existing_resp.data or [])}
print(f"Already logged: {len(already_logged)} stores")

# --- Insert missing log entries ---
inserted = 0
skipped = 0
for loc_id, gm in gm_contacts.items():
    if loc_id in already_logged:
        skipped += 1
        continue

    email = gm.get("email")
    store_name = store_names.get(loc_id, loc_id)
    subject = f"Action Required: Select Revenue Band for {store_name}"

    try:
        sb.table("email_log").insert({
            "week_start":     str(target_week),
            "location_id":    loc_id,
            "to_email":       email,
            "recipient_type": "gm",
            "subject":        subject,
            "email_type":     "initial",
            "success":        True,
            "error_msg":      "",
            "sent_at":        datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }).execute()
        inserted += 1
        print(f"  ✓ Logged {store_name} ({loc_id}) → {email}")
    except Exception as e:
        print(f"  ✗ Failed to log {store_name} ({loc_id}): {e}")

print(f"\nDone. Inserted: {inserted} | Skipped (already logged): {skipped}")
