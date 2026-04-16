"""
Test GM Revenue Band Email — sends the actual GM reminder email to a single recipient.

Uses the real email_service.build_gm_selection_email() so you see exactly what
GMs will receive. Pulls sample store performance data if available.

Usage (via GitHub Actions or local):
  TEST_EMAIL=you@example.com TEST_STORE_ID=112-0001 python test_gm_email.py
"""
import os
import sys
from datetime import date, timedelta

# --- Config ---
TEST_EMAIL          = os.environ.get("TEST_EMAIL", "elliottbae@gmail.com")
TEST_STORE_ID       = os.environ.get("TEST_STORE_ID", "112-0001")
SENDGRID_API_KEY    = os.environ.get("SENDGRID_API_KEY", "")
SENDGRID_FROM_EMAIL = os.environ.get("SENDGRID_FROM_EMAIL", "")
SUPABASE_URL        = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY        = os.environ.get("SUPABASE_KEY", "")

if not SENDGRID_API_KEY:
    print("ERROR: SENDGRID_API_KEY not set")
    sys.exit(1)
if not SENDGRID_FROM_EMAIL:
    print("ERROR: SENDGRID_FROM_EMAIL not set")
    sys.exit(1)

# Make labor module importable
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, "labor"))

try:
    from email_service import build_gm_selection_email, send_email
except ImportError as e:
    print(f"ERROR: Could not import email_service: {e}")
    sys.exit(1)

# --- Build week label ---
today = date.today()
days_until_thu = (3 - today.weekday()) % 7 or 7
week_start = today + timedelta(days=days_until_thu)
week_end   = week_start + timedelta(days=6)
week_label = f"Thu {week_start.strftime('%m/%d')} – Wed {week_end.strftime('%m/%d')}"

# --- Try to pull real store data from Supabase (optional) ---
store_name = "Test Store"
py_sales = 32500.0
prev_week_1_sales = 34200.0
prev_week_2_sales = 33800.0
avg_recent_sales = 34000.0
avg_sos = 42.5
last_week_sos_rank = 125
sos_total_stores = 500
avg_negative_reviews = 2.5
last_week_votg_rank = 180
votg_total_stores = 500

if SUPABASE_URL and SUPABASE_KEY:
    try:
        from supabase import create_client
        sb = create_client(SUPABASE_URL, SUPABASE_KEY)
        store_resp = sb.table("stores").select("store_name").eq(
            "location_id", TEST_STORE_ID
        ).execute()
        if store_resp.data:
            store_name = store_resp.data[0]["store_name"]
            print(f"  Using real store data: {store_name}")
    except Exception as e:
        print(f"  Could not load real store data, using sample values: {e}")

# --- Build email using the real template ---
submission_url = f"https://ramz-accounting-c5jtb93zptb6sw5jmno67q.streamlit.app/?store_id={TEST_STORE_ID}"

subject, html_body = build_gm_selection_email(
    store_name=store_name,
    gm_name="Test GM",
    week_label=week_label,
    submission_url=submission_url,
    py_sales=py_sales,
    prev_week_1_sales=prev_week_1_sales,
    prev_week_2_sales=prev_week_2_sales,
    avg_recent_sales=avg_recent_sales,
    avg_sos=avg_sos,
    last_week_sos_rank=last_week_sos_rank,
    sos_total_stores=sos_total_stores,
    avg_negative_reviews=avg_negative_reviews,
    last_week_votg_rank=last_week_votg_rank,
    votg_total_stores=votg_total_stores,
)

# Prefix subject so you know it's a test
subject = f"[TEST] {subject}"

# --- Send ---
print(f"\nSending test GM revenue band email...")
print(f"  To:      {TEST_EMAIL}")
print(f"  From:    {SENDGRID_FROM_EMAIL}")
print(f"  Store:   {store_name} ({TEST_STORE_ID})")
print(f"  Week:    {week_label}")
print()

try:
    result = send_email(TEST_EMAIL, subject, html_body)
    if result:
        print(f"✓ Email sent successfully — check your inbox!")
    else:
        print(f"✗ send_email returned falsy — something went wrong")
        sys.exit(1)
except Exception as e:
    print(f"ERROR: Failed to send email: {e}")
    sys.exit(1)
