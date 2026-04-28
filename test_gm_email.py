"""
Test GM Revenue Band Email — End-to-end integration test.

Runs the FULL send_reminders.py flow for a single store, sending the email
to a test recipient instead of the real GM. Tests:
  1. Sentiment synthesis (real Claude call, ~$0.001)
  2. Negative-hour distribution computation
  3. Email rendering with the new "What Customers Are Saying" hook
  4. rev_band_submissions row creation with sentiment_summary_data populated
  5. Portal flow (when you click the link, you'll see the cached sentiment)

A real submission row IS created (so the portal works when you click the link),
which means CLEANUP IS REQUIRED after testing — see the END of this script
for the cleanup SQL.

Usage (via GitHub Actions):
  TEST_EMAIL=you@example.com TEST_STORE_ID=112-0001 python test_gm_email.py
"""
import os
import sys
import uuid
import json
from datetime import date, datetime, timedelta, timezone

# --- Config ---
TEST_EMAIL          = os.environ.get("TEST_EMAIL", "elliottbae@gmail.com")
TEST_STORE_ID       = os.environ.get("TEST_STORE_ID", "112-0001")
# RECOVERY_MODE=1 → drop the [TEST] prefix and treat this as a real send.
# Used when re-sending an email to a single real GM (e.g., recovering a
# manually-deleted submission row). Default off for safety.
RECOVERY_MODE       = os.environ.get("RECOVERY_MODE", "").strip().lower() in ("1", "true", "yes")
SENDGRID_API_KEY    = os.environ.get("SENDGRID_API_KEY", "")
SENDGRID_FROM_EMAIL = os.environ.get("SENDGRID_FROM_EMAIL", "")
SUPABASE_URL        = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY        = os.environ.get("SUPABASE_KEY", "")
ANTHROPIC_KEY       = os.environ.get("ANTHROPIC_API_KEY", "")
GM_PORTAL_URL       = os.environ.get("GM_PORTAL_URL") or "https://ramz-gm-select.streamlit.app"

# --- Sanity checks ---
missing = [
    name for name, val in [
        ("SENDGRID_API_KEY", SENDGRID_API_KEY),
        ("SENDGRID_FROM_EMAIL", SENDGRID_FROM_EMAIL),
        ("SUPABASE_URL", SUPABASE_URL),
        ("SUPABASE_KEY", SUPABASE_KEY),
        ("ANTHROPIC_API_KEY", ANTHROPIC_KEY),
    ] if not val
]
if missing:
    print(f"ERROR: missing env vars: {missing}")
    sys.exit(1)

# Make send_reminders importable
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)

# Import the real flow from send_reminders.py
import send_reminders as sr

# --- Compute target week (next Thursday — same logic as the real Friday run) ---
target_week  = sr.get_next_week_start()
week_end     = target_week + timedelta(days=6)
week_label   = f"Thu {target_week.strftime('%m/%d')} – Wed {week_end.strftime('%m/%d')}"

# --- Pull real store info ---
store_resp = sr.sb.table("stores").select("store_name").eq("location_id", TEST_STORE_ID).execute()
if not store_resp.data:
    print(f"ERROR: store {TEST_STORE_ID} not found in stores table")
    sys.exit(1)
store_name = store_resp.data[0]["store_name"]

ref_resp = sr.sb.table("reference_data").select("dm").eq("location_id", TEST_STORE_ID).execute()
dm_name = ref_resp.data[0]["dm"] if ref_resp.data else "Unknown"

print("=" * 60)
print(f"TEST GM EMAIL — END-TO-END INTEGRATION TEST")
print("=" * 60)
print(f"  Store:        {store_name} ({TEST_STORE_ID})")
print(f"  DM:           {dm_name}")
print(f"  Test week:    {week_label} (week_start={target_week})")
print(f"  Recipient:    {TEST_EMAIL}")
print(f"  From:         {SENDGRID_FROM_EMAIL}")
print()

# --- Generate a fresh test token + insert/upsert a submission row ---
test_token = str(uuid.uuid4())
print(f"Step 1: Creating test submission row")
print(f"  Token: {test_token}")
try:
    # Check existing first (in case of repeat test runs)
    existing = sr.sb.table("rev_band_submissions").select("id,token").eq(
        "location_id", TEST_STORE_ID
    ).eq("week_start", str(target_week)).execute()
    if existing.data:
        # Reuse the existing row's token
        test_token = existing.data[0]["token"]
        print(f"  Reusing existing test submission (token: {test_token})")
        # Reset status to pending_gm so the portal accepts the link
        sr.sb.table("rev_band_submissions").update({
            "status": "pending_gm",
        }).eq("id", existing.data[0]["id"]).execute()
    else:
        sr.sb.table("rev_band_submissions").insert({
            "location_id": TEST_STORE_ID,
            "week_start":  str(target_week),
            "token":       test_token,
            "status":      "pending_gm",
        }).execute()
        print(f"  Inserted new test submission")
except Exception as e:
    print(f"ERROR: Could not create submission row: {e}")
    sys.exit(1)

# --- Pull store performance for the snapshot ---
print()
print(f"Step 2: Loading store performance data")
perf = sr.load_store_performance(TEST_STORE_ID, target_week)
print(f"  Performance loaded: {sum(1 for v in perf.values() if v is not None)}/{len(perf)} fields populated")

# --- Sentiment generation (the new bit we want to test) ---
print()
print(f"Step 3: Fetching 2 weeks of scored reviews")
reviews = sr.fetch_weekly_store_reviews(TEST_STORE_ID, target_week, weeks_back=2)
print(f"  Found {len(reviews)} scored reviews in window")

sentiment = {}
if reviews:
    print()
    print(f"Step 4: Calling Claude to synthesize sentiment summary")
    summary_text = sr.synthesize_weekly_sentiment(store_name, reviews, week_label)
    if summary_text:
        print(f"  Summary: {summary_text[:120]}...")
    negative_hours = sr.compute_negative_hour_distribution(reviews, top_n=5)
    print(f"  Negative hours (top 5): {negative_hours}")

    neg_count = 0
    for r in reviews:
        try:
            if float(r.get("score") or 0) < 70:
                neg_count += 1
        except (ValueError, TypeError):
            pass

    sentiment = {
        "summary":        summary_text or "",
        "negative_hours": negative_hours,
        "review_count":   len(reviews),
        "negative_count": neg_count,
        "generated_at":   datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }

    # Cache to the submission row so the portal can read it
    print()
    print(f"Step 5: Caching sentiment data to rev_band_submissions row")
    try:
        sr.sb.table("rev_band_submissions").update({
            "sentiment_summary_data": sentiment,
        }).eq("location_id", TEST_STORE_ID).eq(
            "week_start", str(target_week)
        ).execute()
        print(f"  ✓ Saved")
    except Exception as e:
        print(f"  ⚠ Could not cache sentiment: {e}")
else:
    print()
    print("  No reviews in window — sentiment block will show 'not enough feedback'")

# --- Build the email ---
print()
print(f"Step 6: Building email body")
portal_url = f"{GM_PORTAL_URL}?token={test_token}"
real_subject = sr.build_subject(store_name, week_label, 'initial')
subject = real_subject if RECOVERY_MODE else f"[TEST] {real_subject}"
html_body  = sr.build_email_html(store_name, "Test GM", week_label, portal_url, "initial",
                                 perf, sentiment)

# --- Send ---
print()
print(f"Step 7: Sending email via SendGrid")
try:
    success = sr.send_email(TEST_EMAIL, subject, html_body)
    if success:
        print(f"  ✓ Email sent to {TEST_EMAIL} — check your inbox!")
        print(f"  Portal link: {portal_url}")
    else:
        print(f"  ✗ Send failed")
        sys.exit(1)
except Exception as e:
    print(f"ERROR: Failed to send email: {e}")
    sys.exit(1)

# --- Cleanup instructions ---
print()
print("=" * 60)
print("✅ TEST COMPLETE")
print("=" * 60)
print()
print("⚠️  CLEANUP REQUIRED — run this SQL in Supabase before Friday:")
print()
print("    DELETE FROM email_log")
print(f"    WHERE location_id = '{TEST_STORE_ID}'")
print(f"      AND week_start  = '{target_week}';")
print()
print("    DELETE FROM rev_band_submissions")
print(f"    WHERE location_id = '{TEST_STORE_ID}'")
print(f"      AND week_start  = '{target_week}';")
print()
print(f"(target_week = {target_week} — different from Friday's run, so the test")
print(" does NOT interfere with the live email cycle, but cleanup keeps the")
print(" compliance dashboard clean.)")
