"""
Ram-Z Revenue Band Reminder Script
------------------------------------
Sends reminder emails to GMs who haven't submitted their revenue band
for the upcoming week. Designed to run via GitHub Actions on a schedule.

Reminder escalation:
  1 = 8am CT  — GM email only
  2 = Noon CT — GM email + DM cc'd
  3 = 5pm CT  — GM email + DM + CEO cc'd

Reminder number is auto-detected from UTC hour, or can be set via
the REMINDER_NUMBER environment variable (1, 2, or 3).
"""

import os
import sys
from datetime import date, timedelta, datetime

# ── Dependencies (installed in GitHub Actions via pip) ───────────────────────
try:
    from supabase import create_client
except ImportError:
    print("ERROR: supabase package not installed. Run: pip install supabase")
    sys.exit(1)

try:
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import Mail, Email, To, HtmlContent, Subject, Cc
except ImportError:
    print("ERROR: sendgrid package not installed. Run: pip install sendgrid")
    sys.exit(1)


# ── Environment config ───────────────────────────────────────────────────────
SUPABASE_URL      = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY      = os.environ.get("SUPABASE_KEY", "")
SENDGRID_API_KEY  = os.environ.get("SENDGRID_API_KEY", "")
FROM_EMAIL        = os.environ.get("SENDGRID_FROM_EMAIL", "noreply@ramzrestaurants.com")
GM_PORTAL_URL     = os.environ.get("GM_PORTAL_URL", "https://ramz-gm-portal.streamlit.app")
CEO_EMAIL         = os.environ.get("CEO_EMAIL", "")

if not all([SUPABASE_URL, SUPABASE_KEY, SENDGRID_API_KEY]):
    print("ERROR: Missing required environment variables (SUPABASE_URL, SUPABASE_KEY, SENDGRID_API_KEY)")
    sys.exit(1)

sb = create_client(SUPABASE_URL, SUPABASE_KEY)
sg = SendGridAPIClient(SENDGRID_API_KEY)


# ── Week helpers ─────────────────────────────────────────────────────────────
def get_week_start(ref_date=None):
    """Return the Thursday that starts the current week."""
    d = ref_date or date.today()
    days_since_thursday = (d.weekday() - 3) % 7
    return d - timedelta(days=days_since_thursday)


def get_next_week_start():
    """Return the Thursday that starts the NEXT week."""
    return get_week_start() + timedelta(days=7)


def format_week_label(week_start):
    """Return a human-readable week label like 'Thu 4/3 – Wed 4/9'."""
    end = week_start + timedelta(days=6)
    return f"Thu {week_start.month}/{week_start.day} – Wed {end.month}/{end.day}"


# ── Reminder number detection ────────────────────────────────────────────────
def get_reminder_number():
    """
    Determine which reminder to send.
    Checks REMINDER_NUMBER env var first; falls back to detecting from UTC hour.
    """
    env_val = os.environ.get("REMINDER_NUMBER", "").strip()
    if env_val.isdigit():
        n = int(env_val)
        if 1 <= n <= 3:
            return n

    # Auto-detect from current UTC hour (CDT offsets):
    # Reminder 1 = 8am CT  = 13:00 UTC
    # Reminder 2 = Noon CT = 17:00 UTC
    # Reminder 3 = 5pm CT  = 22:00 UTC
    hour = datetime.utcnow().hour
    if hour < 15:
        return 1
    elif hour < 20:
        return 2
    else:
        return 3


# ── Data loaders (direct Supabase, no Streamlit) ─────────────────────────────
def load_reference_data():
    """Load all stores with DM assignments."""
    resp = sb.table("reference_data").select("*").order("location_id").execute()
    return resp.data or []


def load_gm_contacts():
    """Load GM contacts keyed by location_id."""
    resp = sb.table("gm_contacts").select("*").execute()
    return {r["location_id"]: r for r in (resp.data or [])}


def load_dm_emails():
    """Load DM name → email mapping."""
    resp = sb.table("dm_list").select("dm_name, email").execute()
    return {r["dm_name"]: r.get("email", "") for r in (resp.data or [])}


def load_submitted_store_ids(week_start):
    """
    Return the set of location_ids that have already submitted (non-rejected)
    for the given week. Stores NOT in this set are pending.
    """
    resp = sb.table("rev_band_submissions").select("location_id, status").eq(
        "week_start", str(week_start)
    ).execute()
    return {
        r["location_id"] for r in (resp.data or [])
        if r.get("status") not in ("rejected",)
    }


def log_email(week_start, location_id, to_email, subject, email_type,
              reminder_number, success, error_msg=""):
    """Write an email log entry to Supabase. Never raises."""
    try:
        sb.table("email_log").insert({
            "week_start": str(week_start),
            "location_id": location_id,
            "to_email": to_email,
            "subject": subject,
            "email_type": email_type,
            "reminder_number": reminder_number,
            "success": success,
            "error_msg": error_msg,
            "sent_at": datetime.utcnow().isoformat(),
        }).execute()
    except Exception as e:
        print(f"  ⚠ Could not write email log: {e}")


# ── HTML email builder (no Streamlit dependency) ─────────────────────────────
def build_reminder_html(store_name, gm_name, week_label, portal_url, reminder_number):
    """Build a branded HTML reminder email."""
    labels = {
        1: "Reminder",
        2: "Second Reminder \u2014 Action Needed",
        3: "Final Notice \u2014 Immediate Action Required",
    }
    urgency = labels.get(reminder_number, "Reminder")

    if reminder_number == 1:
        urgency_color = "#333333"
    elif reminder_number == 2:
        urgency_color = "#E67E22"
    else:
        urgency_color = "#E74C3C"

    btn_color   = "#E74C3C" if reminder_number >= 3 else "#C49A5C"
    btn_text    = "Select Revenue Band Now"
    final_block = (
        "<p style='color:#E74C3C;font-size:14px;font-weight:bold;margin-top:8px;'>"
        "This is your final notice. Failure to respond will be logged.</p>"
        if reminder_number >= 3 else ""
    )

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"></head>
<body style="margin:0;padding:0;background:#f5f5f5;font-family:Arial,Helvetica,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f5f5f5;padding:20px 0;">
<tr><td align="center">
<table width="600" cellpadding="0" cellspacing="0"
       style="background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.1);">
  <!-- Header -->
  <tr><td style="background:#2B3A4E;padding:24px 32px;text-align:center;">
    <h1 style="margin:0;color:#C49A5C;font-size:28px;font-weight:bold;letter-spacing:1px;">RAM-Z</h1>
    <p style="margin:4px 0 0;color:#fff;font-size:12px;letter-spacing:2px;">RESTAURANT GROUP</p>
  </td></tr>
  <!-- Store banner -->
  <tr><td style="background:#C49A5C;padding:12px 32px;text-align:center;">
    <p style="margin:0;color:#fff;font-size:16px;font-weight:bold;">{store_name}</p>
  </td></tr>
  <!-- Body -->
  <tr><td style="padding:32px;">
    <p style="color:{urgency_color};font-size:18px;font-weight:bold;margin-top:0;">{urgency}</p>
    <p style="color:#333;font-size:15px;line-height:1.6;">Hi {gm_name or 'Team'},</p>
    <p style="color:#333;font-size:15px;line-height:1.6;">
      You have not yet selected a <strong>revenue band</strong> for
      <strong>{store_name}</strong> for the week of <strong>{week_label}</strong>.
      Please complete your selection as soon as possible.
    </p>
    {final_block}
    <table width="100%" cellpadding="0" cellspacing="0" style="margin:24px 0;">
    <tr><td align="center">
      <a href="{portal_url}"
         style="display:inline-block;background:{btn_color};color:#fff;
                font-size:16px;font-weight:bold;padding:14px 40px;
                text-decoration:none;border-radius:6px;">
        {btn_text}
      </a>
    </td></tr></table>
    <p style="color:#999;font-size:12px;text-align:center;margin-top:16px;">
      If you have questions, contact your District Manager.
    </p>
  </td></tr>
  <!-- Footer -->
  <tr><td style="background:#f8f8f8;padding:16px 32px;text-align:center;border-top:1px solid #eee;">
    <p style="margin:0;color:#999;font-size:11px;">Ram-Z Restaurant Group | Confidential</p>
    <p style="margin:4px 0 0;color:#ccc;font-size:10px;">
      This is an automated message. Please do not reply directly.
    </p>
  </td></tr>
</table>
</td></tr></table>
</body>
</html>"""


# ── Core send function ────────────────────────────────────────────────────────
def send_reminder_email(to_email, subject, html_body, cc_emails=None):
    """Send a single reminder email via SendGrid. Returns True on success."""
    msg = Mail(
        from_email=Email(FROM_EMAIL, "Ram-Z Restaurant Group"),
        to_emails=To(to_email),
        subject=Subject(subject),
        html_content=HtmlContent(html_body),
    )
    if cc_emails:
        for cc in cc_emails:
            if cc and cc.strip():
                msg.add_cc(Cc(cc.strip()))
    response = sg.send(msg)
    return response.status_code in (200, 201, 202)


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    reminder_number = get_reminder_number()
    target_week     = get_next_week_start()
    week_label      = format_week_label(target_week)

    urgency_label = {1: "8am", 2: "Noon", 3: "5pm"}.get(reminder_number, "")
    print(f"\nRam-Z Revenue Band Reminders")
    print(f"Reminder #{reminder_number} ({urgency_label} CT) | Week: {target_week} ({week_label})")
    print("=" * 60)

    # Load data
    ref_data        = load_reference_data()
    gm_contacts     = load_gm_contacts()
    dm_emails       = load_dm_emails()
    submitted_ids   = load_submitted_store_ids(target_week)

    pending_stores = [r for r in ref_data if r["location_id"] not in submitted_ids]

    if not pending_stores:
        print("✓ All stores have submitted. No reminders needed.")
        return

    print(f"{len(pending_stores)} store(s) still pending — sending reminder #{reminder_number}...\n")

    sent_count   = 0
    failed_count = 0
    skipped      = 0

    urgency_map = {
        1: "Reminder",
        2: "Second Reminder \u2014 Action Needed",
        3: "Final Notice \u2014 Immediate Action Required",
    }

    for store in pending_stores:
        loc_id     = store["location_id"]
        store_name = store["store_name"]
        dm_name    = store.get("dm", "")

        gm         = gm_contacts.get(loc_id, {})
        gm_name    = gm.get("gm_name", "")
        gm_email   = gm.get("email", "")
        token      = gm.get("token", "")

        if not gm_email:
            print(f"  SKIP  {store_name} ({loc_id}) — no GM email on file")
            skipped += 1
            continue

        portal_url = f"{GM_PORTAL_URL}?token={token}" if token else GM_PORTAL_URL
        subject    = f"{urgency_map[reminder_number]}: Select Revenue Band for {store_name} \u2014 {week_label}"
        html_body  = build_reminder_html(store_name, gm_name, week_label, portal_url, reminder_number)

        # Build CC list based on escalation level
        cc_emails = []
        if reminder_number >= 2 and dm_name:
            dm_email = dm_emails.get(dm_name, "")
            if dm_email:
                cc_emails.append(dm_email)
        if reminder_number >= 3 and CEO_EMAIL:
            cc_emails.append(CEO_EMAIL)

        try:
            success = send_reminder_email(gm_email, subject, html_body, cc_emails)
            if success:
                cc_note = f" (cc: {', '.join(cc_emails)})" if cc_emails else ""
                print(f"  \u2713 Sent   {store_name} \u2192 {gm_email}{cc_note}")
                sent_count += 1
            else:
                print(f"  \u2717 Failed {store_name} \u2192 {gm_email} (SendGrid returned non-2xx)")
                failed_count += 1
            log_email(target_week, loc_id, gm_email, subject,
                      "gm_reminder", reminder_number, success)
        except Exception as e:
            print(f"  ERROR  {store_name} \u2192 {gm_email} | {e}")
            log_email(target_week, loc_id, gm_email, subject,
                      "gm_reminder", reminder_number, False, str(e))
            failed_count += 1

    print(f"\nDone. {sent_count} sent | {failed_count} failed | {skipped} skipped (no email).")
    if failed_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
