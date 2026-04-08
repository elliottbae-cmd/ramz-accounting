"""
Ram-Z Revenue Band Reminder Script
------------------------------------
Sends reminder emails to GMs who haven't submitted their revenue band
for the upcoming week. Designed to run via GitHub Actions on a schedule.

Escalation logic:
  Monday    → Initial email to GM only (single send)
  Tuesday   → Reminder to GM, DM cc'd (runs at 8am, noon, 5pm)
  Wednesday → Reminder to GM, DM + CEO cc'd (runs at 8am, noon, 5pm)

EMAIL_MODE environment variable controls behavior:
  "initial"   → Monday send (GM only)
  "tuesday"   → Tuesday reminders (GM + DM cc)
  "wednesday" → Wednesday reminders (GM + DM + CEO cc)

If EMAIL_MODE is not set, the script auto-detects from the current day of week.
"""

import os
import sys
from datetime import date, timedelta, datetime, timezone

# ── Dependencies ─────────────────────────────────────────────────────────────
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
SUPABASE_URL     = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY     = os.environ.get("SUPABASE_KEY", "")
SENDGRID_API_KEY = os.environ.get("SENDGRID_API_KEY", "")
FROM_EMAIL       = os.environ.get("SENDGRID_FROM_EMAIL", "")
GM_PORTAL_URL    = os.environ.get("GM_PORTAL_URL", "https://ramz-gm-select.streamlit.app")
CEO_EMAIL        = os.environ.get("CEO_EMAIL", "")
EMAIL_MODE       = os.environ.get("EMAIL_MODE", "").strip().lower()

if not all([SUPABASE_URL, SUPABASE_KEY, SENDGRID_API_KEY]):
    print("ERROR: Missing required env vars (SUPABASE_URL, SUPABASE_KEY, SENDGRID_API_KEY)")
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
    return f"Thu {week_start.month}/{week_start.day} \u2013 Wed {end.month}/{end.day}"


# ── Email mode detection ──────────────────────────────────────────────────────
DAY_NAME_TO_WEEKDAY = {
    "Monday": 0, "Tuesday": 1, "Wednesday": 2,
    "Thursday": 3, "Friday": 4, "Saturday": 5, "Sunday": 6,
}


def load_app_settings():
    """Load app_settings from Supabase. Returns dict of {key: value}."""
    try:
        resp = sb.table("app_settings").select("key, value").execute()
        return {r["key"]: r["value"] for r in (resp.data or [])}
    except Exception as e:
        print(f"  ⚠ Could not load app_settings: {e}")
        return {}


def _current_ct_hour():
    """Return the current hour (0-23) in US Central Time."""
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("America/Chicago")).hour
    except Exception:
        # Fallback: approximate CDT (UTC-5). Accurate Apr–Nov.
        return (datetime.now(timezone.utc).hour - 5) % 24


def _parse_hour(time_str, default):
    """Parse 'HH:MM' string to integer hour. Returns default on failure."""
    try:
        return int(str(time_str).split(":")[0])
    except (ValueError, AttributeError, IndexError):
        return default


def get_email_mode():
    """
    Determine which mode to run in.

    Priority:
      1. EMAIL_MODE env var (manual dispatch / testing) — always wins if set
      2. app_settings in Supabase — reads gm_email_send_day + reminder times:
           send_day + 0 days → 'initial'   (fires at reminder_1_time only)
           send_day + 1 day  → 'tuesday'   (fires at reminder_1/2/3 times)
           send_day + 2 days → 'wednesday' (fires at reminder_1/2/3 times)
      3. Hard fallback to day-of-week if app_settings not configured

    Returns None if today/time is not a configured send window (script exits early).
    """
    # 1. Manual override via env var — skip time check
    if EMAIL_MODE in ("initial", "tuesday", "wednesday"):
        return EMAIL_MODE

    # 2. Load settings
    settings      = load_app_settings()
    send_day_name = settings.get("gm_email_send_day", "").strip()
    send_day_num  = DAY_NAME_TO_WEEKDAY.get(send_day_name)
    today         = date.today().weekday()

    # ── Determine delta (days since send day) ────────────────────────────────
    if send_day_num is not None:
        delta = (today - send_day_num) % 7
        if delta > 2:
            print(f"Today is not a configured send day (send_day={send_day_name}). Skipping.")
            return None
    else:
        # Hard fallback — app_settings not configured yet
        print("WARNING: gm_email_send_day not set in app_settings. Falling back to day-of-week.")
        fallback = {0: 0, 1: 1, 2: 2}
        if today not in fallback:
            print(f"WARNING: Today (weekday={today}) is not Mon/Tue/Wed. Skipping.")
            return None
        delta = fallback[today]

    # ── Check current CT hour against configured reminder windows ────────────
    r1 = _parse_hour(settings.get("reminder_1_time", "08:00"), 8)
    r2 = _parse_hour(settings.get("reminder_2_time", "12:00"), 12)
    r3 = _parse_hour(settings.get("reminder_3_time", "17:00"), 17)
    current_hour = _current_ct_hour()

    if delta == 0:
        # Initial day — one send at 2:00 PM CT (hardcoded so ops data is ready)
        allowed = {14}
        mode    = "initial"
    elif delta == 1:
        allowed = {r1, r2, r3}
        mode    = "tuesday"
    else:  # delta == 2
        allowed = {r1, r2, r3}
        mode    = "wednesday"

    if current_hour not in allowed:
        print(f"Current CT hour ({current_hour}) not in send hours {sorted(allowed)}. Skipping.")
        return None

    return mode


# ── Data loaders (direct Supabase, no Streamlit) ─────────────────────────────
def load_reference_data():
    resp = sb.table("reference_data").select("*").eq("active", True).order("location_id").execute()
    return resp.data or []


def load_gm_contacts():
    resp = sb.table("gm_contacts").select("*").execute()
    return {r["location_id"]: r for r in (resp.data or [])}


def load_dm_emails():
    """Return dict of {dm_name: email}."""
    resp = sb.table("dm_list").select("dm_name, email").execute()
    return {r["dm_name"]: r.get("email", "") for r in (resp.data or [])}


def load_submitted_store_ids(week_start):
    """Return set of location_ids that have already submitted (non-rejected) for the week."""
    resp = sb.table("rev_band_submissions").select("location_id, status").eq(
        "week_start", str(week_start)
    ).execute()
    return {
        r["location_id"] for r in (resp.data or [])
        if r.get("status") not in ("rejected", "pending_gm")
    }


def load_store_performance(location_id, target_week):
    """Load sales, SoS, and VOTG data for a store to populate email snapshot."""
    # Anchor to last COMPLETED Thu-Wed week (not the upcoming target week)
    today = date.today()
    days_since_thu = (today.weekday() - 3) % 7
    current_week_start = today - timedelta(days=days_since_thu)
    last_complete_week  = current_week_start - timedelta(weeks=1)  # most recently closed week
    two_weeks_ago_week  = current_week_start - timedelta(weeks=2)
    py_week             = last_complete_week - timedelta(weeks=52)  # same week prior year

    four_weeks_ago = current_week_start - timedelta(weeks=4)
    eight_weeks_ago = current_week_start - timedelta(weeks=8)

    # ── Recent sales (last 8 completed weeks) ─────────────────────────────────
    sales_resp = sb.table("store_sales").select("sale_date,net_sales").eq(
        "location_id", location_id
    ).gte("sale_date", str(eight_weeks_ago)).lt(
        "sale_date", str(current_week_start)   # exclude current incomplete week
    ).execute()

    week_sales = {}
    for row in (sales_resp.data or []):
        d = date.fromisoformat(row["sale_date"])
        days_since_thu = (d.weekday() - 3) % 7
        w = d - timedelta(days=days_since_thu)
        week_sales[w] = week_sales.get(w, 0) + float(row.get("net_sales") or 0)

    # ── Prior year sales (separate query — ~52 weeks back) ────────────────────
    py_week_end = py_week + timedelta(days=7)
    py_resp = sb.table("store_sales").select("sale_date,net_sales").eq(
        "location_id", location_id
    ).gte("sale_date", str(py_week)).lt("sale_date", str(py_week_end)).execute()

    py_total = sum(float(r.get("net_sales") or 0) for r in (py_resp.data or []))
    py_sales = py_total if py_total > 0 else None

    prev_week_1_sales = week_sales.get(last_complete_week) or None
    prev_week_2_sales = week_sales.get(two_weeks_ago_week) or None
    vals = [v for v in [prev_week_1_sales, prev_week_2_sales] if v is not None]
    avg_recent_sales  = sum(vals) / len(vals) if vals else None

    # ── SoS ───────────────────────────────────────────────────────────────────
    sos_resp = sb.table("store_sos_weekly").select(
        "week_start,good_shift_rank,total_stores,total_time"
    ).eq("location_id", location_id).gte(
        "week_start", str(four_weeks_ago)
    ).lt("week_start", str(current_week_start)).order("week_start", desc=True).execute()

    sos_rows = sos_resp.data or []
    last_week_sos_rank = sos_total_stores = avg_sos = None
    if sos_rows:
        last = sos_rows[0]
        try:
            last_week_sos_rank = int(last.get("good_shift_rank") or 0)
            sos_total_stores   = int(last.get("total_stores") or 0)
        except (ValueError, TypeError):
            pass
        # total_time stored as "MM:SS" — convert to seconds for the template
        secs_list = []
        for r in sos_rows:
            tt = str(r.get("total_time") or "")
            if ":" in tt:
                try:
                    m, s = tt.split(":")
                    secs_list.append(int(m) * 60 + int(s))
                except (ValueError, IndexError):
                    pass
        if secs_list:
            avg_sos = sum(secs_list) / len(secs_list)

    # ── VOTG ──────────────────────────────────────────────────────────────────
    votg_resp = sb.table("store_votg_weekly").select(
        "week_start,total_negative_reviews,votg_rank,total_stores"
    ).eq("location_id", location_id).gte(
        "week_start", str(four_weeks_ago)
    ).lt("week_start", str(current_week_start)).order("week_start", desc=True).execute()

    votg_rows = votg_resp.data or []
    last_week_votg_rank = votg_total_stores = avg_negative_reviews = None
    if votg_rows:
        last = votg_rows[0]
        try:
            last_week_votg_rank = int(last.get("votg_rank") or 0)
            votg_total_stores   = int(last.get("total_stores") or 0)
        except (ValueError, TypeError):
            pass
        neg = [float(r.get("total_negative_reviews") or 0) for r in votg_rows]
        avg_negative_reviews = sum(neg) / len(neg) if neg else None

    return dict(
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


def log_email(week_start, location_id, to_email, subject, email_type, success, error_msg=""):
    """Write an email log entry to Supabase. Never raises."""
    try:
        sb.table("email_log").insert({
            "week_start":    str(week_start),
            "location_id":   location_id,
            "to_email":      to_email,
            "subject":       subject,
            "email_type":    email_type,
            "success":       success,
            "error_msg":     error_msg,
            "sent_at":       datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }).execute()
    except Exception as e:
        print(f"  \u26a0 Could not write email log: {e}")


# ── HTML email builder ────────────────────────────────────────────────────────
def build_email_html(store_name, gm_name, week_label, portal_url, mode, perf=None):
    """Build branded HTML email with store performance snapshot."""
    if mode == "initial":
        heading       = "Action Required: Select Your Revenue Band"
        heading_color = "#333333"
        intro         = "It\u2019s time to select your revenue band for the upcoming week."
        final_block   = ""
        btn_color     = "#C49A5C"
    elif mode == "tuesday":
        heading       = "Reminder \u2014 Revenue Band Selection Pending"
        heading_color = "#E67E22"
        intro         = "We have not yet received your revenue band selection for the upcoming week."
        final_block   = ""
        btn_color     = "#C49A5C"
    else:  # wednesday
        heading       = "Final Notice \u2014 Immediate Action Required"
        heading_color = "#E74C3C"
        intro         = "We still have not received your revenue band selection. This is your final notice."
        final_block   = (
            "<p style='color:#E74C3C;font-size:14px;font-weight:bold;margin-top:8px;'>"
            "Failure to respond today will be escalated and logged.</p>"
        )
        btn_color = "#E74C3C"

    # ── Performance snapshot table ────────────────────────────────────────────
    def fc(val):
        return f"${val:,.0f}" if val is not None else "N/A"

    def fn(val, dec=1):
        return f"{val:.{dec}f}" if val is not None else "N/A"

    def rank_str(rank, total):
        if rank is not None and total is not None:
            return f"{int(rank)} of {int(total)}"
        return "N/A"

    p = perf or {}
    avg_sos_min = fn(p.get("avg_sos") / 60 if p.get("avg_sos") is not None else None) + " min"

    perf_table = f"""
    <table width="100%" cellpadding="8" cellspacing="0" style="margin:20px 0;border-collapse:collapse;">
      <tr style="background:#2B3A4E;">
        <td colspan="2" style="color:#fff;font-size:14px;font-weight:bold;padding:10px 12px;border-radius:4px 4px 0 0;">
          Store Performance Snapshot
        </td>
      </tr>
      <tr style="background:#f9f9f9;">
        <td style="padding:8px 12px;border-bottom:1px solid #eee;color:#666;font-size:13px;">Prior Year — Same Week Sales</td>
        <td style="padding:8px 12px;border-bottom:1px solid #eee;color:#333;font-size:13px;font-weight:bold;text-align:right;">{fc(p.get("py_sales"))}</td>
      </tr>
      <tr>
        <td style="padding:8px 12px;border-bottom:1px solid #eee;color:#666;font-size:13px;">Last Week Sales</td>
        <td style="padding:8px 12px;border-bottom:1px solid #eee;color:#333;font-size:13px;font-weight:bold;text-align:right;">{fc(p.get("prev_week_1_sales"))}</td>
      </tr>
      <tr style="background:#f9f9f9;">
        <td style="padding:8px 12px;border-bottom:1px solid #eee;color:#666;font-size:13px;">Two Weeks Ago Sales</td>
        <td style="padding:8px 12px;border-bottom:1px solid #eee;color:#333;font-size:13px;font-weight:bold;text-align:right;">{fc(p.get("prev_week_2_sales"))}</td>
      </tr>
      <tr>
        <td style="padding:8px 12px;border-bottom:1px solid #eee;color:#666;font-size:13px;">Avg Sales (Last 2 Weeks)</td>
        <td style="padding:8px 12px;border-bottom:1px solid #eee;color:#333;font-size:13px;font-weight:bold;text-align:right;">{fc(p.get("avg_recent_sales"))}</td>
      </tr>
      <tr style="background:#f9f9f9;">
        <td style="padding:8px 12px;border-bottom:1px solid #eee;color:#666;font-size:13px;">Avg Speed of Service (Last 4 Weeks)</td>
        <td style="padding:8px 12px;border-bottom:1px solid #eee;color:#333;font-size:13px;font-weight:bold;text-align:right;">{avg_sos_min}</td>
      </tr>
      <tr>
        <td style="padding:8px 12px;border-bottom:1px solid #eee;color:#666;font-size:13px;">Speed of Service Rank (Last Week)</td>
        <td style="padding:8px 12px;border-bottom:1px solid #eee;color:#333;font-size:13px;font-weight:bold;text-align:right;">{rank_str(p.get("last_week_sos_rank"), p.get("sos_total_stores"))}</td>
      </tr>
      <tr style="background:#f9f9f9;">
        <td style="padding:8px 12px;border-bottom:1px solid #eee;color:#666;font-size:13px;">Avg Negative Reviews (Last 4 Weeks)</td>
        <td style="padding:8px 12px;border-bottom:1px solid #eee;color:#333;font-size:13px;font-weight:bold;text-align:right;">{fn(p.get("avg_negative_reviews"), 0)}</td>
      </tr>
      <tr>
        <td style="padding:8px 12px;color:#666;font-size:13px;">VOTG Rank (Last Week)</td>
        <td style="padding:8px 12px;color:#333;font-size:13px;font-weight:bold;text-align:right;">{rank_str(p.get("last_week_votg_rank"), p.get("votg_total_stores"))}</td>
      </tr>
    </table>"""

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
    <p style="color:{heading_color};font-size:18px;font-weight:bold;margin-top:0;">{heading}</p>
    <p style="color:#333;font-size:15px;line-height:1.6;">Hi {gm_name or 'Team'},</p>
    <p style="color:#333;font-size:15px;line-height:1.6;">
      {intro} Please complete your selection for <strong>{store_name}</strong>
      for the week of <strong>{week_label}</strong>.
    </p>
    {perf_table}
    {final_block}
    <table width="100%" cellpadding="0" cellspacing="0" style="margin:24px 0;">
    <tr><td align="center">
      <a href="{portal_url}"
         style="display:inline-block;background:{btn_color};color:#fff;
                font-size:16px;font-weight:bold;padding:14px 40px;
                text-decoration:none;border-radius:6px;">
        Select Revenue Band
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


def build_subject(store_name, week_label, mode):
    if mode == "initial":
        return f"Action Required: Select Revenue Band for {store_name} \u2014 {week_label}"
    elif mode == "tuesday":
        return f"Reminder: Revenue Band Still Pending for {store_name} \u2014 {week_label}"
    else:
        return f"Final Notice: Select Revenue Band for {store_name} \u2014 {week_label}"


# ── Core send function ────────────────────────────────────────────────────────
def send_email(to_email, subject, html_body, cc_emails=None):
    """Send a single email via SendGrid. Returns True on success."""
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
    mode = get_email_mode()
    if mode is None:
        print("No emails to send today. Exiting.")
        return

    target_week = get_next_week_start()
    week_label  = format_week_label(target_week)

    mode_labels = {
        "initial":   "Initial Email (GM only)",
        "tuesday":   "Reminder Day 1 (GM + DM cc)",
        "wednesday": "Reminder Day 2 — Final (GM + DM + CEO cc)",
    }
    print(f"\nRam-Z Revenue Band Emails")
    print(f"Mode:  {mode_labels.get(mode, mode)}")
    print(f"Week:  {target_week} ({week_label})")
    print("=" * 60)

    # Load data
    ref_data      = load_reference_data()
    gm_contacts   = load_gm_contacts()
    dm_emails     = load_dm_emails()
    submitted_ids = load_submitted_store_ids(target_week)

    pending_stores = [r for r in ref_data if r["location_id"] not in submitted_ids]

    if not pending_stores:
        print("\u2713 All stores have submitted. No emails needed.")
        return

    print(f"{len(pending_stores)} store(s) pending...\n")

    sent_count   = 0
    failed_count = 0
    skipped      = 0

    for store in pending_stores:
        loc_id     = store["location_id"]
        store_name = store["store_name"]
        dm_name    = store.get("dm", "")

        gm       = gm_contacts.get(loc_id, {})
        gm_name  = gm.get("gm_name", "")
        gm_email = gm.get("email", "")
        token    = gm.get("token", "")

        if not gm_email:
            print(f"  SKIP  {store_name} ({loc_id}) \u2014 no GM email on file")
            skipped += 1
            continue

        portal_url = f"{GM_PORTAL_URL}?token={token}" if token else GM_PORTAL_URL
        subject    = build_subject(store_name, week_label, mode)

        # Ensure a submission record exists in rev_band_submissions so the
        # portal can look up the token and identify the store + week.
        if token:
            try:
                existing = sb.table("rev_band_submissions").select("id").eq(
                    "token", token
                ).eq("week_start", str(target_week)).execute()
                if not existing.data:
                    sb.table("rev_band_submissions").insert({
                        "location_id": loc_id,
                        "week_start":  str(target_week),
                        "token":       token,
                        "status":      "pending_gm",
                    }).execute()
            except Exception as e:
                print(f"  ⚠ Could not create submission record for {store_name}: {e}")

        perf       = load_store_performance(loc_id, target_week)
        html_body  = build_email_html(store_name, gm_name, week_label, portal_url, mode, perf)

        # Build CC list based on escalation day
        cc_emails = []
        if mode in ("tuesday", "wednesday"):
            dm_email = dm_emails.get(dm_name, "")
            if dm_email:
                cc_emails.append(dm_email)
        if mode == "wednesday" and CEO_EMAIL:
            cc_emails.append(CEO_EMAIL)

        try:
            success = send_email(gm_email, subject, html_body, cc_emails)
            cc_note = f" (cc: {', '.join(cc_emails)})" if cc_emails else ""
            status  = "\u2713 Sent  " if success else "\u2717 Failed"
            print(f"  {status} {store_name} \u2192 {gm_email}{cc_note}")
            log_email(target_week, loc_id, gm_email, subject, f"gm_{mode}", success)
            if success:
                sent_count += 1
            else:
                failed_count += 1
        except Exception as e:
            print(f"  ERROR  {store_name} \u2192 {gm_email} | {e}")
            log_email(target_week, loc_id, gm_email, subject, f"gm_{mode}", False, str(e))
            failed_count += 1

    print(f"\nDone. {sent_count} sent | {failed_count} failed | {skipped} skipped (no email on file).")
    if failed_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
