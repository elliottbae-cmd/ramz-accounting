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
import json
import requests
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
ANTHROPIC_KEY    = os.environ.get("ANTHROPIC_API_KEY", "")

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
    except ImportError:
        # Python < 3.9 fallback — use dateutil
        try:
            from dateutil import tz
            return datetime.now(tz.gettz("America/Chicago")).hour
        except ImportError:
            # Last resort: approximate CST (UTC-6). NOTE: off by 1hr during CDT.
            import logging
            logging.warning("No timezone library available — using UTC-6 approximation")
            return (datetime.now(timezone.utc).hour - 6) % 24


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
        # Initial day — target 1:00 PM CT, with 1-hour grace window (13-14)
        # for GitHub Actions runner delays. Dedup in log_email() prevents
        # double-sends if both hours fire within the same day.
        allowed = {13, 14}
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

    # Escalation visibility — warn if CEO_EMAIL is missing on Wednesday final notice
    if mode == "wednesday" and not CEO_EMAIL:
        print("  \u26a0 WARNING: CEO_EMAIL env var not set \u2014 Wednesday final notice "
              "will NOT escalate to the CEO. Set CEO_EMAIL in GitHub Secrets.")

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


def load_pending_dm_submissions(week_start):
    """Return list of rev_band_submissions with status=pending_dm for the given week."""
    resp = sb.table("rev_band_submissions").select("*").eq(
        "week_start", str(week_start)
    ).eq("status", "pending_dm").execute()
    return resp.data or []


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

    # ── Recent sales — use weekly_actuals for accuracy (full week totals from AVS upload) ──
    actuals_resp = sb.table("weekly_actuals").select("week_start,net_sales").eq(
        "location_id", location_id
    ).gte("week_start", str(two_weeks_ago_week)).lt(
        "week_start", str(current_week_start)
    ).execute()

    week_sales = {}
    for row in (actuals_resp.data or []):
        w = date.fromisoformat(str(row["week_start"])[:10])
        week_sales[w] = float(row.get("net_sales") or 0)

    # ── Prior year sales — store_sales (weekly_actuals doesn't go back a year yet) ──
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


def log_email(week_start, location_id, to_email, subject, email_type, success,
              error_msg="", recipient_type="gm"):
    """Write an email log entry to Supabase. Deduplicates on (week_start,
    location_id, recipient_email, email_type) to prevent double-logs from
    retried or overlapping workflow runs."""
    # Input validation
    if not to_email or not recipient_type or not location_id:
        print(f"  \u26a0 log_email skipped — missing required fields "
              f"(to_email={bool(to_email)}, type={recipient_type}, loc={location_id})")
        return
    try:
        # Dedup: skip if an identical entry already exists for this week/store/recipient/type
        existing = sb.table("email_log").select("id").eq(
            "week_start", str(week_start)
        ).eq("location_id", location_id).eq(
            "recipient_email", to_email
        ).eq("email_type", email_type).limit(1).execute()
        if existing.data:
            # Already logged — no-op
            return

        sb.table("email_log").insert({
            "week_start":      str(week_start),
            "location_id":     location_id,
            "recipient_email": to_email,
            "recipient_type":  recipient_type,
            "subject":         subject,
            "email_type":      email_type,
            "success":         success,
            "error_msg":       error_msg,
            "sent_at":         datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }).execute()
    except Exception as e:
        print(f"  \u26a0 Could not write email log: {e}")


# ── Sentiment synthesis ───────────────────────────────────────────────────────
def fetch_weekly_store_reviews(location_id, week_start, weeks_back=2):
    """
    Fetch all scored Tattle reviews for a store over the past `weeks_back`
    completed weeks (default 2 — matches sales / SoS / VOTG cadence).

    The window ends the day before `week_start` (the upcoming week being
    emailed about) and goes back `weeks_back` weeks.

    Returns list of review dicts or empty list.
    """
    window_end   = week_start - timedelta(days=1)               # last fully-complete day
    window_start = week_start - timedelta(weeks=weeks_back)
    try:
        resp = (sb.table("tattle_reviews")
                  .select("score,comment,snapshots,sentiment_themes,sentiment_summary,"
                          "questionnaire_title,day_part_label,channel_label,experienced_time")
                  .eq("location_id", location_id)
                  .gte("completed_time", f"{window_start}T00:00:00")
                  .lte("completed_time", f"{window_end}T23:59:59")
                  .not_.is_("sentiment_themes", "null")
                  .execute())
        return resp.data or []
    except Exception as e:
        print(f"  [WARN] Could not fetch reviews for {location_id}: {e}")
        return []


def compute_negative_hour_distribution(reviews, top_n=5):
    """
    From a list of scored reviews, compute the top hours-of-day where
    negative reviews (score < 70) cluster.

    Returns a list of (hour_label, count) tuples, e.g.:
        [("1pm", 3), ("12pm", 2), ("11am", 1)]

    Hours are derived from `experienced_time` (when the guest visited),
    not `completed_time` (when they submitted the review). Matches the
    Tattle Insights heatmap logic.
    """
    from collections import Counter
    hour_counts = Counter()
    for r in reviews:
        try:
            score = float(r.get("score") or 0)
        except (ValueError, TypeError):
            continue
        if score >= 70:
            continue
        ts = r.get("experienced_time")
        if not ts:
            continue
        try:
            # Parse ISO timestamp
            dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
            hour_counts[dt.hour] += 1
        except (ValueError, TypeError):
            continue

    def fmt_hour(h):
        # 0 -> 12am, 12 -> 12pm, 13 -> 1pm, 23 -> 11pm
        if h == 0:   return "12am"
        if h < 12:   return f"{h}am"
        if h == 12:  return "12pm"
        return f"{h - 12}pm"

    top = hour_counts.most_common(top_n)
    return [(fmt_hour(h), c) for h, c in top]


def synthesize_weekly_sentiment(store_name, reviews, week_label):
    """
    Send a store's week of scored reviews to Claude and get back
    a positive paragraph and a negative paragraph.
    Returns (positive_text, negative_text) or (None, None) on failure.
    """
    if not ANTHROPIC_KEY or not reviews:
        return None, None

    # Build a compact summary of all reviews for Claude
    lines = []
    for r in reviews:
        score   = r.get("score", "?")
        themes  = r.get("sentiment_themes") or {}
        overall = themes.get("overall", "unknown") if isinstance(themes, dict) else "unknown"
        comment = r.get("comment", "") or ""
        cats    = themes.get("categories", {}) if isinstance(themes, dict) else {}
        cat_str = ", ".join(f"{k}: {v}" for k, v in cats.items()) if cats else ""
        line    = f"Score:{score}/100 Sentiment:{overall}"
        if cat_str:
            line += f" Categories:[{cat_str}]"
        if comment:
            # Truncate long comments
            line += f" Comment: {comment[:300]}"
        lines.append(line)

    review_block = "\n".join(lines)
    total        = len(reviews)
    positive_ct  = sum(1 for r in reviews
                       if isinstance(r.get("sentiment_themes"), dict)
                       and r["sentiment_themes"].get("overall") == "positive")
    negative_ct  = sum(1 for r in reviews
                       if isinstance(r.get("sentiment_themes"), dict)
                       and r["sentiment_themes"].get("overall") == "negative")

    prompt = f"""You are summarizing guest feedback for {store_name} for the past 2 weeks ending {week_label}.

Total reviews: {total}
Positive: {positive_ct} | Negative: {negative_ct}

Individual reviews:
{review_block}

Write a single short paragraph (2-3 sentences) for a GM weekly email titled "What Customers Are Saying":
- Synthesize the dominant themes — both what guests praised and what they complained about
- Be specific to actual themes mentioned, not generic
- Do not mention individual guests or quote directly
- Keep it under 80 words
- If there are no reviews or no clear themes, write "Not enough recent guest feedback to summarize."
- Return only the paragraph, no headings or formatting"""

    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key":         ANTHROPIC_KEY,
                "anthropic-version": "2023-06-01",
                "content-type":      "application/json",
            },
            data=json.dumps({
                "model":      "claude-haiku-4-5-20251001",
                "max_tokens": 200,
                "messages":   [{"role": "user", "content": prompt}],
            }),
            timeout=30,
        )
        if r.status_code != 200:
            print(f"  [WARN] Claude sentiment synthesis failed: {r.status_code}")
            return None

        summary = r.json()["content"][0]["text"].strip()
        return summary

    except Exception as e:
        print(f"  [WARN] Sentiment synthesis error: {e}")
        return None


# ── HTML email builder ────────────────────────────────────────────────────────
def build_email_html(store_name, gm_name, week_label, portal_url, mode, perf=None,
                     sentiment=None):
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

    # ── Sentiment block (new format: 1 paragraph + top hours) ────────────────
    s = sentiment or {}
    summary_text   = s.get("summary")
    negative_hours = s.get("negative_hours") or []
    if summary_text or negative_hours:
        # Build the hours list HTML
        if negative_hours:
            hour_rows = "".join(
                f'<li style="padding:4px 0;color:#444;font-size:13px;">'
                f'<strong>{hour}</strong> &mdash; {count} negative review{"s" if count != 1 else ""}'
                f'</li>'
                for hour, count in negative_hours
            )
            hours_block = f"""
        <p style="margin:12px 0 4px;color:#666;font-size:13px;font-weight:bold;">
          When complaints are coming in (top hours):
        </p>
        <ul style="margin:0;padding-left:20px;">{hour_rows}</ul>"""
        else:
            hours_block = (
                '<p style="margin:12px 0 0;color:#27AE60;font-size:13px;'
                'font-style:italic;">No negative reviews in this period &mdash; keep it up! 🎉</p>'
            )

        sentiment_block = f"""
    <table width="100%" cellpadding="0" cellspacing="0"
           style="margin:20px 0;border-collapse:collapse;border:1px solid #e0e0e0;border-radius:6px;">
      <tr style="background:#2B3A4E;">
        <td style="color:#fff;font-size:14px;font-weight:bold;padding:10px 14px;border-radius:6px 6px 0 0;">
          💬 What Customers Are Saying (Past 2 Weeks)
        </td>
      </tr>
      <tr><td style="padding:14px;">
        <p style="margin:0;color:#333;font-size:14px;line-height:1.6;">
          {summary_text or 'Not enough recent guest feedback to summarize.'}
        </p>
        {hours_block}
      </td></tr>
    </table>"""
    else:
        sentiment_block = ""

    # ── Performance teaser (hook approach — full data lives on portal) ───────
    perf_teaser = f"""
    <table width="100%" cellpadding="0" cellspacing="0"
           style="margin:16px 0;border-collapse:collapse;background:#F5F0EB;border-radius:6px;">
      <tr><td style="padding:14px 18px;color:#2B3A4E;font-size:13px;line-height:1.5;">
        📊 Your full performance — sales, SoS rank, VOTG rank, and 2-week trends —
        is waiting in your selection portal. Click below to review the data and
        select your band.
      </td></tr>
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
    {sentiment_block}
    {perf_teaser}
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


# ── DM reminder email builder ─────────────────────────────────────────────────
def build_dm_reminder_email(dm_name, pending_stores, week_label, dm_portal_url):
    """Build branded HTML reminder email for a DM listing stores awaiting approval."""
    store_rows = ""
    for i, item in enumerate(pending_stores):
        bg = "#f9f9f9" if i % 2 == 0 else "#ffffff"
        band_changed = item["selected_band"] != item["current_band"]
        band_style = "font-weight:bold;color:#E67E22;" if band_changed else "font-weight:bold;"
        store_rows += f"""
      <tr style="background:{bg};">
        <td style="padding:8px 12px;font-size:13px;color:#333;">{item['store_name']}</td>
        <td style="padding:8px 12px;font-size:13px;color:#333;text-align:center;">{item['current_band']}</td>
        <td style="padding:8px 12px;font-size:13px;{band_style}text-align:center;">{item['selected_band']}</td>
      </tr>"""

    count = len(pending_stores)
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
  <!-- Banner -->
  <tr><td style="background:#C49A5C;padding:12px 32px;text-align:center;">
    <p style="margin:0;color:#fff;font-size:16px;font-weight:bold;">Revenue Band Approvals Pending</p>
  </td></tr>
  <!-- Body -->
  <tr><td style="padding:32px;">
    <p style="color:#E67E22;font-size:18px;font-weight:bold;margin-top:0;">Action Required \u2014 {count} Store(s) Awaiting Your Approval</p>
    <p style="color:#333;font-size:15px;line-height:1.6;">Hi {dm_name},</p>
    <p style="color:#333;font-size:15px;line-height:1.6;">
      The following store(s) have submitted their revenue band selection for
      <strong>{week_label}</strong> and are awaiting your approval.
      Bands highlighted in orange indicate a change from the current band.
    </p>
    <table width="100%" cellpadding="8" cellspacing="0" style="margin:20px 0;border-collapse:collapse;">
      <tr style="background:#2B3A4E;">
        <td style="color:#fff;font-size:13px;font-weight:bold;padding:10px 12px;">Store</td>
        <td style="color:#fff;font-size:13px;font-weight:bold;padding:10px 12px;text-align:center;">Current Band</td>
        <td style="color:#fff;font-size:13px;font-weight:bold;padding:10px 12px;text-align:center;">Selected Band</td>
      </tr>
      {store_rows}
    </table>
    <table width="100%" cellpadding="0" cellspacing="0" style="margin:24px 0;">
    <tr><td align="center">
      <a href="{dm_portal_url}"
         style="display:inline-block;background:#C49A5C;color:#fff;
                font-size:16px;font-weight:bold;padding:14px 40px;
                text-decoration:none;border-radius:6px;">
        Review &amp; Approve Now
      </a>
    </td></tr></table>
    <p style="color:#999;font-size:12px;text-align:center;margin-top:16px;">
      If you have questions, contact your administrator.
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


def send_dm_reminders(target_week, week_label, ref_data, gm_contacts, dm_emails):
    """
    Send approval reminder emails to DMs with pending_dm submissions.
    Only fires Monday and Tuesday at 8AM CT.
    """
    today   = date.today()
    weekday = today.weekday()   # 0=Mon, 1=Tue
    if weekday not in (0, 1):
        return

    current_hour = _current_ct_hour()
    if current_hour != 8:
        return

    day_label    = "Monday" if weekday == 0 else "Tuesday"
    pending_subs = load_pending_dm_submissions(target_week)
    if not pending_subs:
        print(f"\nDM Reminders ({day_label}): No pending_dm submissions — nothing to send.")
        return

    # Build store lookup
    store_lookup = {r["location_id"]: r for r in ref_data}

    # Group submissions by DM
    by_dm = {}
    for sub in pending_subs:
        loc_id  = sub["location_id"]
        store   = store_lookup.get(loc_id, {})
        dm_name = store.get("dm", "")
        if not dm_name:
            continue
        by_dm.setdefault(dm_name, []).append({
            "store_name":    store.get("store_name", loc_id),
            "current_band":  store.get("revenue_band", "N/A"),
            "selected_band": sub.get("selected_band", "N/A"),
            "location_id":   loc_id,
            "token":         gm_contacts.get(loc_id, {}).get("token", ""),
        })

    print(f"\nDM Reminders ({day_label}): {len(by_dm)} DM(s) with pending approvals")

    sent = failed = 0
    for dm_name, stores in by_dm.items():
        dm_email = dm_emails.get(dm_name, "")
        if not dm_email:
            print(f"  SKIP  {dm_name} \u2014 no email on file")
            continue

        # Use first pending store's token to build DM portal link
        token         = next((s["token"] for s in stores if s["token"]), "")
        dm_portal_url = f"{GM_PORTAL_URL}?token={token}&role=dm" if token else GM_PORTAL_URL

        subject   = (f"Action Required: {len(stores)} Revenue Band Approval(s) Pending "
                     f"\u2014 {week_label}")
        html_body = build_dm_reminder_email(dm_name, stores, week_label, dm_portal_url)

        try:
            success = send_email(dm_email, subject, html_body)
            status  = "\u2713 Sent  " if success else "\u2717 Failed"
            print(f"  {status} {dm_name} \u2192 {dm_email} ({len(stores)} store(s))")
            loc_id_for_log = stores[0]["location_id"]
            log_email(target_week, loc_id_for_log, dm_email, subject,
                      f"dm_reminder_{day_label.lower()}", success,
                      recipient_type="dm")
            if success:
                sent += 1
            else:
                failed += 1
        except Exception as e:
            print(f"  ERROR  {dm_name} \u2192 {dm_email} | {e}")
            log_email(target_week, stores[0]["location_id"], dm_email, subject,
                      f"dm_reminder_{day_label.lower()}", False, str(e),
                      recipient_type="dm")
            failed += 1

    print(f"DM Reminders done. {sent} sent | {failed} failed")


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
        print("\u2713 All stores have submitted. No GM emails needed.")
        # DM reminders may still be needed — fall through
    else:
        print(f"{len(pending_stores)} store(s) pending...\n")

        sent_count   = 0
        failed_count = 0
        skipped      = 0
        # Sentiment synthesis observability counters
        sentiment_failures      = 0   # Claude API errored
        sentiment_missing_data  = 0   # No Tattle reviews for the store/week
        sentiment_no_api_key    = False

        for store in pending_stores:
            loc_id     = store["location_id"]
            store_name = store["store_name"]
            dm_name    = store.get("dm", "")

            gm       = gm_contacts.get(loc_id, {})
            gm_name  = gm.get("gm_name", "")
            gm_email = gm.get("email", "")

            if not gm_email:
                print(f"  SKIP  {store_name} ({loc_id}) \u2014 no GM email on file")
                skipped += 1
                continue

            # Determine the submission token for this (store, week).
            # Tokens MUST be unique per submission row — we generate a fresh one
            # if no submission exists yet. NEVER reuse gm_contacts.token for the
            # submission row (it's a persistent per-GM token that would collide
            # with prior weeks' submissions on the unique constraint).
            token = None
            try:
                existing = sb.table("rev_band_submissions").select("id,token").eq(
                    "location_id", loc_id
                ).eq("week_start", str(target_week)).execute()
                if existing.data:
                    # Submission already exists for this week — use its token
                    token = existing.data[0].get("token")
                else:
                    # Generate a fresh unique token and insert
                    import uuid
                    token = str(uuid.uuid4())
                    try:
                        sb.table("rev_band_submissions").insert({
                            "location_id": loc_id,
                            "week_start":  str(target_week),
                            "token":       token,
                            "status":      "pending_gm",
                        }).execute()
                    except Exception:
                        # Race condition — another run inserted first. Re-query.
                        retry = sb.table("rev_band_submissions").select("token").eq(
                            "location_id", loc_id
                        ).eq("week_start", str(target_week)).execute()
                        if retry.data and retry.data[0].get("token"):
                            token = retry.data[0]["token"]
            except Exception as e:
                print(f"  \u26a0 Could not create submission record for {store_name}: {e}")

            if not token:
                print(f"  SKIP  {store_name} ({loc_id}) \u2014 could not obtain submission token")
                skipped += 1
                continue

            portal_url = f"{GM_PORTAL_URL}?token={token}"
            subject    = build_subject(store_name, week_label, mode)

            perf     = load_store_performance(loc_id, target_week)

            # Fetch + synthesize 2-week guest sentiment
            # Saves the result to rev_band_submissions so the portal can display
            # the same data without making its own Claude calls.
            sentiment = {}
            if ANTHROPIC_KEY:
                reviews = fetch_weekly_store_reviews(loc_id, target_week, weeks_back=2)
                if reviews:
                    summary_text  = synthesize_weekly_sentiment(store_name, reviews, week_label)
                    negative_hours = compute_negative_hour_distribution(reviews, top_n=5)
                    if summary_text is None:
                        sentiment_failures += 1
                    else:
                        # Count negatives (score < 70) without pandas dependency
                        neg_count = 0
                        for _r in reviews:
                            try:
                                if float(_r.get("score") or 0) < 70:
                                    neg_count += 1
                            except (ValueError, TypeError):
                                pass
                        sentiment = {
                            "summary":         summary_text,
                            "negative_hours":  negative_hours,
                            "review_count":    len(reviews),
                            "negative_count":  neg_count,
                            "generated_at":    datetime.now(timezone.utc).isoformat(timespec="seconds"),
                        }
                        # Persist to DB so portal can read without its own Claude call
                        try:
                            sb.table("rev_band_submissions").update({
                                "sentiment_summary_data": sentiment,
                            }).eq("location_id", loc_id).eq(
                                "week_start", str(target_week)
                            ).execute()
                        except Exception as _se:
                            print(f"  [WARN] Could not save sentiment to DB for {store_name}: {_se}")
                else:
                    sentiment_missing_data += 1
            else:
                sentiment_no_api_key = True

            html_body  = build_email_html(store_name, gm_name, week_label, portal_url, mode,
                                          perf, sentiment)

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
                log_email(target_week, loc_id, gm_email, subject, mode, success,
                          recipient_type="gm")
                if success:
                    sent_count += 1
                else:
                    failed_count += 1
            except Exception as e:
                print(f"  ERROR  {store_name} \u2192 {gm_email} | {e}")
                log_email(target_week, loc_id, gm_email, subject, mode, False, str(e),
                          recipient_type="gm")
                failed_count += 1

        print(f"\nDone. {sent_count} sent | {failed_count} failed | {skipped} skipped (no email on file).")

        # Visibility alerts — these help ops catch data-quality problems
        total_attempted = sent_count + failed_count + skipped
        if total_attempted > 0:
            skipped_pct = skipped / total_attempted * 100
            if skipped_pct >= 50:
                print(f"\n\u26a0\ufe0f  CRITICAL: {skipped_pct:.0f}% of stores skipped \u2014 "
                      f"most GM emails are missing! Load GM contacts immediately.")
            elif skipped_pct >= 10:
                print(f"\n\u26a0\ufe0f  WARNING: {skipped_pct:.0f}% of stores skipped "
                      f"(missing GM emails). Consider loading missing contacts.")

        # Sentiment synthesis observability
        if sentiment_no_api_key:
            print("\n\u26a0\ufe0f  ANTHROPIC_KEY not set \u2014 no emails include sentiment summaries.")
        if sentiment_failures > 0:
            print(f"\n\u26a0\ufe0f  Sentiment synthesis failed for {sentiment_failures} store(s) "
                  f"(Claude API error). Emails were sent without sentiment.")
        if sentiment_missing_data > 0:
            print(f"\n\u2139\ufe0f  Sentiment skipped for {sentiment_missing_data} store(s) "
                  f"(no Tattle reviews for the week \u2014 expected for low-volume stores).")

        if failed_count > 0:
            sys.exit(1)

    # DM approval reminders — Mon/Tue 8AM CT, runs regardless of GM email status
    send_dm_reminders(target_week, week_label, ref_data, gm_contacts, dm_emails)


if __name__ == "__main__":
    main()
