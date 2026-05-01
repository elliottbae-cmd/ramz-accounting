"""
Tattle Review Ingestion Script
--------------------------------
Pulls guest feedback (ratings + comments) from the Tattle API and stores
them in Supabase for sentiment analysis and reporting.

Run modes:
  --mode weekly     Pull last 7 days only (default, runs via GitHub Actions)
  --mode backfill   Pull all historical data from Tattle (run once to seed)
  --mode custom     Pull a specific date range (use with --start and --end)

Usage:
  python tattle_ingest.py                           # weekly (last 7 days)
  python tattle_ingest.py --mode backfill           # all history
  python tattle_ingest.py --mode custom --start 2026-01-01 --end 2026-04-12
"""

import os
import sys
import json
import time
import argparse
import pathlib
import requests
from datetime import date, timedelta, datetime, timezone

# Sentry — best-effort error reporting; no-op without SENTRY_DSN
try:
    from sentry_init import init_sentry
    init_sentry("tattle_ingest")
except Exception:
    pass

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
_HERE = pathlib.Path(__file__).resolve().parent
_ROOT = _HERE

SUPABASE_URL     = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY     = os.environ.get("SUPABASE_KEY", "")
TATTLE_CLIENT_KEY = os.environ.get("TATTLE_CLIENT_KEY", "")
TATTLE_SECRET_KEY = os.environ.get("TATTLE_SECRET_KEY", "")
TATTLE_MERCHANT_ID = "2668"

# Temporary hardcoded session token for backfill testing.
# Replace with proper OAuth once Tattle provides the token endpoint.
TATTLE_SESSION_TOKEN = os.environ.get(
    "TATTLE_SESSION_TOKEN",
    "dc4d756ead3e874cd003542768cf1924b06e51f4"
)

TATTLE_API_BASE  = "https://api.tattleapp.io"
TATTLE_API_V2    = "https://gettattle.com/v2/api"

# Rate limiting — be friendly to the API
REQUEST_DELAY = 0.25   # seconds between comment fetches

if not all([SUPABASE_URL, SUPABASE_KEY]):
    print("ERROR: Missing required env vars.")
    print("  Required: SUPABASE_URL, SUPABASE_KEY")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Supabase helpers
# ---------------------------------------------------------------------------
def sb_headers():
    return {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "resolution=merge-duplicates,return=minimal",
    }


def sb_upsert(table, rows, chunk=500):
    """Upsert rows into Supabase in chunks."""
    if not rows:
        return
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    for i in range(0, len(rows), chunk):
        r = requests.post(
            url,
            headers=sb_headers(),
            data=json.dumps(rows[i:i + chunk]),
            timeout=30,
        )
        if r.status_code not in (200, 201):
            print(f"  [WARN] Supabase upsert error {r.status_code}: {r.text[:200]}")


def sb_get_existing_ids(start_date, end_date):
    """Return set of Tattle survey IDs already in Supabase for the date range."""
    url = (f"{SUPABASE_URL}/rest/v1/tattle_reviews"
           f"?select=id"
           f"&completed_time=gte.{start_date}T00:00:00"
           f"&completed_time=lte.{end_date}T23:59:59")
    hdrs = {**sb_headers(), "Range-Unit": "items", "Range": "0-9999"}
    r = requests.get(url, headers=hdrs, timeout=30)
    if r.status_code == 200:
        return {row["id"] for row in r.json()}
    return set()


def sb_get_all_existing_ids():
    """Return all Tattle survey IDs already in Supabase (for backfill deduplication)."""
    existing = set()
    offset = 0
    while True:
        url = f"{SUPABASE_URL}/rest/v1/tattle_reviews?select=id"
        hdrs = {**sb_headers(), "Range-Unit": "items", "Range": f"{offset}-{offset+999}"}
        r = requests.get(url, headers=hdrs, timeout=30)
        if r.status_code != 200:
            break
        batch = r.json()
        if not batch:
            break
        existing.update(row["id"] for row in batch)
        if len(batch) < 1000:
            break
        offset += 1000
    return existing


# ---------------------------------------------------------------------------
# Tattle authentication
# ---------------------------------------------------------------------------
def get_tattle_token():
    """
    Returns Tattle Bearer token.
    Currently uses a hardcoded session token grabbed from DevTools.
    TODO: Replace with proper OAuth2 once Tattle provides the token endpoint.
    """
    print("  Using session token (temporary — replace with OAuth once endpoint is known).")
    return TATTLE_SESSION_TOKEN


def _get_tattle_token_via_oauth():
    """
    Authenticate with Tattle using client key + secret.
    Tries multiple common OAuth2 endpoint patterns.
    Returns Bearer token string or raises on failure.
    (Unused until Tattle confirms the correct OAuth endpoint.)
    """
    import base64

    endpoints = [
        f"{TATTLE_API_BASE}/oauth/token",
        f"{TATTLE_API_BASE}/v2/api/oauth/token",
        f"{TATTLE_API_BASE}/v3/api/oauth/token",
        f"{TATTLE_API_V2}/oauth/token",
        "https://app.tattleapp.io/oauth/token",
        "https://auth.tattleapp.io/oauth/token",
        "https://gettattle.com/oauth/token",
        "https://gettattle.com/v3/api/oauth/token",
    ]

    # Attempt 1: JSON body with client_id / client_secret
    payload_json = {
        "client_id":     TATTLE_CLIENT_KEY,
        "client_secret": TATTLE_SECRET_KEY,
        "grant_type":    "client_credentials",
    }
    for url in endpoints:
        try:
            r = requests.post(url, json=payload_json, timeout=15)
            print(f"  [DEBUG] POST {url} → {r.status_code} {r.text[:120]}")
            if r.status_code == 200:
                data = r.json()
                token = data.get("access_token") or data.get("token")
                if token:
                    print(f"  ✓ Authenticated via {url}")
                    return token
        except Exception as e:
            print(f"  [DEBUG] {url} exception: {e}")
            continue

    # Attempt 2: Basic auth header + form body
    creds = base64.b64encode(
        f"{TATTLE_CLIENT_KEY}:{TATTLE_SECRET_KEY}".encode()
    ).decode()
    for url in endpoints:
        try:
            r = requests.post(
                url,
                headers={"Authorization": f"Basic {creds}",
                         "Content-Type": "application/x-www-form-urlencoded"},
                data="grant_type=client_credentials",
                timeout=15,
            )
            print(f"  [DEBUG] Basic POST {url} → {r.status_code} {r.text[:120]}")
            if r.status_code == 200:
                data = r.json()
                token = data.get("access_token") or data.get("token")
                if token:
                    print(f"  ✓ Authenticated (Basic) via {url}")
                    return token
        except Exception as e:
            print(f"  [DEBUG] Basic {url} exception: {e}")
            continue

    # Attempt 3: form body with client_key / client_secret field names
    payload_form = {
        "client_key":    TATTLE_CLIENT_KEY,
        "client_secret": TATTLE_SECRET_KEY,
        "grant_type":    "client_credentials",
    }
    for url in endpoints:
        try:
            r = requests.post(url, data=payload_form, timeout=15)
            print(f"  [DEBUG] Form POST {url} → {r.status_code} {r.text[:120]}")
            if r.status_code == 200:
                data = r.json()
                token = (data.get("access_token") or data.get("token")
                         or data.get("api_token"))
                if token:
                    print(f"  ✓ Authenticated (form/client_key) via {url}")
                    return token
        except Exception as e:
            print(f"  [DEBUG] Form {url} exception: {e}")
            continue

    raise RuntimeError(
        "Could not authenticate with Tattle. "
        "Check TATTLE_CLIENT_KEY and TATTLE_SECRET_KEY, "
        "or contact customersuccess@gettattle.com for OAuth endpoint details."
    )


def tattle_headers(token):
    return {
        "Authorization":   f"Bearer {token}",
        "X-Tattle-Merchant": TATTLE_MERCHANT_ID,
        "Content-Type":    "application/json",
        "Accept":          "application/json",
    }


# ---------------------------------------------------------------------------
# Tattle data fetching
# ---------------------------------------------------------------------------
def fetch_surveys(token, start_date, end_date):
    """
    Fetch all survey responses for the given date range.
    Returns list of survey dicts.
    """
    url = (f"{TATTLE_API_BASE}/v3/api/merchants/{TATTLE_MERCHANT_ID}"
           f"/Surveys/list")
    all_surveys = []
    page = 1
    page_size = 1000

    print(f"  Fetching surveys {start_date} → {end_date}...")

    while True:
        payload = {
            "sort":          "CompletedTime",
            "sortDirection": "Descending",
            "startDate":     str(start_date),
            "endDate":       str(end_date),
        }
        params = {"Page": page, "PageSize": page_size}

        r = requests.post(
            url,
            headers=tattle_headers(token),
            json=payload,
            params=params,
            timeout=60,
        )

        if r.status_code in (401, 403):
            print("=" * 60)
            print("  [FATAL] Tattle session token has EXPIRED or is invalid.")
            print("  ACTION REQUIRED: Log into gettattle.com, open Chrome DevTools,")
            print("  go to Network tab, click any API call, copy the Authorization")
            print("  header value, and update TATTLE_SESSION_TOKEN in GitHub Secrets.")
            print("  Long-term fix: contact customersuccess@gettattle.com for OAuth endpoint.")
            print("=" * 60)
            sys.exit(1)
        if r.status_code != 200:
            print(f"  [WARN] Surveys fetch failed: {r.status_code} {r.text[:200]}")
            break

        data = r.json()
        batch = data.get("data", [])

        # Log all available fields from first survey (one-time debug)
        if page == 1 and batch:
            print(f"\n  [DEBUG] All fields in survey object:")
            for key in sorted(batch[0].keys()):
                val = batch[0][key]
                preview = str(val)[:80] if val is not None else "null"
                print(f"    {key}: {preview}")
            print()

        all_surveys.extend(batch)

        total = data.get("total", 0)
        has_next = data.get("hasNextPage", False)
        print(f"    Page {page}: {len(batch)} surveys (total: {total})")

        if not has_next or len(batch) < page_size:
            break
        page += 1
        time.sleep(0.5)

    return all_surveys


def fetch_snapshots(token, survey_id):
    """
    Fetch category-level snapshots (ratings + comments) for a single survey.
    Each survey has multiple snapshots — one per category (Food Quality,
    Hospitality, Speed of Service, Accuracy, Cleanliness, Atmosphere, etc.)

    Returns:
        snapshots  — list of dicts with category label, rating, comment
        comment    — all category comments concatenated into one string
    """
    url = f"{TATTLE_API_V2}/customer-questionnaire-snapshots"
    params = {
        "customer_questionnaire_id": survey_id,
        "expand": "snapshot,customer_questionnaire,questionnaire,questionnaire_snapshots",
        "order":  "sort_ord ASC, date_time_created ASC",
        "notNull": "customer_questionnaire_snapshots.rating",
    }
    try:
        r = requests.get(
            url,
            headers=tattle_headers(token),
            params=params,
            timeout=15,
        )
        if r.status_code != 200:
            return None, None

        data     = r.json()
        items    = (data.get("_embedded", {})
                       .get("customer_questionnaire_snapshots", []))
        if not items:
            return None, None

        snapshots = []
        texts     = []
        for item in items:
            snap   = item.get("snapshot", {})
            label  = snap.get("label", "")
            rating = item.get("rating")
            text   = (item.get("comment") or "").strip()
            snapshots.append({
                "category": label,
                "rating":   rating,
                "comment":  text or None,
            })
            if text:
                texts.append(f"[{label}] {text}")

        combined_comment = " | ".join(texts) if texts else None
        return snapshots, combined_comment

    except Exception as e:
        print(f"    [WARN] Snapshot fetch failed for survey {survey_id}: {e}")
        return None, None


# ---------------------------------------------------------------------------
# Main ingestion logic
# ---------------------------------------------------------------------------
def ingest(start_date, end_date, mode="weekly"):
    print(f"\nTattle Ingestion — {mode.upper()} mode")
    print(f"Date range: {start_date} → {end_date}")
    print("=" * 60)

    # Authenticate
    print("\nAuthenticating with Tattle...")
    token = get_tattle_token()

    # Load existing IDs to avoid re-fetching comments we already have
    # In "refresh" mode, we re-process ALL surveys to backfill new fields
    if mode == "refresh":
        print("\nRefresh mode — will re-process ALL surveys to update fields...")
        existing_ids = set()  # Don't skip any
    else:
        print("\nLoading existing survey IDs from Supabase...")
        if mode == "backfill":
            existing_ids = sb_get_all_existing_ids()
        else:
            existing_ids = sb_get_existing_ids(start_date, end_date)
        print(f"  {len(existing_ids)} surveys already in Supabase")

    # Fetch surveys from Tattle
    surveys = fetch_surveys(token, start_date, end_date)

    # Filter to our Ram-Z locations (locationExternalId starts with "112-")
    ram_z_surveys = [
        s for s in surveys
        if str(s.get("locationExternalId", "")).startswith("112-")
    ]
    print(f"\n  {len(surveys)} total surveys → {len(ram_z_surveys)} Ram-Z surveys")

    if mode == "refresh":
        # In refresh mode, update survey-level fields without re-fetching snapshots
        print(f"\n  Updating {len(ram_z_surveys)} surveys with new fields (no snapshot re-fetch)...")
        ingested_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        update_rows = []
        for survey in ram_z_surveys:
            update_rows.append({
                "id":                  survey["id"],
                # Core fields (needed if row doesn't exist yet)
                "location_id":         survey.get("locationExternalId", ""),
                "location_label":      survey.get("locationLabel", ""),
                "score":               survey.get("score"),
                "cer":                 survey.get("cer"),
                "completed_time":      survey.get("completedTime"),
                "experienced_time":    survey.get("experiencedTime"),
                "day_part_label":      survey.get("dayPartLabel", ""),
                "channel_label":       survey.get("channelLabel", ""),
                "questionnaire_id":    survey.get("questionnaireId"),
                "questionnaire_title": survey.get("questionnaireTitle", ""),
                # New customer & engagement fields
                "customer_email":      survey.get("customerEmail"),
                "share_email":         survey.get("shareEmail"),
                "customer_id":         survey.get("customerId"),
                "customer_first_name": survey.get("customerFirstName") or None,
                "customer_last_name":  survey.get("customerLastName") or None,
                "incident_id":         survey.get("incidentId"),
                "message_count":       survey.get("messageCount", 0),
                "has_unread_messages": survey.get("hasUnreadMessages", False),
                "tag_labels":          json.dumps(survey.get("tagLabels", [])),
                "reward_redeemed":     survey.get("rewardRedeemedAmount", 0.0),
            })
            if len(update_rows) >= 500:
                sb_upsert("tattle_reviews", update_rows)
                print(f"    {len(update_rows)} rows upserted...")
                update_rows = []
        if update_rows:
            sb_upsert("tattle_reviews", update_rows)
        print(f"\n✓ Refresh complete — {len(ram_z_surveys)} surveys updated with new fields.")
        return

    # Skip already-ingested surveys
    new_surveys = [s for s in ram_z_surveys if s["id"] not in existing_ids]
    print(f"  {len(new_surveys)} new surveys to ingest")

    if not new_surveys:
        print("\n✓ Nothing new to ingest.")
        return

    # Fetch snapshots and build rows
    rows = []
    ingested_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    print(f"\nFetching snapshots for {len(new_surveys)} surveys...")
    for i, survey in enumerate(new_surveys, 1):
        survey_id = survey["id"]

        if i % 50 == 0 or i == len(new_surveys):
            print(f"  {i}/{len(new_surveys)} surveys processed...")

        snapshots, comment = fetch_snapshots(token, survey_id)
        time.sleep(REQUEST_DELAY)

        row = {
            "id":                  survey_id,
            "location_id":         survey.get("locationExternalId", ""),
            "location_label":      survey.get("locationLabel", ""),
            "questionnaire_id":    survey.get("questionnaireId"),
            "questionnaire_title": survey.get("questionnaireTitle", ""),
            "cer":                 survey.get("cer"),
            "score":               survey.get("score"),
            "completed_time":      survey.get("completedTime"),
            "experienced_time":    survey.get("experiencedTime"),
            "day_part_label":      survey.get("dayPartLabel", ""),
            "channel_label":       survey.get("channelLabel", ""),
            "comment":             comment,
            "snapshots":           json.dumps(snapshots) if snapshots else None,
            "sentiment_themes":    None,
            "sentiment_summary":   None,
            "ingested_at":         ingested_at,
            # Customer & engagement fields
            "customer_email":      survey.get("customerEmail"),
            "share_email":         survey.get("shareEmail"),
            "customer_id":         survey.get("customerId"),
            "customer_first_name": survey.get("customerFirstName") or None,
            "customer_last_name":  survey.get("customerLastName") or None,
            "incident_id":         survey.get("incidentId"),
            "message_count":       survey.get("messageCount", 0),
            "has_unread_messages": survey.get("hasUnreadMessages", False),
            "tag_labels":          json.dumps(survey.get("tagLabels", [])),
            "reward_redeemed":     survey.get("rewardRedeemedAmount", 0.0),
        }
        rows.append(row)

    # Upsert to Supabase
    print(f"\nUpserting {len(rows)} rows to Supabase...")
    sb_upsert("tattle_reviews", rows)

    with_comments = sum(1 for r in rows if r["comment"])
    print(f"\n✓ Done.")
    print(f"  Ingested  : {len(rows)} surveys")
    print(f"  With comments : {with_comments}")
    print(f"  Without comments: {len(rows) - with_comments}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Tattle review ingestion")
    parser.add_argument("--mode",  default="weekly",
                        choices=["weekly", "backfill", "custom", "refresh"])
    parser.add_argument("--start", help="Start date YYYY-MM-DD (custom mode)")
    parser.add_argument("--end",   help="End date YYYY-MM-DD (custom mode)")
    args = parser.parse_args()

    today = date.today()

    if args.mode == "weekly":
        start_date = today - timedelta(days=7)
        end_date   = today
    elif args.mode == "backfill":
        start_date = date(2023, 1, 1)   # Pull everything from 2023 onward
        end_date   = today
    elif args.mode == "refresh":
        start_date = date(2023, 1, 1)   # Re-fetch all to update fields
        end_date   = today
    elif args.mode == "custom":
        if not args.start or not args.end:
            print("ERROR: --mode custom requires --start and --end")
            sys.exit(1)
        start_date = date.fromisoformat(args.start)
        end_date   = date.fromisoformat(args.end)

    ingest(start_date, end_date, mode=args.mode)


if __name__ == "__main__":
    main()
