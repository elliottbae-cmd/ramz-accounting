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

TATTLE_API_BASE  = "https://api.tattleapp.io"
TATTLE_API_V2    = "https://gettattle.com/v2/api"

# Rate limiting — be friendly to the API
REQUEST_DELAY = 0.25   # seconds between comment fetches

if not all([SUPABASE_URL, SUPABASE_KEY, TATTLE_CLIENT_KEY, TATTLE_SECRET_KEY]):
    print("ERROR: Missing required env vars.")
    print("  Required: SUPABASE_URL, SUPABASE_KEY, TATTLE_CLIENT_KEY, TATTLE_SECRET_KEY")
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
    Authenticate with Tattle using client key + secret.
    Tries multiple common OAuth2 endpoint patterns.
    Returns Bearer token string or raises on failure.
    """
    payload = {
        "client_id":     TATTLE_CLIENT_KEY,
        "client_secret": TATTLE_SECRET_KEY,
        "grant_type":    "client_credentials",
    }
    endpoints = [
        f"{TATTLE_API_BASE}/oauth/token",
        f"{TATTLE_API_BASE}/v2/api/oauth/token",
        f"{TATTLE_API_BASE}/v3/api/oauth/token",
        f"{TATTLE_API_V2}/oauth/token",
    ]
    for url in endpoints:
        try:
            r = requests.post(url, json=payload, timeout=15)
            if r.status_code == 200:
                data = r.json()
                token = data.get("access_token") or data.get("token")
                if token:
                    print(f"  ✓ Authenticated via {url}")
                    return token
        except Exception:
            continue

    # If OAuth fails, try basic auth header pattern
    import base64
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
            if r.status_code == 200:
                data = r.json()
                token = data.get("access_token") or data.get("token")
                if token:
                    print(f"  ✓ Authenticated (Basic) via {url}")
                    return token
        except Exception:
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

        if r.status_code != 200:
            print(f"  [WARN] Surveys fetch failed: {r.status_code} {r.text[:200]}")
            break

        data = r.json()
        batch = data.get("data", [])
        all_surveys.extend(batch)

        total = data.get("total", 0)
        has_next = data.get("hasNextPage", False)
        print(f"    Page {page}: {len(batch)} surveys (total: {total})")

        if not has_next or len(batch) < page_size:
            break
        page += 1
        time.sleep(0.5)

    return all_surveys


def fetch_comment(token, survey_id):
    """
    Fetch the text comment for a single survey response.
    Returns comment string or None if no comment.
    """
    url = f"{TATTLE_API_V2}/customer-questionnaire-comment"
    params = {
        "customer_questionnaire_id": survey_id,
        "expand": "user",
    }
    try:
        r = requests.get(
            url,
            headers=tattle_headers(token),
            params=params,
            timeout=15,
        )
        if r.status_code != 200:
            return None
        data = r.json()
        comments = (data.get("_embedded", {})
                        .get("customer_questionnaire_comment", []))
        if not comments:
            return None
        # Collect all comment text fields
        texts = []
        for c in comments:
            text = c.get("comment") or c.get("text") or c.get("body") or ""
            if text.strip():
                texts.append(text.strip())
        return " | ".join(texts) if texts else None
    except Exception as e:
        print(f"    [WARN] Comment fetch failed for survey {survey_id}: {e}")
        return None


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

    # Skip already-ingested surveys
    new_surveys = [s for s in ram_z_surveys if s["id"] not in existing_ids]
    print(f"  {len(new_surveys)} new surveys to ingest")

    if not new_surveys:
        print("\n✓ Nothing new to ingest.")
        return

    # Fetch comments and build rows
    rows = []
    ingested_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    print(f"\nFetching comments for {len(new_surveys)} surveys...")
    for i, survey in enumerate(new_surveys, 1):
        survey_id = survey["id"]

        if i % 50 == 0 or i == len(new_surveys):
            print(f"  {i}/{len(new_surveys)} surveys processed...")

        comment = fetch_comment(token, survey_id)
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
            "sentiment_themes":    None,
            "sentiment_summary":   None,
            "ingested_at":         ingested_at,
        }
        rows.append(row)

    # Upsert to Supabase
    print(f"\nUpserting {len(rows)} rows to Supabase...")
    sb_upsert("tattle_reviews", rows)

    with_comments = sum(1 for r in rows if r["comment"])
    print(f"\n✓ Done.")
    print(f"  Ingested : {len(rows)} surveys")
    print(f"  With comments : {with_comments}")
    print(f"  Without comments: {len(rows) - with_comments}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Tattle review ingestion")
    parser.add_argument("--mode",  default="weekly",
                        choices=["weekly", "backfill", "custom"])
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
    elif args.mode == "custom":
        if not args.start or not args.end:
            print("ERROR: --mode custom requires --start and --end")
            sys.exit(1)
        start_date = date.fromisoformat(args.start)
        end_date   = date.fromisoformat(args.end)

    ingest(start_date, end_date, mode=args.mode)


if __name__ == "__main__":
    main()
