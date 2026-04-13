"""
Tattle Sentiment Scoring Script
---------------------------------
Reads tattle_reviews rows that have comments but no sentiment scores,
runs each through Claude, and writes sentiment_themes + sentiment_summary
back to Supabase.

Run modes:
  --mode pending    Score only unscored reviews (default, safe to re-run)
  --mode backfill   Re-score all reviews (use to rebuild from scratch)
  --mode recent     Score only the last N days (use after weekly ingest)

Usage:
  python tattle_sentiment.py                          # pending (unscored only)
  python tattle_sentiment.py --mode backfill          # re-score everything
  python tattle_sentiment.py --mode recent --days 14  # last 14 days
"""

import os
import sys
import json
import time
import argparse
import requests
from datetime import date, timedelta, datetime, timezone

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SUPABASE_URL  = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY  = os.environ.get("SUPABASE_KEY", "")
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

CLAUDE_MODEL  = "claude-haiku-4-5-20251001"   # fast + cheap for bulk scoring
BATCH_SIZE    = 500                            # rows fetched from Supabase at once
REQUEST_DELAY = 13                             # seconds between Claude calls (5 req/min rate limit)

if not all([SUPABASE_URL, SUPABASE_KEY, ANTHROPIC_KEY]):
    print("ERROR: Missing required env vars.")
    print("  Required: SUPABASE_URL, SUPABASE_KEY, ANTHROPIC_API_KEY")
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


def sb_fetch_unscored(limit=BATCH_SIZE, offset=0):
    """Fetch reviews that have a comment but no sentiment score yet."""
    url = (f"{SUPABASE_URL}/rest/v1/tattle_reviews"
           f"?select=id,location_id,location_label,score,completed_time,"
           f"questionnaire_title,comment,snapshots"
           f"&comment=not.is.null"
           f"&sentiment_themes=is.null"
           f"&order=completed_time.desc")
    hdrs = {**sb_headers(),
            "Range-Unit": "items",
            "Range": f"{offset}-{offset + limit - 1}"}
    r = requests.get(url, headers=hdrs, timeout=30)
    if r.status_code in (200, 206):
        return r.json()
    print(f"  [WARN] Supabase fetch error {r.status_code}: {r.text[:200]}")
    return []


def sb_fetch_recent(days=7, limit=BATCH_SIZE, offset=0):
    """Fetch reviews from the last N days regardless of scoring status."""
    cutoff = (date.today() - timedelta(days=days)).isoformat()
    url = (f"{SUPABASE_URL}/rest/v1/tattle_reviews"
           f"?select=id,location_id,location_label,score,completed_time,"
           f"questionnaire_title,comment,snapshots"
           f"&comment=not.is.null"
           f"&completed_time=gte.{cutoff}T00:00:00"
           f"&order=completed_time.desc")
    hdrs = {**sb_headers(),
            "Range-Unit": "items",
            "Range": f"{offset}-{offset + limit - 1}"}
    r = requests.get(url, headers=hdrs, timeout=30)
    if r.status_code in (200, 206):
        return r.json()
    print(f"  [WARN] Supabase fetch error {r.status_code}: {r.text[:200]}")
    return []


def sb_fetch_all_with_comments(limit=BATCH_SIZE, offset=0):
    """Fetch all reviews with comments (for full backfill re-score)."""
    url = (f"{SUPABASE_URL}/rest/v1/tattle_reviews"
           f"?select=id,location_id,location_label,score,completed_time,"
           f"questionnaire_title,comment,snapshots"
           f"&comment=not.is.null"
           f"&order=completed_time.desc")
    hdrs = {**sb_headers(),
            "Range-Unit": "items",
            "Range": f"{offset}-{offset + limit - 1}"}
    r = requests.get(url, headers=hdrs, timeout=30)
    if r.status_code in (200, 206):
        return r.json()
    print(f"  [WARN] Supabase fetch error {r.status_code}: {r.text[:200]}")
    return []


def sb_update_sentiment(review_id, themes, summary):
    """Write sentiment_themes and sentiment_summary back to a single row."""
    url = (f"{SUPABASE_URL}/rest/v1/tattle_reviews"
           f"?id=eq.{review_id}")
    hdrs = {**sb_headers(), "Prefer": "return=minimal"}
    payload = {
        "sentiment_themes":  themes,   # JSONB — pass as dict
        "sentiment_summary": summary,
    }
    r = requests.patch(url, headers=hdrs,
                       data=json.dumps(payload), timeout=15)
    if r.status_code not in (200, 204):
        print(f"  [WARN] Supabase update error {r.status_code} for id={review_id}: "
              f"{r.text[:100]}")


# ---------------------------------------------------------------------------
# Claude sentiment scoring
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = """You are a restaurant guest experience analyst for Freddy's Frozen Custard & Steakburgers.

You will receive a guest survey response that may include an overall comment and/or per-category feedback.
Analyze the text and return a JSON object with exactly this structure:

{
  "overall": "positive" | "neutral" | "negative",
  "categories": {
    "<CategoryName>": "positive" | "neutral" | "negative"
  },
  "themes": ["<theme1>", "<theme2>", ...],
  "summary": "<one concise sentence summarizing the guest experience>"
}

Valid themes (use only from this list, include all that apply):
- food_quality       (taste, freshness, temperature of food)
- speed              (wait time, speed of service)
- accuracy           (order correctness, missing items)
- hospitality        (staff friendliness, greeting, service attitude)
- cleanliness        (tables, floors, restrooms, overall appearance)
- atmosphere         (noise level, ambiance, seating)
- value              (price vs. quality perception)
- wait_time          (lobby or drive-thru wait explicitly mentioned)
- management         (specific mention of manager or leadership)
- repeat_visit       (guest mentions returning or not returning)

Rules:
- Return ONLY the JSON object, no markdown, no explanation
- If a category has no comment text, omit it from "categories"
- Keep summary under 20 words
- Be objective — do not infer sentiment not supported by the text"""


def build_user_message(row):
    """Build the user message for Claude from a review row."""
    parts = []

    # Overall rating context
    score = row.get("score")
    cer   = row.get("cer") if "cer" in row else None
    if score:
        parts.append(f"Overall score: {score}/100")

    # Questionnaire type
    q_title = row.get("questionnaire_title", "")
    if q_title:
        parts.append(f"Survey type: {q_title}")

    # General comment (if any)
    comment = row.get("comment", "") or ""

    # Category snapshots
    snapshots = row.get("snapshots")
    if isinstance(snapshots, str):
        try:
            snapshots = json.loads(snapshots)
        except Exception:
            snapshots = None

    if snapshots:
        parts.append("\nCategory feedback:")
        for snap in snapshots:
            cat     = snap.get("category", "")
            rating  = snap.get("rating")
            text    = snap.get("comment") or ""
            if cat:
                line = f"  {cat}"
                if rating:
                    line += f" ({rating}/5)"
                if text:
                    line += f": {text}"
                parts.append(line)
    elif comment:
        parts.append(f"\nGuest comment: {comment}")

    return "\n".join(parts)


def score_review(row):
    """
    Send a single review to Claude and return (themes_dict, summary_str).
    Returns (None, None) on failure.
    """
    user_msg = build_user_message(row)
    if not user_msg.strip():
        return None, None

    payload = {
        "model":      CLAUDE_MODEL,
        "max_tokens": 300,
        "system":     SYSTEM_PROMPT,
        "messages":   [{"role": "user", "content": user_msg}],
    }
    headers = {
        "x-api-key":         ANTHROPIC_KEY,
        "anthropic-version": "2023-06-01",
        "content-type":      "application/json",
    }

    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers=headers,
            data=json.dumps(payload),
            timeout=30,
        )
        if r.status_code == 429:
            # Rate limited — wait and retry up to 3 times
            for attempt in range(1, 4):
                wait = 30 * attempt  # 30s, 60s, 90s
                print(f"  [RATE LIMIT] Waiting {wait}s before retry {attempt}/3...")
                time.sleep(wait)
                r = requests.post(
                    "https://api.anthropic.com/v1/messages",
                    headers=headers,
                    data=json.dumps(payload),
                    timeout=30,
                )
                if r.status_code != 429:
                    break
            if r.status_code == 429:
                print(f"  [WARN] Still rate limited after 3 retries, skipping id={row['id']}")
                return None, None

        if r.status_code != 200:
            print(f"  [WARN] Claude error {r.status_code}: {r.text[:150]}")
            return None, None

        content = r.json()["content"][0]["text"].strip()

        # Strip markdown fences if present
        if content.startswith("```"):
            content = content.split("```")[1]
            if content.startswith("json"):
                content = content[4:]

        result  = json.loads(content)
        themes  = {
            "overall":    result.get("overall"),
            "categories": result.get("categories", {}),
            "themes":     result.get("themes", []),
        }
        summary = result.get("summary", "")
        return themes, summary

    except json.JSONDecodeError as e:
        print(f"  [WARN] Claude JSON parse error for id={row['id']}: {e}")
        return None, None
    except Exception as e:
        print(f"  [WARN] Claude call failed for id={row['id']}: {e}")
        return None, None


# ---------------------------------------------------------------------------
# Main scoring loop
# ---------------------------------------------------------------------------
def score_all(fetch_fn, label=""):
    """
    Generic scoring loop — fetches batches using fetch_fn, scores each row,
    writes results back to Supabase.
    """
    total_scored  = 0
    total_skipped = 0
    total_failed  = 0
    offset        = 0

    while True:
        batch = fetch_fn(limit=BATCH_SIZE, offset=offset)
        if not batch:
            break

        print(f"  Processing batch of {len(batch)} reviews "
              f"(offset {offset})...")

        for row in batch:
            themes, summary = score_review(row)
            time.sleep(REQUEST_DELAY)

            if themes is None:
                total_failed += 1
                continue

            sb_update_sentiment(row["id"], themes, summary)
            total_scored += 1

            if total_scored % 100 == 0:
                print(f"    {total_scored} reviews scored so far...")

        if len(batch) < BATCH_SIZE:
            break
        offset += BATCH_SIZE

    print(f"\n✓ Done{' — ' + label if label else ''}.")
    print(f"  Scored  : {total_scored}")
    print(f"  Failed  : {total_failed}")
    print(f"  Skipped : {total_skipped}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Tattle sentiment scoring")
    parser.add_argument("--mode", default="pending",
                        choices=["pending", "backfill", "recent"])
    parser.add_argument("--days", type=int, default=14,
                        help="Days back for --mode recent (default: 14)")
    args = parser.parse_args()

    print(f"\nTattle Sentiment Scoring — {args.mode.upper()} mode")
    print("=" * 60)

    if args.mode == "pending":
        print("Scoring all unscored reviews with comments...\n")
        score_all(sb_fetch_unscored, label="pending")

    elif args.mode == "backfill":
        print("Re-scoring ALL reviews with comments...\n")
        score_all(sb_fetch_all_with_comments, label="backfill")

    elif args.mode == "recent":
        print(f"Scoring reviews from last {args.days} days...\n")
        score_all(
            lambda limit, offset: sb_fetch_recent(
                days=args.days, limit=limit, offset=offset
            ),
            label=f"recent {args.days}d",
        )


if __name__ == "__main__":
    main()
