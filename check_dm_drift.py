"""
One-shot diagnostic: compare DM in weekly_locks (status='locked') to live
reference_data, for the current week and any future weeks. Prints any drift
so you can decide whether to manually backfill before the fix takes effect.

Past weeks are intentionally NOT compared — they're supposed to preserve
historical DM assignments.

Run: python check_dm_drift.py
"""
import os
import pathlib
from datetime import date, timedelta
from supabase import create_client


def _load_credentials():
    env_url = (os.environ.get("SUPABASE_URL") or "").strip()
    env_key = (os.environ.get("SUPABASE_KEY") or "").strip()
    if env_url and env_key:
        return env_url, env_key
    secrets_path = pathlib.Path(__file__).resolve().parent / ".streamlit" / "secrets.toml"
    import toml
    s = toml.load(str(secrets_path))
    return s["supabase"]["url"], s["supabase"]["key"]


def _current_week_start():
    """Return the Thursday that starts the current week."""
    today = date.today()
    days_since_thursday = (today.weekday() - 3) % 7
    return today - timedelta(days=days_since_thursday)


def main():
    url, key = _load_credentials()
    sb = create_client(url, key)

    current = _current_week_start()
    print(f"Today: {date.today()}    Current week start: {current}\n")

    # Live ref_data → location_id: dm
    ref_resp = sb.table("reference_data").select(
        "location_id,store_name,dm,active"
    ).eq("active", True).execute()
    live_dm = {r["location_id"]: r.get("dm", "") for r in (ref_resp.data or [])}
    live_name = {r["location_id"]: r.get("store_name", "") for r in (ref_resp.data or [])}

    # Locked rows for current + future weeks
    locks_resp = sb.table("weekly_locks").select(
        "week_start,location_id,store_name,dm,status,source"
    ).gte("week_start", str(current)).eq("status", "locked").order("week_start").execute()
    locks = locks_resp.data or []

    if not locks:
        print("No locked weeks at or after the current week.")
        print("Future-week DMs will be set correctly when they auto-lock.")
        return

    by_week = {}
    for r in locks:
        by_week.setdefault(r["week_start"], []).append(r)

    total_drift = 0
    for week, rows in by_week.items():
        drift = []
        for r in rows:
            lid = r["location_id"]
            locked_dm = (r.get("dm") or "").strip()
            current_dm = (live_dm.get(lid) or "").strip()
            if lid in live_dm and locked_dm != current_dm:
                drift.append((lid, live_name.get(lid, r.get("store_name") or ""),
                              locked_dm, current_dm))

        if drift:
            print(f"Week {week}  ({rows[0].get('source','?')}) — {len(drift)} drift(s):")
            for lid, name, old, new in drift:
                print(f"  {lid}  {name:<30}  locked='{old}'  live='{new}'")
            total_drift += len(drift)
        else:
            print(f"Week {week}  ({rows[0].get('source','?')}) — clean")

    print()
    if total_drift:
        print(f"TOTAL DRIFT: {total_drift} store-week(s) with stale DM in locked config.")
        print()
        print("To backfill (updates locked DMs for current + future weeks ONLY,")
        print("leaving past history untouched):")
        print()
        print("  UPDATE weekly_locks wl")
        print("  SET dm = rd.dm")
        print("  FROM reference_data rd")
        print("  WHERE wl.location_id = rd.location_id")
        print(f"    AND wl.week_start >= '{current}'")
        print("    AND wl.status = 'locked';")
    else:
        print("No drift. The carry-forward fix will keep things accurate going forward.")


if __name__ == "__main__":
    main()
