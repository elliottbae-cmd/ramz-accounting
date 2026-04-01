"""
Supabase Data Access Layer
--------------------------
All database read/write operations for the ramz-accounting app.
Replaces CSV file operations with Supabase PostgreSQL calls.
"""

import logging
from datetime import datetime, timezone

import pandas as pd
import streamlit as st
from supabase import create_client

logger = logging.getLogger(__name__)


@st.cache_resource
def get_supabase():
    """Initialize and cache the Supabase client."""
    url = st.secrets["supabase"]["url"]
    key = st.secrets["supabase"]["key"]
    return create_client(url, key)


# ---------------------------------------------------------------------------
# Reference data (stores, DMs, bands, goals)
# ---------------------------------------------------------------------------
def load_stores():
    """Load store master list. Returns DataFrame with location_id, store_name."""
    sb = get_supabase()
    resp = sb.table("stores").select("*").order("location_id").execute()
    if resp.data:
        return pd.DataFrame(resp.data)
    return pd.DataFrame(columns=["location_id", "store_name"])


def save_store(location_id, store_name):
    """Insert or update a single store."""
    try:
        sb = get_supabase()
        sb.table("stores").upsert(
            {"location_id": location_id, "store_name": store_name}
        ).execute()
    except Exception as e:
        logger.error(f"Failed to save store {location_id}: {e}")
        raise


def delete_store(location_id):
    """Remove a store from the stores table."""
    try:
        sb = get_supabase()
        sb.table("stores").delete().eq("location_id", location_id).execute()
    except Exception as e:
        logger.error(f"Failed to delete store {location_id}: {e}")
        raise


def load_reference_data():
    """Load store reference data. Returns DataFrame with location_id, store_name, dm, revenue_band."""
    sb = get_supabase()
    resp = sb.table("reference_data").select("*").order("location_id").execute()
    if resp.data:
        return pd.DataFrame(resp.data)
    return pd.DataFrame(columns=["location_id", "store_name", "dm", "revenue_band"])


def save_reference_data_row(location_id, store_name, dm, revenue_band):
    """Insert or update a single reference data row."""
    try:
        sb = get_supabase()
        sb.table("reference_data").upsert({
            "location_id": location_id,
            "store_name": store_name,
            "dm": dm,
            "revenue_band": revenue_band,
        }).execute()
    except Exception as e:
        logger.error(f"Failed to save reference data for {location_id}: {e}")
        raise


def save_reference_data_bulk(df):
    """Upsert all rows from a DataFrame into reference_data."""
    try:
        sb = get_supabase()
        records = df[["location_id", "store_name", "dm", "revenue_band"]].to_dict("records")
        sb.table("reference_data").upsert(records).execute()
    except Exception as e:
        logger.error(f"Failed to bulk save reference data: {e}")
        raise


def delete_reference_data(location_id):
    """Remove a store from reference_data."""
    try:
        sb = get_supabase()
        sb.table("reference_data").delete().eq("location_id", location_id).execute()
    except Exception as e:
        logger.error(f"Failed to delete reference data for {location_id}: {e}")
        raise


def load_band_goals():
    """Load revenue band → hourly goal mapping. Returns a dict."""
    sb = get_supabase()
    resp = sb.table("band_goals").select("*").execute()
    if resp.data:
        return {r["revenue_band"]: float(r["hourly_goal"]) for r in resp.data}
    return {}


def save_band_goals(goals_dict):
    """Save all band goals. goals_dict: {revenue_band: hourly_goal}."""
    try:
        sb = get_supabase()
        records = [
            {"revenue_band": band, "hourly_goal": goal}
            for band, goal in goals_dict.items()
        ]
        sb.table("band_goals").upsert(records).execute()
    except Exception as e:
        logger.error(f"Failed to save band goals: {e}")
        raise


def load_dm_list():
    """Load list of DM names. Returns sorted list of strings."""
    sb = get_supabase()
    resp = sb.table("dm_list").select("dm_name").order("dm_name").execute()
    if resp.data:
        return [r["dm_name"] for r in resp.data]
    return []


def add_dm(dm_name):
    """Add a DM to the list."""
    try:
        sb = get_supabase()
        sb.table("dm_list").upsert({"dm_name": dm_name}).execute()
    except Exception as e:
        logger.error(f"Failed to add DM {dm_name}: {e}")
        raise


def remove_dm(dm_name):
    """Remove a DM from the list."""
    try:
        sb = get_supabase()
        sb.table("dm_list").delete().eq("dm_name", dm_name).execute()
    except Exception as e:
        logger.error(f"Failed to remove DM {dm_name}: {e}")
        raise


# ---------------------------------------------------------------------------
# Weekly locks
# ---------------------------------------------------------------------------
def load_all_locks(weeks_back=52):
    """Load recent weekly lock data (locked only, last N weeks). Returns a DataFrame."""
    from datetime import date as _date, timedelta as _td
    sb = get_supabase()
    cutoff = str(_date.today() - _td(days=weeks_back * 7))
    resp = sb.table("weekly_locks").select(
        "week_start, location_id, store_name, dm, revenue_band, hourly_goal, source, status"
    ).eq("status", "locked").gte("week_start", cutoff).order("week_start").execute()
    if resp.data:
        df = pd.DataFrame(resp.data)
        df["hourly_goal"] = pd.to_numeric(df["hourly_goal"], errors="coerce").fillna(0)
        df["week_start"] = df["week_start"].astype(str)
        return df
    return pd.DataFrame(columns=[
        "week_start", "location_id", "store_name", "dm",
        "revenue_band", "hourly_goal", "source", "status",
    ])


def load_locked_config(week_start):
    """Load locked config for a specific week. Returns DataFrame or None."""
    sb = get_supabase()
    week_str = str(week_start)
    resp = sb.table("weekly_locks").select(
        "location_id, store_name, dm, revenue_band, hourly_goal"
    ).eq("week_start", week_str).eq("status", "locked").execute()
    if resp.data:
        df = pd.DataFrame(resp.data)
        df["hourly_goal"] = pd.to_numeric(df["hourly_goal"], errors="coerce").fillna(0)
        return df
    return None


def lock_exists(week_start):
    """Check if a lock exists for the given week."""
    sb = get_supabase()
    week_str = str(week_start)
    resp = sb.table("weekly_locks").select(
        "id", count="exact"
    ).eq("week_start", week_str).eq("status", "locked").limit(1).execute()
    return (resp.count or 0) > 0


def draft_exists(week_start):
    """Check if drafts exist for the given week."""
    sb = get_supabase()
    week_str = str(week_start)
    resp = sb.table("weekly_locks").select(
        "id", count="exact"
    ).eq("week_start", week_str).eq("status", "draft").limit(1).execute()
    return (resp.count or 0) > 0


def load_draft_config(week_start):
    """Load draft config for a specific week. Returns DataFrame or None."""
    sb = get_supabase()
    week_str = str(week_start)
    resp = sb.table("weekly_locks").select(
        "location_id, store_name, dm, revenue_band, hourly_goal"
    ).eq("week_start", week_str).eq("status", "draft").execute()
    if resp.data:
        df = pd.DataFrame(resp.data)
        df["hourly_goal"] = pd.to_numeric(df["hourly_goal"], errors="coerce").fillna(0)
        return df
    return None


def get_week_status(week_start):
    """Return the status of a week: 'locked', 'draft', or None.
    If mixed statuses exist, returns 'locked' (locked takes precedence)."""
    sb = get_supabase()
    week_str = str(week_start)
    resp = sb.table("weekly_locks").select("status").eq(
        "week_start", week_str
    ).execute()
    if not resp.data:
        return None
    statuses = set(r["status"] for r in resp.data)
    if "locked" in statuses:
        return "locked"
    return "draft"


def create_lock(week_start, ref_data, band_goals, source="manual", status="locked"):
    """Create a weekly lock or draft snapshot.
    Uses upsert to avoid data loss if insert fails after delete."""
    try:
        sb = get_supabase()
        week_str = str(week_start)

        # Build new entries
        rows = []
        for _, row in ref_data.iterrows():
            band = row.get("revenue_band", "<25k")
            goal = band_goals.get(band, 0)
            rows.append({
                "week_start": week_str,
                "location_id": row["location_id"],
                "store_name": row["store_name"],
                "dm": row.get("dm", ""),
                "revenue_band": band,
                "hourly_goal": goal,
                "source": source,
                "status": status,
            })

        # Delete then insert (atomic-ish — both happen in quick succession)
        sb.table("weekly_locks").delete().eq("week_start", week_str).execute()
        sb.table("weekly_locks").insert(rows).execute()
        return pd.DataFrame(rows)
    except Exception as e:
        logger.error(f"Failed to create lock for {week_start}: {e}")
        raise


def save_draft_bands(week_start, store_bands, ref_data, band_goals):
    """Save draft revenue bands for a future week.
    store_bands: dict of {location_id: revenue_band}
    """
    try:
        sb = get_supabase()
        week_str = str(week_start)

        rows = []
        for _, row in ref_data.iterrows():
            store_id = row["location_id"]
            band = store_bands.get(store_id, row.get("revenue_band", "<25k"))
            goal = band_goals.get(band, 0)
            rows.append({
                "week_start": week_str,
                "location_id": store_id,
                "store_name": row["store_name"],
                "dm": row.get("dm", ""),
                "revenue_band": band,
                "hourly_goal": goal,
                "source": "draft",
                "status": "draft",
            })

        # Delete then insert
        sb.table("weekly_locks").delete().eq("week_start", week_str).execute()
        sb.table("weekly_locks").insert(rows).execute()
    except Exception as e:
        logger.error(f"Failed to save drafts for {week_start}: {e}")
        raise


def lock_drafts(week_start):
    """Promote drafts to locked for a given week."""
    try:
        sb = get_supabase()
        week_str = str(week_start)
        sb.table("weekly_locks").update(
            {"status": "locked", "source": "manual-lock"}
        ).eq("week_start", week_str).eq("status", "draft").execute()
    except Exception as e:
        logger.error(f"Failed to lock drafts for {week_start}: {e}")
        raise


def delete_week_lock(week_start):
    """Delete all lock entries for a given week."""
    try:
        sb = get_supabase()
        week_str = str(week_start)
        sb.table("weekly_locks").delete().eq("week_start", week_str).execute()
    except Exception as e:
        logger.error(f"Failed to delete week lock for {week_start}: {e}")
        raise


def override_locked_value(week_start, location_id, field, new_value):
    """Override a single field in a locked week's config. Returns old value."""
    valid_fields = {"dm", "revenue_band", "hourly_goal"}
    if field not in valid_fields:
        raise ValueError(f"Invalid field '{field}'. Must be one of: {valid_fields}")
    try:
        sb = get_supabase()
        week_str = str(week_start)

        # Get current value
        resp = sb.table("weekly_locks").select(field).eq(
            "week_start", week_str
        ).eq("location_id", location_id).eq("status", "locked").execute()

        if not resp.data:
            raise ValueError(f"No lock found for week {week_str}, store {location_id}")

        old_value = str(resp.data[0][field])

        # Update
        sb.table("weekly_locks").update(
            {field: new_value}
        ).eq("week_start", week_str).eq("location_id", location_id).eq("status", "locked").execute()

        return old_value
    except ValueError:
        raise
    except Exception as e:
        logger.error(f"Failed to override {field} for {location_id} week {week_start}: {e}")
        raise


def get_locked_weeks():
    """Return a sorted list of all week_start dates that have locks."""
    from datetime import date as date_type
    sb = get_supabase()
    # Only fetch distinct week_start values with status filter
    resp = sb.table("weekly_locks").select("week_start").eq("status", "locked").execute()
    if resp.data:
        weeks = sorted(set(r["week_start"] for r in resp.data))
        return [date_type.fromisoformat(w) for w in weeks]
    return []


# ---------------------------------------------------------------------------
# Change log
# ---------------------------------------------------------------------------
def log_change(user_email, week_start, location_id, field_changed,
               old_value, new_value, action):
    """Append a change log entry. Never raises — logging should not crash the app."""
    try:
        sb = get_supabase()
        sb.table("change_log").insert({
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "user_email": user_email,
            "week_start": str(week_start),
            "location_id": location_id,
            "field_changed": field_changed,
            "old_value": old_value,
            "new_value": new_value,
            "action": action,
        }).execute()
    except Exception as e:
        logger.error(f"Failed to log change: {e}")


def load_change_log():
    """Load the full change log as a DataFrame."""
    sb = get_supabase()
    resp = sb.table("change_log").select("*").order("timestamp", desc=True).execute()
    if resp.data:
        df = pd.DataFrame(resp.data)
        if "id" in df.columns:
            df = df.drop(columns=["id"])
        return df
    return pd.DataFrame(columns=[
        "timestamp", "user_email", "week_start", "location_id",
        "field_changed", "old_value", "new_value", "action",
    ])


# ---------------------------------------------------------------------------
# Admin users
# ---------------------------------------------------------------------------
def load_admin_users():
    """Load list of admin email addresses."""
    sb = get_supabase()
    resp = sb.table("admin_users").select("email").execute()
    if resp.data:
        return sorted([r["email"].strip().lower() for r in resp.data])
    return []


def is_admin(user_email):
    """Check if the given email is an admin."""
    if not user_email:
        return False
    admins = load_admin_users()
    return user_email.strip().lower() in admins


def add_admin(email):
    """Add an admin email."""
    try:
        sb = get_supabase()
        clean = email.strip().lower()
        sb.table("admin_users").upsert({"email": clean}).execute()
    except Exception as e:
        logger.error(f"Failed to add admin {email}: {e}")
        raise


def remove_admin(email):
    """Remove an admin email."""
    try:
        sb = get_supabase()
        clean = email.strip().lower()
        sb.table("admin_users").delete().eq("email", clean).execute()
    except Exception as e:
        logger.error(f"Failed to remove admin {email}: {e}")
        raise


# ---------------------------------------------------------------------------
# Weekly actuals (actual hours from AVS reports)
# ---------------------------------------------------------------------------
def save_weekly_actuals(week_start, df):
    """Save per-store actual hours for a week. df must have location_id, actual_hours, etc."""
    try:
        sb = get_supabase()
        week_str = str(week_start)

        rows = []
        for _, row in df.iterrows():
            rows.append({
                "week_start": week_str,
                "location_id": row["location_id"],
                "actual_hours": float(row.get("actual_hours", 0) or 0),
                "hourly_goal": float(row.get("hourly_goal", 0) or 0),
                "variance": float(row.get("variance", 0) or 0),
                "net_sales": float(row.get("net_sales", 0) or 0),
                "labor_pct": float(row.get("labor_pct", 0) or 0),
            })

        if rows:
            # Delete then insert
            sb.table("weekly_actuals").delete().eq("week_start", week_str).execute()
            sb.table("weekly_actuals").insert(rows).execute()
    except Exception as e:
        logger.error(f"Failed to save weekly actuals for {week_start}: {e}")
        raise


def load_weekly_actuals():
    """Load all weekly actuals. Returns a DataFrame."""
    sb = get_supabase()
    resp = sb.table("weekly_actuals").select(
        "week_start, location_id, actual_hours, hourly_goal, variance, net_sales, labor_pct"
    ).order("week_start").execute()
    if resp.data:
        df = pd.DataFrame(resp.data)
        for col in ["actual_hours", "hourly_goal", "variance", "net_sales", "labor_pct"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        df["week_start"] = df["week_start"].astype(str)
        return df
    return pd.DataFrame(columns=[
        "week_start", "location_id", "actual_hours", "hourly_goal",
        "variance", "net_sales", "labor_pct",
    ])


def delete_weekly_actuals(week_start):
    """Delete all actuals for a given week."""
    try:
        sb = get_supabase()
        sb.table("weekly_actuals").delete().eq("week_start", str(week_start)).execute()
    except Exception as e:
        logger.error(f"Failed to delete weekly actuals for {week_start}: {e}")
        raise


# ---------------------------------------------------------------------------
# Rev Band Submissions
# ---------------------------------------------------------------------------
def load_submissions(week_start=None, status=None):
    """Load rev band submissions, optionally filtered by week and/or status."""
    sb = get_supabase()
    query = sb.table("rev_band_submissions").select("*")
    if week_start:
        query = query.eq("week_start", str(week_start))
    if status:
        query = query.eq("status", status)
    resp = query.order("location_id").execute()
    return pd.DataFrame(resp.data) if resp.data else pd.DataFrame()


def load_all_submissions():
    """Load all submissions across all weeks."""
    sb = get_supabase()
    resp = sb.table("rev_band_submissions").select("*").order("week_start", desc=True).execute()
    return pd.DataFrame(resp.data) if resp.data else pd.DataFrame()


def approve_submission(submission_id, admin_email):
    """Admin approves a submission."""
    try:
        sb = get_supabase()
        sb.table("rev_band_submissions").update({
            "status": "approved",
            "admin_approved_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "admin_approved_by": admin_email,
        }).eq("id", submission_id).execute()
    except Exception as e:
        logger.error(f"Failed to approve submission {submission_id}: {e}")
        raise


def reject_submission(submission_id, admin_email, reason=""):
    """Admin rejects a submission."""
    try:
        sb = get_supabase()
        sb.table("rev_band_submissions").update({
            "status": "rejected",
            "rejected_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "rejected_by": admin_email,
            "rejection_reason": reason,
        }).eq("id", submission_id).execute()
    except Exception as e:
        logger.error(f"Failed to reject submission {submission_id}: {e}")
        raise


def load_email_log(week_start=None):
    """Load email log, optionally filtered by week."""
    sb = get_supabase()
    query = sb.table("email_log").select("*")
    if week_start:
        query = query.eq("week_start", str(week_start))
    resp = query.order("sent_at", desc=True).execute()
    return pd.DataFrame(resp.data) if resp.data else pd.DataFrame()


def load_app_settings():
    """Load all app settings as a dict."""
    sb = get_supabase()
    resp = sb.table("app_settings").select("*").execute()
    return {r["key"]: r["value"] for r in resp.data} if resp.data else {}


def save_app_setting(key, value):
    """Save or update a single app setting."""
    sb = get_supabase()
    sb.table("app_settings").upsert({"key": key, "value": value}).execute()
