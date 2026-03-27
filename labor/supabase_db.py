"""
Supabase Data Access Layer
--------------------------
All database read/write operations for the ramz-accounting app.
Replaces CSV file operations with Supabase PostgreSQL calls.
"""

import pandas as pd
import streamlit as st
from supabase import create_client


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
    sb = get_supabase()
    sb.table("stores").upsert(
        {"location_id": location_id, "store_name": store_name}
    ).execute()


def delete_store(location_id):
    """Remove a store from the stores table."""
    sb = get_supabase()
    sb.table("stores").delete().eq("location_id", location_id).execute()


def load_reference_data():
    """Load store reference data. Returns DataFrame with location_id, store_name, dm, revenue_band."""
    sb = get_supabase()
    resp = sb.table("reference_data").select("*").order("location_id").execute()
    if resp.data:
        return pd.DataFrame(resp.data)
    return pd.DataFrame(columns=["location_id", "store_name", "dm", "revenue_band"])


def save_reference_data_row(location_id, store_name, dm, revenue_band):
    """Insert or update a single reference data row."""
    sb = get_supabase()
    sb.table("reference_data").upsert({
        "location_id": location_id,
        "store_name": store_name,
        "dm": dm,
        "revenue_band": revenue_band,
    }).execute()


def save_reference_data_bulk(df):
    """Upsert all rows from a DataFrame into reference_data."""
    sb = get_supabase()
    records = df[["location_id", "store_name", "dm", "revenue_band"]].to_dict("records")
    sb.table("reference_data").upsert(records).execute()


def delete_reference_data(location_id):
    """Remove a store from reference_data."""
    sb = get_supabase()
    sb.table("reference_data").delete().eq("location_id", location_id).execute()


def load_band_goals():
    """Load revenue band → hourly goal mapping. Returns a dict."""
    sb = get_supabase()
    resp = sb.table("band_goals").select("*").execute()
    if resp.data:
        return {r["revenue_band"]: float(r["hourly_goal"]) for r in resp.data}
    return {}


def save_band_goals(goals_dict):
    """Save all band goals. goals_dict: {revenue_band: hourly_goal}."""
    sb = get_supabase()
    records = [
        {"revenue_band": band, "hourly_goal": goal}
        for band, goal in goals_dict.items()
    ]
    sb.table("band_goals").upsert(records).execute()


def load_dm_list():
    """Load list of DM names. Returns sorted list of strings."""
    sb = get_supabase()
    resp = sb.table("dm_list").select("dm_name").order("dm_name").execute()
    if resp.data:
        return [r["dm_name"] for r in resp.data]
    return []


def add_dm(dm_name):
    """Add a DM to the list."""
    sb = get_supabase()
    sb.table("dm_list").upsert({"dm_name": dm_name}).execute()


def remove_dm(dm_name):
    """Remove a DM from the list."""
    sb = get_supabase()
    sb.table("dm_list").delete().eq("dm_name", dm_name).execute()


# ---------------------------------------------------------------------------
# Weekly locks
# ---------------------------------------------------------------------------
def load_all_locks():
    """Load all weekly lock data (locked only). Returns a DataFrame."""
    sb = get_supabase()
    resp = sb.table("weekly_locks").select(
        "week_start, location_id, store_name, dm, revenue_band, hourly_goal, source, status"
    ).eq("status", "locked").order("week_start").execute()
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
    return resp.count > 0


def draft_exists(week_start):
    """Check if drafts exist for the given week."""
    sb = get_supabase()
    week_str = str(week_start)
    resp = sb.table("weekly_locks").select(
        "id", count="exact"
    ).eq("week_start", week_str).eq("status", "draft").limit(1).execute()
    return resp.count > 0


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
    """Return the status of a week: 'locked', 'draft', or None."""
    sb = get_supabase()
    week_str = str(week_start)
    resp = sb.table("weekly_locks").select("status").eq(
        "week_start", week_str
    ).limit(1).execute()
    if resp.data:
        return resp.data[0]["status"]
    return None


def create_lock(week_start, ref_data, band_goals, source="manual", status="locked"):
    """Create a weekly lock or draft snapshot."""
    sb = get_supabase()
    week_str = str(week_start)

    # Remove existing entries for this week
    sb.table("weekly_locks").delete().eq("week_start", week_str).execute()

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

    sb.table("weekly_locks").insert(rows).execute()
    return pd.DataFrame(rows)


def save_draft_bands(week_start, store_bands, ref_data, band_goals):
    """Save draft revenue bands for a future week.
    store_bands: dict of {location_id: revenue_band}
    """
    sb = get_supabase()
    week_str = str(week_start)

    # Remove existing entries for this week
    sb.table("weekly_locks").delete().eq("week_start", week_str).execute()

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

    sb.table("weekly_locks").insert(rows).execute()


def lock_drafts(week_start):
    """Promote drafts to locked for a given week."""
    sb = get_supabase()
    week_str = str(week_start)
    sb.table("weekly_locks").update(
        {"status": "locked", "source": "manual-lock"}
    ).eq("week_start", week_str).eq("status", "draft").execute()


def delete_week_lock(week_start):
    """Delete all lock entries for a given week."""
    sb = get_supabase()
    week_str = str(week_start)
    sb.table("weekly_locks").delete().eq("week_start", week_str).execute()


def override_locked_value(week_start, location_id, field, new_value):
    """Override a single field in a locked week's config. Returns old value."""
    sb = get_supabase()
    week_str = str(week_start)

    # Get current value
    resp = sb.table("weekly_locks").select(field).eq(
        "week_start", week_str
    ).eq("location_id", location_id).execute()

    if not resp.data:
        raise ValueError(f"No lock found for week {week_str}, store {location_id}")

    old_value = str(resp.data[0][field])

    # Update
    sb.table("weekly_locks").update(
        {field: new_value}
    ).eq("week_start", week_str).eq("location_id", location_id).execute()

    return old_value


def get_locked_weeks():
    """Return a sorted list of all week_start dates that have locks."""
    from datetime import date as date_type
    sb = get_supabase()
    resp = sb.table("weekly_locks").select("week_start").execute()
    if resp.data:
        weeks = sorted(set(r["week_start"] for r in resp.data))
        return [date_type.fromisoformat(w) for w in weeks]
    return []


# ---------------------------------------------------------------------------
# Change log
# ---------------------------------------------------------------------------
def log_change(user_email, week_start, location_id, field_changed,
               old_value, new_value, action):
    """Append a change log entry."""
    from datetime import datetime
    sb = get_supabase()
    sb.table("change_log").insert({
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "user_email": user_email,
        "week_start": str(week_start),
        "location_id": location_id,
        "field_changed": field_changed,
        "old_value": old_value,
        "new_value": new_value,
        "action": action,
    }).execute()


def load_change_log():
    """Load the full change log as a DataFrame."""
    sb = get_supabase()
    resp = sb.table("change_log").select("*").order("timestamp", desc=True).execute()
    if resp.data:
        df = pd.DataFrame(resp.data)
        # Drop the serial id column for display
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
    return ["elliottbae@gmail.com"]


def is_admin(user_email):
    """Check if the given email is an admin."""
    if not user_email:
        return False
    admins = load_admin_users()
    return user_email.strip().lower() in admins


def add_admin(email):
    """Add an admin email."""
    sb = get_supabase()
    clean = email.strip().lower()
    sb.table("admin_users").upsert({"email": clean}).execute()


def remove_admin(email):
    """Remove an admin email."""
    sb = get_supabase()
    clean = email.strip().lower()
    sb.table("admin_users").delete().eq("email", clean).execute()


# ---------------------------------------------------------------------------
# Weekly actuals (actual hours from AVS reports)
# ---------------------------------------------------------------------------
def save_weekly_actuals(week_start, df):
    """Save per-store actual hours for a week. df must have location_id, actual_hours, etc."""
    sb = get_supabase()
    week_str = str(week_start)

    # Delete existing entries for this week (allows re-runs)
    sb.table("weekly_actuals").delete().eq("week_start", week_str).execute()

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
        sb.table("weekly_actuals").insert(rows).execute()


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
    sb = get_supabase()
    sb.table("weekly_actuals").delete().eq("week_start", str(week_start)).execute()
