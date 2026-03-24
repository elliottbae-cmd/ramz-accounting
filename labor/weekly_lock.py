"""
Weekly Lock System
------------------
Manages weekly config snapshots for AVS reports.
Weeks run Thursday to Wednesday. Config (DM, revenue band, hourly goal)
is locked at the start of each week and used for all reports that week.

If config hasn't been updated by Wednesday, the previous week's values
are automatically carried forward.
"""

import csv
from datetime import datetime, date, timedelta
from pathlib import Path

import pandas as pd

_HERE = Path(__file__).parent
WEEKLY_LOCK_PATH = _HERE / "weekly_lock.csv"
CHANGE_LOG_PATH = _HERE / "change_log.csv"
ADMIN_USERS_PATH = _HERE / "admin_users.csv"

LOCK_COLUMNS = [
    "week_start", "location_id", "store_name", "dm",
    "revenue_band", "hourly_goal", "source",
]
LOG_COLUMNS = [
    "timestamp", "user_email", "week_start", "location_id",
    "field_changed", "old_value", "new_value", "action",
]


# ---------------------------------------------------------------------------
# Week helpers
# ---------------------------------------------------------------------------
def get_week_start(ref_date=None):
    """Return the Thursday that starts the week containing ref_date."""
    d = ref_date if isinstance(ref_date, date) else (ref_date or date.today())
    if isinstance(d, datetime):
        d = d.date()
    # Thursday = weekday 3
    days_since_thursday = (d.weekday() - 3) % 7
    return d - timedelta(days=days_since_thursday)


def get_week_end(week_start):
    """Return the Wednesday that ends the week starting on week_start (Thursday)."""
    return week_start + timedelta(days=6)


def get_next_week_start(ref_date=None):
    """Return the Thursday that starts the NEXT week."""
    current = get_week_start(ref_date)
    return current + timedelta(days=7)


def format_week_label(week_start):
    """Return a human-readable label like 'Thu 3/19 – Wed 3/25'."""
    end = get_week_end(week_start)
    return f"Thu {week_start.month}/{week_start.day} – Wed {end.month}/{end.day}"


def is_before_wednesday_deadline(ref_date=None):
    """Check if we're before the Wednesday deadline for next week's config."""
    d = ref_date or date.today()
    if isinstance(d, datetime):
        d = d.date()
    # Wednesday = weekday 2
    return d.weekday() <= 2  # Mon=0, Tue=1, Wed=2


# ---------------------------------------------------------------------------
# Lock file management
# ---------------------------------------------------------------------------
def _ensure_lock_file():
    """Create the lock file with headers if it doesn't exist."""
    if not WEEKLY_LOCK_PATH.exists():
        pd.DataFrame(columns=LOCK_COLUMNS).to_csv(
            WEEKLY_LOCK_PATH, sep="|", index=False
        )


def load_all_locks():
    """Load all weekly lock data. Returns a DataFrame."""
    _ensure_lock_file()
    df = pd.read_csv(WEEKLY_LOCK_PATH, sep="|", dtype=str)
    if "hourly_goal" in df.columns:
        df["hourly_goal"] = pd.to_numeric(df["hourly_goal"], errors="coerce").fillna(0)
    return df


def load_locked_config(week_start):
    """
    Load the locked config for a specific week.
    Returns a DataFrame with columns: location_id, store_name, dm, revenue_band, hourly_goal
    Returns None if no lock exists for that week.
    """
    all_locks = load_all_locks()
    week_str = str(week_start)
    week_data = all_locks[all_locks["week_start"] == week_str]
    if week_data.empty:
        return None
    return week_data[["location_id", "store_name", "dm", "revenue_band", "hourly_goal"]].copy()


def lock_exists(week_start):
    """Check if a lock exists for the given week."""
    all_locks = load_all_locks()
    return str(week_start) in all_locks["week_start"].values


def create_lock(week_start, ref_data, band_goals, source="manual"):
    """
    Create a weekly lock snapshot from reference data and band goals.

    Parameters:
        week_start: date — the Thursday start of the week
        ref_data: DataFrame with location_id, store_name, dm, revenue_band
        band_goals: dict of revenue_band → hourly_goal
        source: 'manual' or 'auto-carry-forward'
    """
    _ensure_lock_file()
    all_locks = load_all_locks()
    week_str = str(week_start)

    # Remove any existing entries for this week (in case of re-lock)
    all_locks = all_locks[all_locks["week_start"] != week_str]

    # Build new lock entries
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
        })

    new_lock = pd.DataFrame(rows)
    all_locks = pd.concat([all_locks, new_lock], ignore_index=True)
    all_locks.to_csv(WEEKLY_LOCK_PATH, sep="|", index=False)
    return new_lock


def ensure_current_week_locked(ref_data, band_goals, ref_date=None):
    """
    Ensure the current week has a locked config.
    If not locked yet:
      - Check if previous week exists → carry forward
      - Otherwise → snapshot current live config
    Returns the locked config DataFrame for the current week.
    """
    week_start = get_week_start(ref_date)

    # Already locked?
    locked = load_locked_config(week_start)
    if locked is not None:
        return locked

    # Check previous week for carry-forward
    prev_week = week_start - timedelta(days=7)
    prev_locked = load_locked_config(prev_week)

    if prev_locked is not None:
        # Carry forward previous week's locked values
        source = "auto-carry-forward"
        # Use previous week's locked data as the reference
        lock_ref = prev_locked.copy()
        # But use current band_goals in case goals were updated
        lock_band_goals = {}
        for _, row in lock_ref.iterrows():
            band = row["revenue_band"]
            if band in band_goals:
                lock_band_goals[band] = band_goals[band]
            else:
                lock_band_goals[band] = row["hourly_goal"]

        create_lock(week_start, lock_ref, lock_band_goals, source=source)
        _log_change(
            user_email="system",
            week_start=week_start,
            location_id="ALL",
            field_changed="week_lock",
            old_value=str(prev_week),
            new_value=str(week_start),
            action="auto-carry-forward",
        )
    else:
        # First-time lock from live config
        source = "initial-snapshot"
        create_lock(week_start, ref_data, band_goals, source=source)
        _log_change(
            user_email="system",
            week_start=week_start,
            location_id="ALL",
            field_changed="week_lock",
            old_value="none",
            new_value=str(week_start),
            action="initial-snapshot",
        )

    return load_locked_config(week_start)


def override_locked_value(week_start, location_id, field, new_value, user_email):
    """
    Override a single field in a locked week's config.
    field must be one of: 'dm', 'revenue_band', 'hourly_goal'
    Logs the change.
    """
    all_locks = load_all_locks()
    week_str = str(week_start)
    mask = (all_locks["week_start"] == week_str) & (all_locks["location_id"] == location_id)

    if not mask.any():
        raise ValueError(f"No lock found for week {week_str}, store {location_id}")

    old_value = str(all_locks.loc[mask, field].values[0])
    all_locks.loc[mask, field] = new_value
    all_locks.to_csv(WEEKLY_LOCK_PATH, sep="|", index=False)

    _log_change(
        user_email=user_email,
        week_start=week_start,
        location_id=location_id,
        field_changed=field,
        old_value=old_value,
        new_value=str(new_value),
        action="admin-override",
    )


def get_locked_weeks():
    """Return a sorted list of all week_start dates that have locks."""
    all_locks = load_all_locks()
    if all_locks.empty:
        return []
    weeks = sorted(all_locks["week_start"].unique().tolist())
    return [date.fromisoformat(w) for w in weeks]


# ---------------------------------------------------------------------------
# Change log
# ---------------------------------------------------------------------------
def _ensure_log_file():
    """Create the change log file with headers if it doesn't exist."""
    if not CHANGE_LOG_PATH.exists():
        pd.DataFrame(columns=LOG_COLUMNS).to_csv(CHANGE_LOG_PATH, index=False)


def _log_change(user_email, week_start, location_id, field_changed,
                old_value, new_value, action):
    """Append a change log entry."""
    _ensure_log_file()
    entry = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "user_email": user_email,
        "week_start": str(week_start),
        "location_id": location_id,
        "field_changed": field_changed,
        "old_value": old_value,
        "new_value": new_value,
        "action": action,
    }
    with open(CHANGE_LOG_PATH, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=LOG_COLUMNS)
        writer.writerow(entry)


def load_change_log():
    """Load the full change log as a DataFrame."""
    _ensure_log_file()
    return pd.read_csv(CHANGE_LOG_PATH, dtype=str)


# ---------------------------------------------------------------------------
# Admin user management
# ---------------------------------------------------------------------------
def _ensure_admin_file():
    """Create admin users file with default admin if it doesn't exist."""
    if not ADMIN_USERS_PATH.exists():
        pd.DataFrame({"email": ["elliottbae@gmail.com"]}).to_csv(
            ADMIN_USERS_PATH, index=False
        )


def load_admin_users():
    """Load list of admin email addresses."""
    _ensure_admin_file()
    df = pd.read_csv(ADMIN_USERS_PATH, dtype=str)
    return df["email"].str.strip().str.lower().tolist()


def is_admin(user_email):
    """Check if the given email is an admin."""
    if not user_email:
        return False
    admins = load_admin_users()
    return user_email.strip().lower() in admins


def add_admin(email):
    """Add an admin email."""
    admins = load_admin_users()
    clean = email.strip().lower()
    if clean not in admins:
        admins.append(clean)
        pd.DataFrame({"email": sorted(admins)}).to_csv(ADMIN_USERS_PATH, index=False)


def remove_admin(email):
    """Remove an admin email."""
    admins = load_admin_users()
    clean = email.strip().lower()
    if clean in admins:
        admins.remove(clean)
        pd.DataFrame({"email": sorted(admins)}).to_csv(ADMIN_USERS_PATH, index=False)
