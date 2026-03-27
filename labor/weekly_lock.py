"""
Weekly Lock System
------------------
Manages weekly config snapshots for AVS reports.
Weeks run Thursday to Wednesday. Config (DM, revenue band, hourly goal)
is locked at the start of each week and used for all reports that week.

If config hasn't been updated by Wednesday, the previous week's values
are automatically carried forward.

Data is stored in Supabase (via db.py).
"""

from datetime import datetime, date, timedelta

import pandas as pd

from db import (
    load_all_locks, load_locked_config, lock_exists,
    create_lock, override_locked_value as _db_override,
    get_locked_weeks, log_change, load_change_log,
    load_admin_users, is_admin, add_admin, remove_admin,
)


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
# Lock management (business logic, delegates to db.py for storage)
# ---------------------------------------------------------------------------
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
        lock_ref = prev_locked.copy()
        # Use current band_goals in case goals were updated
        lock_band_goals = {}
        for _, row in lock_ref.iterrows():
            band = row["revenue_band"]
            if band in band_goals:
                lock_band_goals[band] = band_goals[band]
            else:
                lock_band_goals[band] = row["hourly_goal"]

        create_lock(week_start, lock_ref, lock_band_goals, source=source)
        log_change(
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
        log_change(
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
    old_value = _db_override(week_start, location_id, field, new_value)

    log_change(
        user_email=user_email,
        week_start=week_start,
        location_id=location_id,
        field_changed=field,
        old_value=old_value,
        new_value=str(new_value),
        action="admin-override",
    )
