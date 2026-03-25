# Weekly Lock: Date-Range-Based Config for AVS Reports

## Problem
When a user changes a DM assignment, revenue band, or hourly goal mid-week, it could affect the current in-progress week's AVS reports. We need to "lock" these values at the start of each week (Thursday) so mid-week changes only take effect the following Thursday.

## Design

### Week Definition
- Weeks run **Thursday to Wednesday**
- The "current week" is identified by its Thursday start date (e.g., `2026-03-19` for the week of Thu 3/19 – Wed 3/25)

### New File: `labor/weekly_lock.csv`
A simple CSV that snapshots the config values when a week starts:

```
week_start|location_id|dm|revenue_band|hourly_goal
2026-03-19|112-0001|John|25k-30k|450
2026-03-19|112-0002|Sarah|<25k|380
...
```

### How It Works

1. **On any AVS report page**, when the user uploads data or clicks "Run":
   - Calculate the current week's Thursday start date
   - Check if `weekly_lock.csv` has entries for that `week_start`
   - **If no** → create a snapshot from the current `reference_data.csv` + `band_goals.csv` and save it to `weekly_lock.csv`
   - **If yes** → use the locked values (ignore any changes made to settings since Thursday)

2. **On Settings pages** (Revenue Bands, DM Assignments, Hourly Goals):
   - Changes still save to `reference_data.csv` and `band_goals.csv` immediately (the "live" config)
   - Show a note: "Changes will take effect next Thursday (MM/DD)"
   - The locked week's values are untouched

3. **AVS report engine** uses the locked values (not the live config) when generating reports

### Changes Required

| File | Change |
|------|--------|
| `fz_fees/app.py` | Add `get_current_week_start()` helper. Add lock check/create logic to AVS pages. Add info banners to Settings pages. |
| `labor/avs_engine.py` | Add `load_locked_config(week_start)` function that reads from `weekly_lock.csv` or creates snapshot. Modify report functions to accept optional locked config. |
| `labor/weekly_lock.csv` | New file (auto-created on first AVS run each week) |

### Helper Function
```python
def get_current_week_start(ref_date=None):
    """Return the Thursday that starts the current week."""
    d = ref_date or datetime.today().date()
    # Thursday = weekday 3
    days_since_thursday = (d.weekday() - 3) % 7
    return d - timedelta(days=days_since_thursday)
```

### Settings Page Banners
Each settings page (Revenue Bands, DM Assignments, Hourly Goals) will show:
> "ℹ️ The current week (Thu 3/19 – Wed 3/25) is locked. Changes saved here will take effect starting Thu 3/26."

### No History Storage Needed
- Only the current week's lock is kept active
- Old weeks' rows can be cleaned up automatically (or left — they're small)
