"""
Climate Normals Seed Script
----------------------------
Pulls 3 years of historical weather from Open-Meteo Archive for each active
store and computes per-store, per-month, per-day-of-week averages for:
  - avg_temp_high_f
  - avg_temp_low_f
  - avg_precip_in

These normals are used by the scenario engine to fill in weather inputs
beyond the 16-day Open-Meteo forecast horizon (weeks 3–26 of the 6-month
projection).

Run once to seed the table, then runs automatically in Phase 4 of
monday_job.py each month to stay current.

Usage:
    python labor/seed_climate_normals.py
"""

import json
import pathlib
import requests
import toml
import time
from datetime import date, timedelta
from collections import defaultdict

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
_HERE = pathlib.Path(__file__).resolve().parent   # labor/
_ROOT = _HERE.parent                               # ramz-accounting/
SECRETS_PATH = str(_ROOT / '.streamlit' / 'secrets.toml')
YEARS_BACK   = 3   # pull this many full years of historical weather


# ---------------------------------------------------------------------------
# Supabase helpers (same pattern as monday_job.py)
# ---------------------------------------------------------------------------
def init_sb():
    secrets = toml.load(SECRETS_PATH)
    url = secrets['supabase']['url']
    key = secrets['supabase']['key']
    hdrs = {
        'apikey': key,
        'Authorization': f'Bearer {key}',
        'Content-Type': 'application/json',
        'Prefer': 'resolution=merge-duplicates',
    }
    return url, hdrs


def sb_get_all(url, hdrs, table, extra=''):
    rows, offset = [], 0
    while True:
        h = {**hdrs, 'Range-Unit': 'items', 'Range': f'{offset}-{offset+999}'}
        r = requests.get(f'{url}/rest/v1/{table}?select=*{extra}', headers=h, timeout=60)
        batch = r.json()
        if not isinstance(batch, list) or not batch:
            break
        rows.extend(batch)
        if len(batch) < 1000:
            break
        offset += 1000
    return rows


def sb_upsert(url, hdrs, table, rows, chunk=500):
    upsert_hdrs = {**hdrs, 'Prefer': 'resolution=merge-duplicates,return=minimal'}
    for i in range(0, len(rows), chunk):
        r = requests.post(
            f'{url}/rest/v1/{table}',
            headers=upsert_hdrs,
            data=json.dumps(rows[i:i + chunk]),
            timeout=30,
        )
        if r.status_code not in (200, 201):
            print(f'  ERROR upserting {table}: {r.status_code} {r.text[:200]}')


# ---------------------------------------------------------------------------
# Open-Meteo Archive helper
# ---------------------------------------------------------------------------
def pull_weather_archive(lat, lon, start_date, end_date):
    """
    Fetch historical daily weather from Open-Meteo Archive API.
    Returns the 'daily' dict or {} on failure.
    """
    params = {
        'latitude':           lat,
        'longitude':          lon,
        'start_date':         start_date.strftime('%Y-%m-%d'),
        'end_date':           end_date.strftime('%Y-%m-%d'),
        'daily':              'temperature_2m_max,temperature_2m_min,precipitation_sum',
        'temperature_unit':   'fahrenheit',
        'precipitation_unit': 'inch',
        'timezone':           'America/Chicago',
    }
    for attempt in range(3):
        try:
            r = requests.get(
                'https://archive-api.open-meteo.com/v1/archive',
                params=params,
                timeout=90,
            )
            r.raise_for_status()
            return r.json().get('daily', {})
        except Exception as e:
            if attempt == 2:
                print(f'    ⚠ Archive fetch failed after 3 attempts ({lat},{lon}): {e}')
                return {}
            time.sleep(5)
    return {}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    url, hdrs = init_sb()
    today = date.today()

    # Date window: 3 full years ending yesterday (today may be incomplete)
    end_date   = today - timedelta(days=1)
    start_date = date(today.year - YEARS_BACK, today.month, today.day)

    print(f'Climate Normals Seed')
    print(f'  Date range : {start_date} → {end_date}  ({YEARS_BACK} years)')
    print(f'  Computed at: {today}')

    # Load active traditional stores with lat/lon
    stores_raw = sb_get_all(url, hdrs, 'reference_data',
                            '&active=eq.true&store_type=eq.traditional')
    stores = [s for s in stores_raw if s.get('latitude') and s.get('longitude')]
    print(f'  Stores     : {len(stores)}\n')

    all_normals = []

    for store in stores:
        loc_id     = store['location_id']
        store_name = store['store_name']
        lat        = float(store['latitude'])
        lon        = float(store['longitude'])

        print(f'  {store_name} ({loc_id}) — fetching {YEARS_BACK}-yr archive...')

        daily = pull_weather_archive(lat, lon, start_date, end_date)

        if not daily.get('time'):
            print(f'    ⚠ No data returned — skipping')
            continue

        # Accumulate sums by (month, day_of_week)
        # Each bucket: [sum_high, sum_low, sum_precip, count]
        buckets = defaultdict(lambda: [0.0, 0.0, 0.0, 0])

        dates      = daily['time']                        # list of 'YYYY-MM-DD' strings
        temp_highs = daily.get('temperature_2m_max', [])
        temp_lows  = daily.get('temperature_2m_min', [])
        precips    = daily.get('precipitation_sum', [])

        for i, d_str in enumerate(dates):
            d   = date.fromisoformat(d_str)
            key = (d.month, d.weekday())   # (1-12, 0=Mon … 6=Sun)

            th = temp_highs[i] if i < len(temp_highs) and temp_highs[i] is not None else None
            tl = temp_lows[i]  if i < len(temp_lows)  and temp_lows[i]  is not None else None
            pr = precips[i]    if i < len(precips)     and precips[i]    is not None else None

            if th is not None:
                buckets[key][0] += th
                buckets[key][3] += 1   # count only when we have data
            if tl is not None:
                buckets[key][1] += tl
            if pr is not None:
                buckets[key][2] += pr

        # Build upsert rows
        store_rows = []
        for (month, dow), (sum_h, sum_l, sum_p, cnt) in buckets.items():
            if cnt == 0:
                continue
            store_rows.append({
                'location_id':     loc_id,
                'month':           month,
                'day_of_week':     dow,
                'avg_temp_high_f': round(sum_h / cnt, 2),
                'avg_temp_low_f':  round(sum_l / cnt, 2),
                'avg_precip_in':   round(sum_p / cnt, 4),
                'computed_at':     today.isoformat(),
            })

        if store_rows:
            sb_upsert(url, hdrs, 'climate_normals', store_rows)
            print(f'    ✓ {len(store_rows)} buckets upserted '
                  f'(12 months × 7 days = 84 expected)')
        else:
            print(f'    ⚠ No buckets computed — check data')

        # Brief pause to stay friendly to the Open-Meteo API
        time.sleep(1)

    print(f'\nDone. {len(stores)} stores processed.')
    print('Climate normals are ready for use by the scenario engine.')


if __name__ == '__main__':
    main()
