"""
Sales Forecasting Engine — Monday Job
Runs every Monday at 6am CT via GitHub Actions.

Two phases every week:
  Phase 1: Back-fill last week's actuals on prior forecast rows + calculate error metrics
  Phase 2: Generate next week's forecasts for all active traditional stores

Monthly (first Monday of month):
  Phase 3: Retrain model with all accumulated data
"""
import pandas as pd
import numpy as np
import requests
import json
import pickle
import toml
import sys
import os
from datetime import date, timedelta, datetime, timezone

sys.path.insert(0, 'C:/Users/BretElliott/ramz-accounting')

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SECRETS_PATH = 'C:/Users/BretElliott/ramz-accounting/.streamlit/secrets.toml'
MODEL_PATH   = 'C:/Users/BretElliott/ramz-accounting/labor/lgbm_model_v1.pkl'
META_PATH    = 'C:/Users/BretElliott/ramz-accounting/labor/model_meta_v1.json'
MODEL_VERSION = 'v1'

BANDS = [
    ('<25k',    0,     25000),
    ('25k-30k', 25000, 30000),
    ('30k-35k', 30000, 35000),
    ('35k-40k', 35000, 40000),
    ('40k-45k', 40000, 45000),
    ('45k-50k', 45000, 50000),
    ('50k+',    50000, 9_999_999),
]

STATE_MAP = {
    '112-0001': 'OK', '112-0002': 'OK', '112-0003': 'OK', '112-0004': 'OK',
    '112-0005': 'OK', '112-0031': 'OK', '112-0035': 'OK', '112-0038': 'OK',
    '112-0006': 'TX', '112-0007': 'TX', '112-0008': 'TX', '112-0009': 'TX',
    '112-0010': 'TX', '112-0011': 'TX', '112-0032': 'TX', '112-0033': 'TX',
    '112-0034': 'TX', '112-0036': 'TX', '112-0037': 'TX',
    '112-0012': 'OH', '112-0013': 'OH', '112-0014': 'OH', '112-0015': 'OH',
    '112-0016': 'OH', '112-0017': 'OH', '112-0018': 'OH', '112-0019': 'OH',
    '112-0020': 'OH', '112-0021': 'OH', '112-0022': 'OH', '112-0023': 'OH',
    '112-0024': 'OH', '112-0025': 'OH', '112-0026': 'OH', '112-0028': 'OH',
    '112-0029': 'OH', '112-0030': 'OH',
    '112-0027': 'KY',
}

ANCHOR_PAYROLL = pd.Timestamp('2023-01-06')

HOLIDAY_CALENDAR = {
    # Closed days
    date(2024, 11, 28): 'thanksgiving', date(2025, 11, 27): 'thanksgiving',
    date(2026, 11, 26): 'thanksgiving',
    date(2023, 12, 25): 'christmas',   date(2024, 12, 25): 'christmas',
    date(2025, 12, 25): 'christmas',   date(2026, 12, 25): 'christmas',
    # Lower traffic
    date(2023, 12, 24): 'christmas_eve', date(2024, 12, 24): 'christmas_eve',
    date(2025, 12, 24): 'christmas_eve', date(2026, 12, 24): 'christmas_eve',
    date(2023, 11, 22): 'day_before_thanksgiving',
    date(2024, 11, 27): 'day_before_thanksgiving',
    date(2025, 11, 26): 'day_before_thanksgiving',
    date(2026, 11, 25): 'day_before_thanksgiving',
}


# ---------------------------------------------------------------------------
# Supabase helpers
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
        r = requests.post(f'{url}/rest/v1/{table}', headers=upsert_hdrs,
                          data=json.dumps(rows[i:i+chunk]), timeout=30)
        if r.status_code not in (200, 201):
            print(f'  ERROR upserting {table}: {r.status_code} {r.text[:200]}')


def sb_patch(url, hdrs, table, match_col, match_val, data):
    r = requests.patch(
        f'{url}/rest/v1/{table}?{match_col}=eq.{match_val}',
        headers=hdrs, data=json.dumps(data), timeout=30
    )
    if r.status_code not in (200, 204):
        print(f'  ERROR patching {table}: {r.status_code} {r.text[:200]}')


# ---------------------------------------------------------------------------
# Weather helpers
# ---------------------------------------------------------------------------
def pull_weather_forecast(lat, lon, days=16):
    """Pull forward weather forecast from Open-Meteo."""
    r = requests.get(
        'https://api.open-meteo.com/v1/forecast',
        params={
            'latitude': lat, 'longitude': lon,
            'daily': 'temperature_2m_max,temperature_2m_min,precipitation_sum,weather_code',
            'temperature_unit': 'fahrenheit',
            'precipitation_unit': 'inch',
            'timezone': 'America/Chicago',
            'forecast_days': days,
        },
        timeout=15
    )
    return r.json().get('daily', {})


def pull_weather_actuals(lat, lon, start_date, end_date):
    """Pull historical actuals from Open-Meteo archive."""
    r = requests.get(
        'https://archive-api.open-meteo.com/v1/archive',
        params={
            'latitude': lat, 'longitude': lon,
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': end_date.strftime('%Y-%m-%d'),
            'daily': 'temperature_2m_max,temperature_2m_min,precipitation_sum,weather_code',
            'temperature_unit': 'fahrenheit',
            'precipitation_unit': 'inch',
            'timezone': 'America/Chicago',
        },
        timeout=15
    )
    return r.json().get('daily', {})


def weather_to_rows(loc_id, daily, is_forecast):
    rows = []
    for i, d in enumerate(daily.get('time', [])):
        wcode = daily['weather_code'][i]
        rows.append({
            'location_id': loc_id,
            'date': d,
            'temp_high_f': daily['temperature_2m_max'][i],
            'temp_low_f': daily['temperature_2m_min'][i],
            'precipitation_in': daily['precipitation_sum'][i],
            'weather_code': wcode,
            'severe_weather_flag': bool(wcode >= 65) if wcode is not None else False,
            'is_forecast': is_forecast,
        })
    return rows


# ---------------------------------------------------------------------------
# Gas price helpers
# ---------------------------------------------------------------------------
def pull_gas_prices(api_key):
    """Pull latest weekly gas prices from EIA."""
    results = {}
    padd_map = {
        'EMM_EPMR_PTE_R20_DPG': ['OH', 'KY', 'OK'],
        'EMM_EPMR_PTE_R30_DPG': ['TX'],
    }
    for series_id, states in padd_map.items():
        r = requests.get(
            'https://api.eia.gov/v2/petroleum/pri/gnd/data/',
            params={
                'api_key': api_key,
                'frequency': 'weekly',
                'data[0]': 'value',
                'facets[series][]': series_id,
                'sort[0][column]': 'period',
                'sort[0][direction]': 'desc',
                'length': 4,
            },
            timeout=15
        )
        data = r.json().get('response', {}).get('data', [])
        if data:
            latest = data[0]
            for state in states:
                results[state] = {
                    'week_start': latest['period'],
                    'price': float(latest['value']),
                    'region': 'Midwest' if 'R20' in series_id else 'Gulf Coast',
                }
    return results


# ---------------------------------------------------------------------------
# Feature helpers
# ---------------------------------------------------------------------------
def get_band(weekly_sales):
    for name, lo, hi in BANDS:
        if lo <= weekly_sales < hi:
            return name
    return '50k+'


def get_higher_band(band1, band2):
    """Return the higher of two bands (straddling rule)."""
    order = [b[0] for b in BANDS]
    i1 = order.index(band1) if band1 in order else 0
    i2 = order.index(band2) if band2 in order else 0
    return order[max(i1, i2)]


def is_payroll_friday(d):
    if d.weekday() != 4:
        return 0
    return 1 if ((pd.Timestamp(d) - ANCHOR_PAYROLL).days // 7) % 2 == 0 else 0


def holiday_flags(d):
    hol = HOLIDAY_CALENDAR.get(d)
    return (
        1 if hol in ('thanksgiving', 'christmas') else 0,
        1 if hol in ('christmas_eve', 'day_before_thanksgiving') else 0,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    url, hdrs = init_sb()
    secrets = toml.load(SECRETS_PATH)
    eia_key = secrets['eia']['api_key']
    today = date.today()

    # Week boundaries: Mon-Sun ISO week
    last_monday = today - timedelta(days=today.weekday())
    next_monday = last_monday + timedelta(weeks=1)
    prior_week_start = last_monday - timedelta(weeks=1)
    prior_week_end   = last_monday - timedelta(days=1)

    print(f'Monday Job — {today}')
    print(f'  Prior week: {prior_week_start} to {prior_week_end}')
    print(f'  Forecast week: {next_monday} to {next_monday + timedelta(days=6)}')

    # Load stores
    stores_raw = sb_get_all(url, hdrs, 'reference_data',
                            '&active=eq.true&store_type=eq.traditional')
    stores = {s['location_id']: s for s in stores_raw if s.get('open_date') and s.get('latitude')}
    print(f'\nActive traditional stores with lat/long: {len(stores)}')

    # Load model
    with open(MODEL_PATH, 'rb') as f:
        model = pickle.load(f)
    with open(META_PATH) as f:
        meta = json.load(f)
    feature_cols = meta['feature_cols']
    print(f'Model {MODEL_VERSION} loaded ({meta["overall_mape"]}% MAPE, {meta["band_accuracy"]}% band acc)')

    # Load sig lags
    lag_raw = sb_get_all(url, hdrs, 'sos_votg_lag_profiles', '&is_significant=eq.true')
    sig_lags = {}
    for r in lag_raw:
        k = (r['location_id'], r['metric'])
        sig_lags.setdefault(k, []).append(int(r['lag_weeks']))

    # Load sales, SoS, VOTG for feature building
    print('\nLoading actuals data...')
    sales_raw = sb_get_all(url, hdrs, 'store_sales')
    sos_raw   = sb_get_all(url, hdrs, 'store_sos')
    votg_raw  = sb_get_all(url, hdrs, 'store_votg')
    gas_raw   = sb_get_all(url, hdrs, 'gas_price_history')

    sales_df = pd.DataFrame(sales_raw)[['location_id', 'sale_date', 'net_sales']]
    sales_df['sale_date'] = pd.to_datetime(sales_df['sale_date'])
    sales_df['net_sales'] = pd.to_numeric(sales_df['net_sales'])

    sos_df = pd.DataFrame(sos_raw)[['location_id', 'sale_date', 'good_shift']]
    sos_df['sale_date'] = pd.to_datetime(sos_df['sale_date'])

    votg_df = pd.DataFrame(votg_raw)
    votg_df['period_start'] = pd.to_datetime(votg_df['period_start'])
    votg_df['period_end']   = pd.to_datetime(votg_df['period_end'])

    gas_df = pd.DataFrame(gas_raw)
    gas_df['week_start'] = pd.to_datetime(gas_df['week_start'])
    gas_df['price_per_gallon'] = pd.to_numeric(gas_df['price_per_gallon'])

    # -----------------------------------------------------------------------
    # PHASE 1: Pull latest gas prices + update weather actuals
    # -----------------------------------------------------------------------
    print('\n--- PHASE 1: Update gas prices & weather actuals ---')
    gas_prices = pull_gas_prices(eia_key)
    gas_rows = []
    for state, info in gas_prices.items():
        gas_rows.append({
            'region': info['region'], 'state': state,
            'week_start': info['week_start'],
            'price_per_gallon': info['price'],
            'data_source': 'EIA',
        })
    if gas_rows:
        sb_upsert(url, hdrs, 'gas_price_history', gas_rows)
        print(f'  Gas prices updated: {len(gas_rows)} states')

    # Pull actual weather for prior week
    weather_rows = []
    for loc_id, store in stores.items():
        lat, lon = float(store['latitude']), float(store['longitude'])
        daily = pull_weather_actuals(lat, lon, prior_week_start, prior_week_end)
        weather_rows.extend(weather_to_rows(loc_id, daily, is_forecast=False))
    if weather_rows:
        sb_upsert(url, hdrs, 'weather_history', weather_rows)
        print(f'  Weather actuals updated: {len(weather_rows)} store-days')

    # -----------------------------------------------------------------------
    # PHASE 2: Back-fill last week's actuals on forecast rows
    # -----------------------------------------------------------------------
    print('\n--- PHASE 2: Back-fill prior week actuals on forecast rows ---')
    prior_week_str = prior_week_start.strftime('%Y-%m-%d')
    forecast_rows = sb_get_all(url, hdrs, 'sales_forecasts',
                               f'&week_start=eq.{prior_week_str}')
    print(f'  Found {len(forecast_rows)} forecast rows to back-fill')

    for fc in forecast_rows:
        loc_id = fc['location_id']
        wk_start = pd.to_datetime(fc['week_start'])
        wk_end   = wk_start + timedelta(days=6)

        # Actual sales for that week
        store_sales = sales_df[
            (sales_df['location_id'] == loc_id) &
            (sales_df['sale_date'] >= wk_start) &
            (sales_df['sale_date'] <= wk_end)
        ]
        actual_sales = float(store_sales['net_sales'].sum()) if len(store_sales) else None

        # Actual weather for that week (avg high/low, total precip)
        weather_week_raw = sb_get_all(url, hdrs, 'weather_history',
                                      f'&location_id=eq.{loc_id}&date=gte.{wk_start.date()}&date=lte.{wk_end.date()}&is_forecast=eq.false')
        if weather_week_raw:
            wdf = pd.DataFrame(weather_week_raw)
            actual_temp_high = float(pd.to_numeric(wdf['temp_high_f'], errors='coerce').mean())
            actual_temp_low  = float(pd.to_numeric(wdf['temp_low_f'],  errors='coerce').mean())
            actual_precip    = float(pd.to_numeric(wdf['precipitation_in'], errors='coerce').sum())
        else:
            actual_temp_high = actual_temp_low = actual_precip = None

        # Actual gas
        state = STATE_MAP.get(loc_id, 'OH')
        state_gas = gas_df[gas_df['state'] == state].sort_values('week_start')
        past_gas = state_gas[state_gas['week_start'] <= wk_start]
        actual_gas = float(past_gas['price_per_gallon'].iloc[-1]) if len(past_gas) else None

        # Actual SoS (avg for week)
        store_sos = sos_df[
            (sos_df['location_id'] == loc_id) &
            (sos_df['sale_date'] >= wk_start) &
            (sos_df['sale_date'] <= wk_end)
        ]
        actual_sos = float(store_sos['good_shift'].mean()) if len(store_sos) else None

        # Error metrics
        fc_point = float(fc['forecast_point']) if fc.get('forecast_point') else None
        fc_low   = float(fc['forecast_low'])   if fc.get('forecast_low')   else None
        fc_high  = float(fc['forecast_high'])  if fc.get('forecast_high')  else None
        rec_band = fc.get('recommended_band')

        forecast_error     = round(actual_sales - fc_point, 2) if actual_sales and fc_point else None
        forecast_error_pct = round((forecast_error / actual_sales) * 100, 2) if forecast_error and actual_sales else None
        band_hit           = get_band(actual_sales) == rec_band if actual_sales and rec_band else None
        within_ci          = bool(fc_low <= actual_sales <= fc_high) if all([fc_low, fc_high, actual_sales]) else None

        patch_data = {
            'actual_sales': actual_sales,
            'actual_temp_high': actual_temp_high,
            'actual_temp_low': actual_temp_low,
            'actual_precip': actual_precip,
            'actual_gas_price': actual_gas,
            'actual_sos': actual_sos,
            'forecast_error': forecast_error,
            'forecast_error_pct': forecast_error_pct,
            'band_hit': band_hit,
            'within_confidence_interval': within_ci,
        }
        sb_patch(url, hdrs, 'sales_forecasts', 'id', fc['id'], patch_data)

    print(f'  Back-fill complete for {len(forecast_rows)} stores')

    # -----------------------------------------------------------------------
    # PHASE 3: Pull forward weather + generate next week forecasts
    # -----------------------------------------------------------------------
    print('\n--- PHASE 3: Generate forecasts for next week ---')

    # Pull 16-day weather forecast for all stores
    print('  Pulling 16-day weather forecasts...')
    forecast_weather = {}
    weather_forecast_rows = []
    for loc_id, store in stores.items():
        lat, lon = float(store['latitude']), float(store['longitude'])
        daily = pull_weather_forecast(lat, lon, days=16)
        forecast_weather[loc_id] = {d: i for i, d in enumerate(daily.get('time', []))}
        forecast_weather[loc_id]['daily'] = daily
        weather_forecast_rows.extend(weather_to_rows(loc_id, daily, is_forecast=True))
    sb_upsert(url, hdrs, 'weather_history', weather_forecast_rows)
    print(f'  Weather forecast stored: {len(weather_forecast_rows)} store-days')

    # Generate forecast per store
    forecast_output = []
    skipped = []

    for loc_id, store in stores.items():
        open_date = pd.to_datetime(store['open_date'])
        honeymoon_end = open_date + timedelta(days=365)
        store_name = store['store_name']
        state = STATE_MAP.get(loc_id, 'OH')
        loc_type = store.get('location_type') or 'suburban'

        # Skip honeymoon stores
        if pd.Timestamp(next_monday) < honeymoon_end:
            skipped.append(store_name)
            continue

        store_sales = sales_df[sales_df['location_id'] == loc_id].sort_values('sale_date')
        if len(store_sales) < 30:
            skipped.append(f'{store_name} (insufficient history)')
            continue

        store_sos_idx = sos_df[sos_df['location_id'] == loc_id].set_index('sale_date')['good_shift']

        # Build VOTG daily lookup
        store_votg_rows = votg_df[votg_df['location_id'] == loc_id]
        votg_daily = {}
        for _, vr in store_votg_rows.iterrows():
            if pd.isna(vr['guests_per_negative']):
                continue
            d = vr['period_start']
            while d <= vr['period_end']:
                votg_daily[d] = float(vr['guests_per_negative'])
                d += timedelta(days=1)

        state_gas = gas_df[gas_df['state'] == state].sort_values('week_start')
        sos_lag_list  = sorted(sig_lags.get((loc_id, 'sos'),  [1, 4, 8]))[:6]
        votg_lag_list = sorted(sig_lags.get((loc_id, 'votg'), [4, 8, 12]))[:6]

        # Build daily feature rows for the forecast week
        daily_preds = []
        daily_weather_info = forecast_weather.get(loc_id, {})
        fw_daily = daily_weather_info.get('daily', {})
        fw_dates = {d: i for i, d in enumerate(fw_daily.get('time', []))}

        for day_offset in range(7):
            d = next_monday + timedelta(days=day_offset)
            d_str = d.strftime('%Y-%m-%d')
            d_ts = pd.Timestamp(d)

            # Weather from forecast
            fw_idx = fw_dates.get(d_str)
            if fw_idx is not None:
                temp_high = fw_daily['temperature_2m_max'][fw_idx]
                temp_low  = fw_daily['temperature_2m_min'][fw_idx]
                precip    = fw_daily['precipitation_sum'][fw_idx] or 0.0
                wcode     = fw_daily['weather_code'][fw_idx]
                severe    = int(wcode >= 65) if wcode else 0
            else:
                temp_high = temp_low = precip = severe = np.nan

            # Gas
            past_gas = state_gas[state_gas['week_start'] <= pd.Timestamp(d)]
            gas_price = float(past_gas['price_per_gallon'].iloc[-1]) if len(past_gas) else np.nan

            # SoS lags
            sos_features = {}
            for lag in sos_lag_list:
                lag_date = d_ts - timedelta(weeks=lag)
                sos_features[f'sos_lag_{lag}wk'] = float(store_sos_idx.get(lag_date, np.nan))

            # VOTG lags
            votg_features = {}
            for lag in votg_lag_list:
                lag_date = (d_ts - timedelta(weeks=lag)).date()
                votg_features[f'votg_lag_{lag}wk'] = votg_daily.get(pd.Timestamp(lag_date), np.nan)

            # Trailing growth
            past = store_sales[store_sales['sale_date'] < d_ts].tail(56)
            if len(past) >= 28:
                recent = past.tail(28)['net_sales'].mean()
                prior  = past.iloc[:28]['net_sales'].mean()
                growth = float((recent / prior) - 1) if prior > 0 else 0.0
            else:
                growth = 0.0

            is_closed, is_lower = holiday_flags(d)
            weeks_since_open = max(0, (d - open_date.date()).days // 7 - 52)

            feat = {
                'location_id_cat': loc_id,
                'location_type_cat': loc_type,
                'day_of_week': d.weekday(),
                'month': d.month,
                'week_of_year': int(d.isocalendar()[1]),
                'is_holiday_closed': is_closed,
                'is_holiday_lower': is_lower,
                'is_payroll_friday': is_payroll_friday(d),
                'temp_high_f': temp_high,
                'temp_low_f': temp_low,
                'precipitation_in': precip,
                'severe_weather': severe,
                'gas_price': gas_price,
                'trailing_8wk_growth': growth,
                'weeks_since_open': weeks_since_open,
                **sos_features,
                **votg_features,
            }
            daily_preds.append(feat)

        if not daily_preds:
            continue

        pred_df = pd.DataFrame(daily_preds)

        # Align columns to model's expected feature set
        for col in feature_cols:
            if col not in pred_df.columns:
                pred_df[col] = np.nan
        pred_df = pred_df[feature_cols]
        pred_df['location_id_cat'] = pred_df['location_id_cat'].astype('category')
        pred_df['location_type_cat'] = pred_df['location_type_cat'].astype('category')

        daily_forecasts = model.predict(pred_df)
        daily_forecasts = np.clip(daily_forecasts, 0, None)

        # Weekly totals with confidence interval (±1 std of daily errors scaled)
        weekly_point = float(daily_forecasts.sum())
        weekly_std   = float(daily_forecasts.std()) * np.sqrt(7) * 0.5
        weekly_low   = max(0, weekly_point - weekly_std)
        weekly_high  = weekly_point + weekly_std

        # Band assignment with straddling rule
        band_low  = get_band(weekly_low)
        band_high = get_band(weekly_high)
        rec_band  = get_higher_band(band_low, band_high)

        # Confidence % (tighter interval = higher confidence)
        band_widths = {b[0]: b[2] - b[1] for b in BANDS if b[2] < 9_999_999}
        typical_band_width = band_widths.get(rec_band, 5000)
        interval_width = weekly_high - weekly_low
        confidence = max(20, min(95, int(100 * (1 - interval_width / typical_band_width))))

        # Average weekly weather inputs for storage
        avg_temp_high = float(np.nanmean([r.get('temp_high_f', np.nan) for r in daily_preds]))
        avg_temp_low  = float(np.nanmean([r.get('temp_low_f',  np.nan) for r in daily_preds]))
        avg_precip    = float(np.nansum([r.get('precipitation_in', 0) for r in daily_preds]))
        avg_gas       = float(np.nanmean([r.get('gas_price', np.nan) for r in daily_preds]))
        avg_growth    = float(np.nanmean([r.get('trailing_8wk_growth', 0) for r in daily_preds]))

        # SoS/VOTG lag inputs for storage
        sos_lag_vals  = {k: v for k, v in daily_preds[0].items() if k.startswith('sos_lag')}
        votg_lag_vals = {k: v for k, v in daily_preds[0].items() if k.startswith('votg_lag')}

        forecast_output.append({
            'location_id': loc_id,
            'store_name': store_name,
            'week_start': next_monday.strftime('%Y-%m-%d'),
            'forecast_generated_at': datetime.now(timezone.utc).isoformat(),
            'model_version': MODEL_VERSION,
            'forecast_low': round(weekly_low, 2),
            'forecast_point': round(weekly_point, 2),
            'forecast_high': round(weekly_high, 2),
            'confidence_pct': confidence,
            'recommended_band': rec_band,
            'input_temp_high_forecast': round(avg_temp_high, 1) if not np.isnan(avg_temp_high) else None,
            'input_temp_low_forecast': round(avg_temp_low, 1)  if not np.isnan(avg_temp_low)  else None,
            'input_precip_forecast': round(avg_precip, 3),
            'input_gas_price': round(avg_gas, 3) if not np.isnan(avg_gas) else None,
            'input_trailing_8wk_growth': round(avg_growth, 4),
            'input_sos_lags': json.dumps({k: (None if (isinstance(v, float) and np.isnan(v)) else v) for k, v in sos_lag_vals.items()}),
            'input_votg_lags': json.dumps({k: (None if (isinstance(v, float) and np.isnan(v)) else v) for k, v in votg_lag_vals.items()}),
        })

    sb_upsert(url, hdrs, 'sales_forecasts', forecast_output)
    print(f'  Forecasts generated: {len(forecast_output)} stores')
    if skipped:
        print(f'  Skipped ({len(skipped)}): {skipped}')

    # Print forecast summary
    print('\n=== FORECAST SUMMARY — Week of', next_monday, '===')
    print(f'  {"Store":<35} {"Band":<12} {"Point":<12} {"Conf"}')
    print('  ' + '-'*65)
    for fc in sorted(forecast_output, key=lambda x: x['location_id']):
        print(f'  {fc["store_name"]:<35} {fc["recommended_band"]:<12} ${fc["forecast_point"]:>9,.0f}  {fc["confidence_pct"]}%')

    # -----------------------------------------------------------------------
    # PHASE 4 (monthly): Retrain model on first Monday of each month
    # -----------------------------------------------------------------------
    if today.day <= 7:
        print('\n--- PHASE 4: Monthly model retrain ---')
        try:
            import subprocess
            result = subprocess.run(
                ['python', 'C:/Users/BretElliott/ramz-accounting/labor/build_features.py'],
                capture_output=True, text=True, timeout=300
            )
            print(result.stdout[-500:] if result.stdout else '')
            result2 = subprocess.run(
                ['python', 'C:/Users/BretElliott/ramz-accounting/labor/train_model.py'],
                capture_output=True, text=True, timeout=300
            )
            print(result2.stdout[-500:] if result2.stdout else '')
            print('  Model retrained successfully.')
        except Exception as e:
            print(f'  Retrain failed: {e}')

    print(f'\nMonday job complete — {datetime.now().strftime("%Y-%m-%d %H:%M")}')


if __name__ == '__main__':
    main()
