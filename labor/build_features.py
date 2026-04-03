"""
Sales Forecasting Engine — Step 1: Feature Engineering
Builds the full feature matrix for LightGBM model training.
"""
import pandas as pd
import numpy as np
import toml
import sys
import requests
from datetime import timedelta

sys.path.insert(0, 'C:/Users/BretElliott/ramz-accounting')
secrets = toml.load('C:/Users/BretElliott/ramz-accounting/.streamlit/secrets.toml')
url = secrets['supabase']['url']
key = secrets['supabase']['key']
headers = {'apikey': key, 'Authorization': f'Bearer {key}', 'Content-Type': 'application/json'}


def sb_get_all(table, extra=''):
    rows, offset = [], 0
    while True:
        h = {**headers, 'Range-Unit': 'items', 'Range': f'{offset}-{offset+999}'}
        r = requests.get(f'{url}/rest/v1/{table}?select=*{extra}', headers=h, timeout=60)
        batch = r.json()
        if not isinstance(batch, list) or not batch:
            break
        rows.extend(batch)
        if len(batch) < 1000:
            break
        offset += 1000
    return rows


# State mapping by store
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

ANCHOR_PAYROLL = pd.Timestamp('2023-01-06')  # known payroll Friday


def holiday_flag(d):
    m, day, wd = d.month, d.day, d.weekday()
    if m == 11 and wd == 3 and 22 <= day <= 28:
        return 'thanksgiving'
    if m == 12 and day == 25:
        return 'christmas'
    if m == 12 and day == 24:
        return 'christmas_eve'
    if m == 11 and wd == 2 and 21 <= day <= 27:
        return 'day_before_thanksgiving'
    return None


def is_payroll_friday(d):
    if d.weekday() != 4:
        return 0
    return 1 if ((d - ANCHOR_PAYROLL).days // 7) % 2 == 0 else 0


def main():
    print('Loading all data from Supabase...')
    stores_raw = sb_get_all('reference_data', '&active=eq.true&store_type=eq.traditional')
    stores = {s['location_id']: s for s in stores_raw if s.get('open_date')}

    sales_raw   = sb_get_all('store_sales')
    weather_raw = sb_get_all('weather_history')
    gas_raw     = sb_get_all('gas_price_history')
    sos_raw     = sb_get_all('store_sos')
    votg_raw    = sb_get_all('store_votg')
    lag_raw     = sb_get_all('sos_votg_lag_profiles', '&is_significant=eq.true')

    print(f'  Sales: {len(sales_raw):,} | Weather: {len(weather_raw):,} | Gas: {len(gas_raw):,} | SoS: {len(sos_raw):,} | VOTG: {len(votg_raw):,} | Sig lags: {len(lag_raw)}')

    # Build dataframes
    sales_df = pd.DataFrame(sales_raw)[['location_id', 'sale_date', 'net_sales']]
    sales_df['sale_date'] = pd.to_datetime(sales_df['sale_date'])
    sales_df['net_sales'] = pd.to_numeric(sales_df['net_sales'])

    weather_df = pd.DataFrame(weather_raw)[['location_id', 'date', 'temp_high_f', 'temp_low_f', 'precipitation_in', 'severe_weather_flag']]
    weather_df['date'] = pd.to_datetime(weather_df['date'])
    for col in ['temp_high_f', 'temp_low_f', 'precipitation_in']:
        weather_df[col] = pd.to_numeric(weather_df[col])

    gas_df = pd.DataFrame(gas_raw)[['state', 'week_start', 'price_per_gallon']]
    gas_df['week_start'] = pd.to_datetime(gas_df['week_start'])
    gas_df['price_per_gallon'] = pd.to_numeric(gas_df['price_per_gallon'])

    sos_df = pd.DataFrame(sos_raw)[['location_id', 'sale_date', 'good_shift']]
    sos_df['sale_date'] = pd.to_datetime(sos_df['sale_date'])
    sos_df['good_shift'] = pd.to_numeric(sos_df['good_shift'])

    votg_df = pd.DataFrame(votg_raw)[['location_id', 'period_start', 'period_end', 'guests_per_negative']]
    votg_df['period_start'] = pd.to_datetime(votg_df['period_start'])
    votg_df['period_end'] = pd.to_datetime(votg_df['period_end'])
    votg_df['guests_per_negative'] = pd.to_numeric(votg_df['guests_per_negative'])

    # Significant lag profiles
    sig_lags = {}
    for r in lag_raw:
        k = (r['location_id'], r['metric'])
        sig_lags.setdefault(k, []).append(int(r['lag_weeks']))

    print('Building feature matrix per store...')
    all_rows = []

    for loc_id, store in stores.items():
        open_date = pd.to_datetime(store['open_date'])
        cutoff = open_date + timedelta(days=365)
        state = STATE_MAP.get(loc_id, 'OH')
        loc_type = store.get('location_type') or 'suburban'

        store_sales = sales_df[sales_df['location_id'] == loc_id].copy()
        store_sales = store_sales[store_sales['sale_date'] >= cutoff].sort_values('sale_date').reset_index(drop=True)
        if len(store_sales) < 30:
            print(f'  Skipping {loc_id} — only {len(store_sales)} post-honeymoon days')
            continue

        store_weather = weather_df[weather_df['location_id'] == loc_id].set_index('date')
        store_sos_idx = sos_df[sos_df['location_id'] == loc_id].set_index('sale_date')['good_shift']

        # Expand VOTG to daily lookup
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

        sos_lag_list = sorted(sig_lags.get((loc_id, 'sos'), [1, 4, 8]))[:6]
        votg_lag_list = sorted(sig_lags.get((loc_id, 'votg'), [4, 8, 12]))[:6]

        for idx, row in store_sales.iterrows():
            d = row['sale_date']
            sales = row['net_sales']

            # Weather
            w = store_weather.loc[d] if d in store_weather.index else None
            temp_high = float(w['temp_high_f']) if w is not None and pd.notna(w['temp_high_f']) else np.nan
            temp_low  = float(w['temp_low_f'])  if w is not None and pd.notna(w['temp_low_f'])  else np.nan
            precip    = float(w['precipitation_in']) if w is not None and pd.notna(w['precipitation_in']) else 0.0
            severe    = int(w['severe_weather_flag']) if w is not None and pd.notna(w['severe_weather_flag']) else 0

            # Gas - nearest past week
            past_gas = state_gas[state_gas['week_start'] <= d]
            gas_price = float(past_gas['price_per_gallon'].iloc[-1]) if len(past_gas) else np.nan

            # SoS lags
            sos_features = {}
            for lag in sos_lag_list:
                lag_date = d - timedelta(weeks=lag)
                sos_features[f'sos_lag_{lag}wk'] = float(store_sos_idx.get(lag_date, np.nan))

            # VOTG lags
            votg_features = {}
            for lag in votg_lag_list:
                lag_date = d - timedelta(weeks=lag)
                votg_features[f'votg_lag_{lag}wk'] = votg_daily.get(lag_date, np.nan)

            # Trailing 8-week growth rate (using days 1-28 vs 29-56)
            past = store_sales[store_sales['sale_date'] < d].tail(56)
            if len(past) >= 28:
                recent = past.tail(28)['net_sales'].mean()
                prior  = past.iloc[:28]['net_sales'].mean()
                growth_8wk = float((recent / prior) - 1) if prior > 0 else 0.0
            else:
                growth_8wk = np.nan

            hol = holiday_flag(d)
            weeks_since_open = max(0, (d - open_date).days // 7 - 52)

            feat = {
                'location_id': loc_id,
                'sale_date': d,
                'net_sales': sales,
                'day_of_week': d.weekday(),
                'month': d.month,
                'week_of_year': int(d.isocalendar()[1]),
                'is_holiday_closed': 1 if hol in ('thanksgiving', 'christmas') else 0,
                'is_holiday_lower': 1 if hol in ('christmas_eve', 'day_before_thanksgiving') else 0,
                'is_payroll_friday': is_payroll_friday(d),
                'temp_high_f': temp_high,
                'temp_low_f': temp_low,
                'precipitation_in': precip,
                'severe_weather': severe,
                'gas_price': gas_price,
                'trailing_8wk_growth': growth_8wk,
                'weeks_since_open': weeks_since_open,
                'location_type': loc_type,
                **sos_features,
                **votg_features,
            }
            all_rows.append(feat)

        print(f'  {loc_id} ({store["store_name"]}): {len(store_sales)} days')

    df = pd.DataFrame(all_rows)
    print(f'\nFeature matrix complete:')
    print(f'  Rows: {len(df):,}')
    print(f'  Columns: {len(df.columns)}')
    print(f'  Stores: {df["location_id"].nunique()}')
    print(f'  Date range: {df["sale_date"].min().date()} to {df["sale_date"].max().date()}')
    print(f'  Feature columns: {[c for c in df.columns if c not in ["location_id","sale_date","net_sales"]]}')

    out = 'C:/Users/BretElliott/ramz-accounting/labor/feature_matrix.pkl'
    df.to_pickle(out)
    print(f'\nSaved to {out}')
    return df


if __name__ == '__main__':
    main()
