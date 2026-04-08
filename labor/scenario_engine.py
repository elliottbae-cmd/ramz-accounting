"""
Scenario Engine — 6-Month Forward Sales Projection
----------------------------------------------------
Generates a 26-week forward-looking sales projection for all active stores
using the live LightGBM model. Three scenarios:

  conservative — SOS/VOTG hold flat at current 4-week average; gas flat
  base         — SOS/VOTG continue their current 12-week trend; gas trends
  optimistic   — SOS/VOTG improve to portfolio 75th-percentile by week 26;
                 gas flat (no headwind assumed)

The engine always loads the live model files (lgbm_model_v1.pkl +
model_meta_v1.json), so it automatically reflects the updated model
after each monthly retrain — no code changes needed.

Auto-adjust mechanism:
  - Model files are read fresh on every ScenarioEngine() instantiation.
  - The Streamlit page wraps instantiation in @st.cache_data keyed on
    the model file's mtime, so a retrain invalidates the cache and the
    next page load picks up the new model automatically.

Usage (from Streamlit page):
    from labor.scenario_engine import ScenarioEngine, MODEL_PATH
    engine = ScenarioEngine(sb_client)
    combined_df = engine.run_all_scenarios()
    # combined_df columns:
    #   location_id, store_name, scenario, week_num, week_start, week_end,
    #   forecast_point, forecast_low, forecast_high, recommended_band,
    #   sos_assumed, votg_assumed, gas_assumed

Recursive loop:
  For each scenario, weeks 1-26 are forecast one at a time. Each week's
  predicted daily sales feed into the next week's trailing_8wk_growth
  feature, propagating momentum forward. SOS and VOTG projections are
  pre-computed upfront (trend extrapolation), then used as lag inputs
  for every future date in the loop.
"""

import json
import pickle
import numpy as np
import pandas as pd
from datetime import date, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_LABOR_DIR = Path(__file__).parent
MODEL_PATH = _LABOR_DIR / 'lgbm_model_v1.pkl'
META_PATH  = _LABOR_DIR / 'model_meta_v1.json'

# ---------------------------------------------------------------------------
# Constants (mirrored from monday_job.py)
# ---------------------------------------------------------------------------
BANDS = [
    ('<25k',     0,      25_000),
    ('25k-30k',  25_000, 30_000),
    ('30k-35k',  30_000, 35_000),
    ('35k-40k',  35_000, 40_000),
    ('40k-45k',  40_000, 45_000),
    ('45k-50k',  45_000, 50_000),
    ('50k+',     50_000, 9_999_999),
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
    date(2024, 11, 28): 'thanksgiving',         date(2025, 11, 27): 'thanksgiving',
    date(2026, 11, 26): 'thanksgiving',
    date(2023, 12, 25): 'christmas',             date(2024, 12, 25): 'christmas',
    date(2025, 12, 25): 'christmas',             date(2026, 12, 25): 'christmas',
    date(2023, 12, 24): 'christmas_eve',         date(2024, 12, 24): 'christmas_eve',
    date(2025, 12, 24): 'christmas_eve',         date(2026, 12, 24): 'christmas_eve',
    date(2023, 11, 22): 'day_before_thanksgiving',
    date(2024, 11, 27): 'day_before_thanksgiving',
    date(2025, 11, 26): 'day_before_thanksgiving',
    date(2026, 11, 25): 'day_before_thanksgiving',
}

SCENARIOS   = ('conservative', 'base', 'optimistic')
N_WEEKS     = 26      # 6-month horizon
SOS_MIN     = 0.55    # floor for projected SOS (good-shift rate)
GAS_CAP     = 0.50    # max $ drift from current price over 26 weeks (base scenario)
GROWTH_CAP  = 0.15    # cap trailing_8wk_growth at ±15% to prevent compounding bias


# ---------------------------------------------------------------------------
# ScenarioEngine
# ---------------------------------------------------------------------------
class ScenarioEngine:
    """
    Load the live model once, then run any combination of scenarios on demand.
    Designed to be instantiated once per Streamlit session (via st.cache_data).
    """

    def __init__(self, sb):
        """
        Parameters
        ----------
        sb : supabase.Client
            Authenticated Supabase client (service role).
        """
        self.sb = sb
        self._load_model()
        self._load_all_data()
        self._compute_portfolio_percentiles()

    # ── Model loading ────────────────────────────────────────────────────────

    def _load_model(self):
        with open(MODEL_PATH, 'rb') as f:
            self.model = pickle.load(f)
        with open(META_PATH) as f:
            self.meta = json.load(f)

        self.feature_cols = self.meta['feature_cols']
        self.base_mape    = self.meta.get('overall_mape', 13.5) / 100.0

        # Parse which SOS/VOTG lag numbers appear in the model's feature set
        self.sos_lags_in_model = sorted(
            int(c.replace('sos_lag_', '').replace('wk', ''))
            for c in self.feature_cols if c.startswith('sos_lag_')
        )
        self.votg_lags_in_model = sorted(
            int(c.replace('votg_lag_', '').replace('wk', ''))
            for c in self.feature_cols if c.startswith('votg_lag_')
        )

    # ── Data loading ─────────────────────────────────────────────────────────

    def _fetch(self, table, filters=None, date_col=None, date_gte=None, select='*'):
        """Paginate through a Supabase table and return all rows as a list."""
        all_rows = []
        page_size = 1000
        offset = 0
        while True:
            q = self.sb.table(table).select(select)
            if filters:
                for k, v in filters.items():
                    q = q.eq(k, v)
            if date_col and date_gte:
                q = q.gte(date_col, date_gte)
            q = q.range(offset, offset + page_size - 1)
            batch = (q.execute().data or [])
            all_rows.extend(batch)
            if len(batch) < page_size:
                break
            offset += page_size
        return all_rows

    def _load_all_data(self):
        today       = date.today()
        cutoff_52wk = str(today - timedelta(weeks=52))

        # Active stores
        stores_raw = self._fetch(
            'reference_data',
            filters={'active': True, 'store_type': 'traditional'},
        )
        self.stores = {s['location_id']: s for s in stores_raw if s.get('latitude')}

        # Opening dates
        open_dates_raw = self._fetch('store_open_dates')
        open_map = {r['location_id']: r['opening_date'] for r in open_dates_raw}
        for loc_id, store in self.stores.items():
            store['open_date'] = open_map.get(loc_id)
        self.stores = {k: v for k, v in self.stores.items() if v.get('open_date')}

        # Significant lag profiles (all lags, not capped at 6 like monday_job.py)
        lag_raw = self._fetch('sos_votg_lag_profiles', filters={'is_significant': True})
        self.sig_lags = {}
        for r in lag_raw:
            key = (r['location_id'], r['metric'])
            self.sig_lags.setdefault(key, []).append(int(r['lag_weeks']))

        # SOS history — last 52 weeks
        sos_raw = self._fetch(
            'store_sos',
            date_col='sale_date', date_gte=cutoff_52wk,
            select='location_id,sale_date,good_shift',
        )
        sos_df = pd.DataFrame(sos_raw) if sos_raw else pd.DataFrame(
            columns=['location_id', 'sale_date', 'good_shift'])
        if not sos_df.empty:
            sos_df['sale_date']  = pd.to_datetime(sos_df['sale_date'])
            sos_df['good_shift'] = pd.to_numeric(sos_df['good_shift'], errors='coerce')
        self.sos_df = sos_df

        # VOTG history — all periods (periods can span months; no date filter here)
        votg_raw = self._fetch(
            'store_votg',
            select='location_id,period_start,period_end,guests_per_negative',
        )
        votg_df = pd.DataFrame(votg_raw) if votg_raw else pd.DataFrame(
            columns=['location_id', 'period_start', 'period_end', 'guests_per_negative'])
        if not votg_df.empty:
            votg_df['period_start']        = pd.to_datetime(votg_df['period_start'])
            votg_df['period_end']          = pd.to_datetime(votg_df['period_end'])
            votg_df['guests_per_negative'] = pd.to_numeric(
                votg_df['guests_per_negative'], errors='coerce')
            # Keep only periods relevant to the past 52 weeks
            cutoff_ts = pd.Timestamp(cutoff_52wk)
            votg_df = votg_df[votg_df['period_end'] >= cutoff_ts]
        self.votg_df = votg_df

        # Sales history — last 52 weeks (for trailing growth)
        sales_raw = self._fetch(
            'store_sales',
            date_col='sale_date', date_gte=cutoff_52wk,
            select='location_id,sale_date,net_sales',
        )
        sales_df = pd.DataFrame(sales_raw) if sales_raw else pd.DataFrame(
            columns=['location_id', 'sale_date', 'net_sales'])
        if not sales_df.empty:
            sales_df['sale_date'] = pd.to_datetime(sales_df['sale_date'])
            sales_df['net_sales'] = pd.to_numeric(sales_df['net_sales'], errors='coerce')
        self.sales_df = sales_df

        # Gas prices
        gas_raw = self._fetch(
            'gas_price_history',
            select='state,week_start,price_per_gallon',
        )
        gas_df = pd.DataFrame(gas_raw) if gas_raw else pd.DataFrame(
            columns=['state', 'week_start', 'price_per_gallon'])
        if not gas_df.empty:
            gas_df['week_start']       = pd.to_datetime(gas_df['week_start'])
            gas_df['price_per_gallon'] = pd.to_numeric(
                gas_df['price_per_gallon'], errors='coerce')
        self.gas_df = gas_df

        # Climate normals (seeded by seed_climate_normals.py)
        normals_raw = self._fetch('climate_normals')
        self.climate_normals = {}
        for r in normals_raw:
            key = (r['location_id'], int(r['month']), int(r['day_of_week']))
            self.climate_normals[key] = {
                'temp_high': float(r['avg_temp_high_f'] or 70),
                'temp_low':  float(r['avg_temp_low_f']  or 50),
                'precip':    float(r['avg_precip_in']   or 0),
            }

        # Forward weather forecast (within Open-Meteo's 16-day window)
        weather_raw = self._fetch(
            'weather_history',
            filters={'is_forecast': True},
            date_col='date', date_gte=str(today),
            select='location_id,date,temp_high_f,temp_low_f,precipitation_in,weather_code',
        )
        self.weather_fwd = {}
        for r in weather_raw:
            d   = date.fromisoformat(r['date'])
            key = (r['location_id'], d)
            wc  = r.get('weather_code') or 0
            self.weather_fwd[key] = {
                'temp_high': float(r['temp_high_f']       or 70),
                'temp_low':  float(r['temp_low_f']        or 50),
                'precip':    float(r['precipitation_in']  or 0),
                'severe':    1 if wc >= 65 else 0,
            }

    # ── Portfolio-wide percentiles ────────────────────────────────────────────

    def _compute_portfolio_percentiles(self):
        """
        Pre-compute portfolio-wide 75th-percentile SOS and VOTG.
        Used as the target for the optimistic scenario.
        """
        sos_vals  = self.sos_df['good_shift'].dropna().tolist()  if not self.sos_df.empty  else []
        votg_vals = self.votg_df['guests_per_negative'].dropna().tolist() \
                    if not self.votg_df.empty else []

        self._sos_p75  = float(np.percentile(sos_vals,  75)) if sos_vals  else 0.90
        self._votg_p75 = float(np.percentile(votg_vals, 75)) if votg_vals else 200.0

    # ── Per-store trend helpers ───────────────────────────────────────────────

    def _weekly_sos(self, loc_id):
        """Return sorted list of (week_start_date, avg_good_shift) for this store."""
        df = self.sos_df[self.sos_df['location_id'] == loc_id].copy()
        if df.empty:
            return []
        df['dsth']       = (df['sale_date'].dt.weekday - 3) % 7
        df['week_start'] = df['sale_date'] - pd.to_timedelta(df['dsth'], unit='D')
        weekly = df.groupby('week_start')['good_shift'].mean()
        return sorted((k.date(), float(v)) for k, v in weekly.items())

    def _weekly_votg(self, loc_id):
        """Return sorted list of (week_start_date, avg_guests_per_negative)."""
        df = self.votg_df[self.votg_df['location_id'] == loc_id].copy()
        if df.empty:
            return []
        rows = []
        for _, vr in df.iterrows():
            if pd.isna(vr['guests_per_negative']):
                continue
            d = vr['period_start']
            while d <= vr['period_end']:
                rows.append({'date': d, 'votg': float(vr['guests_per_negative'])})
                d += timedelta(days=1)
        if not rows:
            return []
        ddf = pd.DataFrame(rows)
        ddf['dsth']       = (ddf['date'].dt.weekday - 3) % 7
        ddf['week_start'] = ddf['date'] - pd.to_timedelta(ddf['dsth'], unit='D')
        weekly = ddf.groupby('week_start')['votg'].mean()
        return sorted((k.date(), float(v)) for k, v in weekly.items())

    def _ols_slope(self, values):
        """Return OLS slope (per step) of a list of floats. Returns 0 if too few points."""
        if len(values) < 2:
            return 0.0
        x = np.arange(len(values), dtype=float)
        return float(np.polyfit(x, values, 1)[0])

    # ── Projection helpers ────────────────────────────────────────────────────

    def _project_sos(self, loc_id, scenario):
        """
        Return a list of N_WEEKS projected SOS values (weekly average good-shift rate).
        Index 0 corresponds to forecast week 1.
        """
        weekly = self._weekly_sos(loc_id)
        vals   = [v for _, v in weekly]

        if len(vals) < 4:
            current = 0.80
            slope   = 0.0
        else:
            current = float(np.mean(vals[-4:]))
            slope   = self._ols_slope(vals[-12:] if len(vals) >= 12 else vals)

        p75 = self._sos_p75

        result = []
        for w in range(1, N_WEEKS + 1):
            if scenario == 'conservative':
                val = current
            elif scenario == 'base':
                val = current + slope * w
                val = max(SOS_MIN, min(p75, val))
            else:  # optimistic
                val = current + (p75 - current) * (w / N_WEEKS)
                val = max(SOS_MIN, min(1.0, val))
            result.append(round(val, 4))
        return result

    def _project_votg(self, loc_id, scenario):
        """
        Return a list of N_WEEKS projected VOTG values (guests per negative review).
        Higher = better (more guests between each negative review).
        """
        weekly = self._weekly_votg(loc_id)
        vals   = [v for _, v in weekly]

        if len(vals) < 4:
            current = 100.0
            slope   = 0.0
        else:
            current = float(np.mean(vals[-4:]))
            slope   = self._ols_slope(vals[-12:] if len(vals) >= 12 else vals)

        # Floor the base slope at 0 — if recent trend is negative (declining
        # VOTG), hold flat rather than projecting continued deterioration.
        # Rationale: operational improvements observed over the past 6 months
        # are the strategic direction; short-term noise shouldn't drive a
        # declining forecast in the base case.
        base_slope = max(0.0, slope)

        p75 = self._votg_p75

        # Upper target for optimistic: whichever is higher — p75 or current
        # (so stores already above p75 still show improvement, not a haircut)
        opt_target = max(p75, current * 1.20)   # at least 20% above current

        result = []
        for w in range(1, N_WEEKS + 1):
            if scenario == 'conservative':
                val = current
            elif scenario == 'base':
                # base_slope >= 0, so val >= current — never declines
                val = current + base_slope * w
            else:  # optimistic
                # Steady upward trend: 2× floored slope OR ~0.4%/week minimum
                opt_slope = max(base_slope * 2.0, current * 0.004)
                val = min(current + opt_slope * w, opt_target)
            result.append(round(max(0.0, val), 2))
        return result

    def _project_gas(self, state, scenario):
        """Return a list of N_WEEKS projected gas prices for the given state."""
        sg = self.gas_df[self.gas_df['state'] == state].sort_values('week_start')
        if sg.empty:
            return [3.00] * N_WEEKS

        latest = float(sg['price_per_gallon'].iloc[-1])

        # Gas held constant across all scenarios so the chart isolates the
        # impact of SOS/VOTG improvements on sales without gas price noise.
        return [round(latest, 4)] * N_WEEKS

    # ── Feature helpers ───────────────────────────────────────────────────────

    def _get_weather(self, loc_id, d):
        """Return (temp_high, temp_low, precip, severe) for a specific date."""
        w = self.weather_fwd.get((loc_id, d))
        if w:
            return w['temp_high'], w['temp_low'], w['precip'], w['severe']
        # Beyond the 16-day window — use climate normals
        n = self.climate_normals.get((loc_id, d.month, d.weekday()))
        if n:
            return n['temp_high'], n['temp_low'], n['precip'], 0
        return np.nan, np.nan, 0.0, 0

    def _is_payroll_friday(self, d):
        if d.weekday() != 4:
            return 0
        return 1 if ((pd.Timestamp(d) - ANCHOR_PAYROLL).days // 7) % 2 == 0 else 0

    def _holiday_flags(self, d):
        hol = HOLIDAY_CALENDAR.get(d)
        return (
            1 if hol in ('thanksgiving', 'christmas') else 0,
            1 if hol in ('christmas_eve', 'day_before_thanksgiving') else 0,
        )

    def _get_band(self, s):
        for name, lo, hi in BANDS:
            if lo <= s < hi:
                return name
        return '50k+'

    def _get_higher_band(self, b1, b2):
        order = [b[0] for b in BANDS]
        return order[max(
            order.index(b1) if b1 in order else 0,
            order.index(b2) if b2 in order else 0,
        )]

    def _confidence_band(self, weekly_point, week_num, scenario):
        """
        Widening confidence interval.
        Week 1  → ±base_mape   (model's native accuracy, ~13.5%)
        Week 26 → ±2.5× base_mape (~33.75%)
        Optimistic → additional 15% buffer for speculative assumptions.
        """
        multiplier = 1.0 + (week_num - 1) * (1.5 / (N_WEEKS - 1))
        if scenario == 'optimistic':
            multiplier *= 1.15
        half = weekly_point * self.base_mape * multiplier
        return max(0.0, weekly_point - half), weekly_point + half

    # ── Core forecast loop ────────────────────────────────────────────────────

    def run_scenario(self, scenario):
        """
        Run the full N_WEEKS recursive forecast for all active, post-honeymoon stores.

        Parameters
        ----------
        scenario : str
            One of 'conservative', 'base', 'optimistic'.

        Returns
        -------
        pd.DataFrame
            One row per (store, week) with columns:
            location_id, store_name, scenario, week_num, week_start, week_end,
            forecast_point, forecast_low, forecast_high, recommended_band,
            sos_assumed, votg_assumed, gas_assumed
        """
        assert scenario in SCENARIOS, f'Unknown scenario: {scenario}'

        today            = date.today()
        days_since_thu   = (today.weekday() - 3) % 7
        current_week_thu = today - timedelta(days=days_since_thu)
        forecast_start   = current_week_thu + timedelta(weeks=1)  # first forecast Thu

        # ── Per-store pre-computation ────────────────────────────────────────
        store_data = {}
        for loc_id, store in self.stores.items():
            open_date     = pd.to_datetime(store['open_date'])
            honeymoon_end = open_date + timedelta(days=365)
            if pd.Timestamp(forecast_start) < honeymoon_end:
                continue  # skip honeymoon-period stores

            state    = STATE_MAP.get(loc_id, 'OH')
            loc_type = store.get('location_type') or 'suburban'

            # Significant lags for this store (all, not capped at 6)
            sos_sig_lags  = sorted(self.sig_lags.get((loc_id, 'sos'),  [1, 4, 8]))
            votg_sig_lags = sorted(self.sig_lags.get((loc_id, 'votg'), [4, 8, 12]))

            # Scenario projections (lists, index 0 = week 1)
            proj_sos  = self._project_sos(loc_id, scenario)
            proj_votg = self._project_votg(loc_id, scenario)
            proj_gas  = self._project_gas(state, scenario)

            # Historical SOS daily lookup: date -> good_shift
            hist_sos = {}
            store_sos = self.sos_df[self.sos_df['location_id'] == loc_id]
            for _, row in store_sos.iterrows():
                hist_sos[row['sale_date'].date()] = float(row['good_shift'])

            # Historical VOTG daily lookup: date -> guests_per_negative
            hist_votg = {}
            store_votg = self.votg_df[self.votg_df['location_id'] == loc_id]
            for _, vr in store_votg.iterrows():
                if pd.isna(vr['guests_per_negative']):
                    continue
                d = vr['period_start']
                while d <= vr['period_end']:
                    hist_votg[d.date()] = float(vr['guests_per_negative'])
                    d += timedelta(days=1)

            # Extend SOS/VOTG into future forecast weeks
            # For lag lookups inside the loop, a single dict covers all dates.
            ext_sos  = dict(hist_sos)
            ext_votg = dict(hist_votg)
            for w_off in range(N_WEEKS):
                wk_start = forecast_start + timedelta(weeks=w_off)
                for d_off in range(7):
                    d = (wk_start + timedelta(days=d_off))
                    ext_sos[d]  = proj_sos[w_off]
                    ext_votg[d] = proj_votg[w_off]

            # Historical sales for trailing growth (date -> net_sales)
            store_sales_df = self.sales_df[self.sales_df['location_id'] == loc_id] \
                                 .sort_values('sale_date')
            hist_sales = {
                row['sale_date'].date(): float(row['net_sales'])
                for _, row in store_sales_df.iterrows()
            }

            weeks_open_base = max(
                0, (forecast_start - open_date.date()).days // 7 - 52
            )

            store_data[loc_id] = {
                'store_name':      store['store_name'],
                'loc_type':        loc_type,
                'proj_sos':        proj_sos,
                'proj_votg':       proj_votg,
                'proj_gas':        proj_gas,
                'sos_sig_lags':    sos_sig_lags,
                'votg_sig_lags':   votg_sig_lags,
                'ext_sos':         ext_sos,
                'ext_votg':        ext_votg,
                'hist_sales':      hist_sales,       # grows with synthetic preds
                'weeks_open_base': weeks_open_base,
            }

        if not store_data:
            return pd.DataFrame()

        # ── Week-by-week recursive loop ──────────────────────────────────────
        results = []
        loc_ids = list(store_data.keys())

        for w_off in range(N_WEEKS):
            week_num   = w_off + 1
            week_start = forecast_start + timedelta(weeks=w_off)

            # Build batch: 7 rows per store (one per day of the forecast week)
            batch_rows = []
            batch_meta = []   # (loc_id, day_date) for each row

            for loc_id in loc_ids:
                sd           = store_data[loc_id]
                hist_sales   = sd['hist_sales']
                ext_sos      = sd['ext_sos']
                ext_votg     = sd['ext_votg']
                sos_sig_lags = sd['sos_sig_lags']
                vtg_sig_lags = sd['votg_sig_lags']
                gas_price    = sd['proj_gas'][w_off]
                weeks_open   = sd['weeks_open_base'] + w_off
                loc_type     = sd['loc_type']

                for d_off in range(7):
                    d    = week_start + timedelta(days=d_off)
                    d_ts = pd.Timestamp(d)

                    temp_h, temp_l, precip, severe = self._get_weather(loc_id, d)
                    is_closed, is_lower            = self._holiday_flags(d)
                    pay_fri                        = self._is_payroll_friday(d)

                    # Trailing 8-week growth: blend actual + synthetic past sales
                    # (hist_sales is updated with each week's predictions below)
                    past_vals = [
                        v for k, v in sorted(hist_sales.items()) if k < d
                    ][-56:]
                    if len(past_vals) >= 28:
                        recent_avg = np.mean(past_vals[-28:])
                        prior_avg  = np.mean(past_vals[:28])
                        growth = float((recent_avg / prior_avg) - 1) if prior_avg > 0 else 0.0
                        growth = max(-GROWTH_CAP, min(GROWTH_CAP, growth))
                    else:
                        growth = 0.0

                    # SOS lag features (significant lags only; others NaN via alignment)
                    sos_feats = {}
                    for lag in sos_sig_lags:
                        lag_d = (d_ts - timedelta(weeks=lag)).date()
                        sos_feats[f'sos_lag_{lag}wk'] = ext_sos.get(lag_d, np.nan)

                    # VOTG lag features
                    votg_feats = {}
                    for lag in vtg_sig_lags:
                        lag_d = (d_ts - timedelta(weeks=lag)).date()
                        votg_feats[f'votg_lag_{lag}wk'] = ext_votg.get(lag_d, np.nan)

                    feat = {
                        'location_id_cat':    loc_id,
                        'location_type_cat':  loc_type,
                        'day_of_week':        d.weekday(),
                        'month':              d.month,
                        'week_of_year':       int(d.isocalendar()[1]),
                        'is_holiday_closed':  is_closed,
                        'is_holiday_lower':   is_lower,
                        'is_payroll_friday':  pay_fri,
                        'temp_high_f':        temp_h,
                        'temp_low_f':         temp_l,
                        'precipitation_in':   precip,
                        'severe_weather':     severe,
                        'gas_price':          gas_price,
                        'trailing_8wk_growth': growth,
                        'weeks_since_open':   weeks_open,
                        **sos_feats,
                        **votg_feats,
                    }
                    batch_rows.append(feat)
                    batch_meta.append((loc_id, d))

            # ── Batch predict all stores for this week ───────────────────────
            pred_df = pd.DataFrame(batch_rows)
            for col in self.feature_cols:
                if col not in pred_df.columns:
                    pred_df[col] = np.nan
            pred_df = pred_df[self.feature_cols].copy()
            pred_df['location_id_cat']   = pred_df['location_id_cat'].astype('category')
            pred_df['location_type_cat'] = pred_df['location_type_cat'].astype('category')

            daily_preds = np.clip(self.model.predict(pred_df), 0, None)

            # ── Aggregate daily preds to weekly per store ────────────────────
            store_daily = {}
            for i, (loc_id, d) in enumerate(batch_meta):
                store_daily.setdefault(loc_id, []).append((d, daily_preds[i]))

            for loc_id, day_vals in store_daily.items():
                sd           = store_data[loc_id]
                daily_sales  = [v for _, v in day_vals]
                weekly_point = float(np.sum(daily_sales))
                weekly_low, weekly_high = self._confidence_band(
                    weekly_point, week_num, scenario)

                band_low  = self._get_band(weekly_low)
                band_high = self._get_band(weekly_high)
                rec_band  = self._get_higher_band(band_low, band_high)

                # Feed predicted daily sales back into hist_sales so future
                # weeks' trailing_8wk_growth reflects this week's forecast.
                for d, pred in day_vals:
                    sd['hist_sales'][d] = pred

                results.append({
                    'location_id':     loc_id,
                    'store_name':      sd['store_name'],
                    'scenario':        scenario,
                    'week_num':        week_num,
                    'week_start':      str(week_start),
                    'week_end':        str(week_start + timedelta(days=6)),
                    'forecast_point':  round(weekly_point, 2),
                    'forecast_low':    round(weekly_low,   2),
                    'forecast_high':   round(weekly_high,  2),
                    'recommended_band': rec_band,
                    'sos_assumed':     sd['proj_sos'][w_off],
                    'votg_assumed':    sd['proj_votg'][w_off],
                    'gas_assumed':     sd['proj_gas'][w_off],
                })

        return pd.DataFrame(results)

    def run_all_scenarios(self):
        """
        Run all three scenarios and return a combined DataFrame.
        Use the 'scenario' column to distinguish rows.
        """
        frames = [self.run_scenario(sc) for sc in SCENARIOS]
        return pd.concat(frames, ignore_index=True) if any(
            not f.empty for f in frames) else pd.DataFrame()

    # ── UI helpers ────────────────────────────────────────────────────────────

    def get_model_meta(self):
        """Return model metadata dict for display badges in the UI."""
        return self.meta

    def get_assumption_summary(self, scenario, loc_id=None):
        """
        Return a dict summarising scenario assumptions for display.
        If loc_id is given, returns store-specific values; otherwise portfolio averages.
        """
        if loc_id and loc_id in self.stores:
            store    = self.stores[loc_id]
            state    = STATE_MAP.get(loc_id, 'OH')
            sos_proj = self._project_sos(loc_id, scenario)
            vtg_proj = self._project_votg(loc_id, scenario)
            gas_proj = self._project_gas(state, scenario)
            return {
                'sos_week1':   sos_proj[0],
                'sos_week26':  sos_proj[-1],
                'votg_week1':  vtg_proj[0],
                'votg_week26': vtg_proj[-1],
                'gas_week1':   gas_proj[0],
                'gas_week26':  gas_proj[-1],
                'weather_src': 'Open-Meteo forecast (weeks 1–2), climate normals (weeks 3–26)',
                'conf_week1':  f'±{self.base_mape*100:.1f}%',
                'conf_week26': f'±{self.base_mape*2.5*100:.1f}%'
                               + (' (+15% buffer)' if scenario == 'optimistic' else ''),
            }

        # Portfolio average
        all_sos_w1, all_sos_w26, all_vtg_w1, all_vtg_w26 = [], [], [], []
        for lid in self.stores:
            sp = self._project_sos(lid, scenario)
            vp = self._project_votg(lid, scenario)
            all_sos_w1.append(sp[0]);   all_sos_w26.append(sp[-1])
            all_vtg_w1.append(vp[0]);   all_vtg_w26.append(vp[-1])
        state_gas = self._project_gas('OH', scenario)  # representative state
        return {
            'sos_week1':   round(np.mean(all_sos_w1),  4) if all_sos_w1  else None,
            'sos_week26':  round(np.mean(all_sos_w26), 4) if all_sos_w26 else None,
            'votg_week1':  round(np.mean(all_vtg_w1),  2) if all_vtg_w1  else None,
            'votg_week26': round(np.mean(all_vtg_w26), 2) if all_vtg_w26 else None,
            'gas_week1':   state_gas[0],
            'gas_week26':  state_gas[-1],
            'weather_src': 'Open-Meteo forecast (weeks 1–2), climate normals (weeks 3–26)',
            'conf_week1':  f'±{self.base_mape*100:.1f}%',
            'conf_week26': f'±{self.base_mape*2.5*100:.1f}%'
                           + (' (+15% buffer)' if scenario == 'optimistic' else ''),
        }
