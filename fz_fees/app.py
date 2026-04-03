"""
Ram-Z Accounting Toolbox — Streamlit App
-----------------------------------------
Run with:
    cd C:\\Users\\BretElliott\\ramz-accounting
    streamlit run fz_fees/app.py
"""

import os
import sys
from pathlib import Path
from datetime import datetime, date, timedelta
from io import BytesIO, StringIO

import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# Import paths
# ---------------------------------------------------------------------------
_FZ_DIR = Path(__file__).parent
_LABOR_DIR = _FZ_DIR.parent / "labor"
sys.path.insert(0, str(_FZ_DIR))
sys.path.insert(0, str(_LABOR_DIR))

from reconcile import (
    load_locations, load_fz_schedule, detect_bank_date,
    load_bank_data, reconcile, generate_invoices, write_report,
)
from avs_engine import (
    generate_weekly_report, generate_midweek_report, generate_archive_excel,
)
from weekly_lock import (
    get_week_start, get_next_week_start, format_week_label,
    ensure_current_week_locked, override_locked_value,
)
from supabase_db import (
    load_stores, save_store, delete_store,
    load_reference_data, load_band_goals, load_dm_list,
    save_reference_data_row, save_reference_data_bulk, delete_reference_data,
    save_band_goals, add_dm, remove_dm as db_remove_dm,
    load_all_locks, delete_week_lock, log_change,
    load_locked_config, lock_exists, create_lock, get_locked_weeks,
    load_change_log, is_admin, load_admin_users, add_admin, remove_admin,
    save_weekly_actuals, load_weekly_actuals, delete_weekly_actuals,
    draft_exists, load_draft_config, save_draft_bands, lock_drafts,
    get_week_status,
    load_submissions, load_all_submissions, approve_submission, reject_submission,
    load_email_log, load_app_settings, save_app_setting,
)


# ---------------------------------------------------------------------------
# Revenue band options (for dropdowns)
# ---------------------------------------------------------------------------
BAND_OPTIONS = [
    "<25k", "25k-30k", "30k-35k", "35k-40k", "40k-45k",
    "45k-50k", "50k+", "NRO Seasoned", "NRO",
]

# Hours variance threshold for red/green coloring in performance tables
VARIANCE_GOAL_THRESHOLD = 30

# Days of week used in Email Settings dropdowns
DAYS_OF_WEEK = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

# Revenue band dollar ranges (min, max) — None means no numeric range (NRO stores)
BAND_RANGES = {
    "<25k":         (0,      25_000),
    "25k-30k":      (25_000, 30_000),
    "30k-35k":      (30_000, 35_000),
    "35k-40k":      (35_000, 40_000),
    "40k-45k":      (40_000, 45_000),
    "45k-50k":      (45_000, 50_000),
    "50k+":         (50_000, float("inf")),
    "NRO Seasoned": None,
    "NRO":          None,
}


def _band_classify(band, actual):
    """Return (result, variance) for a band + actual sales pair.
    variance is positive (Over) or negative (Under) or 0 (On Target).
    """
    ranges = BAND_RANGES.get(band)
    if ranges is None or actual is None:
        return "N/A", None
    band_min, band_max = ranges
    if actual < band_min:
        return "Under", actual - band_min          # negative
    elif band_max != float("inf") and actual > band_max:
        return "Over", actual - band_max           # positive
    else:
        return "On Target", 0.0


def _fmt_variance(result, variance):
    """Format variance as '+$2,400 (Over)' / '-$1,800 (Under)' / 'On Target'."""
    if result == "N/A":
        return "N/A"
    if result == "On Target":
        return "On Target"
    if variance is None:
        return result
    sign = "+" if variance >= 0 else ""
    return f"{sign}${variance:,.0f} ({result})"

# ---------------------------------------------------------------------------
# Cached data loaders (avoid re-reading on every Streamlit rerun)
# ---------------------------------------------------------------------------
@st.cache_data(ttl=60)
def _cached_reference_data():
    return load_reference_data()

@st.cache_data(ttl=60)
def _cached_band_goals():
    return load_band_goals()

@st.cache_data(ttl=60)
def _cached_dm_list():
    return load_dm_list()

@st.cache_data(ttl=60)
def _cached_all_locks():
    return load_all_locks()

@st.cache_data(ttl=60)
def _cached_weekly_actuals():
    return load_weekly_actuals()

@st.cache_data(ttl=300)
def _cached_store_sales():
    """Load all store sales data. TTL=5min — heavier table, changes less often."""
    from supabase_db import get_supabase
    sb = get_supabase()
    try:
        resp = sb.table("store_sales").select("*").execute()
        return pd.DataFrame(resp.data) if resp.data else pd.DataFrame()
    except Exception:
        return pd.DataFrame()

# ---------------------------------------------------------------------------
# Shared UI Helpers
# ---------------------------------------------------------------------------
def _period_filter(locked_weeks, key_prefix):
    """Render a period filter (Current Week/All/Month/Quarter/Year) and return filtered week list."""
    col_period, col_period_val = st.columns(2)
    with col_period:
        period_filter = st.selectbox(
            "View by", ["Current Week", "All Weeks", "Month", "Quarter", "Year"],
            key=f"{key_prefix}_period",
        )
    with col_period_val:
        if period_filter == "Current Week":
            # Most recent completed week in the dataset
            last_week = max(locked_weeks) if locked_weeks else None
            filtered_weeks = [last_week] if last_week else []
            if last_week:
                st.caption(f"Week of {last_week}")
        elif period_filter == "Month":
            months_available = sorted(set((w.year, w.month) for w in locked_weeks))
            month_labels = [f"{y}-{m:02d}" for y, m in months_available]
            selected_month = st.selectbox("Select Month", month_labels, key=f"{key_prefix}_month")
            sel_year, sel_month = int(selected_month[:4]), int(selected_month[5:])
            filtered_weeks = [w for w in locked_weeks if w.year == sel_year and w.month == sel_month]
        elif period_filter == "Quarter":
            quarters_available = sorted(set((w.year, (w.month - 1) // 3 + 1) for w in locked_weeks))
            quarter_labels = [f"{y} Q{q}" for y, q in quarters_available]
            selected_quarter = st.selectbox("Select Quarter", quarter_labels, key=f"{key_prefix}_quarter")
            sel_year = int(selected_quarter[:4])
            sel_q = int(selected_quarter[-1])
            q_months = [(sel_q - 1) * 3 + 1, (sel_q - 1) * 3 + 2, (sel_q - 1) * 3 + 3]
            filtered_weeks = [w for w in locked_weeks if w.year == sel_year and w.month in q_months]
        elif period_filter == "Year":
            years_available = sorted(set(w.year for w in locked_weeks))
            selected_year = st.selectbox("Select Year", years_available, key=f"{key_prefix}_year")
            filtered_weeks = [w for w in locked_weeks if w.year == selected_year]
        else:
            filtered_weeks = locked_weeks
    return filtered_weeks


def _rename_week_cols(week_cols):
    """Build a {ISO_date: 'Wk M/D'} rename map for pivot table week columns."""
    col_map = {}
    for col in week_cols:
        try:
            d = date.fromisoformat(col)
            end_d = d + timedelta(days=6)
            col_map[col] = f"Wk {end_d.month}/{end_d.day}"
        except (ValueError, TypeError):
            pass
    return col_map


def _color_variance_cells(row, week_col_map, has_actuals_weeks, skip_cols):
    """Apply red/green background to variance cells based on VARIANCE_GOAL_THRESHOLD.
    Total column is colored based on its own value regardless of actuals weeks."""
    styles = [""] * len(row)
    for i, col in enumerate(row.index):
        if col in skip_cols:
            continue
        if col == "Total":
            v = row[col]
            if v > VARIANCE_GOAL_THRESHOLD:
                styles[i] = "background-color: #FADBD8"
            elif v < -VARIANCE_GOAL_THRESHOLD:
                styles[i] = "background-color: #D5F5E3"
            continue
        orig_week = next((k for k, v in week_col_map.items() if v == col), None)
        if orig_week not in has_actuals_weeks:
            styles[i] = ""
        elif abs(row[col]) > VARIANCE_GOAL_THRESHOLD:
            styles[i] = "background-color: #FADBD8"
        else:
            styles[i] = "background-color: #D5F5E3"
    return styles


# ---------------------------------------------------------------------------
# In-App Report Rendering Helpers
# ---------------------------------------------------------------------------
def _render_weekly_preview(df):
    """Render the AVS Weekly Report as a styled in-app table."""
    st.subheader("AvS Summary")

    display_rows = []
    for dm_name, group in df.groupby("DM", sort=True):
        for _, row in group.iterrows():
            hours = float(row["actual_hours"]) if pd.notna(row["actual_hours"]) else 0.0
            goal_v = float(row["Hourly Goal"]) if pd.notna(row["Hourly Goal"]) else 0.0
            variance = float(row["Variance"]) if pd.notna(row["Variance"]) else 0.0
            sales_v = float(row.get("Last Week Net Sales", 0)) if pd.notna(row.get("Last Week Net Sales")) else 0.0
            payroll_v = float(row.get("loaded_payroll", 0)) if pd.notna(row.get("loaded_payroll")) else 0.0
            labor_pct = payroll_v / sales_v if sales_v else 0.0

            display_rows.append({
                "Store": row["Store Name"],
                "DM": row["DM"],
                "Rev Band": row["Rev Band"],
                "Hourly Goal": round(goal_v),
                "Net Sales": round(sales_v, 2),
                "Actual Hours": round(hours, 2),
                "Variance": round(variance),
                "Est. Payroll": round(payroll_v, 2),
                "Est. Labor %": round(labor_pct * 100, 1),
                "_abs_var": abs(variance),
            })

    preview_df = pd.DataFrame(display_rows)
    preview_df = preview_df.sort_values("_abs_var").reset_index(drop=True)
    preview_df.insert(0, "Rank", range(1, len(preview_df) + 1))
    preview_df = preview_df.drop(columns=["_abs_var"])

    def color_weekly(row):
        v = row["Variance"]
        if abs(v) > VARIANCE_GOAL_THRESHOLD:
            color = "#FADBD8"   # red — over threshold (significant deviation)
        else:
            color = "#D5F5E3"   # green — within threshold (on goal)
        return [f"background-color: {color}"] * len(row)

    styled = preview_df.style.apply(color_weekly, axis=1).format({
        "Net Sales": "${:,.2f}",
        "Actual Hours": "{:,.2f}",
        "Est. Payroll": "${:,.2f}",
        "Est. Labor %": "{:.1f}%",
    })

    st.dataframe(styled, use_container_width=False, hide_index=True,
                 height=(len(preview_df) + 1) * 35 + 3)

    total_goal = preview_df["Hourly Goal"].sum()
    total_hours = preview_df["Actual Hours"].sum()
    total_variance = preview_df["Variance"].sum()
    total_sales = preview_df["Net Sales"].sum()
    total_payroll = preview_df["Est. Payroll"].sum()
    total_labor = (total_payroll / total_sales * 100) if total_sales else 0

    m1, m2, m3, m4, m5, m6 = st.columns(6)
    m1.metric("Total Goal", f"{total_goal:,.0f}")
    m2.metric("Actual Hours", f"{total_hours:,.0f}")
    m3.metric("Variance", f"{total_variance:+,.0f}")
    m4.metric("Net Sales", f"${total_sales:,.0f}")
    m5.metric("Est. Payroll", f"${total_payroll:,.0f}")
    m6.metric("Est. Labor %", f"{total_labor:.1f}%")


def _render_midweek_preview(df, through_day, thresholds):
    """Render the Mid-Week Pulse as a styled in-app table."""
    green_min = thresholds["green_min"]
    green_max = thresholds["green_max"]
    red_above = thresholds["red_above"]

    display_rows = []
    for _, row in df.iterrows():
        hours = float(row["actual_hours"]) if pd.notna(row["actual_hours"]) else 0.0
        goal_v = float(row["Hourly Goal"]) if pd.notna(row["Hourly Goal"]) else 0.0
        variance = float(row["Variance"]) if pd.notna(row["Variance"]) else 0.0
        pct_used = hours / goal_v if goal_v > 0 else 0.0

        if pct_used > red_above:
            status = "Over Pacing"
        elif green_min <= pct_used <= green_max:
            status = "On Pace"
        elif pct_used < green_min:
            status = "Under Pacing"
        else:
            status = ""

        display_rows.append({
            "Store": row["Store Name"],
            "DM": row["DM"],
            "Hourly Goal": round(goal_v),
            "Actual Hours": round(hours, 2),
            "Variance": round(variance),
            "% Used": round(pct_used * 100, 1),
            "Status": status,
            "_abs_var": abs(variance),
        })

    preview_df = pd.DataFrame(display_rows)
    preview_df = preview_df.sort_values("_abs_var").reset_index(drop=True)
    preview_df.insert(0, "Rank", range(1, len(preview_df) + 1))
    preview_df = preview_df.drop(columns=["_abs_var"])

    def color_midweek(row):
        pct = row["% Used"] / 100
        if pct > red_above:
            return ["background-color: #ffcccc"] * len(row)
        elif green_min <= pct <= green_max:
            return ["background-color: #ccffcc"] * len(row)
        elif pct < green_min:
            return ["background-color: #ffe0b2"] * len(row)
        return [""] * len(row)

    styled = preview_df.style.apply(color_midweek, axis=1).format({
        "Actual Hours": "{:,.2f}",
        "% Used": "{:.1f}%",
    })

    st.dataframe(styled, use_container_width=False, hide_index=True,
                 height=(len(preview_df) + 1) * 35 + 3)

    total_goal = preview_df["Hourly Goal"].sum()
    total_hours = preview_df["Actual Hours"].sum()
    total_variance = preview_df["Variance"].sum()
    total_pct = (total_hours / total_goal * 100) if total_goal else 0

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Goal", f"{total_goal:,.0f}")
    m2.metric("Actual Hours", f"{total_hours:,.0f}")
    m3.metric("Variance", f"{total_variance:+,.0f}")
    m4.metric("% Used", f"{total_pct:.1f}%")


# ---------------------------------------------------------------------------
# Page setup
# ---------------------------------------------------------------------------
st.set_page_config(page_title="Ram-Z Accounting Toolbox", layout="wide")

# ---------------------------------------------------------------------------
# Compact sidebar CSS
# ---------------------------------------------------------------------------
st.markdown("""
<style>
/* --- Ram-Z Brand Colors --- */
:root {
    --ramz-navy: #2B3A4E;
    --ramz-gold: #C49A5C;
    --ramz-gold-light: #F5F0EB;
}

/* Sidebar background */
section[data-testid="stSidebar"] {
    background-color: var(--ramz-navy);
}
section[data-testid="stSidebar"] * {
    color: #FFFFFF !important;
}

/* Tighten sidebar spacing */
section[data-testid="stSidebar"] .block-container { padding-top: 1rem; }
section[data-testid="stSidebar"] [data-testid="stVerticalBlock"] { gap: 0.15rem; }

/* Compact nav buttons — left aligned */
section[data-testid="stSidebar"] button[kind="secondary"] {
    padding: 0.2rem 0.5rem;
    font-size: 0.85rem;
    min-height: 0;
    height: auto;
    line-height: 1.3;
    border: none;
    background: transparent;
    text-align: left !important;
    justify-content: flex-start !important;
}
section[data-testid="stSidebar"] button[kind="secondary"] p,
section[data-testid="stSidebar"] button[kind="secondary"] span,
section[data-testid="stSidebar"] button[kind="secondary"] div {
    text-align: left !important;
    width: 100%;
}
section[data-testid="stSidebar"] button[kind="secondary"]:hover {
    background: rgba(196, 154, 92, 0.25);
}

/* Compact expanders */
section[data-testid="stSidebar"] details {
    margin-top: 0.25rem;
    margin-bottom: 0.25rem;
    background: transparent !important;
    border: none !important;
}
section[data-testid="stSidebar"] details summary {
    padding: 0.3rem 0;
    font-size: 0.85rem;
    background: transparent !important;
}
section[data-testid="stSidebar"] details[open] {
    background: transparent !important;
}
section[data-testid="stSidebar"] details[open] [data-testid="stVerticalBlock"] {
    gap: 0.1rem;
    padding-left: 0.5rem;
}
/* Remove white background from expander container */
section[data-testid="stSidebar"] [data-testid="stExpander"] {
    background: transparent !important;
    border: none !important;
}
section[data-testid="stSidebar"] [data-testid="stExpander"] summary,
section[data-testid="stSidebar"] [data-testid="stExpander"] > div {
    background: transparent !important;
}

/* Sidebar header — gold accent */
section[data-testid="stSidebar"] h2 {
    font-size: 1rem;
    margin-bottom: 0.75rem;
    padding-bottom: 0.25rem;
    border-bottom: 1px solid rgba(196, 154, 92, 0.5) !important;
    color: var(--ramz-gold) !important;
}

/* Main content — accent colors */
.stButton > button[kind="primary"] {
    background-color: var(--ramz-gold);
    border-color: var(--ramz-gold);
    color: white;
}
.stButton > button[kind="primary"]:hover {
    background-color: #B08A4E;
    border-color: #B08A4E;
}
h1 { color: var(--ramz-navy) !important; }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# User identity (Streamlit Cloud provides this)
# ---------------------------------------------------------------------------
def get_current_user_email():
    """Get the current user's email from Streamlit Cloud auth."""
    try:
        # Streamlit >= 1.37 uses st.context
        if hasattr(st, "context") and hasattr(st.context, "user"):
            user_info = st.context.user
            if user_info and hasattr(user_info, "email") and user_info.email:
                return user_info.email.strip().lower()
        # Older Streamlit uses st.experimental_user
        if hasattr(st, "experimental_user"):
            user_info = st.experimental_user
            if user_info and hasattr(user_info, "email") and user_info.email:
                return user_info.email.strip().lower()
    except Exception:
        pass
    return ""

current_user = get_current_user_email()
# If no auth is configured, check if running locally (allow admin) vs cloud (deny admin)
if current_user:
    user_is_admin = is_admin(current_user)
else:
    # Allow admin access locally (no auth configured) but deny on Streamlit Cloud
    user_is_admin = not os.environ.get("STREAMLIT_SHARING_MODE", False)

# ---------------------------------------------------------------------------
# Navigation — use session state to avoid sticky radio buttons
# ---------------------------------------------------------------------------
ALL_PAGES = {
    "accounting": [
        "FZ Fee Reconciliation",
    ],
    "labor": [
        "AVS Weekly Report",
        "AVS Mid-Week Pulse",
        "Prior Week's Reports",
        "AVS Performance - Store Level",
        "AVS Performance - DMs",
        "GM Hot Streak",
    ],
    "settings": [
        "Manage Stores",
        "Store Revenue Bands",
        "DM Assignments",
        "Hourly Goals",
    ],
    "admin": [
        "Rev Band Approvals",
        "Compliance Report",
        "Rev Band Report",
        "Weekly Config",
        "Email Settings",
        "Change Log",
        "Admin Users",
    ],
}

if "active_page" not in st.session_state:
    st.session_state["active_page"] = "FZ Fee Reconciliation"
if "active_section" not in st.session_state:
    st.session_state["active_section"] = "accounting"

# Upload key counters — incrementing these resets file uploaders
for _ctr in ("fz_upload_ctr", "weekly_upload_ctr", "mw_upload_ctr"):
    if _ctr not in st.session_state:
        st.session_state[_ctr] = 0


def set_page(section, page_name):
    st.session_state["active_page"] = page_name
    st.session_state["active_section"] = section


def render_nav_section(header, section_key, use_expander=False):
    """Render a navigation section in the sidebar."""
    pages = ALL_PAGES[section_key]
    if use_expander:
        is_expanded = st.session_state["active_section"] == section_key
        with st.sidebar.expander(header, expanded=is_expanded):
            for p in pages:
                is_active = st.session_state["active_page"] == p
                label = f"**{p}**" if is_active else p
                if st.button(label, key=f"nav_{section_key}_{p}", use_container_width=True):
                    set_page(section_key, p)
                    st.rerun()
    else:
        st.sidebar.header(header)
        for p in pages:
            is_active = st.session_state["active_page"] == p
            label = f"**{p}**" if is_active else p
            if st.sidebar.button(label, key=f"nav_{section_key}_{p}", use_container_width=True):
                set_page(section_key, p)
                st.rerun()


# --- Sidebar logo ---
_LOGO_PATH = _FZ_DIR / "ramz_logo.png"
if _LOGO_PATH.exists():
    st.sidebar.image(str(_LOGO_PATH), use_container_width=True)
    st.sidebar.markdown("---")

render_nav_section("Accounting", "accounting", use_expander=True)
render_nav_section("Labor", "labor", use_expander=True)
render_nav_section("Settings", "settings", use_expander=True)
if user_is_admin:
    render_nav_section("Admin", "admin", use_expander=True)

page = st.session_state["active_page"]

# ---------------------------------------------------------------------------
# Week deadline banner helper
# ---------------------------------------------------------------------------
def show_week_deadline_banner():
    """Show a banner about the weekly lock schedule on settings pages."""
    today = date.today()
    current_ws = get_week_start(today)
    next_ws = get_next_week_start(today)
    current_label = format_week_label(current_ws)
    next_label = format_week_label(next_ws)

    if lock_exists(current_ws):
        st.info(
            f"The current week ({current_label}) is **locked**. "
            f"Changes saved here will take effect starting **{next_label}**. "
            f"Review settings by Wednesday to lock them for next week."
        )
    else:
        st.warning(
            f"The current week ({current_label}) is **not yet locked**. "
            f"Settings will be locked when the first AVS report runs this week. "
            f"Review and update settings now before running a report."
        )


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: FZ Fee Reconciliation
# ═══════════════════════════════════════════════════════════════════════════════
if page == "FZ Fee Reconciliation":
    st.title("FZ Fee Reconciliation")

    # --- Input Files (main content area) ---
    st.subheader("Input Files")

    loc_df = load_stores()
    if not loc_df.empty:
        st.success(f"Locations master: {len(loc_df)} stores")
        locations_source = "supabase"
    else:
        st.warning("No stores found — please upload a locations CSV or add stores in Settings.")
        loc_upload = st.file_uploader("Locations Master (.csv)", type=["csv"], key="loc")
        locations_source = loc_upload

    _fz_ctr = st.session_state["fz_upload_ctr"]
    col_fz, col_bank = st.columns(2)
    with col_fz:
        fz_file = st.file_uploader("FZ Fee Schedule (.xlsx)", type=["xlsx"],
                                    key=f"fz_{_fz_ctr}",
                                    help="Weekly fee schedule from the franchisor.")
    with col_bank:
        bank_file = st.file_uploader("Bank Data (.xlsx) — optional", type=["xlsx"],
                                      key=f"bank_{_fz_ctr}",
                                      help="Bank ACH transaction export. Leave blank to run FZ-only.")

    detected_bank_date = None
    if bank_file is not None:
        detected_str = detect_bank_date(bank_file)
        bank_file.seek(0)
        if detected_str:
            try:
                detected_bank_date = datetime.strptime(detected_str, "%m/%d/%Y").date()
            except ValueError:
                pass

    bank_date = st.date_input("Bank Date",
                               value=detected_bank_date or datetime.today().date(),
                               help="Date label for the bank data. Auto-detected when possible.")
    if detected_bank_date:
        st.caption(f"Auto-detected from file: {detected_bank_date.strftime('%m/%d/%Y')}")

    st.divider()
    run_btn = st.button("Run Reconciliation", type="primary", use_container_width=True)

    if run_btn:
        if locations_source is None:
            st.error("Please upload a locations.csv file.")
            st.stop()
        if fz_file is None:
            st.error("Please upload the FZ fee schedule.")
            st.stop()

        try:
            with st.status("Running reconciliation...", expanded=True) as status:
                st.write("Loading locations master...")
                if locations_source == "supabase":
                    locations = loc_df[["location_id", "store_name"]].copy()
                    locations["location_id"] = locations["location_id"].str.strip().str.upper()
                else:
                    locations = load_locations(locations_source)

                st.write("Loading FZ fee schedule...")
                fz_df, fz_week_end_dt = load_fz_schedule(fz_file)
                if fz_week_end_dt is None:
                    st.error("Could not detect the week-end date from the FZ file.")
                    st.stop()

                fiscal_yr = fz_df["fiscal_year"].iloc[0]
                week_num = fz_df["week_num"].iloc[0]

                bank_date_str = bank_date.strftime("%m/%d/%Y")
                if bank_file is not None:
                    st.write("Loading bank data...")
                    bank_file.seek(0)
                    bank_df = load_bank_data(bank_file)
                else:
                    st.write("No bank data — payment columns will be blank.")
                    bank_df = pd.DataFrame(columns=[
                        "store_id", "royalty_paid", "marketing_paid",
                        "franchise_paid", "helpdesk_paid",
                    ])

                st.write("Reconciling fees vs payments...")
                results = reconcile(locations, fz_df, bank_df)

                st.write("Generating Excel report...")
                report_buf = BytesIO()
                write_report(results, fz_week_end_dt, bank_date_str, output=report_buf)
                report_buf.seek(0)

                st.write("Generating invoice CSV...")
                invoice_buf = StringIO()
                invoices = generate_invoices(results, fz_week_end_dt, fiscal_yr, week_num, output=invoice_buf)
                invoice_csv = invoice_buf.getvalue()

                status.update(label="Reconciliation complete!", state="complete")

            st.session_state["results"] = results
            st.session_state["report_buf"] = report_buf
            st.session_state["invoice_csv"] = invoice_csv
            st.session_state["fz_week_end_dt"] = fz_week_end_dt
            st.session_state["bank_date_str"] = bank_date_str
            st.session_state["fiscal_yr"] = fiscal_yr
            st.session_state["week_num"] = week_num
            st.session_state["invoices"] = invoices
            # Auto-clear file uploaders
            st.session_state["fz_upload_ctr"] += 1
            st.rerun()

        except ValueError as e:
            st.error(f"Error: {e}")
            st.stop()
        except Exception as e:
            st.error(f"Unexpected error: {e}")
            st.stop()

    if "results" in st.session_state:
        results = st.session_state["results"]
        fz_week_end_dt = st.session_state["fz_week_end_dt"]
        bank_date_str = st.session_state["bank_date_str"]
        week_num = st.session_state["week_num"]

        flagged = results[results["Flag Count"] > 0]
        clean = results[results["Flag Count"] == 0]

        st.subheader(
            f"Week {int(week_num)} — FZ Week End: {fz_week_end_dt.strftime('%m/%d/%Y')}  |  "
            f"Bank Date: {bank_date_str}"
        )
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Stores", len(results))
        col2.metric("Clean", len(clean))
        col3.metric("Flagged", len(flagged),
                     delta=f"-{len(flagged)}" if len(flagged) > 0 else None,
                     delta_color="inverse")
        col4.metric("Total Net Sales", f"${results['Reported Net Sales'].sum():,.2f}")

        st.subheader("Results")
        display_cols = [
            "Store #", "Store Name", "Reported Net Sales",
            "Royalty Fee Owed", "Royalty Fee Paid", "Royalty Variance",
            "Marketing Fee Owed", "Marketing Fee Paid", "Marketing Variance",
            "Franchise Fee Owed", "Franchise Fee Paid", "Franchise Variance",
            "Help Desk Fee Owed", "Help Desk Fee Paid", "Help Desk Variance",
            "Flag Count", "Flag Details",
        ]
        display_df = results[display_cols].copy()

        def highlight_rows(row):
            if row["Flag Count"] > 0:
                return ["background-color: #FFC7CE"] * len(row)
            return ["background-color: #C6EFCE"] * len(row)

        styled = display_df.style.apply(highlight_rows, axis=1)
        styled = styled.format({
            "Reported Net Sales": "${:,.2f}",
            "Royalty Fee Owed": "${:,.2f}", "Royalty Fee Paid": "${:,.2f}", "Royalty Variance": "${:+,.2f}",
            "Marketing Fee Owed": "${:,.2f}", "Marketing Fee Paid": "${:,.2f}", "Marketing Variance": "${:+,.2f}",
            "Franchise Fee Owed": "${:,.2f}", "Franchise Fee Paid": "${:,.2f}", "Franchise Variance": "${:+,.2f}",
            "Help Desk Fee Owed": "${:,.2f}", "Help Desk Fee Paid": "${:,.2f}", "Help Desk Variance": "${:+,.2f}",
        }, na_rep="—")
        st.dataframe(styled, use_container_width=True, height=600)

        if not flagged.empty:
            with st.expander(f"Flagged Stores ({len(flagged)})", expanded=True):
                for _, row in flagged.iterrows():
                    st.markdown(f"**{row['Store #']}** {row['Store Name']}  \n_{row['Flag Details']}_")

        st.subheader("Downloads")
        fz_str = fz_week_end_dt.strftime("%m%d%Y")
        bank_str = bank_date_str.replace("/", "")
        dl_col1, dl_col2 = st.columns(2)
        with dl_col1:
            st.download_button(label="Download Excel Report",
                               data=st.session_state["report_buf"],
                               file_name=f"reconciliation_FZ-week-end-{fz_str}_bank-{bank_str}.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                               use_container_width=True)
        with dl_col2:
            inv = st.session_state["invoices"][0]
            st.download_button(label="Download Invoice CSV",
                               data=st.session_state["invoice_csv"],
                               file_name=inv[1], mime="text/csv", use_container_width=True)



# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: AVS Weekly Report
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "AVS Weekly Report":
    st.title("AVS Weekly Labor Report")
    st.caption("Full weekly report with AvS Summary, Store Rankings, and DM Rankings.")

    _wk_default_start = get_week_start()
    _wk_default_end = _wk_default_start + timedelta(days=6)
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Report Start Date", value=_wk_default_start, key="weekly_start")
    with col2:
        end_date = st.date_input("Report End Date", value=_wk_default_end, key="weekly_end")

    report_dates = f"{start_date.month}.{start_date.day}.{start_date.strftime('%y')} - {end_date.month}.{end_date.day}.{end_date.strftime('%y')}"

    _wk_ctr = st.session_state["weekly_upload_ctr"]
    st.divider()
    adp_file = st.file_uploader("ADP Payroll CSV", type=["csv"], key=f"weekly_adp_{_wk_ctr}",
                                 help="Upload the ADP payroll export for this period.")
    sales_file = st.file_uploader("End of Week Net Sales (.xlsx)", type=["xlsx"], key=f"weekly_sales_{_wk_ctr}",
                                   help="Upload the weekly net sales file.")

    st.divider()
    if st.button("Generate Report", type="primary", use_container_width=True, key="weekly_run"):
        if adp_file is None:
            st.error("Please upload the ADP Payroll CSV.")
            st.stop()
        if sales_file is None:
            st.error("Please upload the Net Sales file.")
            st.stop()

        try:
            with st.spinner("Generating weekly report..."):
                # Use locked config for the report week
                ref_data = _cached_reference_data()
                band_goals = _cached_band_goals()
                locked = ensure_current_week_locked(ref_data, band_goals, start_date)
                # Build ref_data and band_goals from locked values
                locked_band_goals = dict(zip(locked["revenue_band"], locked["hourly_goal"]))
                buf, report_df = generate_weekly_report(adp_file, sales_file, locked, locked_band_goals, report_dates)
                # Save actual hours to Supabase for performance pages
                week_start = get_week_start(start_date)
                actuals_for_db = report_df.rename(columns={
                    "Store #": "location_id", "Variance": "variance", "Hourly Goal": "hourly_goal",
                }).copy()
                if "Last Week Net Sales" in actuals_for_db.columns:
                    actuals_for_db["net_sales"] = actuals_for_db["Last Week Net Sales"]
                if "loaded_payroll" in actuals_for_db.columns and "Last Week Net Sales" in actuals_for_db.columns:
                    actuals_for_db["labor_pct"] = (
                        actuals_for_db["loaded_payroll"] / actuals_for_db["Last Week Net Sales"]
                    ).fillna(0)
                save_weekly_actuals(week_start, actuals_for_db)
            st.cache_data.clear()
            # Store results in session state so they persist after rerun
            st.session_state["weekly_report_buf"] = buf
            st.session_state["weekly_report_df"] = report_df
            st.session_state["weekly_report_fname"] = f"AVS_Labor_Report_{start_date.strftime('%m%d%Y')}.xlsx"
            st.session_state["weekly_report_week"] = format_week_label(get_week_start(start_date))
            # Auto-clear file uploaders
            st.session_state["weekly_upload_ctr"] += 1
            st.rerun()
        except Exception as e:
            st.error(f"Error: {e}")

    if "weekly_report_buf" in st.session_state:
        st.success("Report generated!")
        st.caption(f"Used locked config for week: {st.session_state['weekly_report_week']}")

        # --- In-App Preview ---
        if "weekly_report_df" in st.session_state:
            _render_weekly_preview(st.session_state["weekly_report_df"])

        st.divider()
        st.download_button("Export to Excel",
                           data=st.session_state["weekly_report_buf"],
                           file_name=st.session_state["weekly_report_fname"],
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           use_container_width=True)



# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: Prior Week's Reports
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "Prior Week's Reports":
    st.title("📂 Prior Week's Reports")
    st.caption("View previously generated weekly reports from stored data.")

    actuals = _cached_weekly_actuals()
    if not actuals.empty:
        available_weeks = sorted(actuals["week_start"].unique(), reverse=True)
        week_labels = {}
        for ws_str in available_weeks:
            try:
                d = date.fromisoformat(ws_str)
                end_d = d + timedelta(days=6)
                week_labels[ws_str] = f"Thu {d.month}/{d.day} – Wed {end_d.month}/{end_d.day}"
            except (ValueError, TypeError):
                week_labels[ws_str] = ws_str

        selected_archive_week = st.selectbox(
            "Select a past week", available_weeks,
            format_func=lambda x: week_labels.get(x, x), key="archive_week_select",
        )

        if st.button("Load Report", key="load_archive", type="primary", use_container_width=True):
            all_locks = _cached_all_locks()
            week_locks = all_locks[all_locks["week_start"] == selected_archive_week]
            week_actuals = actuals[actuals["week_start"] == selected_archive_week]

            if week_locks.empty:
                st.warning("No lock data found for this week.")
            elif week_actuals.empty:
                st.warning("No actuals data found for this week.")
            else:
                merged = week_locks.merge(week_actuals, on="location_id", how="left", suffixes=("", "_act"))
                archive_df = pd.DataFrame({
                    "Store #": merged["location_id"],
                    "Store Name": merged["store_name"],
                    "DM": merged["dm"],
                    "Rev Band": merged["revenue_band"],
                    "Hourly Goal": pd.to_numeric(merged["hourly_goal"], errors="coerce").fillna(0),
                    "actual_hours": pd.to_numeric(merged["actual_hours"], errors="coerce").fillna(0),
                    "Variance": pd.to_numeric(merged["variance"], errors="coerce").fillna(0),
                    "Last Week Net Sales": pd.to_numeric(merged["net_sales"], errors="coerce").fillna(0),
                })
                labor_pct = pd.to_numeric(merged.get("labor_pct", 0), errors="coerce").fillna(0)
                archive_df["loaded_payroll"] = labor_pct * archive_df["Last Week Net Sales"]

                st.session_state["archive_df"] = archive_df
                st.session_state["archive_week_label"] = week_labels.get(selected_archive_week, selected_archive_week)

        if "archive_df" in st.session_state:
            st.markdown(f"**{st.session_state['archive_week_label']}**")
            _render_weekly_preview(st.session_state["archive_df"])

            archive_label = st.session_state["archive_week_label"]
            archive_buf, _ = generate_archive_excel(
                st.session_state["archive_df"], archive_label
            )
            st.download_button(
                "📥 Export to Excel",
                data=archive_buf,
                file_name=f"AVS_Archive_{archive_label.replace(' ', '_').replace('–', '-')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
    else:
        st.info("No archived reports yet. Generate weekly reports to start building the archive.")


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: AVS Mid-Week Pulse
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "AVS Mid-Week Pulse":
    st.title("AVS Mid-Week Labor Pulse")
    st.caption("Cumulative hours vs. weekly goal with day-specific color thresholds.")

    DAY_OPTIONS = ["Friday", "Saturday", "Sunday", "Monday", "Tuesday", "Wednesday"]

    _mw_default_start = get_week_start()
    _mw_default_end = _mw_default_start + timedelta(days=6)
    col1, col2, col3 = st.columns(3)
    with col1:
        start_date = st.date_input("Report Start Date", value=_mw_default_start, key="mw_start")
    with col2:
        end_date = st.date_input("Report End Date", value=_mw_default_end, key="mw_end")
    with col3:
        through_day = st.selectbox("Data Through (Day)", DAY_OPTIONS, key="mw_day",
                                    help="Select the last day of data in this upload. "
                                         "Color thresholds adjust based on the day.")

    report_dates = f"{start_date.month}.{start_date.day}.{start_date.strftime('%y')} - {end_date.month}.{end_date.day}.{end_date.strftime('%y')}"

    _mw_ctr = st.session_state["mw_upload_ctr"]
    st.divider()
    adp_file = st.file_uploader("ADP Payroll CSV", type=["csv"], key=f"mw_adp_{_mw_ctr}",
                                 help="Upload the ADP payroll export.")

    st.divider()
    if st.button("Generate Report", type="primary", use_container_width=True, key="mw_run"):
        if adp_file is None:
            st.error("Please upload the ADP Payroll CSV.")
            st.stop()

        try:
            with st.spinner("Generating mid-week report..."):
                ref_data = _cached_reference_data()
                band_goals = _cached_band_goals()
                locked = ensure_current_week_locked(ref_data, band_goals, start_date)
                locked_band_goals = dict(zip(locked["revenue_band"], locked["hourly_goal"]))
                buf, mw_df = generate_midweek_report(adp_file, locked, locked_band_goals, report_dates, through_day)
            # Store results in session state so they persist after rerun
            st.session_state["mw_report_buf"] = buf
            st.session_state["mw_report_df"] = mw_df
            st.session_state["mw_report_fname"] = f"AVS_MidWeek_Report_{start_date.strftime('%m%d%Y')}.xlsx"
            st.session_state["mw_report_week"] = format_week_label(get_week_start(start_date))
            st.session_state["mw_report_day"] = through_day
            # Auto-clear file uploaders
            st.session_state["mw_upload_ctr"] += 1
            st.rerun()
        except Exception as e:
            st.error(f"Error: {e}")

    if "mw_report_buf" in st.session_state:
        st.success("Report generated!")
        st.caption(f"Used locked config for week: {st.session_state['mw_report_week']} | Thresholds: {st.session_state['mw_report_day']}")

        # --- In-App Preview ---
        if "mw_report_df" in st.session_state:
            from avs_engine import DAY_THRESHOLDS
            day = st.session_state["mw_report_day"]
            thresholds = DAY_THRESHOLDS.get(day, DAY_THRESHOLDS["Friday"])
            _render_midweek_preview(st.session_state["mw_report_df"], day, thresholds)

        st.divider()
        st.download_button("Export to Excel",
                           data=st.session_state["mw_report_buf"],
                           file_name=st.session_state["mw_report_fname"],
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: AVS Performance - Store Level
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "AVS Performance - Store Level":
    st.title("AVS Performance - Store Level")
    st.caption("Compare store performance (Goal vs Actual hours) across weeks.")

    locked_weeks = get_locked_weeks()
    # Only show completed weeks — exclude any week whose Wednesday end date hasn't passed yet
    _today = date.today()
    locked_weeks = [w for w in locked_weeks if (w + timedelta(days=6)) < _today]
    if not locked_weeks:
        st.info("No completed weekly data available yet. Run AVS reports to build history.")
        st.stop()

    # --- Load all data up front (actuals needed for Most Recent / Prior Week logic) ---
    all_locks = _cached_all_locks()
    actuals = _cached_weekly_actuals()
    actuals_week_strs = set(actuals["week_start"].unique()) if not actuals.empty else set()
    # Weeks that have actual data, sorted ascending
    weeks_with_actuals = sorted([w for w in locked_weeks if str(w) in actuals_week_strs])

    # --- Period filter (custom for Store Level — uses actuals to anchor Most Recent / Prior Week) ---
    col_period, col_period_val = st.columns(2)
    with col_period:
        period_filter = st.selectbox(
            "View by",
            ["Most Recent", "Prior Week", "All Weeks", "Month", "Quarter", "Year"],
            key="perf_period",
        )
    with col_period_val:
        if period_filter == "Most Recent":
            # Last week with actual AVS data submitted
            last_week = weeks_with_actuals[-1] if weeks_with_actuals else (max(locked_weeks) if locked_weeks else None)
            filtered_weeks = [last_week] if last_week else []
            if last_week:
                st.caption(format_week_label(last_week))
        elif period_filter == "Prior Week":
            # Second-to-last week with actual data (or let user pick from all weeks with actuals)
            if len(weeks_with_actuals) >= 2:
                prior_week = weeks_with_actuals[-2]
                filtered_weeks = [prior_week]
                st.caption(format_week_label(prior_week))
            elif len(weeks_with_actuals) == 1:
                filtered_weeks = [weeks_with_actuals[0]]
                st.caption(f"{format_week_label(weeks_with_actuals[0])} (only 1 week available)")
            else:
                filtered_weeks = [max(locked_weeks)] if locked_weeks else []
                st.caption("No submitted data yet — showing most recent locked week")
        elif period_filter == "Month":
            months_available = sorted(set((w.year, w.month) for w in locked_weeks))
            month_labels = [f"{y}-{m:02d}" for y, m in months_available]
            selected_month = st.selectbox("Select Month", month_labels, index=len(month_labels)-1, key="perf_month")
            sel_year, sel_month = int(selected_month[:4]), int(selected_month[5:])
            filtered_weeks = [w for w in locked_weeks if w.year == sel_year and w.month == sel_month]
        elif period_filter == "Quarter":
            quarters_available = sorted(set((w.year, (w.month - 1) // 3 + 1) for w in locked_weeks))
            quarter_labels = [f"{y} Q{q}" for y, q in quarters_available]
            selected_quarter = st.selectbox("Select Quarter", quarter_labels, index=len(quarter_labels)-1, key="perf_quarter")
            sel_year = int(selected_quarter[:4])
            sel_q = int(selected_quarter[-1])
            q_months = [(sel_q - 1) * 3 + 1, (sel_q - 1) * 3 + 2, (sel_q - 1) * 3 + 3]
            filtered_weeks = [w for w in locked_weeks if w.year == sel_year and w.month in q_months]
        elif period_filter == "Year":
            years_available = sorted(set(w.year for w in locked_weeks))
            selected_year = st.selectbox("Select Year", years_available, index=len(years_available)-1, key="perf_year")
            filtered_weeks = [w for w in locked_weeks if w.year == selected_year]
        else:  # All Weeks
            filtered_weeks = locked_weeks

    if not filtered_weeks:
        st.warning("No data for the selected period.")
        st.stop()

    # --- Filter by Store and DM ---
    week_strs = [str(w) for w in filtered_weeks]
    perf_data = all_locks[all_locks["week_start"].isin(week_strs)].copy()
    perf_data["hourly_goal"] = pd.to_numeric(perf_data["hourly_goal"], errors="coerce").fillna(0)

    col_dm_filter, col_store_filter = st.columns(2)
    with col_dm_filter:
        dm_list = sorted(perf_data["dm"].dropna().unique().tolist())
        selected_dms = st.multiselect("Filter by DM", dm_list, default=[], key="perf_dm_filter")
    with col_store_filter:
        store_pool = perf_data if not selected_dms else perf_data[perf_data["dm"].isin(selected_dms)]
        store_list = sorted(store_pool["store_name"].dropna().unique().tolist())
        selected_stores = st.multiselect("Filter by Store", store_list, default=[], key="perf_store_filter")

    # Apply filters
    if selected_dms:
        perf_data = perf_data[perf_data["dm"].isin(selected_dms)]
    if selected_stores:
        perf_data = perf_data[perf_data["store_name"].isin(selected_stores)]

    if perf_data.empty:
        st.warning("No data matches the selected filters.")
        st.stop()

    st.divider()
    st.subheader(f"Weeks: {format_week_label(filtered_weeks[0])} → {format_week_label(filtered_weeks[-1])}")
    st.caption(f"{len(filtered_weeks)} week(s)  ·  {perf_data['store_name'].nunique()} store(s)")

    week_strs_set = set(week_strs)

    # Filter actuals to the selected period
    if not actuals.empty:
        actuals_filtered = actuals[actuals["week_start"].isin(week_strs_set)].copy()
    else:
        actuals_filtered = pd.DataFrame(columns=["week_start", "location_id", "actual_hours", "variance"])

    # Build pivot using locked config as the backbone so ALL weeks in the
    # selected period always appear as columns — actuals filled in where available,
    # 0 where no report has been run yet for that week.
    grid = perf_data[["week_start", "location_id", "store_name"]].drop_duplicates()
    if not actuals_filtered.empty:
        grid = grid.merge(
            actuals_filtered[["week_start", "location_id", "variance"]],
            on=["week_start", "location_id"],
            how="left",
        )
    else:
        grid["variance"] = 0.0
    grid["variance"] = grid["variance"].fillna(0.0)

    pivot = grid.pivot_table(
        index="store_name",
        columns="week_start",
        values="variance",
        aggfunc="first",
    ).fillna(0).reset_index()

    pivot.columns.name = None
    pivot = pivot.rename(columns={"store_name": "Store"})
    week_cols = [c for c in pivot.columns if c != "Store"]

    # Round to whole numbers
    for wc in week_cols:
        pivot[wc] = pivot[wc].round(0).astype(int)

    # --- Ranking + Total ---
    # Multi-week: rank by absolute value of Total variance for the period
    # Single-week: rank by absolute value of that week
    if len(week_cols) > 1:
        period_total = pivot[week_cols].sum(axis=1)
        pivot["_sort_var"] = period_total.abs()
    else:
        pivot["_sort_var"] = pivot[week_cols[0]].abs() if week_cols else 0

    pivot = pivot.sort_values("_sort_var").reset_index(drop=True)
    pivot.insert(0, "Rank", range(1, len(pivot) + 1))
    pivot = pivot.drop(columns=["_sort_var"])

    # Rename week columns to end date (Wednesday) labels
    week_col_map = _rename_week_cols(week_cols)
    pivot = pivot.rename(columns=week_col_map)
    renamed_week_cols = [week_col_map.get(c, c) for c in week_cols]

    # Total column at the end (sum of all weeks shown) — only for multi-week views
    if len(renamed_week_cols) > 1:
        pivot["Total"] = pivot[renamed_week_cols].sum(axis=1)

    # --- Color coding: variance-based (Total is handled inside _color_variance_cells) ---
    skip_cols = ("Rank", "Store")
    has_actuals_weeks = set(actuals_filtered["week_start"].unique()) if not actuals_filtered.empty else set()
    styled = pivot.style.apply(
        _color_variance_cells, week_col_map=week_col_map,
        has_actuals_weeks=has_actuals_weeks, skip_cols=skip_cols, axis=1,
    )

    # Display all rows — no internal scroll, user scrolls the browser
    st.dataframe(
        styled,
        use_container_width=False,
        hide_index=True,
        height=(len(pivot) + 1) * 35 + 3,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: AVS Performance - DMs
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "AVS Performance - DMs":
    st.title("AVS Performance - DMs")
    st.caption("Summarized DM performance across all their stores by week.")

    locked_weeks = get_locked_weeks()
    # Only show completed weeks — exclude any week whose Wednesday end date hasn't passed yet
    _today_dm = date.today()
    locked_weeks = [w for w in locked_weeks if (w + timedelta(days=6)) < _today_dm]
    if not locked_weeks:
        st.info("No completed weekly data available yet. Run AVS reports to build history.")
        st.stop()

    # --- Load all data up front (actuals needed for Most Recent / Prior Week logic) ---
    all_locks = _cached_all_locks()
    actuals = _cached_weekly_actuals()
    actuals_week_strs = set(actuals["week_start"].unique()) if not actuals.empty else set()
    weeks_with_actuals = sorted([w for w in locked_weeks if str(w) in actuals_week_strs])

    # --- Period filter (custom — mirrors Store Level page) ---
    col_period, col_period_val = st.columns(2)
    with col_period:
        period_filter = st.selectbox(
            "View by",
            ["Most Recent", "Prior Week", "All Weeks", "Month", "Quarter", "Year"],
            key="dm_perf_period",
        )
    with col_period_val:
        if period_filter == "Most Recent":
            last_week = weeks_with_actuals[-1] if weeks_with_actuals else (max(locked_weeks) if locked_weeks else None)
            filtered_weeks = [last_week] if last_week else []
            if last_week:
                st.caption(format_week_label(last_week))
        elif period_filter == "Prior Week":
            if len(weeks_with_actuals) >= 2:
                prior_week = weeks_with_actuals[-2]
                filtered_weeks = [prior_week]
                st.caption(format_week_label(prior_week))
            elif len(weeks_with_actuals) == 1:
                filtered_weeks = [weeks_with_actuals[0]]
                st.caption(f"{format_week_label(weeks_with_actuals[0])} (only 1 week available)")
            else:
                filtered_weeks = [max(locked_weeks)] if locked_weeks else []
                st.caption("No submitted data yet — showing most recent locked week")
        elif period_filter == "Month":
            months_available = sorted(set((w.year, w.month) for w in locked_weeks))
            month_labels = [f"{y}-{m:02d}" for y, m in months_available]
            selected_month = st.selectbox("Select Month", month_labels, index=len(month_labels)-1, key="dm_perf_month")
            sel_year, sel_month = int(selected_month[:4]), int(selected_month[5:])
            filtered_weeks = [w for w in locked_weeks if w.year == sel_year and w.month == sel_month]
        elif period_filter == "Quarter":
            quarters_available = sorted(set((w.year, (w.month - 1) // 3 + 1) for w in locked_weeks))
            quarter_labels = [f"{y} Q{q}" for y, q in quarters_available]
            selected_quarter = st.selectbox("Select Quarter", quarter_labels, index=len(quarter_labels)-1, key="dm_perf_quarter")
            sel_year = int(selected_quarter[:4])
            sel_q = int(selected_quarter[-1])
            q_months = [(sel_q - 1) * 3 + 1, (sel_q - 1) * 3 + 2, (sel_q - 1) * 3 + 3]
            filtered_weeks = [w for w in locked_weeks if w.year == sel_year and w.month in q_months]
        elif period_filter == "Year":
            years_available = sorted(set(w.year for w in locked_weeks))
            selected_year = st.selectbox("Select Year", years_available, index=len(years_available)-1, key="dm_perf_year")
            filtered_weeks = [w for w in locked_weeks if w.year == selected_year]
        else:  # All Weeks
            filtered_weeks = locked_weeks

    if not filtered_weeks:
        st.warning("No data for the selected period.")
        st.stop()

    # --- Filter by DM ---
    week_strs = [str(w) for w in filtered_weeks]
    perf_data = all_locks[all_locks["week_start"].isin(week_strs)].copy()
    perf_data["hourly_goal"] = pd.to_numeric(perf_data["hourly_goal"], errors="coerce").fillna(0)

    dm_list = sorted(perf_data["dm"].dropna().unique().tolist())
    selected_dms = st.multiselect("Filter by DM", dm_list, default=[], key="dm_perf_dm_filter")
    if selected_dms:
        perf_data = perf_data[perf_data["dm"].isin(selected_dms)]

    if perf_data.empty:
        st.warning("No data matches the selected filters.")
        st.stop()

    st.divider()
    st.subheader(f"Weeks: {format_week_label(filtered_weeks[0])} → {format_week_label(filtered_weeks[-1])}")
    st.caption(f"{len(filtered_weeks)} week(s)  ·  {perf_data['dm'].nunique()} DM(s)  ·  {perf_data['store_name'].nunique()} store(s)")

    week_strs_set = set(week_strs)

    if not actuals.empty:
        actuals_filtered = actuals[actuals["week_start"].isin(week_strs_set)].copy()
    else:
        actuals_filtered = pd.DataFrame(columns=["week_start", "location_id", "variance"])

    # Build pivot using locked config as backbone so ALL weeks in the period
    # appear as columns — actuals filled in where available, 0 where not yet run.
    store_dm = perf_data[["location_id", "dm", "week_start"]].drop_duplicates()

    if not actuals_filtered.empty:
        dm_grid = store_dm.merge(
            actuals_filtered[["week_start", "location_id", "variance"]],
            on=["week_start", "location_id"],
            how="left",
        )
    else:
        dm_grid = store_dm.copy()
        dm_grid["variance"] = 0.0
    dm_grid["variance"] = dm_grid["variance"].fillna(0.0)

    dm_summary = dm_grid.groupby(["dm", "week_start"]).agg(
        total_variance=("variance", "sum"),
    ).reset_index()

    pivot = dm_summary.pivot_table(
        index="dm",
        columns="week_start",
        values="total_variance",
        aggfunc="first",
    ).fillna(0).reset_index()

    # Add store count per DM
    dm_store_counts = perf_data.groupby("dm")["store_name"].nunique().reset_index()
    dm_store_counts.columns = ["DM", "Stores"]

    pivot.columns.name = None
    pivot = pivot.rename(columns={"dm": "DM"})
    pivot = pivot.merge(dm_store_counts, on="DM", how="left")

    # Move Stores column to second position
    cols = pivot.columns.tolist()
    cols.remove("Stores")
    cols.insert(1, "Stores")
    pivot = pivot[cols]

    week_cols = [c for c in pivot.columns if c not in ("DM", "Stores")]

    # Round to whole numbers
    for wc in week_cols:
        pivot[wc] = pivot[wc].round(0).astype(int)

    # --- Ranking + Total (same logic as Store Level) ---
    if len(week_cols) > 1:
        period_total = pivot[week_cols].sum(axis=1)
        pivot["_sort_var"] = period_total.abs()
    else:
        pivot["_sort_var"] = pivot[week_cols[0]].abs() if week_cols else 0

    pivot = pivot.sort_values("_sort_var").reset_index(drop=True)
    pivot.insert(0, "Rank", range(1, len(pivot) + 1))
    pivot = pivot.drop(columns=["_sort_var"])

    # Rename week columns
    week_col_map = _rename_week_cols(week_cols)
    pivot = pivot.rename(columns=week_col_map)
    renamed_week_cols = [week_col_map.get(c, c) for c in week_cols]

    # Total column at the end (multi-week only)
    if len(renamed_week_cols) > 1:
        pivot["Total"] = pivot[renamed_week_cols].sum(axis=1)

    # --- Color coding (Total handled inside _color_variance_cells) ---
    has_actuals_weeks = set(actuals_filtered["week_start"].unique()) if not actuals_filtered.empty else set()
    styled = pivot.style.apply(
        _color_variance_cells, week_col_map=week_col_map,
        has_actuals_weeks=has_actuals_weeks, skip_cols=("Rank", "DM", "Stores"), axis=1,
    )

    st.dataframe(
        styled,
        use_container_width=False,
        hide_index=True,
        height=(len(pivot) + 1) * 35 + 3,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: GM Hot Streak
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "GM Hot Streak":
    st.title("🔥 GM Hot Streak")
    st.caption("Stores that have hit their goal 2 or more consecutive weeks. A store hits its goal when actual hours are within 30 hours of the target.")

    actuals = _cached_weekly_actuals()
    if actuals.empty:
        st.info("No actuals data yet. Run AVS Weekly Reports to build history.")
        st.stop()

    all_locks = _cached_all_locks()

    # Get store info (name, DM) from locks
    store_info = all_locks[["location_id", "store_name", "dm"]].drop_duplicates()
    store_info = store_info.drop_duplicates(subset="location_id", keep="last")

    # Get all weeks with actuals, sorted chronologically
    sorted_weeks = sorted(actuals["week_start"].unique().tolist())
    stores = actuals["location_id"].unique().tolist()

    # Calculate current streak for each store
    streak_data = []
    for store_id in stores:
        store_actuals = actuals[actuals["location_id"] == store_id].copy()
        streak = 0
        for week_str in reversed(sorted_weeks):
            week_row = store_actuals[store_actuals["week_start"] == week_str]
            if week_row.empty:
                break
            variance = abs(week_row["variance"].values[0])
            if variance <= 30:
                streak += 1
            else:
                break
        if streak >= 2:
            info = store_info[store_info["location_id"] == store_id]
            store_name = info["store_name"].values[0] if len(info) > 0 else store_id
            dm_name = info["dm"].values[0] if len(info) > 0 else ""
            streak_data.append({"Store": store_name, "DM": dm_name, "Streak (Weeks)": streak})

    if not streak_data:
        st.info("No stores are currently on a streak (2+ consecutive weeks hitting their goal).")
    else:
        streak_df = pd.DataFrame(streak_data)
        streak_df = streak_df.sort_values("Streak (Weeks)", ascending=False).reset_index(drop=True)
        streak_df.insert(0, "Rank", range(1, len(streak_df) + 1))

        st.metric("Stores on a Streak", len(streak_df))

        st.dataframe(
            streak_df,
            use_container_width=False,
            hide_index=True,
            height=(len(streak_df) + 1) * 35 + 3,
        )

    st.info(
        "A streak means hitting the goal (within 30 hours) for 2+ consecutive weeks. "
        "Streaks are counted backwards from the most recent week."
    )


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: Manage Stores
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "Manage Stores":
    st.title("Manage Store Locations")
    st.caption("Add or remove stores from the master locations list.")

    stores_df = load_stores()
    if not stores_df.empty:
        stores_df["location_id"] = stores_df["location_id"].str.strip().str.upper()
        stores_df["store_name"] = stores_df["store_name"].str.strip()

    st.subheader(f"Current Stores ({len(stores_df)})")
    if not stores_df.empty:
        display = stores_df.copy()
        display.columns = ["Store Number", "Store Name"]
        display = display.sort_values("Store Number").reset_index(drop=True)
        display.index = display.index + 1
        st.dataframe(display, use_container_width=True, height=400)
    else:
        st.info("No stores in the master list yet.")

    st.divider()
    st.subheader("Add a Store")
    with st.form("add_store_form", clear_on_submit=True):
        col1, col2 = st.columns(2)
        with col1:
            new_id = st.text_input("Store Number", placeholder="e.g. 112-0039")
        with col2:
            new_name = st.text_input("Store Name", placeholder="e.g. Springfield (MO)")
        add_btn = st.form_submit_button("Add Store", type="primary")

    if add_btn:
        new_id_clean = new_id.strip().upper()
        new_name_clean = new_name.strip()
        if not new_id_clean:
            st.error("Please enter a store number.")
        elif not new_name_clean:
            st.error("Please enter a store name.")
        elif new_id_clean in stores_df["location_id"].values:
            st.error(f"Store {new_id_clean} already exists.")
        else:
            save_store(new_id_clean, new_name_clean)

            # Also add to reference_data with defaults
            dm_list = _cached_dm_list()
            save_reference_data_row(
                new_id_clean, new_name_clean,
                dm_list[0] if dm_list else "",
                "<25k",
            )

            st.success(f"Added store {new_id_clean} — {new_name_clean}")
            st.cache_data.clear()
            st.rerun()

    st.divider()
    st.subheader("Remove a Store")
    if not stores_df.empty:
        options = stores_df.sort_values("location_id").apply(
            lambda r: f"{r['location_id']}  —  {r['store_name']}", axis=1
        ).tolist()
        selected = st.selectbox("Select store to remove", options, index=None, placeholder="Choose a store...")
        if selected:
            remove_id = selected.split("  —  ")[0].strip()
            st.warning(f"This will remove **{selected}** from the master list.")
            if st.button("Confirm Remove", type="primary"):
                delete_store(remove_id)
                delete_reference_data(remove_id)

                st.success(f"Removed store {remove_id}")
                st.cache_data.clear()
                st.rerun()
    else:
        st.info("No stores to remove.")


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: Store Revenue Bands
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "Store Revenue Bands":
    st.title("Store Revenue Bands")
    st.caption("Assign revenue bands per store for current and future weeks. Lock weeks to freeze their config.")

    ref_df = _cached_reference_data()
    if ref_df.empty:
        st.error("No reference data found. Please add stores first.")
        st.stop()

    ref_df = ref_df.sort_values("location_id").reset_index(drop=True)
    band_goals = _cached_band_goals()

    # Build 5-week range: current + 4 future
    current_week = get_week_start()
    weeks = [current_week + timedelta(days=7 * i) for i in range(5)]
    week_labels = [format_week_label(w) for w in weeks]
    week_statuses = [get_week_status(w) for w in weeks]

    # Load existing data for each week (locked, draft, or None)
    week_data = {}
    for w, status in zip(weeks, week_statuses):
        if status == "locked":
            cfg = load_locked_config(w)
            week_data[str(w)] = {r["location_id"]: r["revenue_band"] for _, r in cfg.iterrows()} if cfg is not None else {}
        elif status == "draft":
            cfg = load_draft_config(w)
            week_data[str(w)] = {r["location_id"]: r["revenue_band"] for _, r in cfg.iterrows()} if cfg is not None else {}
        else:
            week_data[str(w)] = {}

    # --- Header row with status badges ---
    st.markdown("""<style>
    .week-locked { color: #fff; background: #2B3A4E; padding: 2px 8px; border-radius: 4px; font-size: 0.8em; }
    .week-draft { color: #2B3A4E; background: #C49A5C; padding: 2px 8px; border-radius: 4px; font-size: 0.8em; }
    .week-open { color: #666; background: #eee; padding: 2px 8px; border-radius: 4px; font-size: 0.8em; }
    </style>""", unsafe_allow_html=True)

    header_cols = st.columns([2, 3] + [2] * 5)
    with header_cols[0]:
        st.markdown("**Store #**")
    with header_cols[1]:
        st.markdown("**Store Name**")
    for i, (w, label, status) in enumerate(zip(weeks, week_labels, week_statuses)):
        with header_cols[i + 2]:
            if w == current_week:
                st.markdown(f"**{label}**<br><span class='week-locked'>Current</span>", unsafe_allow_html=True)
            elif status == "locked":
                st.markdown(f"**{label}**<br><span class='week-locked'>Locked</span>", unsafe_allow_html=True)
            elif status == "draft":
                st.markdown(f"**{label}**<br><span class='week-draft'>Draft</span>", unsafe_allow_html=True)
            else:
                st.markdown(f"**{label}**<br><span class='week-open'>Open</span>", unsafe_allow_html=True)
    st.divider()

    # --- Grid form ---
    with st.form("revenue_bands_grid"):
        grid_data = {str(w): {} for w in weeks}

        for _, row in ref_df.iterrows():
            store_id = row["location_id"]
            store_name = row["store_name"]
            current_band = row["revenue_band"] if pd.notna(row["revenue_band"]) else "<25k"

            cols = st.columns([2, 3] + [2] * 5)
            with cols[0]:
                st.text(store_id)
            with cols[1]:
                st.text(store_name)

            for i, (w, status) in enumerate(zip(weeks, week_statuses)):
                w_str = str(w)
                # Get band for this week: from DB if exists, else from current config
                existing_band = week_data[w_str].get(store_id, current_band)
                band_idx = BAND_OPTIONS.index(existing_band) if existing_band in BAND_OPTIONS else 0

                with cols[i + 2]:
                    if status == "locked" or w == current_week:
                        st.text(existing_band)
                        grid_data[w_str][store_id] = existing_band
                    else:
                        grid_data[w_str][store_id] = st.selectbox(
                            f"Band {store_id} {w_str}",
                            BAND_OPTIONS,
                            index=band_idx,
                            key=f"band_{store_id}_{w_str}",
                            label_visibility="collapsed",
                        )

        st.divider()

        # Save buttons — one per unlocked week
        save_cols = st.columns([2, 3] + [2] * 5)
        with save_cols[0]:
            st.write("")  # spacer
        with save_cols[1]:
            st.write("")  # spacer
        for i, (w, label, status) in enumerate(zip(weeks, week_labels, week_statuses)):
            with save_cols[i + 2]:
                if w == current_week or status == "locked":
                    st.write("")  # locked — no button
                else:
                    st.form_submit_button(f"Save", key=f"save_draft_{w}")

    # Handle per-week saves
    for w, label, status in zip(weeks, week_labels, week_statuses):
        if w == current_week or status == "locked":
            continue
        if st.session_state.get(f"save_draft_{w}"):
            w_str = str(w)
            # Always grab the latest hourly goals from Supabase (bypass cache)
            st.cache_data.clear()
            fresh_goals = load_band_goals()
            save_draft_bands(w, grid_data[w_str], ref_df, fresh_goals)
            st.success(f"Draft saved for {label}!")
            st.rerun()

    # Lock buttons (outside form since forms can only have one submit)
    st.subheader("Lock a Week")
    st.caption("Locking a week freezes its bands. Once locked, changes require admin override.")
    lock_cols = st.columns(5)
    for i, (w, label, status) in enumerate(zip(weeks, week_labels, week_statuses)):
        with lock_cols[i]:
            if status == "locked":
                st.success(f"{label}: Locked")
            elif status == "draft":
                if st.button(f"Lock {label}", key=f"lock_btn_{w}"):
                    lock_drafts(w)
                    log_change(
                        user_email=current_user or "admin",
                        week_start=w,
                        location_id="ALL",
                        field_changed="week_lock",
                        old_value="draft",
                        new_value="locked",
                        action="manual-lock",
                    )
                    st.success(f"Locked {label}!")
                    st.cache_data.clear()
                    st.rerun()
            else:
                st.info(f"{label}: Save drafts first")

    # --- Export to Excel ---
    st.divider()
    st.subheader("Export to Excel")
    st.caption("Export store config (Store #, Store Name, Revenue Band, Hourly Goal) for selected weeks.")

    export_options = []
    for w, label, status in zip(weeks, week_labels, week_statuses):
        if status in ("locked", "draft"):
            export_options.append((w, label, status))

    if not export_options:
        st.info("No locked or drafted weeks available to export.")
    else:
        export_choices = [f"{label} ({status})" for _, label, status in export_options]
        selected_exports = st.multiselect(
            "Select weeks to export",
            export_choices,
            default=[export_choices[0]] if export_choices else [],
            key="rev_band_export_select",
        )

        if st.button("Export to Excel", key="rev_band_export_btn", type="primary"):
            if not selected_exports:
                st.warning("Please select at least one week.")
            else:
                from openpyxl import Workbook
                from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

                wb = Workbook()
                wb.remove(wb.active)

                NAVY = "2B3A4E"
                GOLD = "C49A5C"
                GOLD_LIGHT = "F5F0EB"
                WHITE = "FFFFFF"
                GRAY_BG = "F7F7F7"

                header_font = Font(name="Calibri", bold=True, color=WHITE, size=11)
                header_fill = PatternFill("solid", fgColor=NAVY)
                subheader_font = Font(name="Calibri", bold=True, color=NAVY, size=10)
                data_font = Font(name="Calibri", size=10)
                center = Alignment(horizontal="center", vertical="center")
                left = Alignment(horizontal="left", vertical="center")
                thin_border = Border(
                    left=Side(style="thin", color="CCCCCC"),
                    right=Side(style="thin", color="CCCCCC"),
                    top=Side(style="thin", color="CCCCCC"),
                    bottom=Side(style="thin", color="CCCCCC"),
                )
                stripe_fill = PatternFill("solid", fgColor=GRAY_BG)

                for choice in selected_exports:
                    idx = export_choices.index(choice)
                    w, label, status = export_options[idx]

                    if status == "locked":
                        cfg = load_locked_config(w)
                    elif status == "draft":
                        cfg = load_draft_config(w)
                    else:
                        # Open week — build from current settings
                        cfg = None

                    if cfg is None or (hasattr(cfg, 'empty') and cfg.empty):
                        # Build from current base config
                        ref_data = _cached_reference_data()
                        current_goals = _cached_band_goals()
                        cfg = ref_data.copy()
                        cfg["hourly_goal"] = cfg["revenue_band"].map(current_goals).fillna(0)

                    cfg = cfg.sort_values("location_id").reset_index(drop=True)

                    # Sheet name (max 31 chars for Excel)
                    sheet_name = label.replace(" – ", "-").replace("/", "-")[:31]
                    ws = wb.create_sheet(title=sheet_name)

                    # Title row
                    ws.merge_cells("A1:D1")
                    title_cell = ws["A1"]
                    title_cell.value = f"Ram-Z Restaurant Group — Store Config: {label}"
                    title_cell.font = Font(name="Calibri", bold=True, color=NAVY, size=14)
                    title_cell.alignment = left

                    # Status row
                    ws.merge_cells("A2:D2")
                    status_cell = ws["A2"]
                    status_cell.value = f"Status: {status.upper()}"
                    status_cell.font = Font(name="Calibri", bold=True, color=GOLD[0:6], size=10)

                    # Headers
                    headers = ["Store #", "Store Name", "Revenue Band", "Hourly Goal"]
                    for col_idx, h in enumerate(headers, 1):
                        cell = ws.cell(row=4, column=col_idx, value=h)
                        cell.font = header_font
                        cell.fill = header_fill
                        cell.alignment = center
                        cell.border = thin_border

                    # Data rows
                    for row_idx, (_, r) in enumerate(cfg.iterrows(), 5):
                        values = [
                            r.get("location_id", ""),
                            r.get("store_name", ""),
                            r.get("revenue_band", ""),
                            float(r.get("hourly_goal", 0)) if pd.notna(r.get("hourly_goal")) else 0,
                        ]
                        for col_idx, val in enumerate(values, 1):
                            cell = ws.cell(row=row_idx, column=col_idx, value=val)
                            cell.font = data_font
                            cell.border = thin_border
                            if col_idx in (1, 3, 4):
                                cell.alignment = center
                            else:
                                cell.alignment = left
                            if row_idx % 2 == 1:
                                cell.fill = stripe_fill
                            # Format hourly goal as integer
                            if col_idx == 4:
                                cell.number_format = "#,##0"

                    # Auto-fit column widths
                    for col_idx in range(1, 5):
                        max_len = len(headers[col_idx - 1])
                        for row_idx in range(5, 5 + len(cfg)):
                            val = ws.cell(row=row_idx, column=col_idx).value
                            if val:
                                max_len = max(max_len, len(str(val)))
                        ws.column_dimensions[chr(64 + col_idx)].width = max_len + 3

                buf = BytesIO()
                wb.save(buf)
                buf.seek(0)

                week_count = len(selected_exports)
                filename = f"RamZ_Store_Config_{'_'.join(str(export_options[export_choices.index(c)][0].strftime('%m%d')) for c in selected_exports)}.xlsx"
                st.download_button(
                    label=f"Download ({week_count} week{'s' if week_count > 1 else ''})",
                    data=buf,
                    file_name=filename,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="rev_band_download",
                )


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: DM Assignments
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "DM Assignments":
    st.title("DM Assignments")
    st.caption("Manage the list of District Managers and assign stores to DMs.")

    show_week_deadline_banner()

    # --- DM List Management ---
    st.subheader("DM List")
    dm_list = _cached_dm_list()
    st.write("Current DMs: " + ", ".join(dm_list) if dm_list else "No DMs defined.")

    col1, col2 = st.columns(2)
    with col1:
        with st.form("add_dm_form", clear_on_submit=True):
            new_dm = st.text_input("New DM Name", placeholder="e.g. John")
            add_dm_btn = st.form_submit_button("Add DM")
        if add_dm_btn and new_dm.strip():
            dm_name = new_dm.strip()
            if dm_name in dm_list:
                st.error(f"{dm_name} already exists.")
            else:
                add_dm(dm_name)
                st.success(f"Added DM: {dm_name}")
                st.cache_data.clear()
                st.rerun()

    with col2:
        if dm_list:
            with st.form("remove_dm_form"):
                remove_dm = st.selectbox("Remove DM", dm_list, index=None, placeholder="Choose a DM...")
                remove_dm_btn = st.form_submit_button("Remove DM")
            if remove_dm_btn and remove_dm:
                db_remove_dm(remove_dm)
                st.success(f"Removed DM: {remove_dm}")
                st.cache_data.clear()
                st.rerun()

    st.divider()

    # --- Store-to-DM Assignment ---
    st.subheader("Store-to-DM Assignments")

    ref_df = _cached_reference_data()
    if ref_df.empty:
        st.error("No reference data found.")
        st.stop()

    ref_df = ref_df.sort_values("location_id").reset_index(drop=True)
    dm_list = _cached_dm_list()

    if not dm_list:
        st.warning("No DMs defined. Add DMs above first.")
        st.stop()

    with st.form("dm_assignments_form"):
        new_dms = {}
        for idx, row in ref_df.iterrows():
            current_dm = row["dm"] if pd.notna(row["dm"]) else ""
            current_idx = dm_list.index(current_dm) if current_dm in dm_list else 0

            col1, col2, col3 = st.columns([2, 3, 3])
            with col1:
                st.text(row["location_id"])
            with col2:
                st.text(row["store_name"])
            with col3:
                new_dms[row["location_id"]] = st.selectbox(
                    f"DM for {row['location_id']}",
                    dm_list,
                    index=current_idx,
                    key=f"dm_{row['location_id']}",
                    label_visibility="collapsed",
                )

        save_btn = st.form_submit_button("Save Changes", type="primary", use_container_width=True)

    if save_btn:
        for store_id, dm in new_dms.items():
            ref_df.loc[ref_df["location_id"] == store_id, "dm"] = dm
        save_reference_data_bulk(ref_df)
        st.success("DM assignments saved! These will apply to the next unlocked week.")
        st.cache_data.clear()
        st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: Hourly Goals
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "Hourly Goals":
    st.title("Hourly Goals by Revenue Band")
    st.caption("Edit the weekly hourly goal for each revenue band. Changes apply to the next unlocked week.")

    show_week_deadline_banner()

    band_goals = _cached_band_goals()

    with st.form("hourly_goals_form"):
        new_goals = {}
        for band in BAND_OPTIONS:
            current_goal = band_goals.get(band, 0)
            col1, col2 = st.columns([2, 2])
            with col1:
                st.markdown(f"**{band}**")
            with col2:
                new_goals[band] = st.number_input(
                    f"Goal for {band}",
                    value=int(current_goal),
                    min_value=0,
                    max_value=9999,
                    step=1,
                    key=f"goal_{band}",
                    label_visibility="collapsed",
                )

        save_btn = st.form_submit_button("Save Changes", type="primary", use_container_width=True)

    if save_btn:
        save_band_goals(new_goals)
        st.success("Hourly goals saved! These will apply to the next unlocked week.")
        st.cache_data.clear()
        st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: Weekly Config (Admin)
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "Weekly Config":
    st.title("Weekly Config Override")
    st.caption("View and override locked weekly configurations. Admin only.")

    if not user_is_admin:
        st.error("You do not have admin access.")
        st.stop()

    locked_weeks = get_locked_weeks()
    if not locked_weeks:
        st.info("No weeks have been locked yet. Locks are created when AVS reports are run.")
        st.stop()

    # Week selector
    week_labels = {w: format_week_label(w) for w in locked_weeks}
    selected_week = st.selectbox(
        "Select Week",
        locked_weeks,
        format_func=lambda w: f"{w} — {week_labels[w]}",
        index=len(locked_weeks) - 1,  # default to most recent
    )

    locked = load_locked_config(selected_week)
    if locked is None:
        st.error("Could not load locked config for this week.")
        st.stop()

    week_status = get_week_status(selected_week)
    st.subheader(f"Config: {week_labels[selected_week]}  ({week_status or 'unknown'})")

    # Show source info
    all_locks = _cached_all_locks()
    week_data = all_locks[all_locks["week_start"] == str(selected_week)]
    sources = week_data["source"].unique().tolist() if "source" in week_data.columns else []
    if sources:
        source_str = ", ".join(sources)
        if "auto-carry-forward" in sources:
            st.warning(f"Source: {source_str} — This week's config was auto-carried from the previous week.")
        else:
            st.success(f"Source: {source_str}")

    # Display locked config
    display = locked[["location_id", "store_name", "dm", "revenue_band", "hourly_goal"]].copy()
    display.columns = ["Store #", "Store Name", "DM", "Revenue Band", "Hourly Goal"]
    display = display.sort_values("Store #").reset_index(drop=True)
    display.index = display.index + 1
    st.dataframe(display, use_container_width=True, height=400)

    # Override form
    st.divider()
    st.subheader("Override a Value")
    st.caption("Changes are logged with your email and timestamp.")

    store_options = locked.sort_values("location_id").apply(
        lambda r: f"{r['location_id']}  —  {r['store_name']}", axis=1
    ).tolist()

    with st.form("override_form"):
        override_store = st.selectbox("Store", store_options, index=None, placeholder="Choose a store...")
        override_field = st.selectbox("Field to Override", ["dm", "revenue_band", "hourly_goal"])

        if override_field == "revenue_band":
            override_value = st.selectbox("New Value", BAND_OPTIONS)
        elif override_field == "hourly_goal":
            override_value = st.number_input("New Value", min_value=0, max_value=9999, step=1)
        else:
            dm_list = _cached_dm_list()
            override_value = st.selectbox("New Value", dm_list if dm_list else [""])

        override_btn = st.form_submit_button("Apply Override", type="primary")

    if override_btn:
        if override_store is None:
            st.error("Please select a store.")
        else:
            store_id = override_store.split("  —  ")[0].strip()
            try:
                override_locked_value(
                    selected_week, store_id, override_field,
                    override_value, current_user or "admin"
                )
                st.success(f"Override applied: {store_id} {override_field} = {override_value}")
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")

    # Delete week lock
    st.divider()
    st.subheader("Delete Week Lock")
    st.caption("Remove the entire lock for this week. This allows the report to be re-run with fresh data.")
    st.warning(f"This will delete the locked config for **{week_labels[selected_week]}** and all associated data.")
    if st.button("Delete This Week's Lock", type="primary"):
        delete_week_lock(selected_week)
        delete_weekly_actuals(selected_week)
        log_change(
            user_email=current_user or "admin",
            week_start=selected_week,
            location_id="ALL",
            field_changed="week_lock",
            old_value=str(selected_week),
            new_value="deleted",
            action="admin-delete",
        )
        st.success(f"Lock deleted for {week_labels[selected_week]}. You can now re-run the report.")
        st.cache_data.clear()
        st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: Change Log (Admin)
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "Change Log":
    st.title("Change Log")
    st.caption("Audit trail of all weekly config changes and overrides. Admin only.")

    if not user_is_admin:
        st.error("You do not have admin access.")
        st.stop()

    log_df = load_change_log()

    if log_df.empty:
        st.info("No changes logged yet.")
        st.stop()

    # Filters
    col1, col2, col3 = st.columns(3)
    with col1:
        action_filter = st.multiselect(
            "Filter by Action",
            log_df["action"].unique().tolist(),
            default=log_df["action"].unique().tolist(),
        )
    with col2:
        if "week_start" in log_df.columns:
            week_filter = st.multiselect(
                "Filter by Week",
                sorted(log_df["week_start"].unique().tolist()),
                default=sorted(log_df["week_start"].unique().tolist()),
            )
        else:
            week_filter = []
    with col3:
        if "user_email" in log_df.columns:
            user_filter = st.multiselect(
                "Filter by User",
                sorted(log_df["user_email"].unique().tolist()),
                default=sorted(log_df["user_email"].unique().tolist()),
            )
        else:
            user_filter = []

    filtered = log_df.copy()
    if action_filter:
        filtered = filtered[filtered["action"].isin(action_filter)]
    if week_filter:
        filtered = filtered[filtered["week_start"].isin(week_filter)]
    if user_filter:
        filtered = filtered[filtered["user_email"].isin(user_filter)]

    filtered = filtered.sort_values("timestamp", ascending=False).reset_index(drop=True)
    filtered.index = filtered.index + 1

    st.subheader(f"Log Entries ({len(filtered)})")
    st.dataframe(filtered, use_container_width=True, height=500)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: Admin Users (Admin)
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "Admin Users":
    st.title("Admin Users")
    st.caption("Manage who has admin access to weekly config overrides and change logs.")

    if not user_is_admin:
        st.error("You do not have admin access.")
        st.stop()

    admins = load_admin_users()

    st.subheader(f"Current Admins ({len(admins)})")
    for i, email in enumerate(sorted(admins), 1):
        st.text(f"{i}. {email}")

    st.divider()
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Add Admin")
        with st.form("add_admin_form", clear_on_submit=True):
            new_admin_email = st.text_input("Email Address", placeholder="e.g. user@example.com")
            add_admin_btn = st.form_submit_button("Add Admin", type="primary")

        if add_admin_btn:
            email_clean = new_admin_email.strip().lower()
            if not email_clean:
                st.error("Please enter an email address.")
            elif "@" not in email_clean:
                st.error("Please enter a valid email address.")
            elif email_clean in admins:
                st.error(f"{email_clean} is already an admin.")
            else:
                add_admin(email_clean)
                st.success(f"Added admin: {email_clean}")
                st.cache_data.clear()
                st.rerun()

    with col2:
        st.subheader("Remove Admin")
        if len(admins) > 1:
            with st.form("remove_admin_form"):
                remove_email = st.selectbox("Select Admin to Remove", sorted(admins), index=None, placeholder="Choose...")
                remove_admin_btn = st.form_submit_button("Remove Admin")

            if remove_admin_btn and remove_email:
                if remove_email == current_user:
                    st.error("You cannot remove yourself as admin.")
                else:
                    remove_admin(remove_email)
                    st.success(f"Removed admin: {remove_email}")
                    st.cache_data.clear()
                    st.rerun()
        else:
            st.info("Cannot remove the last admin.")

# ==========================================
# Rev Band Approvals (Admin Dashboard)
# ==========================================
elif page == "Rev Band Approvals":
    if not user_is_admin:
        st.error("You do not have permission to access this page.")
        st.stop()

    st.title("📋 Rev Band Approvals")
    st.caption("Review GM revenue band submissions. Approve or reject for each store.")

    # Week selector
    all_subs = load_all_submissions()

    if all_subs.empty:
        st.info("No submissions yet. Submissions will appear here once GMs begin selecting revenue bands.")
    else:
        available_weeks = sorted(all_subs["week_start"].unique(), reverse=True)
        selected_week = st.selectbox("Select Week", available_weeks, index=0,
                                      format_func=lambda w: format_week_label(get_week_start(pd.Timestamp(w).date())))

        week_subs = all_subs[all_subs["week_start"] == selected_week].copy()

        # Summary metrics
        total = len(week_subs)
        pending_gm = len(week_subs[week_subs["status"] == "pending_gm"])
        pending_dm = len(week_subs[week_subs["status"] == "pending_dm"])
        pending_admin = len(week_subs[week_subs["status"] == "pending_admin"])
        approved = len(week_subs[week_subs["status"] == "approved"])
        rejected = len(week_subs[week_subs["status"] == "rejected"])

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Awaiting GM", pending_gm)
        c2.metric("Awaiting DM", pending_dm)
        c3.metric("Awaiting Admin", pending_admin)
        c4.metric("Approved", approved)
        c5.metric("Rejected", rejected)

        st.divider()

        # Filter options
        ref_data = _cached_reference_data()
        dm_list_vals = sorted(ref_data["dm"].dropna().unique().tolist())
        filter_col1, filter_col2 = st.columns(2)
        with filter_col1:
            status_filter = st.selectbox("Filter by Status", ["All", "pending_gm", "pending_dm", "pending_admin", "approved", "rejected"])
        with filter_col2:
            dm_filter = st.selectbox("Filter by DM", ["All"] + dm_list_vals)

        filtered = week_subs.copy()
        if status_filter != "All":
            filtered = filtered[filtered["status"] == status_filter]

        # Join with reference data to get DM
        if "dm" not in filtered.columns and not ref_data.empty:
            filtered = filtered.merge(ref_data[["location_id", "dm"]], on="location_id", how="left")

        if dm_filter != "All":
            filtered = filtered[filtered["dm"] == dm_filter]

        if filtered.empty:
            st.info("No submissions match the selected filters.")
        else:
            # Load band goals for hourly goal display
            band_goals = _cached_band_goals()

            for _, row in filtered.iterrows():
                store_name = ref_data[ref_data["location_id"] == row["location_id"]]["store_name"].values
                store_name = store_name[0] if len(store_name) > 0 else row["location_id"]
                current_band = ref_data[ref_data["location_id"] == row["location_id"]]["revenue_band"].values
                current_band = current_band[0] if len(current_band) > 0 else "N/A"

                status = row["status"]
                selected_band = row.get("selected_band", "")

                # Color code status
                if status == "approved":
                    status_color = "🟢"
                elif status == "rejected":
                    status_color = "🔴"
                elif status == "pending_admin":
                    status_color = "🟡"
                elif status == "pending_dm":
                    status_color = "🔵"
                else:
                    status_color = "⚪"

                with st.container():
                    col1, col2, col3, col4 = st.columns([3, 2, 2, 2])
                    with col1:
                        st.markdown(f"**{store_name}** ({row['location_id']})")
                        dm_name = row.get("dm", "N/A")
                        st.caption(f"DM: {dm_name}")
                    with col2:
                        st.markdown(f"Current: **{current_band}**")
                        if selected_band:
                            hourly_goal = band_goals.get(selected_band, "N/A")
                            st.markdown(f"Selected: **{selected_band}** ({hourly_goal} hrs)")
                    with col3:
                        st.markdown(f"{status_color} **{status.replace('_', ' ').title()}**")
                        if row.get("submitted_at"):
                            st.caption(f"Submitted: {str(row['submitted_at'])[:16]}")
                    with col4:
                        if status == "pending_admin":
                            if st.button("✅ Approve", key=f"approve_{row['id']}"):
                                approve_submission(row["id"], "admin")
                                st.cache_data.clear()
                                st.rerun()
                            reject_reason = st.text_input("Reason", key=f"reason_{row['id']}", placeholder="Optional")
                            if st.button("❌ Reject", key=f"reject_{row['id']}"):
                                reject_submission(row["id"], "admin", reject_reason)
                                st.cache_data.clear()
                                st.rerun()
                        elif status == "approved":
                            st.caption(f"By: {row.get('admin_approved_by', 'N/A')}")
                        elif status == "rejected":
                            st.caption(f"Reason: {row.get('rejection_reason', 'N/A')}")

                    # Expandable performance data
                    with st.expander(f"📊 View {store_name} Performance Data"):
                        perf_cols = st.columns(3)

                        # Sales data
                        with perf_cols[0]:
                            st.markdown("**📈 Sales**")
                            try:
                                sb = get_supabase()
                                sales_resp = sb.table("store_sales").select("*").eq(
                                    "location_id", row["location_id"]
                                ).order("week_start", desc=True).limit(4).execute()
                                if sales_resp.data:
                                    for s in sales_resp.data:
                                        st.caption(f"Wk {s['week_start']}: ${s.get('net_sales', 0):,.0f}")
                                else:
                                    st.caption("No sales data available")
                            except Exception:
                                st.caption("No sales data available")

                        # SoS data
                        with perf_cols[1]:
                            st.markdown("**⏱️ Speed of Service**")
                            try:
                                sos_resp = sb.table("store_sos").select("*").eq(
                                    "location_id", row["location_id"]
                                ).order("week_start", desc=True).limit(4).execute()
                                if sos_resp.data:
                                    sos_vals = [s.get("sos_seconds", 0) for s in sos_resp.data if s.get("sos_seconds")]
                                    if sos_vals:
                                        avg_sos = sum(sos_vals) / len(sos_vals)
                                        st.caption(f"Avg (last {len(sos_vals)} wks): {avg_sos/60:.1f} min")
                                    for s in sos_resp.data:
                                        rank = s.get("sos_rank", "N/A")
                                        total = s.get("total_stores", "")
                                        rank_str = f" ({rank} of {total})" if rank != "N/A" and total else ""
                                        st.caption(f"Wk {s['week_start']}: {s.get('sos_seconds', 0)/60:.1f} min{rank_str}")
                                else:
                                    st.caption("No SoS data available")
                            except Exception:
                                st.caption("No SoS data available")

                        # VOTG data
                        with perf_cols[2]:
                            st.markdown("**⭐ Voice of the Guest**")
                            try:
                                votg_resp = sb.table("store_votg").select("*").eq(
                                    "location_id", row["location_id"]
                                ).order("week_start", desc=True).limit(4).execute()
                                if votg_resp.data:
                                    neg_vals = [v.get("total_negative_reviews", 0) for v in votg_resp.data if v.get("total_negative_reviews") is not None]
                                    if neg_vals:
                                        avg_neg = sum(neg_vals) / len(neg_vals)
                                        st.caption(f"Avg Neg Reviews (last {len(neg_vals)} wks): {avg_neg:.1f}")
                                    for v in votg_resp.data:
                                        rank = v.get("votg_rank", "N/A")
                                        total = v.get("total_stores", "")
                                        rank_str = f" ({rank} of {total})" if rank != "N/A" and total else ""
                                        neg = v.get("total_negative_reviews", "N/A")
                                        st.caption(f"Wk {v['week_start']}: {neg} neg reviews{rank_str}")
                                else:
                                    st.caption("No VOTG data available")
                            except Exception:
                                st.caption("No VOTG data available")

                    st.divider()

# ==========================================
# Email Settings (Admin)
# ==========================================
elif page == "Email Settings":
    if not user_is_admin:
        st.error("You do not have permission to access this page.")
        st.stop()

    st.title("📧 Email Settings")
    st.caption("Configure the email workflow for GM/DM revenue band selection.")

    settings = load_app_settings()

    with st.form("email_settings_form"):
        col1, col2 = st.columns(2)
        with col1:
            gm_send_day = st.selectbox("GM Email Send Day", DAYS_OF_WEEK,
                index=DAYS_OF_WEEK.index(settings.get("gm_email_send_day", "Monday")))
            gm_deadline = st.selectbox("GM Deadline Day", DAYS_OF_WEEK,
                index=DAYS_OF_WEEK.index(settings.get("gm_deadline_day", "Wednesday")))
        with col2:
            dm_deadline = st.selectbox("DM Deadline Day", DAYS_OF_WEEK,
                index=DAYS_OF_WEEK.index(settings.get("dm_deadline_day", "Thursday")))
            ceo_email = st.text_input("CEO Email (for escalation)", value=settings.get("ceo_email", ""))

        st.subheader("Reminder Times")
        r1, r2, r3 = st.columns(3)
        with r1:
            reminder_1 = st.text_input("Reminder 1 (next morning)", value=settings.get("reminder_1_time", "08:00"))
        with r2:
            reminder_2 = st.text_input("Reminder 2 (midday)", value=settings.get("reminder_2_time", "12:00"))
        with r3:
            reminder_3 = st.text_input("Reminder 3 (evening + escalation)", value=settings.get("reminder_3_time", "17:00"))

        save_btn = st.form_submit_button("Save Settings", type="primary")

    if save_btn:
        save_app_setting("gm_email_send_day", gm_send_day)
        save_app_setting("gm_deadline_day", gm_deadline)
        save_app_setting("dm_deadline_day", dm_deadline)
        save_app_setting("ceo_email", ceo_email)
        save_app_setting("reminder_1_time", reminder_1)
        save_app_setting("reminder_2_time", reminder_2)
        save_app_setting("reminder_3_time", reminder_3)
        st.success("Email settings saved!")
        st.cache_data.clear()
        st.rerun()

    st.divider()

    # DM Email Management
    st.subheader("DM Emails")
    st.caption("Manage DM email addresses for the approval workflow.")
    from supabase_db import get_supabase as _get_supabase
    sb = _get_supabase()
    dm_resp = sb.table("dm_list").select("*").order("dm_name").execute()
    dm_data = dm_resp.data if dm_resp.data else []

    if dm_data:
        for dm in dm_data:
            c1, c2 = st.columns([1, 2])
            with c1:
                st.markdown(f"**{dm['dm_name']}**")
            with c2:
                new_email = st.text_input(f"Email for {dm['dm_name']}", value=dm.get("email", "") or "",
                                          key=f"dm_email_{dm['dm_name']}")
                if new_email != (dm.get("email", "") or ""):
                    if st.button(f"Save", key=f"save_dm_email_{dm['dm_name']}"):
                        sb.table("dm_list").update({"email": new_email}).eq("dm_name", dm["dm_name"]).execute()
                        st.success(f"Updated email for {dm['dm_name']}")
                        st.cache_data.clear()
                        st.rerun()
    else:
        st.info("No DMs found. Add DMs in the DM Assignments page first.")

# ==========================================
# Compliance Report (Admin)
# ==========================================
elif page == "Compliance Report":
    if not user_is_admin:
        st.error("You do not have permission to access this page.")
        st.stop()

    st.title("📋 Compliance Report")
    st.caption(
        "GM submission timeliness and DM approval compliance. "
        "On Time = GM submitted before Tuesday 8am · DM On Time = approved before Wednesday 8am."
    )

    # --- Load base data ---
    ref_data   = _cached_reference_data()
    all_subs   = load_all_submissions()
    email_log_df = load_email_log()

    if all_subs.empty and email_log_df.empty:
        st.info("No compliance data yet. Data will appear once GMs begin submitting revenue bands.")
        st.stop()

    # --- Build unified week list ---
    weeks_set = set()
    if not all_subs.empty:
        weeks_set.update(all_subs["week_start"].dropna().unique())
    if not email_log_df.empty:
        weeks_set.update(email_log_df["week_start"].dropna().unique())

    all_week_dates = sorted(
        [pd.Timestamp(w).date() for w in weeks_set],
        reverse=False,
    )

    if not all_week_dates:
        st.info("No week data available yet.")
        st.stop()

    # --- Filters ---
    filtered_week_dates = _period_filter(all_week_dates, "compliance")
    filtered_week_strs  = [str(w) for w in filtered_week_dates]

    col_f1, col_f2 = st.columns(2)
    dm_options = sorted(ref_data["dm"].dropna().unique().tolist()) if not ref_data.empty else []
    with col_f1:
        dm_filter = st.selectbox("Filter by DM", ["All"] + dm_options, key="compliance_dm")
    with col_f2:
        status_filter = st.selectbox(
            "Filter by GM Status", ["All", "On Time", "Late", "Never Submitted"],
            key="compliance_status",
        )

    # --- Build compliance rows ---
    stores = (
        ref_data[["location_id", "store_name", "dm"]].drop_duplicates()
        if not ref_data.empty
        else pd.DataFrame(columns=["location_id", "store_name", "dm"])
    )

    compliance_rows = []

    for week_str in filtered_week_strs:
        try:
            week_date = pd.Timestamp(week_str).date()
        except Exception:
            continue

        # Week starts Thursday; +5 days = Tuesday, +6 = Wednesday
        gm_deadline  = datetime(week_date.year, week_date.month, week_date.day) + timedelta(days=5, hours=8)
        dm_deadline  = datetime(week_date.year, week_date.month, week_date.day) + timedelta(days=6, hours=8)

        week_log  = email_log_df[email_log_df["week_start"] == week_str].copy() if not email_log_df.empty else pd.DataFrame()
        week_subs = all_subs[all_subs["week_start"] == week_str].copy() if not all_subs.empty else pd.DataFrame()

        for _, store_row in stores.iterrows():
            loc_id     = store_row["location_id"]
            store_name = store_row["store_name"]
            dm_name    = store_row.get("dm", "")

            if dm_filter != "All" and dm_name != dm_filter:
                continue

            # ── Email log for this store ──────────────────────────────────
            store_log = week_log[week_log["location_id"] == loc_id] if not week_log.empty else pd.DataFrame()
            if not store_log.empty and "email_type" in store_log.columns:
                initial_sent   = (store_log["email_type"] == "initial").any()
                reminder_count = int((store_log["email_type"].isin(["tuesday", "wednesday"])).sum())
            else:
                initial_sent   = False
                reminder_count = 0

            # ── Submission for this store ─────────────────────────────────
            store_sub = week_subs[week_subs["location_id"] == loc_id] if not week_subs.empty else pd.DataFrame()

            if store_sub.empty:
                gm_status      = "Never Submitted"
                gm_on_time     = False
                submitted_str  = ""
                final_status   = "Pending GM"
                approved_str   = ""
                dm_on_time_val = False
            else:
                sub = store_sub.iloc[0]
                final_status = sub.get("status", "")
                if isinstance(final_status, str):
                    final_status = final_status.replace("_", " ").title()

                submitted_at = sub.get("submitted_at")
                if submitted_at:
                    try:
                        sub_dt        = pd.Timestamp(submitted_at).to_pydatetime().replace(tzinfo=None)
                        submitted_str = str(submitted_at)[:16]
                        gm_on_time    = sub_dt < gm_deadline
                        gm_status     = "On Time" if gm_on_time else "Late"
                    except Exception:
                        submitted_str = str(submitted_at)[:16]
                        gm_status     = "Unknown"
                        gm_on_time    = False
                else:
                    submitted_str = ""
                    gm_status     = "Never Submitted"
                    gm_on_time    = False

                approved_at = sub.get("admin_approved_at")
                if approved_at:
                    try:
                        app_dt        = pd.Timestamp(approved_at).to_pydatetime().replace(tzinfo=None)
                        approved_str  = str(approved_at)[:16]
                        dm_on_time_val = app_dt < dm_deadline
                    except Exception:
                        approved_str   = str(approved_at)[:16]
                        dm_on_time_val = False
                else:
                    approved_str   = ""
                    dm_on_time_val = False

            if status_filter != "All" and gm_status != status_filter:
                continue

            compliance_rows.append({
                "Week":               week_str,
                "Store":              store_name,
                "DM":                 dm_name,
                "Initial Email Sent": "✅" if initial_sent else "—",
                "Reminders Sent":     reminder_count,
                "GM Submitted At":    submitted_str,
                "GM Status":          gm_status,
                "Final Status":       final_status,
                "Approved At":        approved_str,
                "DM Approved On Time": "✅" if dm_on_time_val else ("—" if not approved_str else "❌"),
                # Raw booleans for DM rollup
                "_gm_on_time":        gm_on_time,
                "_dm_on_time":        dm_on_time_val,
            })

    if not compliance_rows:
        st.info("No compliance data matches the selected filters.")
        st.stop()

    compliance_df = pd.DataFrame(compliance_rows)

    # --- Summary Metrics ---
    total    = len(compliance_df)
    on_time  = int((compliance_df["GM Status"] == "On Time").sum())
    late     = int((compliance_df["GM Status"] == "Late").sum())
    never    = int((compliance_df["GM Status"] == "Never Submitted").sum())
    avg_rem  = compliance_df["Reminders Sent"].mean()

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Total Stores",     total)
    m2.metric("On Time",          f"{on_time} ({on_time / total * 100:.0f}%)")
    m3.metric("Late",             f"{late} ({late / total * 100:.0f}%)")
    m4.metric("Never Submitted",  f"{never} ({never / total * 100:.0f}%)")
    m5.metric("Avg Reminders",    f"{avg_rem:.1f}")

    st.divider()

    # --- GM Detail Table ---
    st.subheader("GM Submission Detail")
    display_cols = [
        "Week", "Store", "DM", "Initial Email Sent", "Reminders Sent",
        "GM Submitted At", "GM Status", "Final Status",
        "Approved At", "DM Approved On Time",
    ]
    st.dataframe(compliance_df[display_cols], use_container_width=True, hide_index=True)

    st.divider()

    # --- DM Portfolio Rollup ---
    st.subheader("DM Portfolio Summary")
    dm_rollup = (
        compliance_df.groupby("DM")
        .agg(
            Stores          = ("Store",       "count"),
            On_Time         = ("_gm_on_time", "sum"),
            Late            = ("GM Status",   lambda x: (x == "Late").sum()),
            Never_Submitted = ("GM Status",   lambda x: (x == "Never Submitted").sum()),
            Total_Reminders = ("Reminders Sent", "sum"),
            DM_On_Time      = ("_dm_on_time", "sum"),
        )
        .reset_index()
    )
    dm_rollup["On Time %"] = (
        (dm_rollup["On_Time"].astype(float) / dm_rollup["Stores"].astype(float) * 100)
        .round(0).astype(int).astype(str) + "%"
    )
    dm_rollup = dm_rollup.rename(columns={
        "On_Time":         "On Time",
        "Never_Submitted": "Never Submitted",
        "Total_Reminders": "Total Reminders",
        "DM_On_Time":      "DM Approved On Time",
    })
    rollup_cols = ["DM", "Stores", "On Time", "Late", "Never Submitted",
                   "On Time %", "Total Reminders", "DM Approved On Time"]
    st.dataframe(dm_rollup[rollup_cols], use_container_width=True, hide_index=True)

    st.divider()

    # --- Excel Export (only generated on click) ---
    st.subheader("Export")
    st.caption("Click the button below to generate the Excel file — it is not built automatically to keep the page fast.")
    if st.button("📥 Generate Excel Report", type="primary"):
        output = BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            # Sheet 1: GM Detail
            compliance_df[display_cols].to_excel(writer, sheet_name="GM Compliance", index=False)
            # Sheet 2: DM Rollup
            dm_rollup[rollup_cols].to_excel(writer, sheet_name="DM Summary", index=False)

            workbook   = writer.book
            hdr_fmt    = workbook.add_format({
                "bold": True, "bg_color": "#2B3A4E",
                "font_color": "#FFFFFF", "border": 1,
            })
            on_time_fmt = workbook.add_format({"bg_color": "#D5F5E3"})
            late_fmt    = workbook.add_format({"bg_color": "#FADBD8"})

            for sheet_name in ["GM Compliance", "DM Summary"]:
                ws = writer.sheets[sheet_name]
                ws.set_row(0, 18, hdr_fmt)
                ws.set_column("A:Z", 18)

        output.seek(0)
        period_label = (
            f"{filtered_week_strs[0]}_to_{filtered_week_strs[-1]}"
            if len(filtered_week_strs) > 1
            else (filtered_week_strs[0] if filtered_week_strs else "all")
        )
        st.download_button(
            label="⬇️ Download Excel",
            data=output,
            file_name=f"compliance_report_{period_label}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

# ==========================================
# Rev Band Report (Admin)
# ==========================================
elif page == "Rev Band Report":
    if not user_is_admin:
        st.error("You do not have permission to access this page.")
        st.stop()

    st.title("🎯 Rev Band Hit/Miss Report")
    st.caption(
        "Compare GM revenue band predictions against actual weekly sales. "
        "Green = Over (outperformed) · Red = Under (missed) · Neutral = On Target."
    )

    # --- Load data ---
    ref_data  = _cached_reference_data()
    all_subs  = load_all_submissions()
    sales_df  = _cached_store_sales()

    if all_subs.empty:
        st.info("No submissions yet. This report will populate once GMs begin selecting revenue bands.")
        st.stop()

    # --- Build week list from submissions ---
    all_week_dates = sorted(
        {pd.Timestamp(w).date() for w in all_subs["week_start"].dropna().unique()},
    )

    # --- Period filter ---
    filtered_week_dates = _period_filter(all_week_dates, "revband")
    filtered_week_strs  = [str(w) for w in filtered_week_dates]

    # --- DM + Result filters ---
    col_f1, col_f2 = st.columns(2)
    dm_options = sorted(ref_data["dm"].dropna().unique().tolist()) if not ref_data.empty else []
    with col_f1:
        dm_filter = st.selectbox("Filter by DM", ["All"] + dm_options, key="revband_dm")
    with col_f2:
        result_filter = st.selectbox(
            "Filter by Result", ["All", "On Target", "Over", "Under", "N/A (NRO)"],
            key="revband_result",
        )

    # --- Join submissions → sales → ref_data ---
    week_subs = all_subs[all_subs["week_start"].isin(filtered_week_strs)].copy()

    # Merge store info
    if not ref_data.empty:
        week_subs = week_subs.merge(
            ref_data[["location_id", "store_name", "dm"]],
            on="location_id", how="left",
        )

    # Merge sales
    if not sales_df.empty:
        week_subs = week_subs.merge(
            sales_df[["location_id", "week_start", "net_sales"]],
            on=["location_id", "week_start"], how="left",
        )
    else:
        week_subs["net_sales"] = None

    # --- Apply band classification ---
    rows = []
    for _, r in week_subs.iterrows():
        band   = r.get("selected_band", "")
        actual = r.get("net_sales")
        dm     = r.get("dm", "")

        if dm_filter != "All" and dm != dm_filter:
            continue

        try:
            actual_num = float(actual) if actual is not None else None
        except (TypeError, ValueError):
            actual_num = None

        result, variance = _band_classify(band, actual_num)
        variance_fmt = _fmt_variance(result, variance)

        if result_filter != "All":
            mapped = "N/A (NRO)" if result == "N/A" else result
            if mapped != result_filter:
                continue

        band_min, band_max = BAND_RANGES.get(band, (None, None)) or (None, None)
        band_range_str = (
            "N/A"
            if band_min is None
            else (
                f"${band_min:,.0f}+"
                if band_max == float("inf")
                else f"${band_min:,.0f} – ${band_max:,.0f}"
            )
        )

        rows.append({
            "Week":          r.get("week_start", ""),
            "Store":         r.get("store_name", r.get("location_id", "")),
            "DM":            dm,
            "Selected Band": band,
            "Band Range":    band_range_str,
            "Actual Sales":  f"${actual_num:,.0f}" if actual_num is not None else "No Data",
            "Result":        result,
            "$ Variance":    variance_fmt,
            "_variance_raw": variance if variance is not None else 0.0,
            "_result_raw":   result,
        })

    if not rows:
        st.info("No data matches the selected filters.")
        st.stop()

    report_df = pd.DataFrame(rows)

    # --- Summary Metrics ---
    has_sales = report_df[report_df["Actual Sales"] != "No Data"]
    total      = len(has_sales)
    on_target  = int((has_sales["Result"] == "On Target").sum())
    over       = int((has_sales["Result"] == "Over").sum())
    under      = int((has_sales["Result"] == "Under").sum())
    no_data    = int((report_df["Actual Sales"] == "No Data").sum())
    avg_var    = has_sales["_variance_raw"].mean() if total else 0

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("With Sales Data",  total)
    m2.metric("On Target",        f"{on_target} ({on_target/total*100:.0f}%)" if total else "—")
    m3.metric("Over",             f"{over} ({over/total*100:.0f}%)"           if total else "—")
    m4.metric("Under",            f"{under} ({under/total*100:.0f}%)"         if total else "—")
    m5.metric("Avg $ Variance",   f"${avg_var:+,.0f}"                         if total else "—")

    if no_data:
        st.caption(f"⚠ {no_data} store/week(s) have submissions but no sales data loaded yet.")

    st.divider()

    # --- Main table with color coding ---
    st.subheader("Store Detail")
    display_cols = ["Week", "Store", "DM", "Selected Band", "Band Range",
                    "Actual Sales", "Result", "$ Variance"]

    def _color_result(row):
        result = row["_result_raw"]
        styles = [""] * len(row)
        for i, col in enumerate(row.index):
            if col in ("Result", "$ Variance"):
                if result == "Over":
                    styles[i] = "background-color: #D5F5E3; color: #1E8449"
                elif result == "Under":
                    styles[i] = "background-color: #FADBD8; color: #922B21"
        return styles

    styled = report_df[display_cols + ["_result_raw"]].style.apply(_color_result, axis=1)
    st.dataframe(
        styled,
        use_container_width=True,
        hide_index=True,
        column_config={"_result_raw": None},  # hide raw column
    )

    st.divider()

    # --- DM Rollup ---
    st.subheader("DM Summary")
    dm_grp = has_sales.groupby("DM").agg(
        Stores      = ("Store",         "count"),
        On_Target   = ("Result",        lambda x: (x == "On Target").sum()),
        Over        = ("Result",        lambda x: (x == "Over").sum()),
        Under       = ("Result",        lambda x: (x == "Under").sum()),
        Avg_Variance= ("_variance_raw", "mean"),
    ).reset_index()
    dm_grp["On Time %"]     = (dm_grp["On_Target"].astype(float) / dm_grp["Stores"].astype(float) * 100).round(0).astype(int).astype(str) + "%"
    dm_grp["Avg $ Variance"]= dm_grp["Avg_Variance"].astype(float).apply(lambda v: f"${v:+,.0f}")
    dm_grp = dm_grp.rename(columns={"On_Target": "On Target"})

    st.dataframe(
        dm_grp[["DM", "Stores", "On Target", "Over", "Under", "On Time %", "Avg $ Variance"]],
        use_container_width=True,
        hide_index=True,
    )

    st.divider()

    # --- Excel Export (generated on click only) ---
    st.subheader("Export")
    st.caption("Click to generate — not built automatically to keep the page fast.")
    if st.button("📥 Generate Excel Report", type="primary", key="revband_export"):
        output = BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            # Sheet 1: Store Detail
            report_df[display_cols].to_excel(writer, sheet_name="Store Detail", index=False)
            # Sheet 2: DM Summary
            dm_grp[["DM", "Stores", "On Target", "Over", "Under", "On Time %", "Avg $ Variance"]].to_excel(
                writer, sheet_name="DM Summary", index=False,
            )

            workbook  = writer.book
            hdr_fmt   = workbook.add_format({"bold": True, "bg_color": "#2B3A4E", "font_color": "#FFFFFF", "border": 1})
            green_fmt = workbook.add_format({"bg_color": "#D5F5E3", "font_color": "#1E8449"})
            red_fmt   = workbook.add_format({"bg_color": "#FADBD8", "font_color": "#922B21"})

            # Style Store Detail sheet
            ws = writer.sheets["Store Detail"]
            ws.set_row(0, 18, hdr_fmt)
            ws.set_column("A:H", 18)

            # Color Result + $ Variance columns per row
            result_col_idx   = display_cols.index("Result")
            variance_col_idx = display_cols.index("$ Variance")
            for row_idx, row_data in enumerate(report_df[display_cols].itertuples(), start=1):
                result_val = getattr(row_data, "Result", "")
                fmt = green_fmt if result_val == "Over" else (red_fmt if result_val == "Under" else None)
                if fmt:
                    ws.write(row_idx, result_col_idx,   row_data.Result,     fmt)
                    ws.write(row_idx, variance_col_idx, getattr(row_data, "_7", ""), fmt)

            # Style DM Summary sheet
            ws2 = writer.sheets["DM Summary"]
            ws2.set_row(0, 18, hdr_fmt)
            ws2.set_column("A:G", 18)

        output.seek(0)
        period_label = (
            f"{filtered_week_strs[0]}_to_{filtered_week_strs[-1]}"
            if len(filtered_week_strs) > 1
            else (filtered_week_strs[0] if filtered_week_strs else "all")
        )
        st.download_button(
            label="⬇️ Download Excel",
            data=output,
            file_name=f"revband_report_{period_label}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="revband_download",
        )
