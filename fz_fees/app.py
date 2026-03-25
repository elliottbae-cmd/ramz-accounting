"""
Ram-Z Accounting Toolbox — Streamlit App
-----------------------------------------
Run with:
    cd C:\\Users\\BretElliott\\ramz-accounting
    streamlit run fz_fees/app.py
"""

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
    HELPDESK_FEE_WEEKLY,
)
from avs_engine import (
    load_reference_data, load_band_goals, load_dm_list,
    generate_weekly_report, generate_midweek_report,
    REFERENCE_DATA_PATH, BAND_GOALS_PATH, DM_LIST_PATH,
)
from weekly_lock import (
    get_week_start, get_week_end, get_next_week_start, format_week_label,
    load_locked_config, lock_exists, create_lock,
    ensure_current_week_locked, override_locked_value, get_locked_weeks,
    load_change_log, is_admin, load_admin_users, add_admin, remove_admin,
    WEEKLY_LOCK_PATH, CHANGE_LOG_PATH, ADMIN_USERS_PATH,
)

# ---------------------------------------------------------------------------
# Config paths
# ---------------------------------------------------------------------------
LOCATIONS_PATH = _FZ_DIR / "locations.csv"

# ---------------------------------------------------------------------------
# Revenue band options (for dropdowns)
# ---------------------------------------------------------------------------
BAND_OPTIONS = [
    "<25k", "25k-30k", "30k-35k", "35k-40k", "40k-45k",
    "45k-50k", "50k+", "NRO Seasoned", "NRO",
]

# ---------------------------------------------------------------------------
# Page setup
# ---------------------------------------------------------------------------
st.set_page_config(page_title="Ram-Z Accounting Toolbox", layout="wide")

# ---------------------------------------------------------------------------
# Compact sidebar CSS
# ---------------------------------------------------------------------------
st.markdown("""
<style>
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
    background: rgba(151, 166, 195, 0.15);
}

/* Compact expanders */
section[data-testid="stSidebar"] details {
    margin-top: 0.25rem;
    margin-bottom: 0.25rem;
}
section[data-testid="stSidebar"] details summary {
    padding: 0.3rem 0;
    font-size: 0.85rem;
}
section[data-testid="stSidebar"] details[open] [data-testid="stVerticalBlock"] {
    gap: 0.1rem;
    padding-left: 0.5rem;
}

/* Sidebar header */
section[data-testid="stSidebar"] h2 {
    font-size: 1rem;
    margin-bottom: 0.25rem;
    padding-bottom: 0.25rem;
    border-bottom: 1px solid rgba(151, 166, 195, 0.2);
}
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
user_is_admin = is_admin(current_user) if current_user else True  # Default admin if no auth

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
        "Performance Review",
    ],
    "settings": [
        "Manage Stores",
        "Store Revenue Bands",
        "DM Assignments",
        "Hourly Goals",
    ],
    "admin": [
        "Weekly Config",
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


render_nav_section("Accounting", "accounting")
render_nav_section("Labor", "labor")
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

    if LOCATIONS_PATH.exists():
        loc_df = pd.read_csv(LOCATIONS_PATH, sep="|", dtype=str)
        st.success(f"Locations master: {len(loc_df)} stores")
        locations_source = str(LOCATIONS_PATH)
    else:
        st.warning("locations.csv not found — please upload it.")
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

        st.divider()
        if st.button("Clear & Run New Report", use_container_width=True, key="fz_clear"):
            for k in ("results", "report_buf", "invoice_csv", "fz_week_end_dt",
                       "bank_date_str", "fiscal_yr", "week_num", "invoices"):
                st.session_state.pop(k, None)
            st.session_state["fz_upload_ctr"] += 1
            st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: AVS Weekly Report
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "AVS Weekly Report":
    st.title("AVS Weekly Labor Report")
    st.caption("Full weekly report with AvS Summary, Store Rankings, and DM Rankings.")

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Report Start Date", key="weekly_start")
    with col2:
        end_date = st.date_input("Report End Date", key="weekly_end")

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
                ref_data = load_reference_data()
                band_goals = load_band_goals()
                locked = ensure_current_week_locked(ref_data, band_goals, start_date)
                # Build ref_data and band_goals from locked values
                locked_band_goals = dict(zip(locked["revenue_band"], locked["hourly_goal"]))
                buf = generate_weekly_report(adp_file, sales_file, locked, locked_band_goals, report_dates)
            st.success("Report generated!")
            ws = get_week_start(start_date)
            st.caption(f"Used locked config for week: {format_week_label(ws)}")
            st.download_button("Download Weekly Report",
                               data=buf,
                               file_name=f"AVS_Labor_Report_{start_date.strftime('%m%d%Y')}.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                               use_container_width=True)
            st.divider()
            if st.button("Clear & Run New Report", use_container_width=True, key="weekly_clear"):
                st.session_state["weekly_upload_ctr"] += 1
                st.rerun()
        except Exception as e:
            st.error(f"Error: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: AVS Mid-Week Pulse
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "AVS Mid-Week Pulse":
    st.title("AVS Mid-Week Labor Pulse")
    st.caption("Cumulative hours vs. weekly goal with day-specific color thresholds.")

    DAY_OPTIONS = ["Friday", "Saturday", "Sunday", "Monday", "Tuesday", "Wednesday"]

    col1, col2, col3 = st.columns(3)
    with col1:
        start_date = st.date_input("Report Start Date", key="mw_start")
    with col2:
        end_date = st.date_input("Report End Date", key="mw_end")
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
                ref_data = load_reference_data()
                band_goals = load_band_goals()
                locked = ensure_current_week_locked(ref_data, band_goals, start_date)
                locked_band_goals = dict(zip(locked["revenue_band"], locked["hourly_goal"]))
                buf = generate_midweek_report(adp_file, locked, locked_band_goals, report_dates, through_day)
            st.success("Report generated!")
            ws = get_week_start(start_date)
            st.caption(f"Used locked config for week: {format_week_label(ws)} | Thresholds: {through_day}")
            st.download_button("Download Mid-Week Report",
                               data=buf,
                               file_name=f"AVS_MidWeek_Report_{start_date.strftime('%m%d%Y')}.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                               use_container_width=True)
            st.divider()
            if st.button("Clear & Run New Report", use_container_width=True, key="mw_clear"):
                st.session_state["mw_upload_ctr"] += 1
                st.rerun()
        except Exception as e:
            st.error(f"Error: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: Performance Review
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "Performance Review":
    st.title("Performance Review")
    st.caption("Compare store performance (Goal vs Actual hours) across weeks.")

    locked_weeks = get_locked_weeks()
    if not locked_weeks:
        st.info("No weekly data available yet. Run AVS reports to build history.")
        st.stop()

    # --- Filter controls ---
    col_filter1, col_filter2, col_filter3 = st.columns(3)
    with col_filter1:
        period_filter = st.selectbox("View by", ["All Weeks", "Month", "Quarter", "Year"], key="perf_period")
    with col_filter2:
        if period_filter == "Month":
            months_available = sorted(set((w.year, w.month) for w in locked_weeks))
            month_labels = [f"{y}-{m:02d}" for y, m in months_available]
            selected_month = st.selectbox("Select Month", month_labels, key="perf_month")
            sel_year, sel_month = int(selected_month[:4]), int(selected_month[5:])
            filtered_weeks = [w for w in locked_weeks if w.year == sel_year and w.month == sel_month]
        elif period_filter == "Quarter":
            quarters_available = sorted(set((w.year, (w.month - 1) // 3 + 1) for w in locked_weeks))
            quarter_labels = [f"{y} Q{q}" for y, q in quarters_available]
            selected_quarter = st.selectbox("Select Quarter", quarter_labels, key="perf_quarter")
            sel_year = int(selected_quarter[:4])
            sel_q = int(selected_quarter[-1])
            q_months = [(sel_q - 1) * 3 + 1, (sel_q - 1) * 3 + 2, (sel_q - 1) * 3 + 3]
            filtered_weeks = [w for w in locked_weeks if w.year == sel_year and w.month in q_months]
        elif period_filter == "Year":
            years_available = sorted(set(w.year for w in locked_weeks))
            selected_year = st.selectbox("Select Year", years_available, key="perf_year")
            filtered_weeks = [w for w in locked_weeks if w.year == selected_year]
        else:
            filtered_weeks = locked_weeks
    with col_filter3:
        group_by = st.selectbox("Group by", ["Store", "DM", "Revenue Band"], key="perf_group")

    if not filtered_weeks:
        st.warning("No data for the selected period.")
        st.stop()

    # --- Build performance data from locked weeks ---
    from weekly_lock import load_all_locks
    all_locks = load_all_locks()
    week_strs = [str(w) for w in filtered_weeks]
    perf_data = all_locks[all_locks["week_start"].isin(week_strs)].copy()
    perf_data["hourly_goal"] = pd.to_numeric(perf_data["hourly_goal"], errors="coerce").fillna(0)

    st.subheader(f"Weeks: {format_week_label(filtered_weeks[0])} to {format_week_label(filtered_weeks[-1])}")
    st.caption(f"{len(filtered_weeks)} week(s) selected")

    if group_by == "Store":
        # Pivot: rows = stores, columns = weeks
        pivot = perf_data.pivot_table(
            index=["location_id", "store_name"],
            columns="week_start",
            values="hourly_goal",
            aggfunc="first",
        ).reset_index()
        pivot.columns.name = None
        display = pivot.rename(columns={"location_id": "Store #", "store_name": "Store Name"})
        display = display.sort_values("Store #").reset_index(drop=True)
        st.dataframe(display, use_container_width=True, height=500)

    elif group_by == "DM":
        dm_summary = perf_data.groupby(["week_start", "dm"]).agg(
            total_goal=("hourly_goal", "sum"),
            store_count=("location_id", "count"),
        ).reset_index()
        pivot = dm_summary.pivot_table(
            index="dm", columns="week_start", values="total_goal", aggfunc="first"
        ).reset_index()
        pivot.columns.name = None
        pivot = pivot.rename(columns={"dm": "DM"}).sort_values("DM")
        st.dataframe(pivot, use_container_width=True, height=400)

    elif group_by == "Revenue Band":
        band_summary = perf_data.groupby(["week_start", "revenue_band"]).agg(
            avg_goal=("hourly_goal", "mean"),
            store_count=("location_id", "count"),
        ).reset_index()
        pivot = band_summary.pivot_table(
            index="revenue_band", columns="week_start", values="avg_goal", aggfunc="first"
        ).reset_index()
        pivot.columns.name = None
        pivot = pivot.rename(columns={"revenue_band": "Revenue Band"})
        st.dataframe(pivot, use_container_width=True, height=400)

    st.info(
        "This page currently shows the locked hourly goals per week. "
        "Once you start running AVS reports, actual labor hours will also be "
        "captured here for Goal vs Actual comparison."
    )


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: Manage Stores
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "Manage Stores":
    st.title("Manage Store Locations")
    st.caption("Add or remove stores from the master locations list.")

    if LOCATIONS_PATH.exists():
        stores_df = pd.read_csv(LOCATIONS_PATH, sep="|", dtype=str)
        stores_df["location_id"] = stores_df["location_id"].str.strip().str.upper()
        stores_df["store_name"] = stores_df["store_name"].str.strip()
    else:
        stores_df = pd.DataFrame(columns=["location_id", "store_name"])

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
            new_row = pd.DataFrame([{"location_id": new_id_clean, "store_name": new_name_clean}])
            updated_df = pd.concat([stores_df, new_row], ignore_index=True)
            updated_df = updated_df.sort_values("location_id").reset_index(drop=True)
            updated_df.to_csv(LOCATIONS_PATH, sep="|", index=False)

            # Also add to reference_data.csv with defaults
            if REFERENCE_DATA_PATH.exists():
                ref_df = pd.read_csv(REFERENCE_DATA_PATH, sep="|", dtype=str)
                if new_id_clean not in ref_df["location_id"].values:
                    dm_list = load_dm_list()
                    new_ref = pd.DataFrame([{
                        "location_id": new_id_clean,
                        "store_name": new_name_clean,
                        "dm": dm_list[0] if dm_list else "",
                        "revenue_band": "<25k",
                    }])
                    ref_df = pd.concat([ref_df, new_ref], ignore_index=True)
                    ref_df = ref_df.sort_values("location_id").reset_index(drop=True)
                    ref_df.to_csv(REFERENCE_DATA_PATH, sep="|", index=False)

            st.success(f"Added store {new_id_clean} — {new_name_clean}")
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
                updated_df = stores_df[stores_df["location_id"] != remove_id].copy()
                updated_df.to_csv(LOCATIONS_PATH, sep="|", index=False)

                # Also remove from reference_data.csv
                if REFERENCE_DATA_PATH.exists():
                    ref_df = pd.read_csv(REFERENCE_DATA_PATH, sep="|", dtype=str)
                    ref_df = ref_df[ref_df["location_id"] != remove_id]
                    ref_df.to_csv(REFERENCE_DATA_PATH, sep="|", index=False)

                st.success(f"Removed store {remove_id}")
                st.rerun()
    else:
        st.info("No stores to remove.")


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: Store Revenue Bands
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "Store Revenue Bands":
    st.title("Store Revenue Bands")
    st.caption("Assign a revenue band to each store. Changes apply to the next unlocked week.")

    show_week_deadline_banner()

    if not REFERENCE_DATA_PATH.exists():
        st.error("Reference data file not found. Please add stores first.")
        st.stop()

    ref_df = pd.read_csv(REFERENCE_DATA_PATH, sep="|", dtype=str)
    ref_df = ref_df.sort_values("location_id").reset_index(drop=True)

    # Load band goals for the info display
    band_goals = load_band_goals()

    st.info("Each revenue band maps to an hourly goal. You can edit the goals on the **Hourly Goals** page.")

    # Build the form with dropdowns for each store
    st.subheader(f"Assign Bands ({len(ref_df)} stores)")

    with st.form("revenue_bands_form"):
        new_bands = {}
        for idx, row in ref_df.iterrows():
            current_band = row["revenue_band"] if pd.notna(row["revenue_band"]) else "<25k"
            current_idx = BAND_OPTIONS.index(current_band) if current_band in BAND_OPTIONS else 0
            goal_str = f"({band_goals.get(current_band, '?')} hrs)" if current_band in band_goals else ""

            col1, col2, col3 = st.columns([2, 3, 3])
            with col1:
                st.text(row["location_id"])
            with col2:
                st.text(row["store_name"])
            with col3:
                new_bands[row["location_id"]] = st.selectbox(
                    f"Band for {row['location_id']}",
                    BAND_OPTIONS,
                    index=current_idx,
                    key=f"band_{row['location_id']}",
                    label_visibility="collapsed",
                )

        save_btn = st.form_submit_button("Save Changes", type="primary", use_container_width=True)

    if save_btn:
        for store_id, band in new_bands.items():
            ref_df.loc[ref_df["location_id"] == store_id, "revenue_band"] = band
        ref_df.to_csv(REFERENCE_DATA_PATH, sep="|", index=False)
        st.success("Revenue bands saved! These will apply to the next unlocked week.")
        st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: DM Assignments
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "DM Assignments":
    st.title("DM Assignments")
    st.caption("Manage the list of District Managers and assign stores to DMs.")

    show_week_deadline_banner()

    # --- DM List Management ---
    st.subheader("DM List")
    dm_list = load_dm_list()
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
                dm_list.append(dm_name)
                dm_list.sort()
                pd.DataFrame({"dm_name": dm_list}).to_csv(DM_LIST_PATH, index=False)
                st.success(f"Added DM: {dm_name}")
                st.rerun()

    with col2:
        if dm_list:
            with st.form("remove_dm_form"):
                remove_dm = st.selectbox("Remove DM", dm_list, index=None, placeholder="Choose a DM...")
                remove_dm_btn = st.form_submit_button("Remove DM")
            if remove_dm_btn and remove_dm:
                dm_list.remove(remove_dm)
                pd.DataFrame({"dm_name": dm_list}).to_csv(DM_LIST_PATH, index=False)
                st.success(f"Removed DM: {remove_dm}")
                st.rerun()

    st.divider()

    # --- Store-to-DM Assignment ---
    st.subheader("Store-to-DM Assignments")

    if not REFERENCE_DATA_PATH.exists():
        st.error("Reference data file not found.")
        st.stop()

    ref_df = pd.read_csv(REFERENCE_DATA_PATH, sep="|", dtype=str)
    ref_df = ref_df.sort_values("location_id").reset_index(drop=True)
    dm_list = load_dm_list()

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
        ref_df.to_csv(REFERENCE_DATA_PATH, sep="|", index=False)
        st.success("DM assignments saved! These will apply to the next unlocked week.")
        st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: Hourly Goals
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "Hourly Goals":
    st.title("Hourly Goals by Revenue Band")
    st.caption("Edit the weekly hourly goal for each revenue band. Changes apply to the next unlocked week.")

    show_week_deadline_banner()

    band_goals = load_band_goals()

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
        goals_df = pd.DataFrame([
            {"revenue_band": band, "hourly_goal": goal}
            for band, goal in new_goals.items()
        ])
        goals_df.to_csv(BAND_GOALS_PATH, sep="|", index=False)
        st.success("Hourly goals saved! These will apply to the next unlocked week.")
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

    st.subheader(f"Locked Config: {week_labels[selected_week]}")

    # Show source info
    from weekly_lock import load_all_locks
    all_locks = load_all_locks()
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
            dm_list = load_dm_list()
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
                st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")


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
                    st.rerun()
        else:
            st.info("Cannot remove the last admin.")
