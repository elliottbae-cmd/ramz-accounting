"""
Ram-Z Accounting Toolbox — Streamlit App
-----------------------------------------
Run with:
    cd C:\\Users\\BretElliott\\ramz-accounting
    streamlit run fz_fees/app.py
"""

import sys
from pathlib import Path
from datetime import datetime
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
    generate_weekly_report, generate_midweek_report, generate_tuesday_report,
    REFERENCE_DATA_PATH, BAND_GOALS_PATH, DM_LIST_PATH,
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
# Navigation
# ---------------------------------------------------------------------------
st.sidebar.header("Accounting")
accounting_pages = [
    "FZ Fee Reconciliation",
    "AVS Weekly Report",
    "AVS Mid-Week Pulse",
    "AVS Tuesday Report",
]
st.sidebar.header("Settings")
settings_pages = [
    "Manage Stores",
    "Store Revenue Bands",
    "DM Assignments",
    "Hourly Goals",
]

all_pages = accounting_pages + settings_pages
page = st.sidebar.radio(
    "Navigation",
    all_pages,
    label_visibility="collapsed",
)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: FZ Fee Reconciliation
# ═══════════════════════════════════════════════════════════════════════════════
if page == "FZ Fee Reconciliation":
    st.title("FZ Fee Reconciliation")

    with st.sidebar:
        st.divider()
        st.subheader("Input Files")

        if LOCATIONS_PATH.exists():
            loc_df = pd.read_csv(LOCATIONS_PATH, sep="|", dtype=str)
            st.success(f"Locations master: {len(loc_df)} stores")
            locations_source = str(LOCATIONS_PATH)
        else:
            st.warning("locations.csv not found — please upload it.")
            loc_upload = st.file_uploader("Locations Master (.csv)", type=["csv"], key="loc")
            locations_source = loc_upload

        st.divider()
        fz_file = st.file_uploader("FZ Fee Schedule (.xlsx)", type=["xlsx"], key="fz",
                                    help="Weekly fee schedule from the franchisor.")
        st.divider()
        bank_file = st.file_uploader("Bank Data (.xlsx) — optional", type=["xlsx"], key="bank",
                                      help="Bank ACH transaction export. Leave blank to run FZ-only.")

        st.divider()
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

    st.divider()
    adp_file = st.file_uploader("ADP Payroll CSV", type=["csv"], key="weekly_adp",
                                 help="Upload the ADP payroll export for this period.")
    sales_file = st.file_uploader("End of Week Net Sales (.xlsx)", type=["xlsx"], key="weekly_sales",
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
                ref_data = load_reference_data()
                band_goals = load_band_goals()
                buf = generate_weekly_report(adp_file, sales_file, ref_data, band_goals, report_dates)
            st.success("Report generated!")
            st.download_button("Download Weekly Report",
                               data=buf,
                               file_name=f"AVS_Labor_Report_{start_date.strftime('%m%d%Y')}.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                               use_container_width=True)
        except Exception as e:
            st.error(f"Error: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: AVS Mid-Week Pulse
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "AVS Mid-Week Pulse":
    st.title("AVS Mid-Week Labor Pulse")
    st.caption("Thu-Sun hours vs. weekly goal. Red >65%, Grey 60-65%, Green <60%.")

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Report Start Date", key="mw_start")
    with col2:
        end_date = st.date_input("Report End Date", key="mw_end")

    report_dates = f"{start_date.month}.{start_date.day}.{start_date.strftime('%y')} - {end_date.month}.{end_date.day}.{end_date.strftime('%y')}"

    st.divider()
    adp_file = st.file_uploader("ADP Payroll CSV", type=["csv"], key="mw_adp",
                                 help="Upload the ADP payroll export for Thu-Sun.")

    st.divider()
    if st.button("Generate Report", type="primary", use_container_width=True, key="mw_run"):
        if adp_file is None:
            st.error("Please upload the ADP Payroll CSV.")
            st.stop()

        try:
            with st.spinner("Generating mid-week report..."):
                ref_data = load_reference_data()
                band_goals = load_band_goals()
                buf = generate_midweek_report(adp_file, ref_data, band_goals, report_dates)
            st.success("Report generated!")
            st.download_button("Download Mid-Week Report",
                               data=buf,
                               file_name=f"AVS_MidWeek_Report_{start_date.strftime('%m%d%Y')}.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                               use_container_width=True)
        except Exception as e:
            st.error(f"Error: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: AVS Tuesday Report
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "AVS Tuesday Report":
    st.title("AVS Tuesday Labor Report")
    st.caption("Full week through Tuesday. Red >90%, Grey 87-90%, Green <85%.")

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Report Start Date", key="tue_start")
    with col2:
        end_date = st.date_input("Report End Date", key="tue_end")

    report_dates = f"{start_date.month}.{start_date.day}.{start_date.strftime('%y')} - {end_date.month}.{end_date.day}.{end_date.strftime('%y')}"

    st.divider()
    adp_file = st.file_uploader("ADP Payroll CSV", type=["csv"], key="tue_adp",
                                 help="Upload the ADP payroll export through Tuesday.")

    st.divider()
    if st.button("Generate Report", type="primary", use_container_width=True, key="tue_run"):
        if adp_file is None:
            st.error("Please upload the ADP Payroll CSV.")
            st.stop()

        try:
            with st.spinner("Generating Tuesday report..."):
                ref_data = load_reference_data()
                band_goals = load_band_goals()
                buf = generate_tuesday_report(adp_file, ref_data, band_goals, report_dates)
            st.success("Report generated!")
            st.download_button("Download Tuesday Report",
                               data=buf,
                               file_name=f"AVS_Tuesday_Report_{start_date.strftime('%m%d%Y')}.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                               use_container_width=True)
        except Exception as e:
            st.error(f"Error: {e}")


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
    st.caption("Assign a revenue band to each store. Changes are saved and used by all AVS reports.")

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
        st.success("Revenue bands saved!")
        st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: DM Assignments
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "DM Assignments":
    st.title("DM Assignments")
    st.caption("Manage the list of District Managers and assign stores to DMs.")

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
        st.success("DM assignments saved!")
        st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE: Hourly Goals
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "Hourly Goals":
    st.title("Hourly Goals by Revenue Band")
    st.caption("Edit the weekly hourly goal for each revenue band. Changes apply to all AVS reports.")

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
        st.success("Hourly goals saved!")
        st.rerun()
