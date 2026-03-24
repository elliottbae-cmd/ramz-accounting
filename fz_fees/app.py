"""
Streamlit UI for FZ Fee Reconciliation
---------------------------------------
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

# Ensure reconcile.py is importable from the same directory
sys.path.insert(0, str(Path(__file__).parent))
from reconcile import (
    load_locations,
    load_fz_schedule,
    detect_bank_date,
    load_bank_data,
    reconcile,
    generate_invoices,
    write_report,
    HELPDESK_FEE_WEEKLY,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
LOCATIONS_PATH = Path(__file__).parent / "locations.csv"

# ---------------------------------------------------------------------------
# Page setup
# ---------------------------------------------------------------------------
st.set_page_config(page_title="FZ Fee Reconciliation", layout="wide")

# ---------------------------------------------------------------------------
# Navigation
# ---------------------------------------------------------------------------
page = st.sidebar.radio("Navigation", ["Reconciliation", "Manage Stores"], index=0)


# ===========================================================================
# PAGE 1 — Reconciliation
# ===========================================================================
if page == "Reconciliation":
    st.title("FZ Fee Reconciliation")

    with st.sidebar:
        st.divider()
        st.header("Input Files")

        # Locations file
        if LOCATIONS_PATH.exists():
            loc_df = pd.read_csv(LOCATIONS_PATH, sep="|", dtype=str)
            st.success(f"Locations master: {len(loc_df)} stores")
            locations_source = str(LOCATIONS_PATH)
        else:
            st.warning("locations.csv not found — please upload it.")
            loc_upload = st.file_uploader("Locations Master (.csv)", type=["csv"], key="loc")
            locations_source = loc_upload

        st.divider()

        # FZ fee schedule
        fz_file = st.file_uploader(
            "FZ Fee Schedule (.xlsx)", type=["xlsx"], key="fz",
            help="Weekly fee schedule from the franchisor."
        )

        st.divider()

        # Bank data (optional)
        bank_file = st.file_uploader(
            "Bank Data (.xlsx) — optional", type=["xlsx"], key="bank",
            help="Bank ACH transaction export. Leave blank to run FZ-only."
        )

        # Bank date
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

        bank_date = st.date_input(
            "Bank Date",
            value=detected_bank_date or datetime.today().date(),
            help="Date label for the bank data. Auto-detected when possible."
        )
        if detected_bank_date:
            st.caption(f"Auto-detected from file: {detected_bank_date.strftime('%m/%d/%Y')}")

        st.divider()
        run_btn = st.button("Run Reconciliation", type="primary", use_container_width=True)

    # -----------------------------------------------------------------------
    # Main area — run reconciliation on button click
    # -----------------------------------------------------------------------
    if run_btn:
        # Validate inputs
        if locations_source is None:
            st.error("Please upload a locations.csv file.")
            st.stop()
        if fz_file is None:
            st.error("Please upload the FZ fee schedule.")
            st.stop()

        try:
            with st.status("Running reconciliation...", expanded=True) as status:
                # Step 1: Load locations
                st.write("Loading locations master...")
                locations = load_locations(locations_source)

                # Step 2: Load FZ schedule
                st.write("Loading FZ fee schedule...")
                fz_df, fz_week_end_dt = load_fz_schedule(fz_file)
                if fz_week_end_dt is None:
                    st.error(
                        "Could not detect the week-end date from the FZ file. "
                        "Please check that the 'Week End' column contains valid dates."
                    )
                    st.stop()

                fiscal_yr = fz_df["fiscal_year"].iloc[0]
                week_num = fz_df["week_num"].iloc[0]

                # Step 3: Load bank data
                bank_date_str = bank_date.strftime("%m/%d/%Y")
                if bank_file is not None:
                    st.write("Loading bank data...")
                    bank_file.seek(0)
                    bank_df = load_bank_data(bank_file)
                else:
                    st.write("No bank data provided — payment columns will be blank.")
                    bank_df = pd.DataFrame(columns=[
                        "store_id", "royalty_paid", "marketing_paid",
                        "franchise_paid", "helpdesk_paid",
                    ])

                # Step 4: Reconcile
                st.write("Reconciling fees vs payments...")
                results = reconcile(locations, fz_df, bank_df)

                # Step 5: Generate outputs in memory
                st.write("Generating Excel report...")
                report_buf = BytesIO()
                write_report(results, fz_week_end_dt, bank_date_str, output=report_buf)
                report_buf.seek(0)

                st.write("Generating invoice CSV...")
                invoice_buf = StringIO()
                invoices = generate_invoices(
                    results, fz_week_end_dt, fiscal_yr, week_num, output=invoice_buf
                )
                invoice_csv = invoice_buf.getvalue()

                status.update(label="Reconciliation complete!", state="complete")

            # Store in session state
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

    # -----------------------------------------------------------------------
    # Display results (persists via session state across reruns)
    # -----------------------------------------------------------------------
    if "results" in st.session_state:
        results = st.session_state["results"]
        fz_week_end_dt = st.session_state["fz_week_end_dt"]
        bank_date_str = st.session_state["bank_date_str"]
        fiscal_yr = st.session_state["fiscal_yr"]
        week_num = st.session_state["week_num"]

        flagged = results[results["Flag Count"] > 0]
        clean = results[results["Flag Count"] == 0]

        # --- Summary metrics ---
        st.subheader(
            f"Week {int(week_num)} — FZ Week End: {fz_week_end_dt.strftime('%m/%d/%Y')}  |  "
            f"Bank Date: {bank_date_str}"
        )

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Stores", len(results))
        col2.metric("Clean", len(clean))
        col3.metric("Flagged", len(flagged), delta=f"-{len(flagged)}" if len(flagged) > 0 else None, delta_color="inverse")
        col4.metric("Total Net Sales", f"${results['Reported Net Sales'].sum():,.2f}")

        # --- Results table with color coding ---
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
            "Royalty Fee Owed": "${:,.2f}",
            "Royalty Fee Paid": "${:,.2f}",
            "Royalty Variance": "${:+,.2f}",
            "Marketing Fee Owed": "${:,.2f}",
            "Marketing Fee Paid": "${:,.2f}",
            "Marketing Variance": "${:+,.2f}",
            "Franchise Fee Owed": "${:,.2f}",
            "Franchise Fee Paid": "${:,.2f}",
            "Franchise Variance": "${:+,.2f}",
            "Help Desk Fee Owed": "${:,.2f}",
            "Help Desk Fee Paid": "${:,.2f}",
            "Help Desk Variance": "${:+,.2f}",
        }, na_rep="—")

        st.dataframe(styled, use_container_width=True, height=600)

        # --- Flagged stores detail ---
        if not flagged.empty:
            with st.expander(f"Flagged Stores ({len(flagged)})", expanded=True):
                for _, row in flagged.iterrows():
                    st.markdown(
                        f"**{row['Store #']}** {row['Store Name']}  \n"
                        f"_{row['Flag Details']}_"
                    )

        # --- Downloads ---
        st.subheader("Downloads")
        fz_str = fz_week_end_dt.strftime("%m%d%Y")
        bank_str = bank_date_str.replace("/", "")

        dl_col1, dl_col2 = st.columns(2)
        with dl_col1:
            st.download_button(
                label="Download Excel Report",
                data=st.session_state["report_buf"],
                file_name=f"reconciliation_FZ-week-end-{fz_str}_bank-{bank_str}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
        with dl_col2:
            inv = st.session_state["invoices"][0]
            st.download_button(
                label="Download Invoice CSV",
                data=st.session_state["invoice_csv"],
                file_name=inv[1],
                mime="text/csv",
                use_container_width=True,
            )


# ===========================================================================
# PAGE 2 — Manage Stores
# ===========================================================================
elif page == "Manage Stores":
    st.title("Manage Store Locations")
    st.caption("Add or remove stores from the master locations list used by the reconciliation.")

    # -----------------------------------------------------------------------
    # Load current stores
    # -----------------------------------------------------------------------
    if LOCATIONS_PATH.exists():
        stores_df = pd.read_csv(LOCATIONS_PATH, sep="|", dtype=str)
        stores_df["location_id"] = stores_df["location_id"].str.strip().str.upper()
        stores_df["store_name"] = stores_df["store_name"].str.strip()
    else:
        stores_df = pd.DataFrame(columns=["location_id", "store_name"])

    # -----------------------------------------------------------------------
    # Current store list
    # -----------------------------------------------------------------------
    st.subheader(f"Current Stores ({len(stores_df)})")

    if not stores_df.empty:
        display = stores_df.copy()
        display.columns = ["Store Number", "Store Name"]
        display = display.sort_values("Store Number").reset_index(drop=True)
        display.index = display.index + 1  # 1-based display index
        st.dataframe(display, use_container_width=True, height=400)
    else:
        st.info("No stores in the master list yet.")

    st.divider()

    # -----------------------------------------------------------------------
    # Add a store
    # -----------------------------------------------------------------------
    st.subheader("Add a Store")

    with st.form("add_store_form", clear_on_submit=True):
        col1, col2 = st.columns(2)
        with col1:
            new_id = st.text_input(
                "Store Number",
                placeholder="e.g. 112-0039",
                help="Format: 112-XXXX"
            )
        with col2:
            new_name = st.text_input(
                "Store Name",
                placeholder="e.g. Springfield (MO)",
                help="City and state, matching the franchisor format"
            )
        add_btn = st.form_submit_button("Add Store", type="primary")

    if add_btn:
        new_id_clean = new_id.strip().upper()
        new_name_clean = new_name.strip()

        if not new_id_clean:
            st.error("Please enter a store number.")
        elif not new_name_clean:
            st.error("Please enter a store name.")
        elif new_id_clean in stores_df["location_id"].values:
            st.error(f"Store {new_id_clean} already exists in the master list.")
        else:
            # Append and save
            new_row = pd.DataFrame([{
                "location_id": new_id_clean,
                "store_name": new_name_clean,
            }])
            updated_df = pd.concat([stores_df, new_row], ignore_index=True)
            updated_df = updated_df.sort_values("location_id").reset_index(drop=True)
            updated_df.to_csv(LOCATIONS_PATH, sep="|", index=False)
            st.success(f"Added store {new_id_clean} — {new_name_clean}")
            st.rerun()

    st.divider()

    # -----------------------------------------------------------------------
    # Remove a store
    # -----------------------------------------------------------------------
    st.subheader("Remove a Store")

    if not stores_df.empty:
        # Build display labels for the selectbox
        options = stores_df.sort_values("location_id").apply(
            lambda r: f"{r['location_id']}  —  {r['store_name']}", axis=1
        ).tolist()

        selected = st.selectbox("Select store to remove", options, index=None,
                                placeholder="Choose a store...")

        if selected:
            remove_id = selected.split("  —  ")[0].strip()
            st.warning(
                f"This will remove **{selected}** from the master list. "
                f"It will no longer appear in future reconciliations."
            )
            if st.button("Confirm Remove", type="primary"):
                updated_df = stores_df[stores_df["location_id"] != remove_id].copy()
                updated_df.to_csv(LOCATIONS_PATH, sep="|", index=False)
                st.success(f"Removed store {remove_id}")
                st.rerun()
    else:
        st.info("No stores to remove.")
