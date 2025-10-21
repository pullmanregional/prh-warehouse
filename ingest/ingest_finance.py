import os
import re
import logging
import warnings
import pandas as pd
from datetime import datetime
from sqlmodel import Session
from finance import sanity, parse, transform
from util import util, prw_meta_utils
from prw_common.model.prw_finance_model import *
from prw_common.cli_utils import cli_parser
from prw_common.db_utils import (
    TableData,
    get_db_connection,
    mask_conn_pw,
    clear_tables_and_insert_data,
)

# -------------------------------------------------------
# Config
# -------------------------------------------------------
# Unique identifier for this ingest dataset
INGEST_DATASET_ID = "finance"

# Opt into pandas 3 behavior for replace() and fillna(), where columns of object dtype are NOT changed to something more specific,
# like int/float/str. This option can be removed once upgrading pandas 2-> 3.
pd.set_option("future.no_silent_downcasting", True)

# Logging definitions
logging.basicConfig(level=logging.INFO)
SHOW_SQL_IN_LOG = False

# Suppress openpyxl warnings about missing default styles
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# Specific worksheet names containing data to ingest in the Dashboard Supporting Data spreadsheet
VOLUMES_BUDGET_SHEET = "Data"
HRS_PER_VOLUME_SHEET = "Prod.MH UOS"
CONTRACTED_HRS_SHEET = "ProdHrs"
# Historical file format (2024 and older)
HISTORICAL_VOLUMES_SHEET = "STATS"
HISTORICAL_UOS_SHEET = "UOS"
HISTORICAL_VOLUMES_YEAR = 2024
# Current file format (2025+)
VOLUMES_SHEET = "STATS Actual"
UOS_SHEET = "UOS Actual"

# The historical hours data returned by get_file_paths() as historical_hours_file contains data for 2022
HISTORICAL_HOURS_YEAR = 2022


# -------------------------------------------------------
# Extract from Source Files
# -------------------------------------------------------
def get_file_paths(base_path, epic_in):
    """
    Find all required source files for finance data ingestion.
    Returns paths for:
    - Historical volume data file (Dashboard Supporting Data 2024)
    - Current format files organized by year
    - Income statement path
    - Hours path and historical hours file
    - Accounts Receivable totals by age
    - Miscellaneous volumes that are pulled by exporter/misc-volumes.sql - needed for the KPI dashboard until the actual source data is ingested into warehouse
    """
    # Historical volume data file
    historical_volumes_file = os.path.join(
        base_path, "Supporting Data", "Dashboard Supporting Data 2024 v3.xlsx"
    )

    # The "Supporting Data/YYYY/" directories contains the latest Dashboard Supporting Data spreadsheet files
    volumes_path = os.path.join(base_path, "Supporting Data")

    # Miscellaneous volumes file (temp summary data from Epic) is in Sources/epic dir
    misc_volumes_file = os.path.join(epic_in, "misc-volumes.csv")

    # The Natural Class subdir contains income statement in one Excel file per month, eg,
    # ./Natural Class/2022/(01) Jan 2022 Natural Class.xlsx
    income_stmt_path = os.path.join(base_path, "Natural Class")

    # The Balance subdir contains balance statements in one Excel file per month, eg,
    # ./Balance/2022/(01) Jan 2022 Balance Sheet.xlsx
    balance_path = os.path.join(base_path, "Balance")

    # PayPeriod subdir contains productive / non-productive hours and FTE data per month. eg,
    #   ./PayPeriod/2023/PP#1 2023 Payroll_Productivity_by_Cost_Center.xlsx
    # In addition, historical data for 2022 PP#1-25, which includes the clinic network, is lumped together a separate file:
    #   ./PayPeriod/2023/PP#1-PP#25 Payroll Productivity.xlsx
    historical_hours_file = os.path.join(
        base_path, "PayPeriod", "2022", "PP#1-PP#26 Payroll Productivity.xlsx"
    )
    hours_path = os.path.join(base_path, "PayPeriod")

    # Accounts receivable data. Aged AR contains totals in AR by age bucket (eg 1-30 days, 31-60 days, etc)
    aged_ar_file = os.path.join(epic_in, "aged-ar.csv")

    return (
        historical_volumes_file,
        volumes_path,
        misc_volumes_file,
        income_stmt_path,
        balance_path,
        hours_path,
        historical_hours_file,
        aged_ar_file,
    )


def find_volumes_files(volumes_path, min_year):
    """
    Find the latest Dashboard Supporting Data file for each year from the DOMO directory
    """
    ret = []
    all_files = util.find_data_files(volumes_path)

    # Filter for Dashboard Supporting Data files
    dashboard_files = [f for f in all_files if "dashboard supporting data" in f.lower()]

    # Group files by year
    files_by_year = {}
    for files in dashboard_files:
        # Extract year from filename
        year_match = re.search(r"(\d{4}) Dashboard", files)
        if year_match:
            year = year_match.group(1)
            if year not in files_by_year:
                files_by_year[year] = []
            files_by_year[year].append(files)

    # For each year, get the latest file based on month number in the filename
    for year, files in files_by_year.items():
        if int(year) >= int(min_year) and files:
            # Extract month number from filename format (MM) and sort
            def get_month_num(file):
                month_match = re.search(r"\((\d+)\)", os.path.basename(file))
                return int(month_match.group(1)) if month_match else 0

            latest_file = sorted(files, key=get_month_num, reverse=True)[0]
            ret.append(latest_file)

    return ret


# -------------------------------------------------------
# Main entry point
# -------------------------------------------------------
def parse_arguments():
    parser = cli_parser(
        description="Ingest finance data into PRH warehouse.",
        require_prw=True,
        require_in=True,
    )

    # Add script-specific arguments
    parser.add_argument(
        "--epic-in",
        type=str,
        help="Path to epic source directory",
    )
    parser.add_argument(
        "--drop",
        action="store_true",
        help="Drop and recreate all tables before ingesting data",
    )
    return parser.parse_args()


def main():
    # Load config from cmd line
    args = parse_arguments()
    base_path = args.input
    epic_in = args.epic_in
    output_conn = args.prw
    drop_tables = args.drop
    logging.info(f"Data dir: {base_path}, output: {mask_conn_pw(output_conn)}")
    logging.info(f"Drop tables before writing: {drop_tables}")

    # Source file paths
    (
        historical_volumes_file,
        volumes_path,
        misc_volumes_file,
        income_stmt_path,
        balance_path,
        hours_path,
        historical_hours_file,
        aged_ar_file,
    ) = get_file_paths(base_path, epic_in)

    # Sanity check data directory expected location and files
    if not sanity.check_data_dir(
        base_path,
        historical_volumes_file,
        misc_volumes_file,
        income_stmt_path,
        balance_path,
        hours_path,
        aged_ar_file,
    ):
        logging.error("ERROR: data directory error (see above). Terminating.")
        exit(1)

    # Get list of dynamic data files, ie data organized as one Excel workbook per month
    volumes_files = find_volumes_files(volumes_path, HISTORICAL_VOLUMES_YEAR + 1)
    income_stmt_files = util.find_data_files(income_stmt_path)
    balance_files = util.find_data_files(balance_path)
    hours_files = util.find_data_files(hours_path, exclude=[historical_hours_file])
    source_files = (
        [historical_volumes_file]
        + volumes_files
        + [misc_volumes_file]
        + [historical_hours_file]
        + income_stmt_files
        + balance_files
        + hours_files
        + [aged_ar_file]
    )
    source_files_str = "\n  ".join(source_files)
    logging.info(f"Discovered source files:\n  {source_files_str}")

    # TODO: data verification
    # - volumes_file, List worksheet: verify same data as static_data.WDID_TO_DEPTNAME
    # - Each income statement sheet has Ledger Account cell, and data in columns A:Q
    # - hours and income data is present for the latest month we have volume data for
    # - fte, volumes should all be non-negative. Hours can be negative for adjustments
    if not sanity.check_data_files(historical_volumes_file, income_stmt_files):
        logging.error("ERROR: data files sanity check failed (see above).")
        exit(1)

    # Extract and perform basic transformation of data from spreadsheets
    # First process the historical Dashboard Supporting Data 2024 file
    volumes_df = parse.read_historical_volume_and_uos_data(
        historical_volumes_file, HISTORICAL_VOLUMES_SHEET
    )
    uos_df = parse.read_historical_volume_and_uos_data(
        historical_volumes_file, HISTORICAL_UOS_SHEET
    )
    budget_df = parse.read_historical_budget_data(
        HISTORICAL_VOLUMES_YEAR,
        historical_volumes_file,
        VOLUMES_BUDGET_SHEET,
        HRS_PER_VOLUME_SHEET,
        HISTORICAL_UOS_SHEET,
    )
    contracted_hours_updated_month, contracted_hours_df = (
        parse.read_historical_contracted_hours_data(
            HISTORICAL_VOLUMES_YEAR,
            historical_volumes_file,
            CONTRACTED_HRS_SHEET,
        )
    )

    # Process monthly Dashboard Supporting Data files if exists
    for volumes_file in volumes_files:
        logging.info(f"Processing supporting data file: {volumes_file}")

        # Get year from filename which is in format "(MM) MMM YYYY Dashboard ..."
        year_match = re.search(r"\d{4}", os.path.basename(volumes_file))
        year = int(year_match.group(0))

        current_volumes_df = parse.read_volume_and_uos_data(
            year, volumes_file, VOLUMES_SHEET
        )
        current_uos_df = parse.read_volume_and_uos_data(year, volumes_file, UOS_SHEET)
        current_budget_df = parse.read_budget_data(volumes_file, VOLUMES_BUDGET_SHEET)
        current_contracted_hours_updated_month, current_contracted_hours_df = (
            parse.read_contracted_hours_data(year, volumes_file, CONTRACTED_HRS_SHEET)
        )

        volumes_df = pd.concat([volumes_df, current_volumes_df])
        uos_df = pd.concat([uos_df, current_uos_df])
        budget_df = pd.concat([budget_df, current_budget_df])
        contracted_hours_df = pd.concat(
            [contracted_hours_df, current_contracted_hours_df]
        )
        contracted_hours_updated_month = max(
            contracted_hours_updated_month, current_contracted_hours_updated_month
        )

    # Store the updated contracted hours month in its own table
    contracted_hours_updated_month_df = pd.DataFrame(
        {"contracted_hours_updated_month": [str(contracted_hours_updated_month)]}
    )

    # Get extra volume metrics from Epic data
    misc_volumes_df = parse.read_misc_volumes_data(misc_volumes_file)

    # Read income statement and hours data
    income_stmt_df = parse.read_income_stmt_data(income_stmt_files)
    historical_hours_df = parse.read_historical_hours_and_fte_data(
        historical_hours_file, HISTORICAL_HOURS_YEAR
    )
    hours_by_pay_period_df = parse.read_hours_and_fte_data(hours_files)
    hours_by_pay_period_df = pd.concat([historical_hours_df, hours_by_pay_period_df])

    # # Read balance sheets
    balance_df = parse.read_balance_sheets(balance_files)

    # Read accounts receivable data
    aged_ar_df = parse.read_aged_ar_data(aged_ar_file)

    # Transform hours data to months
    hours_by_month_df = transform.transform_hours_from_pay_periods_to_months(
        hours_by_pay_period_df
    )

    # Get connection to output DBs
    prw_engine = get_db_connection(output_conn, echo=SHOW_SQL_IN_LOG)
    if prw_engine is None:
        logging.error("ERROR: cannot open output DB (see above). Terminating.")
        exit(1)
    prw_session = Session(prw_engine)

    # Create tables if they do not exist
    if drop_tables:
        logging.info("Dropping existing tables")
        PrwMetaModel.metadata.drop_all(prw_engine)
        PrwFinanceModel.metadata.drop_all(prw_engine)
    logging.info("Creating tables")
    PrwMetaModel.metadata.create_all(prw_engine)
    PrwFinanceModel.metadata.create_all(prw_engine)

    # Load data into DB. Clear each table prior to loading from dataframe
    clear_tables_and_insert_data(
        prw_session,
        [
            TableData(table=PrwVolume, df=volumes_df),
            TableData(table=PrwMiscVolume, df=misc_volumes_df),
            TableData(table=PrwUOS, df=uos_df),
            TableData(table=PrwBudget, df=budget_df),
            TableData(table=PrwHoursByPayPeriod, df=hours_by_pay_period_df),
            TableData(table=PrwHours, df=hours_by_month_df),
            TableData(table=PrwContractedHours, df=contracted_hours_df),
            TableData(
                table=PrwContractedHoursMeta, df=contracted_hours_updated_month_df
            ),
            TableData(table=PrwIncomeStmt, df=income_stmt_df),
            TableData(table=PrwAgedAR, df=aged_ar_df),
            TableData(table=PrwBalanceSheet, df=balance_df),
        ],
    )

    # Update last ingest time and modified times for source data files
    modified = {
        file: datetime.fromtimestamp(os.path.getmtime(file)) for file in source_files
    }
    prw_meta_utils.write_meta(prw_session, INGEST_DATASET_ID, modified)

    # Cleanup
    prw_session.commit()
    prw_session.close()
    prw_engine.dispose()

    logging.info("Done")


if __name__ == "__main__":
    main()
