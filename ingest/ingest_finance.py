import os
import logging
import argparse
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from sqlmodel import Session
from finance import sanity, parse, transform
from util import util, db_utils, prw_meta_utils
from util.db_utils import TableData, clear_tables, clear_tables_and_insert_data
from prw_model.prw_finance_model import *

# -------------------------------------------------------
# Config
# -------------------------------------------------------
# Unique identifier for this ingest dataset
INGEST_DATASET_ID = "finance"

# Load environment from .env file, does not overwrite existing env variables
load_dotenv()

# Default output to local SQLite DB.
DEFAULT_PRW_DB_ODBC = "sqlite:///../prw.sqlite3"

# Input files
DEFAULT_DATA_DIR = os.path.join("data", "finance")

# Opt into pandas 3 behavior for replace() and fillna(), where columns of object dtype are NOT changed to something more specific,
# like int/float/str. This option can be removed once upgrading pandas 2-> 3.
pd.set_option("future.no_silent_downcasting", True)

# Logging definitions
logging.basicConfig(level=logging.INFO)
SHOW_SQL_IN_LOG = False

# Specific worksheet names containing data to ingest in the Dashboard Supporting Data spreadsheet
VOLUMES_SHEET = "STATS"
UOS_SHEET = "UOS"
VOLUMES_BUDGET_SHEET = "Data"
HRS_PER_VOLUME_SHEET = "Prod.MH UOS"
CONTRACTED_HRS_SHEET = "ProdHrs"

# The historical hours data returned by get_file_paths() as historical_hours_file contains data for 2022
HISTORICAL_HOURS_YEAR = 2022


# -------------------------------------------------------
# Extract from Source Files
# -------------------------------------------------------
def get_file_paths(base_path):
    # Historical volume data is ion the Dashboard Supporting Data spreadsheet
    volumes_file = os.path.join(base_path, "Dashboard Supporting Data 2024 v3.xlsx")

    # The Natural Class subdir contains income statment in one Excel file per month, eg,
    # ./Natural Class/2022/(01) Jan 2022 Natural Class.xlsx
    income_stmt_path = os.path.join(base_path, "Natural Class")

    # PayPeriod subdir contians productive / non-productive hours and FTE data per month. eg,
    #   ./PayPeriod/2023/PP#1 2023 Payroll_Productivity_by_Cost_Center.xlsx
    # In addition, historical data for 2022 PP#1-25, which includes the clinic network, is lumped together a separate file:
    #   ./PayPeriod/2023/PP#1-PP#25 Payroll Productivity.xlsx
    historical_hours_file = os.path.join(
        base_path, "PayPeriod", "2022", "PP#1-PP#26 Payroll Productivity.xlsx"
    )
    hours_path = os.path.join(base_path, "PayPeriod")

    return volumes_file, income_stmt_path, hours_path, historical_hours_file


# -------------------------------------------------------
# Main entry point
# -------------------------------------------------------
def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Ingest finance data into PRH warehouse."
    )
    parser.add_argument(
        "-i",
        "--input_dir",
        help="Path to the source data directory.",
        default=DEFAULT_DATA_DIR,
    )
    parser.add_argument(
        "-o",
        "--out",
        help='Output DB connection string, including credentials if needed. Look for Azure SQL connection string in Settings > Connection strings, eg. "mssql+pyodbc:///?odbc_connect=Driver={ODBC Driver 18 for SQL Server};Server=tcp:{your server name},1433;Database={your db name};Uid={your user};Pwd={your password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"',
        default=DEFAULT_PRW_DB_ODBC,
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
    base_path = args.input_dir
    output_odbc = args.out
    drop_tables = args.drop

    logging.info(f"Data dir: {base_path}, output: {util.mask_pw(output_odbc)}")
    logging.info(f"Drop tables before writing: {drop_tables}")

    # Source file paths
    volumes_file, income_stmt_path, hours_path, historical_hours_file = get_file_paths(
        base_path
    )

    # Sanity check data directory expected location and files
    if not sanity.check_data_dir(base_path, volumes_file, income_stmt_path, hours_path):
        logging.error("ERROR: data directory error (see above). Terminating.")
        exit(1)

    # Get list of dynamic data files, ie data organized as one Excel workbook per month
    income_stmt_files = util.find_data_files(income_stmt_path)
    hours_files = util.find_data_files(hours_path, exclude=[historical_hours_file])
    source_files = (
        [volumes_file, historical_hours_file] + income_stmt_files + hours_files
    )
    source_files_str = "\n  ".join(source_files)
    logging.info(f"Discovered source files:\n  {source_files_str}")

    # TODO: data verification
    # - volumes_file, List worksheet: verify same data as static_data.WDID_TO_DEPTNAME
    # - Each income statement sheet has Ledger Account cell, and data in columns A:Q
    # - hours and income data is present for the latest month we have volume data for
    # - fte, volumes should all be non-negative. Hours can be negative for adjustments
    if not sanity.check_data_files(volumes_file, income_stmt_files):
        logging.error("ERROR: data files sanity check failed (see above).")
        exit(1)

    # Extract and perform basic transformation of data from spreadsheets
    volumes_df = parse.read_volume_and_uos_data(volumes_file, VOLUMES_SHEET)
    uos_df = parse.read_volume_and_uos_data(volumes_file, UOS_SHEET)
    budget_df = parse.read_budget_data(
        volumes_file, VOLUMES_BUDGET_SHEET, HRS_PER_VOLUME_SHEET, UOS_SHEET
    )
    contracted_hours_updated_month_df, contracted_hours_df = (
        parse.read_contracted_hours_data(volumes_file, CONTRACTED_HRS_SHEET)
    )
    income_stmt_df = parse.read_income_stmt_data(income_stmt_files)
    historical_hours_df = parse.read_historical_hours_and_fte_data(
        historical_hours_file, HISTORICAL_HOURS_YEAR
    )
    hours_by_pay_period_df = parse.read_hours_and_fte_data(hours_files)
    hours_by_pay_period_df = pd.concat([historical_hours_df, hours_by_pay_period_df])

    # Transform hours data to months
    hours_by_month_df = transform.transform_hours_from_pay_periods_to_months(
        hours_by_pay_period_df
    )

    # Get connection to output DBs
    prw_engine = db_utils.get_db_connection(output_odbc, echo=SHOW_SQL_IN_LOG)
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
            TableData(table=PrwUOS, df=uos_df),
            TableData(table=PrwBudget, df=budget_df),
            TableData(table=PrwHoursByPayPeriod, df=hours_by_pay_period_df),
            TableData(table=PrwHours, df=hours_by_month_df),
            TableData(table=PrwContractedHours, df=contracted_hours_df),
            TableData(
                table=PrwContractedHoursMeta, df=contracted_hours_updated_month_df
            ),
            TableData(table=PrwIncomeStmt, df=income_stmt_df),
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
