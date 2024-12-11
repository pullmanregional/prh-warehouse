import os
import logging
import argparse
import pandas as pd
from datetime import datetime
from sqlmodel import Session
from util import util, db_utils

# -------------------------------------------------------
# Config
# -------------------------------------------------------
# Unique identifier for this ingest dataset
INGEST_DATASET_ID = "patient_panel"

# Default output to local SQLite DB.
DEFAULT_PRW_DB_ODBC = "sqlite:///prw.sqlite3"

# Logging definitions
logging.basicConfig(level=logging.INFO)
SHOW_SQL_IN_LOG = False


# -------------------------------------------------------
# Main entry point
# -------------------------------------------------------
def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Recalculate patient panel data in-place in PRH warehouse."
    )
    parser.add_argument(
        "-db",
        help='DB connection string including credentials. Look for Azure SQL connection string in Settings > Connection strings, eg. "mssql+pyodbc:///?odbc_connect=Driver={ODBC Driver 18 for SQL Server};Server=tcp:{your server name},1433;Database={your db name};Uid={your user};Pwd={your password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"',
        default=DEFAULT_PRW_DB_ODBC,
    )
    return parser.parse_args()


def main():
    # Load config from cmd line
    args = parse_arguments()
    db_url = args.db

    logging.info(f"DB: {util.mask_pw(db_url)}")

    # Get connection to DB
    prw_engine = db_utils.get_db_connection(db_url, echo=SHOW_SQL_IN_LOG)
    if prw_engine is None:
        logging.error("ERROR: cannot open output DB (see above). Terminating.")
        exit(1)
    prw_session = Session(prw_engine)

    # Cleanup
    prw_session.close()
    logging.info("Done")


if __name__ == "__main__":
    main()
