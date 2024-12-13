import re
import logging
import argparse
from sqlmodel import Session, select
from util import util, db_utils, prw_meta_utils
from prw_model.prw_model import *

# -------------------------------------------------------
# Config
# -------------------------------------------------------
# Unique identifier for this ingest dataset
DATASET_ID = "CLEAN_ENCOUNTERS"

# Default output to local SQLite DB.
DEFAULT_PRW_DB_ODBC = "sqlite:///prw.sqlite3"

# Logging definitions
logging.basicConfig(level=logging.INFO)
SHOW_SQL_IN_LOG = False


# -------------------------------------------------------
# Transform
# -------------------------------------------------------
def clean_encounters(prw_session: Session):
    """
    Updates db tables directly to clean source data
    """
    logging.info("Cleaning encounters table")

    # Stream rows from SQL table, rewrite with regexes:
    # - Remove trailing " [<id>]" in dept, encounter_type, service_provider, billing_provider
    query = prw_session.exec(select(PrwEncounter)).yield_per(10000)
    trailing_id_re = re.compile(r"\s*\[[^\]]*\]\s*$")
    for encounter in query:
        if encounter.dept:
            dept = trailing_id_re.sub("", encounter.dept)
            if dept != encounter.dept:
                encounter.dept = dept
        if encounter.encounter_type:
            encounter_type = trailing_id_re.sub("", encounter.encounter_type)
            if encounter_type != encounter.encounter_type:
                encounter.encounter_type = encounter_type
        if encounter.service_provider:
            service_provider = trailing_id_re.sub("", encounter.service_provider)
            if service_provider != encounter.service_provider:
                encounter.service_provider = service_provider
        if encounter.billing_provider:
            billing_provider = trailing_id_re.sub("", encounter.billing_provider)
            if billing_provider != encounter.billing_provider:
                encounter.billing_provider = billing_provider

    prw_session.flush()


# -------------------------------------------------------
# Main entry point
# -------------------------------------------------------
def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Clean up raw encounter data in PRH warehouse."
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
        util.error_exit("ERROR: cannot open output DB (see above). Terminating.")
    prw_session = Session(prw_engine)

    # Clean up data
    clean_encounters(prw_session)

    # Update last modified time
    prw_meta_utils.write_meta(prw_session, DATASET_ID)

    # Cleanup
    prw_session.commit()
    prw_session.close()
    prw_engine.dispose()
    logging.info("Done")


if __name__ == "__main__":
    main()
