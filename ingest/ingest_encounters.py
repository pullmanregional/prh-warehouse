import os
import logging
import pandas as pd
from datetime import datetime
from sqlmodel import Session, select
from prw_common.model import prw_model, prw_id_model
from util import prw_meta_utils
from prw_common.cli_utils import cli_parser
from prw_common.db_utils import (
    TableData,
    get_db_connection,
    mask_conn_pw,
    clear_tables_and_insert_data,
)

# Unique identifier for this ingest dataset
DATASET_ID = "encounters"

# -------------------------------------------------------
# Config
# -------------------------------------------------------
# Logging configuration
SHOW_SQL_IN_LOG = False
logging.basicConfig(level=logging.INFO)


# -------------------------------------------------------
# Extract from Source Files
# -------------------------------------------------------
def read_mrn_to_prw_id_table(engine):
    """
    Read existing ID to MRN mapping and other details needed to calculate encounter fields from the PRW ID DB
    """
    with Session(engine) as session:
        results = session.exec(
            select(
                prw_id_model.PrwIdDetails.prw_id,
                prw_id_model.PrwIdDetails.mrn,
                prw_id_model.PrwIdDetails.dob,
            )
        )
        return pd.DataFrame(results, columns=["prw_id", "mrn", "dob"])


def read_encounters(csv_file: str, mrn_to_prw_id_df: pd.DataFrame = None):
    # -------------------------------------------------------
    # Extract data from CSV file
    # -------------------------------------------------------
    logging.info(f"Reading {csv_file}")
    df = pd.read_csv(
        csv_file,
        skiprows=1,
        names=[
            "mrn",
            "name",
            "sex",
            "dob",
            "address",
            "city",
            "state",
            "zip",
            "phone",
            "email",
            "pcp",
            "dept",
            "encounter_date",
            "encounter_time",
            "encounter_type",
            "service_provider",
            "with_pcp",
            "appt_status",
            "diagnoses",
            "diagnoses_icd",
            "level_of_service",
            "level_of_service_name",
        ],
        dtype={
            "mrn": str,
            "name": str,
            "sex": str,
            "dob": str,
            "address": str,
            "city": str,
            "state": str,
            "zip": str,
            "phone": str,
            "email": str,
            "pcp": str,
            "dept": str,
            "encounter_date": str,
            "encounter_time": str,
            "encounter_type": str,
            "service_provider": str,
            "with_pcp": "Int8",
            "appt_status": str,
            "diagnoses": str,
            "diagnoses_icd": str,
            "level_of_service": str,
            "level_of_service_name": str,
        },
        index_col=False,
    )

    # Retain columns needed for tranform and load
    encounters_df = df[
        [
            "mrn",
            "dept",
            "encounter_date",
            "encounter_time",
            "encounter_type",
            "service_provider",
            "with_pcp",
            "appt_status",
            "diagnoses",
            "diagnoses_icd",
            "level_of_service",
            "level_of_service_name",
        ]
    ].copy()

    # -------------------------------------------------------
    # Fix data types
    # -------------------------------------------------------
    # Convert MRN to string
    encounters_df["mrn"] = encounters_df["mrn"].astype(str)

    # Convert with_pcp to boolean (CSV might have 0/1 instead of True/False)
    encounters_df["with_pcp"] = encounters_df["with_pcp"].astype(bool)

    # Handle date/time formats
    # AppointmentDateKey is in YYYYMMDD format (e.g., 20240301)
    # AppointmentTimeOfDayKey is in HHMM 24-hour format (e.g., 1400)
    encounters_df["encounter_date"] = pd.to_datetime(
        encounters_df["encounter_date"].astype(str), format="%Y%m%d"
    )
    encounters_df["encounter_time"] = (
        encounters_df["encounter_time"].astype(str).str.zfill(4)
    )

    # -------------------------------------------------------
    # Add prw_id to encounters
    # -------------------------------------------------------
    encounters_df = encounters_df.merge(
        mrn_to_prw_id_df[["prw_id", "mrn"]], on="mrn", how="left"
    )
    encounters_df.drop(columns=["mrn"], inplace=True)

    return encounters_df


# -------------------------------------------------------
# Transform
# -------------------------------------------------------
def calc_age_at_encounter(encounters_df: pd.DataFrame, mrn_to_prw_id_df: pd.DataFrame):
    """
    Calculate encounter age and encounter_age_in_mo_under_3 based on encounter_date and patient's DOB
    """
    logging.info("Calculating patient ages at encounter")
    # Get each patient's DOB
    encounters_df = encounters_df.merge(
        mrn_to_prw_id_df[["prw_id", "dob"]], on="prw_id", how="left"
    )

    # Get dob and encounter_date as datetimes
    dates_df = encounters_df[["dob", "encounter_date"]]

    # Calculate age of patient at encounter in years (if under 3)
    # Adjust by 1 year if birthday hasn't occurred in encounter year
    encounters_df["encounter_age"] = (
        dates_df["encounter_date"].dt.year - dates_df["dob"].dt.year
    )
    mask = (dates_df["encounter_date"].dt.month < dates_df["dob"].dt.month) | (
        (dates_df["encounter_date"].dt.month == dates_df["dob"].dt.month)
        & (dates_df["encounter_date"].dt.day < dates_df["dob"].dt.day)
    )
    encounters_df.loc[mask, "encounter_age"] -= 1

    # Calculate age in months, but only retain if under 3
    encounters_df["encounter_age_in_mo_under_3"] = (
        (dates_df["encounter_date"].dt.year - dates_df["dob"].dt.year) * 12
        + (dates_df["encounter_date"].dt.month - dates_df["dob"].dt.month)
        - (dates_df["encounter_date"].dt.day < dates_df["dob"].dt.day)
    )
    encounters_df.loc[
        encounters_df["encounter_age"] > 2, "encounter_age_in_mo_under_3"
    ] = None
    encounters_df["encounter_age_in_mo_under_3"] = encounters_df[
        "encounter_age_in_mo_under_3"
    ].astype("Int64")

    return encounters_df


def fix_missing_los_cpt(encounters_df: pd.DataFrame):
    """
    Fix missing level_of_service values by extracting CPT codes from level_of_service_name
    when they follow the pattern 'HC PR ' followed by a code format.
    """
    mask = (
        encounters_df["level_of_service"].isna()
        & encounters_df["level_of_service_name"].notna()
        & encounters_df["level_of_service_name"].str.contains("HC PR ", na=False)
    )

    # For matching rows, extract the CPT code that follows 'HC PR '
    count = 0
    if mask.any():
        # Extract codes that match the pattern: letter + 4 numbers, 4 numbers + letter, or 5 numbers
        pattern = r"HC PR ([A-Z]\d{4}|\d{4}[A-Z]|\d{5}) "
        extracted_codes = encounters_df.loc[mask, "level_of_service_name"].str.extract(
            pattern, expand=False
        )

        # Count how many row to update
        count = extracted_codes.notna().sum()

        # Update the level_of_service column with the extracted codes
        encounters_df.loc[mask, "level_of_service"] = extracted_codes

    logging.info(f"Updated {count} encounter LOS values")
    return encounters_df


# -------------------------------------------------------
# Main entry point
# -------------------------------------------------------
def parse_arguments():
    parser = cli_parser(
        description="Ingest source data into PRH warehouse.",
        require_prw=True,
        require_prwid=True,
        require_in=True,
    )

    # Add script-specific arguments
    parser.add_argument(
        "--drop",
        action="store_true",
        help="Drop and recreate all tables before ingesting data",
    )
    return parser.parse_args()


def main():
    # Load config from cmd line
    args = parse_arguments()
    in_path = args.input
    output_conn = args.prw
    id_output_conn = args.prwid if args.prwid.lower() != "none" else None
    drop_tables = args.drop
    logging.info(
        f"Input: {in_path}, output: {mask_conn_pw(output_conn)}, id output: {mask_conn_pw(id_output_conn)}"
    )
    logging.info(f"Drop tables before writing: {drop_tables}")

    # Input files
    encounters_file = os.path.join(in_path, "encounters.csv")
    if not os.path.isfile(encounters_file):
        logging.error(f"ERROR: encounters file does not exist: {encounters_file}")
        exit(1)

    # Read existing ID mappings
    prw_id_engine = get_db_connection(id_output_conn, echo=SHOW_SQL_IN_LOG)
    if prw_id_engine is None:
        logging.error("ERROR: cannot open ID DB (see above). Terminating.")
        exit(1)
    mrn_to_prw_id_df = read_mrn_to_prw_id_table(prw_id_engine)

    # Read source file into memory
    encounters_df = read_encounters(encounters_file, mrn_to_prw_id_df)

    # Transform data: only data correction and handling PHI.
    # All other data transformations should be done by later flows in the pipeline.
    encounters_df = calc_age_at_encounter(encounters_df, mrn_to_prw_id_df)
    encounters_df = fix_missing_los_cpt(encounters_df)

    # Get connection to output DBs
    prw_engine = get_db_connection(output_conn, echo=SHOW_SQL_IN_LOG)
    if prw_engine is None:
        logging.error("ERROR: cannot open output DB (see above). Terminating.")
        exit(1)
    prw_session = Session(prw_engine)

    # Drop tables so DDL is reissued if requested
    if drop_tables:
        logging.info("Dropping existing tables")
        prw_model.PrwEncounterOutpt.__table__.drop(prw_engine, checkfirst=True)

    logging.info("Creating tables")
    prw_model.PrwMetaModel.metadata.create_all(prw_engine)
    prw_model.PrwModel.metadata.create_all(prw_engine)

    # Write into DB
    clear_tables_and_insert_data(
        prw_session,
        [
            TableData(table=prw_model.PrwEncounterOutpt, df=encounters_df),
        ],
    )

    # Update last ingest time and modified times for source data files
    modified = {in_path: datetime.fromtimestamp(os.path.getmtime(in_path))}
    prw_meta_utils.write_meta(prw_session, DATASET_ID, modified)

    # Cleanup
    prw_session.commit()
    prw_session.close()
    prw_engine.dispose()
    logging.info("Done")


if __name__ == "__main__":
    main()
