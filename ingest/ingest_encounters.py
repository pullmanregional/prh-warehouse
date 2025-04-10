import os
import logging
import warnings
import pandas as pd
import json
from typing import List
from datetime import datetime
from sqlmodel import Session, select, inspect
from prw_common.model import prw_model, prw_id_model
from util import prw_id_utils, prw_meta_utils
from prw_common.cli_utils import cli_parser
from prw_common.db_utils import (
    TableData,
    get_db_connection,
    mask_conn_pw,
    clear_tables,
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
def sanity_check_files(encounters_file, mychart_file, allergy_file):
    """
    Executed once at the beginning of ingest to validate CSV file
    meets basic requirements.
    """
    error = None
    if not os.path.isfile(encounters_file):
        error = f"ERROR: encounters file does not exist: {encounters_file}"
    elif not os.path.isfile(mychart_file):
        error = f"ERROR: mychart file does not exist: {mychart_file}"
    elif not os.path.isfile(allergy_file):
        error = f"ERROR: allergy file does not exist: {allergy_file}"
    if error is not None:
        print(error)

    return error is None


def read_mrn_to_prw_id_table(engine):
    """
    Read existing ID to MRN mapping from the PRW ID DB
    """
    with Session(engine) as session:
        results = session.exec(
            select(prw_id_model.PrwId.prw_id, prw_id_model.PrwId.mrn)
        )
        return pd.DataFrame(results, columns=["prw_id", "mrn"])


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
    patients_df = df[
        [
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
        ]
    ].copy()
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
        ]
    ].copy()

    # -------------------------------------------------------
    # Fix data types
    # -------------------------------------------------------
    # Convert MRN to string
    patients_df["mrn"] = patients_df["mrn"].astype(str)
    encounters_df["mrn"] = encounters_df["mrn"].astype(str)

    # Convert with_pcp to boolean (CSV might have 0/1 instead of True/False)
    encounters_df["with_pcp"] = encounters_df["with_pcp"].astype(bool)

    # Handle date/time formats
    # BirthDate column is in YYYY-MM-DD format
    patients_df["dob"] = pd.to_datetime(patients_df["dob"])

    # AppointmentDateKey is in YYYYMMDD format (e.g., 20240301)
    # AppointmentTimeOfDayKey is in HHMM 24-hour format (e.g., 1400)
    encounters_df["encounter_date"] = pd.to_datetime(
        encounters_df["encounter_date"].astype(str), format="%Y%m%d"
    )
    encounters_df["encounter_time"] = (
        encounters_df["encounter_time"].astype(str).str.zfill(4)
    )

    # -------------------------------------------------------
    # Add prw_id to patients and encounters
    # -------------------------------------------------------
    # Drop duplicate patients based on 'mrn' and keep the first occurrence
    patients_df = patients_df.drop_duplicates(subset=["mrn"], keep="first").copy()

    # Calculate prw_id from MRN, and store translation table
    patients_df = prw_id_utils.calc_prw_id(
        patients_df,
        src_id_col="mrn",
        id_col="prw_id",
        src_id_to_id_df=mrn_to_prw_id_df,
    )
    mrn_to_prw_id_df = patients_df[["mrn", "prw_id"]].copy()

    # Convert encounter MRN to prw_id
    encounters_df = encounters_df.merge(mrn_to_prw_id_df, on="mrn", how="left")
    encounters_df.drop(columns=["mrn"], inplace=True)

    return patients_df, encounters_df, mrn_to_prw_id_df


def read_allergy(allergy_file: str, mrn_to_prw_id_df: pd.DataFrame):
    """
    Read allergy file and add info into patients_df
    """
    logging.info(f"Reading {allergy_file}")
    allergy_df = pd.read_csv(
        allergy_file,
        skiprows=1,
        index_col=False,
        names=[
            "mrn",
            "allergen_key",
            "name",
            "reaction",
            "type",
            "severity",
        ],
    )

    # Group by MRN, and merge the name, reaction, type, and severity into an object, then convert to json string
    allergy_df["mrn"] = allergy_df["mrn"].astype(str)
    allergy_df = (
        allergy_df.groupby("mrn")[["name", "reaction", "type", "severity"]]
        .apply(lambda x: x.to_json(orient="records"))
        .reset_index(name="allergy")
    )

    # Convert MRN to prw_id
    allergy_df = allergy_df.merge(mrn_to_prw_id_df, on="mrn", how="right")
    allergy_df.drop(columns=["mrn"], inplace=True)

    return allergy_df


def read_mychart_status(mychart_file: str, mrn_to_prw_id_df: pd.DataFrame):
    """
    Read mychart file and add info into patients_df
    """
    mychart_df = pd.read_csv(
        mychart_file,
        skiprows=1,
        index_col=False,
        names=["mrn", "mychart_status", "mychart_activation_date"],
        dtype={"mrn": str, "mychart_status": str, "mychart_activation_date": str},
    )

    # Convert non null mychart_activation_date records to datetime
    mychart_df["mychart_activation_date"] = mychart_df["mychart_activation_date"].apply(
        lambda x: datetime.strptime(x, "%Y%m%d") if x is not None else None
    )

    # Drop duplicate MRNs and map to prw_id
    mychart_df = mychart_df.drop_duplicates(subset=["mrn"], keep="first")
    mychart_df = mychart_df.merge(mrn_to_prw_id_df, on="mrn", how="inner")
    mychart_df.drop(columns=["mrn"], inplace=True)

    return mychart_df


# -------------------------------------------------------
# Transform
# -------------------------------------------------------
def calc_patient_age(patients_df: pd.DataFrame):
    """
    Calculate age and age_in_mo_under_3 from dob
    """
    # Calculate age and age in months (if <3yo) from dob
    logging.info("Calculating patient ages")
    now = pd.Timestamp.now()

    # Get dob as datetime
    dob_df = patients_df["dob"]

    # Calculate age in years
    # Adjust by 1 year if birthday hasn't occurred this year
    patients_df["age"] = now.year - dob_df.dt.year
    mask = (now.month < dob_df.dt.month) | (
        (now.month == dob_df.dt.month) & (now.day < dob_df.dt.day)
    )
    patients_df.loc[mask, "age"] -= 1

    # Calculate age in months, but only retain if under 3
    patients_df["age_in_mo_under_3"] = (
        (now.year - dob_df.dt.year) * 12
        + (now.month - dob_df.dt.month)
        - (now.day < dob_df.dt.day)
    )
    patients_df.loc[patients_df["age"] > 2, "age_in_mo_under_3"] = None
    patients_df["age_in_mo_under_3"] = patients_df["age_in_mo_under_3"].astype("Int64")
    return patients_df


def calc_age_at_encounter(encounters_df: pd.DataFrame, patients_df: pd.DataFrame):
    """
    Calculate encounter age and encounter_age_in_mo_under_3 based on encounter_date and patient's DOB
    """
    logging.info("Calculating patient ages at encounter")
    # Get each patient's DOB
    encounters_df = encounters_df.merge(
        patients_df[["prw_id", "dob"]], on="prw_id", how="left"
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


def partition_phi(patients_df: pd.DataFrame):
    """
    Partition patients_df into PHI, which will be stored in ID DB, and non-PHI, which will be stored in the main DB
    """
    logging.info("Partitioning PHI")
    PHI_COLUMNS = [
        "mrn",
        "name",
        "dob",
        "address",
        "zip",
        "phone",
        "email",
    ]
    LOCATION_COLUMNS = [
        "city",
        "state",
    ]
    id_details_df = patients_df[["prw_id"] + PHI_COLUMNS + LOCATION_COLUMNS]

    # Remove PHI columns from main dataset
    patients_df = patients_df.drop(columns=PHI_COLUMNS).copy()

    return patients_df, id_details_df


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
        f"Input: {in_path}, output: {mask_conn_pw(output_conn)}, id output: {mask_conn_pw(id_output_conn or 'None')}"
    )
    logging.info(f"Drop tables before writing: {drop_tables}")

    # Input files
    encounters_file = os.path.join(in_path, "encounters.csv")
    mychart_file = os.path.join(in_path, "mychart.csv")
    allergy_file = os.path.join(in_path, "allergy.csv")

    # Sanity check the input file
    if not sanity_check_files(encounters_file, mychart_file, allergy_file):
        logging.error("ERROR: input error (see above). Terminating.")
        exit(1)

    # If ID DB is specified, read existing ID mappings
    prw_id_engine, mrn_to_prw_id_df = None, None
    if id_output_conn:
        prw_id_engine = get_db_connection(id_output_conn, echo=SHOW_SQL_IN_LOG)
        if prw_id_engine is None:
            logging.error("ERROR: cannot open ID DB (see above). Terminating.")
            exit(1)
        if inspect(prw_id_engine).has_table(prw_id_model.PrwId.__tablename__):
            logging.info("Using existing MRN to PRW ID mappings")
            mrn_to_prw_id_df = read_mrn_to_prw_id_table(prw_id_engine)
        else:
            logging.info("ID DB table does not exist, will generate new ID mappings")

    # Read source file into memory
    patients_df, encounters_df, mrn_to_prw_id_df = read_encounters(
        encounters_file, mrn_to_prw_id_df
    )
    mychart_df = read_mychart_status(mychart_file, mrn_to_prw_id_df)
    allergy_df = read_allergy(allergy_file, mrn_to_prw_id_df)

    # Add allergy data to patients_df
    patients_df = patients_df.merge(allergy_df, on="prw_id", how="left")

    # Transform data only to partition PHI into separate DB. All other data
    # transformations should be done by later flows in the pipeline.
    patients_df = calc_patient_age(patients_df)
    encounters_df = calc_age_at_encounter(encounters_df, patients_df)
    patients_df, id_details_df = partition_phi(patients_df)

    # Get connection to output DBs
    prw_engine = get_db_connection(output_conn, echo=SHOW_SQL_IN_LOG)
    if prw_engine is None:
        logging.error("ERROR: cannot open output DB (see above). Terminating.")
        exit(1)
    prw_session = Session(prw_engine)

    # Create tables if they do not exist
    if drop_tables:
        logging.info("Dropping existing tables")
        prw_model.PrwMetaModel.metadata.drop_all(prw_engine)
        prw_model.PrwModel.metadata.drop_all(prw_engine)

    logging.info("Creating tables")
    prw_model.PrwMetaModel.metadata.create_all(prw_engine)
    prw_model.PrwModel.metadata.create_all(prw_engine)

    # Write into DB
    clear_tables_and_insert_data(
        prw_session,
        [
            TableData(table=prw_model.PrwPatient, df=patients_df),
            TableData(table=prw_model.PrwEncounterOutpt, df=encounters_df),
            TableData(table=prw_model.PrwMyChart, df=mychart_df),
        ],
    )

    # Update last ingest time and modified times for source data files
    modified = {in_path: datetime.fromtimestamp(os.path.getmtime(in_path))}
    prw_meta_utils.write_meta(prw_session, DATASET_ID, modified)

    # Cleanup
    prw_session.commit()
    prw_session.close()
    prw_engine.dispose()

    # Write ID data to separate DB if provided
    if prw_id_engine:
        logging.info("Writing ID data")
        id_df = id_details_df[["prw_id", "mrn"]]
        prw_id_session = Session(prw_id_engine)
        prw_id_model.PrwIdModel.metadata.create_all(prw_id_engine)
        prw_id_model.PrwIdDetails.metadata.create_all(prw_id_engine)
        clear_tables_and_insert_data(
            prw_id_session,
            [
                TableData(table=prw_id_model.PrwId, df=id_df),
                TableData(table=prw_id_model.PrwIdDetails, df=id_details_df),
            ],
        )
        prw_id_session.commit()
        prw_id_session.close()
        prw_id_engine.dispose()

    logging.info("Done")


if __name__ == "__main__":
    main()
