import os
import logging
import argparse
import warnings
import pandas as pd
from typing import List
from datetime import datetime
from dateutil.relativedelta import relativedelta
from sqlmodel import Session, select, inspect
from prw_model import prw_model, prw_id_model
from util import prw_id_utils, db_utils, util, prw_meta_utils
from util.db_utils import TableData, clear_tables, clear_tables_and_insert_data

# Unique identifier for this ingest dataset
DATASET_ID = "encounters"

# -------------------------------------------------------
# Config
# -------------------------------------------------------
# Default output to local SQLite DB.
DEFAULT_PRW_DB_ODBC = "sqlite:///prw.sqlite3"
DEFAULT_PRW_ID_DB_ODBC = "sqlite:///prw_id.sqlite3"

# Input files
DEFAULT_DATA_DIR = "./data/encounters"

# Logging configuration
SHOW_SQL_IN_LOG = False
logging.basicConfig(level=logging.INFO)


# -------------------------------------------------------
# Extract from Source Files
# -------------------------------------------------------
def sanity_check_data_dir(base_path, encounters_files):
    """
    Executed once at the beginning of ingest to validate data directory and files
    meet basic requirements.
    """
    error = None
    if not os.path.isdir(base_path):
        error = f"ERROR: data directory path does not exist: {base_path}"

    for encounters_file in encounters_files:
        if not os.path.isfile(encounters_file):
            error = f"ERROR: data file missing: {encounters_file}"

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


def read_encounters(files: List[str], mrn_to_prw_id_df: pd.DataFrame = None):
    # -------------------------------------------------------
    # Extract data from first sheet from excel worksheet
    # -------------------------------------------------------
    logging.info(f"Reading {files}")
    df = pd.DataFrame()
    for file in files:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            part = pd.read_excel(file, sheet_name=0, usecols="A:V", header=0)

        df = pd.concat([df, part])

    # Rename columns to match the required column names
    df.rename(
        columns={
            "MRN": "mrn",
            "Patient": "name",
            "Sex": "sex",
            "DOB": "dob",
            "Address": "address",
            "City": "city",
            "State": "state",
            "ZIP": "zip",
            "Phone": "phone",
            "Pt. E-mail Address": "email",
            "PCP": "pcp",
            "Location": "location",
            "Dept": "dept",
            "Visit Date": "encounter_date",
            "Time": "encounter_time",
            "Encounter Type": "UNUSED",
            "Type": "encounter_type",
            "Provider/Resource": "service_provider",
            "With PCP?": "with_pcp",
            "Appt Status": "appt_status",
            "Encounter Diagnoses": "diagnoses",
            "Level of Service": "level_of_service",
        },
        inplace=True,
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
            "location",
            "dept",
            "encounter_date",
            "encounter_time",
            "encounter_type",
            "service_provider",
            "with_pcp",
            "appt_status",
            "diagnoses",
            "level_of_service",
        ]
    ].copy()

    # -------------------------------------------------------
    # Fix data types
    # -------------------------------------------------------
    # Convert MRN to string
    patients_df["mrn"] = patients_df["mrn"].astype(str)
    encounters_df["mrn"] = encounters_df["mrn"].astype(str)

    # Force encounter_date to be date only, no time
    encounters_df["encounter_date"] = pd.to_datetime(
        encounters_df["encounter_date"]
    ).dt.date

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

    return patients_df, encounters_df


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
    patients_df["age"] = patients_df["dob"].apply(
        lambda dob: relativedelta(now, dob).years
    )
    patients_df["age_in_mo_under_3"] = patients_df["dob"].apply(
        lambda dob: (
            relativedelta(now, dob).years * 12 + relativedelta(now, dob).months
            if relativedelta(now, dob).years <= 2
            else None
        )
    )
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

    # Calculate age of patient at encounter in years and months (if under 3) encounter_date and dob
    encounters_df["encounter_age"] = encounters_df.apply(
        lambda row: relativedelta(
            pd.Timestamp(row["encounter_date"]), row["dob"]
        ).years,
        axis=1,
    )
    encounters_df["encounter_age_in_mo_under_3"] = encounters_df.apply(
        lambda row: (
            relativedelta(pd.Timestamp(row["encounter_date"]), row["dob"]).years * 12
            + relativedelta(pd.Timestamp(row["encounter_date"]), row["dob"]).months
            if relativedelta(pd.Timestamp(row["encounter_date"]), row["dob"]).years <= 2
            else None
        ),
        axis=1,
    )
    encounters_df.drop(columns=["dob"], inplace=True)

    return encounters_df


def partition_phi(patients_df: pd.DataFrame):
    """
    Partition patients_df into PHI, which will be stored in ID DB, and non-PHI, which will be stored in the main DB
    """
    logging.info("Partitioning PHI")
    id_details_df = patients_df[
        [
            "prw_id",
            "mrn",
            "name",
            "dob",
            "address",
            "city",
            "state",
            "zip",
            "phone",
            "email",
        ]
    ]
    patients_df = patients_df[
        ["prw_id", "sex", "age", "age_in_mo_under_3", "city", "state", "pcp"]
    ]

    return patients_df, id_details_df


# -------------------------------------------------------
# Main entry point
# -------------------------------------------------------
def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Ingest source data into PRH warehouse."
    )
    parser.add_argument(
        "-i",
        "--input_dir",
        help="Path to the source data directory. All .xlsx files in the directory will be read.",
        default=DEFAULT_DATA_DIR,
    )
    parser.add_argument(
        "-o",
        "--out",
        help='Output DB connection string, including credentials if needed. Look for Azure SQL connection string in Settings > Connection strings, eg. "mssql+pyodbc:///?odbc_connect=Driver={ODBC Driver 18 for SQL Server};Server=tcp:{your server name},1433;Database={your db name};Uid={your user};Pwd={your password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"',
        default=DEFAULT_PRW_DB_ODBC,
    )
    parser.add_argument(
        "--id_out",
        help="Output connection string for ID DB, or 'None' to skip",
        default=DEFAULT_PRW_ID_DB_ODBC,
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
    dir_path = args.input_dir
    output_odbc = args.out
    id_output_odbc = args.id_out if args.id_out.lower() != "none" else None
    drop_tables = args.drop
    logging.info(
        f"Data dir: {dir_path}, output: {util.mask_pw(output_odbc)}, id output: {util.mask_pw(id_output_odbc or 'None')}"
    )

    # Source file paths
    encounters_files = util.get_excel_files(dir_path)
    logging.info(f"Source files: {encounters_files}")

    # Sanity check data directory expected location and files
    if not sanity_check_data_dir(dir_path, encounters_files):
        logging.error("ERROR: data directory error (see above). Terminating.")
        exit(1)

    # If ID DB is specified, read existing ID mappings
    prw_id_engine, mrn_to_prw_id_df = None, None
    if id_output_odbc:
        prw_id_engine = db_utils.get_db_connection(id_output_odbc, echo=SHOW_SQL_IN_LOG)
        if prw_id_engine is None:
            logging.error("ERROR: cannot open ID DB (see above). Terminating.")
            exit(1)
        if inspect(prw_id_engine).has_table(prw_id_model.PrwId.__tablename__):
            logging.info("Using existing MRN to PRW ID mappings")
            mrn_to_prw_id_df = read_mrn_to_prw_id_table(prw_id_engine)
        else:
            logging.info("ID DB table does not exist, will generate new ID mappings")

    # Read source files into memory
    patients_df, encounters_df = read_encounters(encounters_files, mrn_to_prw_id_df)

    # Transform data only to partition PHI into separate DB. All other data
    # transformations should be done by later flows in the pipeline.
    patients_df = calc_patient_age(patients_df)
    encounters_df = calc_age_at_encounter(encounters_df, patients_df)
    patients_df, id_details_df = partition_phi(patients_df)

    # Get connection to output DBs
    prw_engine = db_utils.get_db_connection(output_odbc, echo=SHOW_SQL_IN_LOG)
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

    # Explicitly drop data in tables in order for foreign key constraints to be met
    clear_tables(prw_session, [prw_model.PrwEncounter, prw_model.PrwPatient])

    # Write into DB
    clear_tables_and_insert_data(
        prw_session,
        [
            TableData(table=prw_model.PrwPatient, df=patients_df),
            TableData(table=prw_model.PrwEncounter, df=encounters_df),
        ],
    )

    # Update last ingest time and modified times for source data files
    modified = {
        file: datetime.fromtimestamp(os.path.getmtime(file))
        for file in encounters_files
    }
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
