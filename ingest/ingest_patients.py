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
DATASET_ID = "patients"

# -------------------------------------------------------
# Config
# -------------------------------------------------------
# Logging configuration
SHOW_SQL_IN_LOG = False
logging.basicConfig(level=logging.INFO)


# -------------------------------------------------------
# Extract from Source Files
# -------------------------------------------------------
def line_count(file_path):
    with open(file_path, "rb") as f:
        return sum(1 for _ in f)


def sanity_check_files(patients_file, mychart_file, allergy_file, problem_list_file):
    """
    Executed once at the beginning of ingest to validate CSV file
    meets basic requirements.
    """
    error = None
    if not os.path.isfile(patients_file) or line_count(patients_file) < 10000:
        error = f"ERROR: invalid patients file: {patients_file}"
    elif not os.path.isfile(mychart_file) or line_count(mychart_file) < 10000:
        error = f"ERROR: invalid mychart file: {mychart_file}"
    elif not os.path.isfile(allergy_file) or line_count(allergy_file) < 10000:
        error = f"ERROR: invalid allergy file: {allergy_file}"
    elif not os.path.isfile(problem_list_file) or line_count(problem_list_file) < 10000:
        error = f"ERROR: invalid problem list file: {problem_list_file}"
    if error is not None:
        print(error)

    return error is None


def read_patients(csv_file: str):
    # -------------------------------------------------------
    # Extract data from CSV file
    # -------------------------------------------------------
    logging.info(f"Reading {csv_file}")
    patients_df = pd.read_csv(
        csv_file,
        skiprows=1,
        names=[
            "prw_id",
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
        ],
        dtype={
            "prw_id": str,
            "mrn": str,
            "name": str,
            "sex": str,
            "address": str,
            "city": str,
            "state": str,
            "zip": str,
            "phone": str,
            "email": str,
            "pcp": str,
        },
        parse_dates=["dob"],
        index_col=False,
    )
    # Extract just prw_id -> MRN mapping into separate dataframe to persist
    mrn_to_prw_id_df = patients_df[["mrn", "prw_id"]].copy()

    return patients_df, mrn_to_prw_id_df


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


def read_problem_list(problem_list_file: str, mrn_to_prw_id_df: pd.DataFrame):
    """
    Read problem lists file and add info into patients_df
    """
    logging.info(f"Reading {problem_list_file}")
    problem_list_df = pd.read_csv(
        problem_list_file,
        skiprows=1,
        index_col=False,
        names=[
            "mrn",
            "diagnosis",
            "icd",
            "start_date",
            "end_date",
            "status",
            "is_cancer",
        ],
        dtype={
            "mrn": str,
            "start_date": str,
        },
    )

    # Keep only active problems
    problem_list_df = problem_list_df[problem_list_df["status"] == "Active"]

    # Group by MRN, and merge data into a json string
    problem_list_df = (
        problem_list_df.groupby("mrn")[["diagnosis", "icd", "start_date"]]
        .apply(lambda x: x.to_json(orient="records"))
        .reset_index(name="problem_list")
    )

    # Convert MRN to prw_id
    problem_list_df = problem_list_df.merge(mrn_to_prw_id_df, on="mrn", how="right")
    problem_list_df.drop(columns=["mrn"], inplace=True)

    return problem_list_df


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
def drop_dup_mrn(patients_df: pd.DataFrame):
    """
    Drop duplicate MRNs, retain the occurrence with the greatest prw_id (ie DurableKey)
    """
    idx = patients_df.groupby("mrn")["prw_id"].idxmax()
    patients_df = patients_df.loc[idx].reset_index(drop=True)
    return patients_df


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
    patients_df["age"] = patients_df["age"].astype("Int64")

    # Calculate age in months, but only retain if under 3
    patients_df["age_in_mo_under_3"] = (
        (now.year - dob_df.dt.year) * 12
        + (now.month - dob_df.dt.month)
        - (now.day < dob_df.dt.day)
    )
    patients_df.loc[patients_df["age"] > 2, "age_in_mo_under_3"] = None
    patients_df["age_in_mo_under_3"] = patients_df["age_in_mo_under_3"].astype("Int64")
    return patients_df


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
    id_output_conn = args.prwid
    drop_tables = args.drop
    logging.info(
        f"Input: {in_path}, output: {mask_conn_pw(output_conn)}, id output: {mask_conn_pw(id_output_conn)}"
    )
    logging.info(f"Drop tables before writing: {drop_tables}")

    # Input files
    patients_file = os.path.join(in_path, "patients.csv")
    mychart_file = os.path.join(in_path, "mychart.csv")
    allergy_file = os.path.join(in_path, "allergy.csv")
    problem_list_file = os.path.join(in_path, "problem-list.csv")

    # Sanity check the input file
    if not sanity_check_files(
        patients_file, mychart_file, allergy_file, problem_list_file
    ):
        logging.error("ERROR: input error (see above). Terminating.")
        exit(1)

    # Read source file into memory
    patients_df, mrn_to_prw_id_df = read_patients(patients_file)
    mychart_df = read_mychart_status(mychart_file, mrn_to_prw_id_df)
    allergy_df = read_allergy(allergy_file, mrn_to_prw_id_df)
    problem_list_df = read_problem_list(problem_list_file, mrn_to_prw_id_df)

    # Add allergies and problem lists to patients_df
    patients_df = patients_df.merge(allergy_df, on="prw_id", how="left")
    patients_df = patients_df.merge(problem_list_df, on="prw_id", how="left")

    # Transform data: only data correction and partitioning of PHI into separate DB.
    # All other data transformations should be done by later flows in the pipeline.
    patients_df = drop_dup_mrn(patients_df)
    patients_df = calc_patient_age(patients_df)
    patients_df, id_details_df = partition_phi(patients_df)
    id_df = id_details_df[["prw_id", "mrn"]]

    # Get connection to output DBs
    prw_engine = get_db_connection(output_conn, echo=SHOW_SQL_IN_LOG)
    prw_id_engine = get_db_connection(id_output_conn, echo=SHOW_SQL_IN_LOG)
    if prw_engine is None:
        logging.error("ERROR: cannot open output DB (see above). Terminating.")
        exit(1)
    if prw_id_engine is None:
        logging.error("ERROR: cannot open ID DB (see above). Terminating.")
        exit(1)
    prw_session = Session(prw_engine)
    prw_id_session = Session(prw_id_engine)

    # Drop tables so DDL is reissued if requested
    if drop_tables:
        logging.info("Dropping existing tables")
        prw_model.PrwPatient.__table__.drop(prw_engine, checkfirst=True)
        prw_model.PrwMyChart.__table__.drop(prw_engine, checkfirst=True)
        prw_id_model.PrwId.__table__.drop(prw_id_engine, checkfirst=True)
        prw_id_model.PrwIdDetails.__table__.drop(prw_id_engine, checkfirst=True)

    # Create tables if they do not exist
    logging.info("Creating tables")
    prw_model.PrwMetaModel.metadata.create_all(prw_engine)
    prw_model.PrwModel.metadata.create_all(prw_engine)
    prw_id_model.PrwIdModel.metadata.create_all(prw_id_engine)
    prw_id_model.PrwIdDetails.metadata.create_all(prw_id_engine)

    # Write into DB
    clear_tables_and_insert_data(
        prw_session,
        [
            TableData(table=prw_model.PrwPatient, df=patients_df),
            TableData(table=prw_model.PrwMyChart, df=mychart_df),
        ],
    )

    # Update last ingest time and modified times for source data files
    modified = {in_path: datetime.fromtimestamp(os.path.getmtime(in_path))}
    prw_meta_utils.write_meta(prw_session, DATASET_ID, modified)

    # Commit and clean up main DB
    prw_session.commit()
    prw_session.close()
    prw_engine.dispose()

    # Write ID data to separate DB
    logging.info("Writing ID data")
    clear_tables_and_insert_data(
        prw_id_session,
        [
            TableData(table=prw_id_model.PrwId, df=id_df),
            TableData(table=prw_id_model.PrwIdDetails, df=id_details_df),
        ],
    )

    # Cleanup
    prw_id_session.commit()
    prw_id_session.close()
    prw_id_engine.dispose()

    logging.info("Done")


if __name__ == "__main__":
    main()
