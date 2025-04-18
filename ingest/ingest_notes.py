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
    clear_tables_and_insert_data,
)

# Unique identifier for this ingest dataset
DATASET_ID = "notes"

# -------------------------------------------------------
# Config
# -------------------------------------------------------
# Logging configuration
SHOW_SQL_IN_LOG = False
logging.basicConfig(level=logging.INFO)


# -------------------------------------------------------
# Extract from Source Files
# -------------------------------------------------------
def sanity_check_files(notes_inpt_ed_file: str):
    """
    Executed once at the beginning of ingest to validate CSV file
    meets basic requirements.
    """
    error = None
    if not os.path.isfile(notes_inpt_ed_file):
        error = f"ERROR: notes_inpt_ed file does not exist: {notes_inpt_ed_file}"
    if error is not None:
        logging.error(error)

    return error is None


def read_mrn_to_prw_id_table(engine):
    """
    Read existing ID to MRN mapping from the PRW ID DB
    """
    with Session(engine) as session:
        results = session.exec(
            select(prw_id_model.PrwId.prw_id, prw_id_model.PrwId.mrn)
        )
        return pd.DataFrame(results)


def read_notes_inpt(csv_file: str):
    # -------------------------------------------------------
    # Extract data from CSV file
    # -------------------------------------------------------
    logging.info(f"Reading {csv_file}")
    df = pd.read_csv(
        csv_file,
        dtype={
            "mrn": str,
            "name": str,
            "dept": str,
            "note_type": str,
            "primary_diag": str,
            "author_name": str,
            "author_type": str,
            "author_service": str,
            "first_author_name": str,
            "cosigner_name": str,
        },
        parse_dates=["service_dt"],
        index_col=False,
    )

    # Rename columns to match model
    df = df.rename(
        columns={
            "service_dt": "service_date",
            "primary_diag": "diagnosis",
            "author_service": "service",
        }
    )

    return df


# -------------------------------------------------------
# Transform
# -------------------------------------------------------
def unspecified_to_null(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert "*Unspecified" values to null in string columns
    """
    columns_to_clean = [
        "dept",
        "service",
        "note_type",
        "diagnosis",
        "author_name",
        "author_type",
        "first_author_name",
        "cosigner_name",
    ]
    for col in columns_to_clean:
        if col in df.columns:
            df[col] = df[col].replace("*Unspecified", None)
    return df


def mrn_to_prw_id(
    df: pd.DataFrame, mrn_to_prw_id_df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    # Calculate PRW ID from MRN for patients that don't have one
    patients_df = df.drop_duplicates(subset=["mrn"], keep="first")
    patients_df = prw_id_utils.calc_prw_id(
        patients_df,
        src_id_to_id_df=mrn_to_prw_id_df,
    )

    # Get new PRW IDs that were added and combine with existing map
    new_ids_df = patients_df[~patients_df["prw_id"].isin(mrn_to_prw_id_df["prw_id"])]
    mrn_to_prw_id_df = pd.concat([mrn_to_prw_id_df, new_ids_df[["prw_id", "mrn"]]])

    # Merge into data
    df = df.merge(mrn_to_prw_id_df, on="mrn", how="left")

    # Return data with PRW IDs and new IDs that were created
    return df, new_ids_df


def add_id_details(ids_df: pd.DataFrame, data_df: pd.DataFrame) -> pd.DataFrame:
    # Add back details (ie name field) for given PRW IDs
    ids_df = ids_df.merge(data_df[["prw_id", "name"]], on="prw_id", how="left")
    return ids_df


def partition_inpt_ed(
    notes_inpt_ed_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    # Separate notes from the dept "CC WPL EMERGENCY CENTER" or with note_type "ED Provider Notes"
    # into ED notes and the rest into inpatient notes
    ed_mask = (notes_inpt_ed_df["dept"] == "CC WPL EMERGENCY CENTER") | (
        notes_inpt_ed_df["note_type"] == "ED Provider Notes"
    )
    notes_ed_df = notes_inpt_ed_df.loc[ed_mask]
    notes_inpt_df = notes_inpt_ed_df.loc[~ed_mask]
    return notes_inpt_df, notes_ed_df


# -------------------------------------------------------
# Load
# -------------------------------------------------------
def update_id_tables(prw_id_engine, new_ids_df: pd.DataFrame):
    """
    Update the prw_id and prw_id_details tables with new mappings and PHI.
    """
    logging.info(f"Writing {len(new_ids_df)} new PRW IDs")
    with Session(prw_id_engine) as prw_id_session:
        prw_id_model.PrwIdModel.metadata.create_all(prw_id_engine)

        # Add new rows to ID tables
        for table, df in [
            (prw_id_model.PrwId, new_ids_df[["prw_id", "mrn"]]),
            (prw_id_model.PrwIdDetails, new_ids_df[["prw_id", "mrn", "name"]]),
        ]:
            # Write data from dataframe using bulk operations
            df.to_sql(
                name=table.__tablename__,
                con=prw_id_session.connection(),
                if_exists="append",
                index=False,
                chunksize=50000,
            )

        # Commit updates
        prw_id_session.commit()


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
    return parser.parse_args()


def main():
    # Load config from cmd line
    args = parse_arguments()
    in_path = args.input
    output_conn = args.prw
    id_output_conn = args.prwid if args.prwid.lower() != "none" else None
    logging.info(
        f"Input: {in_path}, output: {mask_conn_pw(output_conn)}, id output: {mask_conn_pw(id_output_conn or 'None')}"
    )

    # Input files
    notes_inpt_ed_file = os.path.join(in_path, "notes-inpt-ed.csv")

    # Sanity check the input file
    if not sanity_check_files(notes_inpt_ed_file):
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
            mrn_to_prw_id_df = pd.DataFrame(columns=["prw_id", "mrn"])
            logging.info("ID DB table does not exist, will generate new ID mappings")

    # Read source file into memory
    notes_inpt_ed_df = read_notes_inpt(notes_inpt_ed_file)

    # Basic transforms - convert to PRW IDs and separate inpatient and ED notes
    notes_inpt_ed_df = unspecified_to_null(notes_inpt_ed_df)
    notes_inpt_ed_df, new_ids_df = mrn_to_prw_id(notes_inpt_ed_df, mrn_to_prw_id_df)
    notes_inpt_df, notes_ed_df = partition_inpt_ed(notes_inpt_ed_df)

    # Get connection to output DBs
    prw_engine = get_db_connection(output_conn, echo=SHOW_SQL_IN_LOG)
    if prw_engine is None:
        logging.error("ERROR: cannot open output DB (see above). Terminating.")
        exit(1)
    prw_session = Session(prw_engine)

    # Create tables if they do not exist
    logging.info("Creating tables")
    prw_model.PrwModel.metadata.create_all(prw_engine)

    # Write into DB
    clear_tables_and_insert_data(
        prw_session,
        [
            TableData(table=prw_model.PrwNotesInpt, df=notes_inpt_df),
            TableData(table=prw_model.PrwNotesEd, df=notes_ed_df),
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
        update_id_tables(prw_id_engine, new_ids_df)
        prw_id_engine.dispose()

    logging.info("Done")


if __name__ == "__main__":
    main()
