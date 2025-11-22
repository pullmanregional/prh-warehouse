import os
import logging
import pandas as pd
from datetime import datetime
from sqlmodel import Session
from prw_common.model import prw_model
from util import prw_meta_utils
from prw_common.cli_utils import cli_parser
from prw_common.db_utils import (
    TableData,
    get_db_connection,
    mask_conn_pw,
    clear_tables_and_insert_data,
)

# Unique identifier for this ingest dataset
DATASET_ID = "imaging"

# -------------------------------------------------------
# Config
# -------------------------------------------------------
# Logging configuration
SHOW_SQL_IN_LOG = False
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


# -------------------------------------------------------
# Extract from Source Files
# -------------------------------------------------------
def sanity_check_files(filename: str):
    """
    Executed once at the beginning of ingest to validate CSV file
    meets basic requirements.
    """
    error = None
    if not os.path.isfile(filename):
        error = f"ERROR: source file does not exist: {filename}"
    if error is not None:
        logger.error(error)

    return error is None


def read_imaging(csv_file: str):
    # -------------------------------------------------------
    # Extract data from CSV file
    # -------------------------------------------------------
    logger.info(f"Reading {csv_file}")
    df = pd.read_csv(
        csv_file,
        dtype={
            "ImagingKey": int,
            "DepartmentName": str,
            "ScheduledExamDateKey": int,
            "ExamEndDateKey": int,
            "Modality": str,
            "StudyStatus": str,
        },
        index_col=False,
    )
    # Convert YYYYMMDD integer dates to datetime
    df["ScheduledExamDateKey"] = pd.to_datetime(
        df["ScheduledExamDateKey"].astype(str), format="%Y%m%d"
    )
    # Convert -1 values to null before converting to datetime
    df["ExamEndDateKey"] = df["ExamEndDateKey"].replace(-1, pd.NA)
    df["ExamEndDateKey"] = pd.to_datetime(
        df["ExamEndDateKey"].astype("string"), format="%Y%m%d"
    )

    # Rename columns to match model
    df = df.rename(
        columns={
            "ImagingKey": "imaging_key",
            "DepartmentName": "dept",
            "ScheduledExamDateKey": "scheduled_date",
            "ExamEndDateKey": "service_date",
            "Modality": "modality",
            "StudyStatus": "study_status",
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
        "modality",
        "study_status",
    ]
    for col in columns_to_clean:
        if col in df.columns:
            df[col] = df[col].replace("*Unspecified", None)
    return df


# -------------------------------------------------------
# Main entry point
# -------------------------------------------------------
def parse_arguments():
    parser = cli_parser(
        description="Ingest source data into PRH warehouse.",
        require_prw=True,
        require_in=True,
    )
    return parser.parse_args()


def main():
    # Load config from cmd line
    args = parse_arguments()
    in_path = args.input
    output_conn = args.prw
    logger.info(f"Input: {in_path}, output: {mask_conn_pw(output_conn)}")

    # Input files
    imaging_file = os.path.join(in_path, "imaging.csv")

    # Sanity check the input file
    if not sanity_check_files(imaging_file):
        logger.error("ERROR: input error (see above). Terminating.")
        exit(1)

    # Read source file into memory
    imaging_df = read_imaging(imaging_file)

    # Basic transforms - remove PHI / convert to PRW IDs
    imaging_df = unspecified_to_null(imaging_df)

    # Get connection to output DBs
    prw_engine = get_db_connection(output_conn, echo=SHOW_SQL_IN_LOG)
    if prw_engine is None:
        logger.error("ERROR: cannot open output DB (see above). Terminating.")
        exit(1)
    prw_session = Session(prw_engine)

    # Create tables if they do not exist
    logger.info("Creating tables")
    prw_model.PrwModel.metadata.create_all(prw_engine)

    # Write into DB
    clear_tables_and_insert_data(
        prw_session,
        [
            TableData(table=prw_model.PrwImaging, df=imaging_df),
        ],
    )

    # Update last ingest time and modified times for source data files
    modified = {in_path: datetime.fromtimestamp(os.path.getmtime(in_path))}
    prw_meta_utils.write_meta(prw_session, DATASET_ID, modified)

    # Cleanup
    prw_session.commit()
    prw_session.close()
    prw_engine.dispose()

    logger.info("Done")


if __name__ == "__main__":
    main()
