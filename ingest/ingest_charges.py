import os
import logging
import pandas as pd
from datetime import datetime
from sqlmodel import Session, select, inspect
from prw_common.model import prw_model, prw_id_model
from util import util, prw_id_utils, prw_meta_utils
from prw_common.cli_utils import cli_parser
from prw_common.db_utils import (
    TableData,
    get_db_connection,
    mask_conn_pw,
    upsert_data,
)

# Unique identifier for this ingest dataset
DATASET_ID = "charges"

# -------------------------------------------------------
# Config
# -------------------------------------------------------
# Logging configuration
SHOW_SQL_IN_LOG = False
logging.basicConfig(level=logging.INFO)


# -------------------------------------------------------
# Extract from Source Files
# -------------------------------------------------------
def sanity_check_files(charges_files: list[str]) -> bool:
    """
    Executed once at the beginning of ingest to validate CSV file
    meets basic requirements.
    """
    error = None
    if len(charges_files) == 0:
        error = f"ERROR: no charges files found"
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


def read_charges(csv_file: str):
    # -------------------------------------------------------
    # Extract data from CSV file
    # -------------------------------------------------------
    logging.info(f"Reading {csv_file}")
    df = pd.read_csv(
        csv_file,
        dtype={
            "BillingTransactionKey": int,
            "PrimaryMRN": str,
            "EncounterCSN": str,
            "ServiceDateKey": int,
            "PostDateKey": int,
            "BillingProviderName": str,
            "PostingUserName": str,
            "BillingProcedureCode": str,
            "ModifierCodes": str,
            "BillingProcedureQuantity": int,
            "BillingProcedureDescription": str,
            "RevenueCode": str,
            "RevenueCodeName": str,
            "ReversalReason": str,
            "IsInactive": bool,
            "PrimaryPayorClass": str,
            "CostCenterName": str,
            "PlaceOfServiceName": str,
        },
        index_col=False,
    )

    # Convert YYYYMMDD integer dates to datetime
    df["ServiceDateKey"] = pd.to_datetime(
        df["ServiceDateKey"].astype(str), format="%Y%m%d"
    )
    df["PostDateKey"] = pd.to_datetime(df["PostDateKey"].astype(str), format="%Y%m%d")

    # Rename columns to match model
    df = df.rename(
        columns={
            "BillingTransactionKey": "id",
            "PrimaryMRN": "mrn",
            "EncounterCSN": "encounter_csn",
            "ServiceDateKey": "service_date",
            "PostDateKey": "post_date",
            "BillingProviderName": "billing_provider",
            "PostingUserName": "posting_user",
            "BillingProcedureCode": "procedure_code",
            "ModifierCodes": "modifiers",
            "BillingProcedureQuantity": "quantity",
            "BillingProcedureDescription": "procedure_desc",
            "RevenueCode": "rev_code",
            "RevenueCodeName": "rev_code_desc",
            "RvuWork": "wrvu",
            "ReversalReason": "reversal_reason",
            "ChargeAmount": "charge_amount",
            "IsInactive": "is_inactive",
            "PrimaryPayorClass": "primary_payor_class",
            "CostCenterName": "dept",
            "PlaceOfServiceName": "location",
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
        "billing_provider",
        "modifiers",
        "reversal_reason",
    ]
    for col in columns_to_clean:
        if col in df.columns:
            df[col] = df[col].replace("*Unspecified", None)
    return df


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
            (prw_id_model.PrwIdDetails, new_ids_df[["prw_id", "mrn"]]),
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
        description="Ingest charges data into PRH warehouse.",
        require_prw=True,
        require_prwid=True,
        require_in=True,
    )
    parser.add_argument(
        "--backfill",
        action="store_true",
        default=False,
        help="When false (default), only the last two data files sorted by filename are processed. When true (backfill mode), all charge source files are processed. ",
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
    charges_files = util.find_data_files(in_path)
    if args.backfill:
        logging.info(f"Processing all files: {charges_files}")
    else:
        charges_files = sorted(charges_files)[-2:]
        logging.info(
            f"Incremental mode: processing only the last 2 files: {charges_files}"
        )

    # Sanity check the input files
    if not sanity_check_files(charges_files):
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

    # Get connection to output DBs
    prw_engine = get_db_connection(output_conn, echo=SHOW_SQL_IN_LOG)
    if prw_engine is None:
        logging.error("ERROR: cannot open output DB (see above). Terminating.")
        exit(1)
    prw_session = Session(prw_engine)

    # Create tables if they do not exist
    logging.info("Creating tables")
    prw_model.PrwModel.metadata.create_all(prw_engine)

    # Process each source file
    all_new_ids_df = pd.DataFrame(columns=["prw_id", "mrn"])

    for charges_file in charges_files:
        logging.info(f"Processing file: {charges_file}")

        # Read source file into memory
        charges_df = read_charges(charges_file)

        # Basic transforms - remove PHI / convert to PRW IDs
        charges_df = unspecified_to_null(charges_df)
        charges_df, new_ids_df = prw_id_utils.mrn_to_prw_id_col(
            charges_df, mrn_to_prw_id_df
        )

        # Insert/update into DB
        upsert_data(
            prw_session,
            [
                TableData(table=prw_model.PrwCharges, df=charges_df),
            ],
            chunk_size=20000,
        )

        # Accumulate new IDs
        if not new_ids_df.empty:
            all_new_ids_df = pd.concat([all_new_ids_df, new_ids_df], ignore_index=True)
            mrn_to_prw_id_df = pd.concat(
                [mrn_to_prw_id_df, new_ids_df[["prw_id", "mrn"]]], ignore_index=True
            )

    # Write new PRW IDs to separate ID DB
    if prw_id_engine and not all_new_ids_df.empty:
        update_id_tables(prw_id_engine, all_new_ids_df)

    # Update last ingest time and modified times for source data files
    modified = {
        file: datetime.fromtimestamp(os.path.getmtime(file)) for file in charges_files
    }
    prw_meta_utils.write_meta(prw_session, DATASET_ID, modified)

    # Cleanup
    prw_session.commit()
    prw_session.close()
    prw_engine.dispose()

    if prw_id_engine:
        prw_id_engine.dispose()

    logging.info("Done")


if __name__ == "__main__":
    main()
