import os
import logging
import glob
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
            "RvuWork": float,
            "RvuTotal": float,
            "ReversalReason": str,
            "ChargeAmount": float,
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
            "RvuTotal": "trvu",
            "ReversalReason": "reversal_reason",
            "ChargeAmount": "charge_amount",
            "IsInactive": "is_inactive",
            "PrimaryPayorClass": "primary_payor_class",
            "CostCenterName": "dept",
            "PlaceOfServiceName": "location",
        }
    )

    return df


def read_rvu_mappings(rvu_mapping_path: str) -> pd.DataFrame:
    """
    Read the CMS RVU CSV file and return a dataframe mapping procedure code to tRVU values.
    Skips the first 10 lines, then retains columns: 1 (HCPCS), 6 (wRVU),12 (non-facility tRVU), 13 (facility tRVU).
    The totals are floats.
    """
    # Find the CMS RVU CSV file, PPRRVU*.csv, with the latest timestamp
    cms_rvu_files = glob.glob(os.path.join(rvu_mapping_path, "PPRRVU*.csv"))
    if len(cms_rvu_files) == 0:
        return None
    cms_rvu_file = max(cms_rvu_files, key=os.path.getmtime)
    logging.info(f"Using CMS RVU file: {cms_rvu_file}")

    # Supplemental RVU mapping file from data provided from Samaritan. This file is currently manually
    # exported from the Excel worksheet with the same name in "EpicReports/SamaritanExternal/Rev_by_Rev_Code/MASTER CDM Report 120423.xlsx"
    samaritan_rvu_file = os.path.join(rvu_mapping_path, "Samaritan_CDM.csv")

    df = pd.read_csv(
        cms_rvu_file,
        skiprows=10,
        header=0,
        usecols=[0, 5, 11, 12],
        names=["hcpcs", "wrvu", "nonfacility_trvu", "facility_trvu"],
        dtype={
            "hcpcs": str,
            "wrvu": float,
            "nonfacility_trvu": float,
            "facility_trvu": float,
        },
    )
    samaritan_df = pd.read_csv(
        samaritan_rvu_file,
        skiprows=10,
        header=0,
        usecols=[0, 3, 5, 11, 31],
        names=["hcpcs", "status_code", "wrvu", "nonfacility_trvu", "facility_trvu"],
        dtype={
            "hcpcs": str,
            "wrvu": float,
            "nonfacility_trvu": float,
            "facility_trvu": float,
        },
    )
    # Retain only supplemental data, which is marked as "Imputed" in column D (Status Code)
    samaritan_df = samaritan_df[samaritan_df["status_code"] == "Imputed"]
    samaritan_df = samaritan_df.drop(columns=["status_code"])

    # Combine CMS and supplemental data
    df = pd.concat([df, samaritan_df], ignore_index=True)
    df = df.drop_duplicates(subset=["hcpcs"])
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
        "posting_user",
        "modifiers",
        "reversal_reason",
    ]
    for col in columns_to_clean:
        if col in df.columns:
            df[col] = df[col].replace(["*Unspecified", ""], None)
    return df


def calculate_trvu(
    charges_df: pd.DataFrame, rvu_mappings_df: pd.DataFrame | None
) -> pd.DataFrame:
    """
    If mapping is provided, map from CPT code to facility tRVUs in charges. Otherwise, just use current values,
    which are from Epic.
    """
    if rvu_mappings_df is None:
        return charges_df

    # Map from CPT code to facility tRVUs
    charges_df = charges_df.merge(
        rvu_mappings_df[["hcpcs", "facility_trvu"]],
        left_on="procedure_code",
        right_on="hcpcs",
        how="left",
    )
    charges_df = charges_df.drop(columns=["trvu", "hcpcs"])
    charges_df = charges_df.rename(columns={"facility_trvu": "trvu"})

    # Multiply tRVU by quantity
    charges_df["trvu"] = charges_df["trvu"] * charges_df["quantity"]
    return charges_df


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
        "--rvu-mapping",
        type=str,
        default=None,
        help="Path to the RVU files to use to calculate tRVU values. If not provided, tRVU values will be taken from Epic.",
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
    rvu_mapping_path = args.rvu_mapping
    logging.info(
        f"Input: {in_path} / {rvu_mapping_path}, output: {mask_conn_pw(output_conn)}, id output: {mask_conn_pw(id_output_conn or 'None')}"
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

    # Read RVU mapping file if provided
    rvu_mappings_df = None
    if rvu_mapping_path:
        logging.info(f"Using RVU mappings from: {rvu_mapping_path}")
        rvu_mappings_df = read_rvu_mappings(rvu_mapping_path)
    if rvu_mappings_df is None:
        logging.info(f"No RVU mappings found. Will use tRVUs from Epic.")

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

        # Calculate tRVU values if CMS RVU mappings are provided
        charges_df = calculate_trvu(charges_df, rvu_mappings_df)

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
