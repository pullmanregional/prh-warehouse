import os
import pathlib
import asyncio
import argparse
from dotenv import load_dotenv
from prefect import flow
from prefect.blocks.system import Secret
from prefect_datamarts import datamart_ingest
from prefect_util import pipenv_install_task, ingest_shell_op

# Update path to include pipenv in the worker user's local bin
os.environ["PATH"] = f"{os.environ['PATH']}:{pathlib.Path.home()}/.local/bin"

# Path to current dir (prefect/) and ../ingest/ source directory
CODE_ROOT = os.path.join(os.path.dirname(__file__))
INGEST_CODE_ROOT = pathlib.Path(__file__).parent.parent / "ingest"

# Default to prod environment file
PRW_ENV = os.getenv("PRW_ENV", "prod")
ENVS = {
    "dev": ".env.dev",
    "prod": ".env.prod",
}
ENV_FILE = os.path.join(CODE_ROOT, ENVS.get(PRW_ENV))

# Load env vars from .env file, does not overwrite existing env variables,
# then store config from env vars into constants
print(f"Using environment: {ENV_FILE}")
load_dotenv(dotenv_path=ENV_FILE)
PRW_ENCOUNTERS_SOURCE_DIR = os.environ.get("PRW_ENCOUNTERS_SOURCE_DIR")
PRW_FINANCE_SOURCE_DIR = os.environ.get("PRW_FINANCE_SOURCE_DIR")
PRW_DB_ODBC = os.environ.get("PRW_DB_ODBC") or Secret.load("prw-db-url").get()
PRW_ID_DB_ODBC = os.environ.get("PRW_ID_DB_ODBC") or Secret.load("prw-id-db-url").get()

# Subflows should drop tables before ingesting data. Will be set by --drop command line arg.
DROP_FLAG = ""


# -----------------------------------------
# Ingest source data processes
# -----------------------------------------
@flow
async def prw_ingest_encounters():
    cmd = f'pipenv run python ingest_encounters.py -i "{PRW_ENCOUNTERS_SOURCE_DIR}" -o "{PRW_DB_ODBC}" --id_out "{PRW_ID_DB_ODBC}" {DROP_FLAG}'
    print(f"Executing: {cmd}")
    return await ingest_shell_op([cmd], working_dir=INGEST_CODE_ROOT)


@flow
async def prw_ingest_finance():
    cmd = f'pipenv run python ingest_finance.py -i "{PRW_FINANCE_SOURCE_DIR}" -o "{PRW_DB_ODBC}" {DROP_FLAG}'
    print(f"Executing: {cmd}")
    return await ingest_shell_op([cmd], working_dir=INGEST_CODE_ROOT)


# -----------------------------------------
# Additional transforms to source data
# -----------------------------------------
@flow
async def prw_transform_clean_encounters():
    return await ingest_shell_op(
        [f'pipenv run python transform_clean_encounters.py -db "{PRW_DB_ODBC}"'],
        working_dir=INGEST_CODE_ROOT,
    )


@flow
async def prw_transform_patient_panel():
    return await ingest_shell_op(
        [f'pipenv run python transform_patient_panel.py -db "{PRW_DB_ODBC}"'],
        working_dir=INGEST_CODE_ROOT,
    )


# -----------------------------------------
# Main entry point / parent flow
# -----------------------------------------
@flow(retries=0, retry_delay_seconds=300)
async def prw_ingest(run_ingest=True, run_transform=True, run_datamart=True):
    # First, create/update the python virtual environment which is used by all subflows in ../ingest/
    await pipenv_install_task(working_dir=INGEST_CODE_ROOT)

    if run_ingest:
        # Run ingest subflows
        ingest_flows = [prw_ingest_encounters(), prw_ingest_finance()]
        await asyncio.gather(*ingest_flows)

    if run_transform:
        # Clean data
        await prw_transform_clean_encounters()

        # After ingest flows are complete, run transform flows, which calculate
        # additional common columns that will be used across multiple applications
        transform_flows = [prw_transform_patient_panel()]
        await asyncio.gather(*transform_flows)

    if run_datamart:
        # Lastly create datamarts for each application
        await datamart_ingest()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Main Ingest Prefect Flow for PRH warehouse."
    )
    parser.add_argument(
        "--stage1-only",
        action="store_true",
        help="Only ingest data, do not run transforms or datamart ingest",
    )
    parser.add_argument(
        "--stage2-only",
        action="store_true",
        help="Only run transforms, do not run ingest or datamart subflows",
    )
    parser.add_argument(
        "--stage3-only",
        action="store_true",
        help="Only run datamart ingest, do not run ingest or transform subflows",
    )
    parser.add_argument(
        "--drop",
        action="store_true",
        help="Drop and recreate tables before ingesting data",
    )
    args = parser.parse_args()
    DROP_FLAG = "--drop" if args.drop else ""

    # Determine which subflows to run based on command line args
    run_ingest = args.stage1_only or not (args.stage2_only or args.stage3_only)
    run_transform = args.stage2_only or not (args.stage1_only or args.stage3_only)
    run_datamart = args.stage3_only or not (args.stage1_only or args.stage2_only)

    # Run the main ingest flow
    asyncio.run(
        prw_ingest(
            run_ingest=run_ingest,
            run_transform=run_transform,
            run_datamart=run_datamart,
        )
    )
