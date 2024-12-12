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

# Path to ../ingest/ source directory
INGEST_CODE_DIR = pathlib.Path(__file__).parent.parent / "ingest"

# Load env vars from .env file, does not overwrite existing env variables.
# Then load config from env vars
load_dotenv()
PRW_ENCOUNTERS_SOURCE_DIR = os.environ.get("PRW_ENCOUNTERS_SOURCE_DIR")
PRW_FINANCE_SOURCE_DIR = os.environ.get("PRW_FINANCE_SOURCE_DIR")
PRW_DB_ODBC = os.environ.get("PRW_DB_ODBC", Secret.load("prw-db-url").get())

# Subflows should drop tables before ingesting data
DROP_FLAG = ""


# -----------------------------------------
# Ingest source data processes
# -----------------------------------------
@flow
async def ingest_source_encounters():
    return await ingest_shell_op(
        [
            f'pipenv run python ingest_encounters.py -i "{PRW_ENCOUNTERS_SOURCE_DIR}" -o "{PRW_DB_ODBC}" {DROP_FLAG}',
        ],
        working_dir=INGEST_CODE_DIR,
    )


@flow
async def ingest_source_finance():
    return await ingest_shell_op(
        [
            f'pipenv run python ingest_finance.py -i "{PRW_FINANCE_SOURCE_DIR}" -o "{PRW_DB_ODBC}" {DROP_FLAG}',
        ],
        working_dir=INGEST_CODE_DIR,
    )


# -----------------------------------------
# Additional transforms to source data
# -----------------------------------------
@flow
async def transform_patient_panel():
    return await ingest_shell_op(
        [f'pipenv run python transform_patient_panel.py -db "{PRW_DB_ODBC}"'],
        working_dir=INGEST_CODE_DIR,
    )


# -----------------------------------------
# Main entry point / parent flow
# -----------------------------------------
@flow(retries=0, retry_delay_seconds=300)
async def prw_ingest():
    # First, create/update the python virtual environment which is used by all subflows in ../ingest/
    await pipenv_install_task(working_dir=INGEST_CODE_DIR)

    # Run ingest subflows
    ingest_flows = [ingest_source_encounters(), ingest_source_finance()]
    await asyncio.gather(*ingest_flows)

    # After ingest flows are complete, run transform flows, which calculate
    # additional common columns that will be used across multiple applications
    transform_flows = [transform_patient_panel()]
    await asyncio.gather(*transform_flows)

    # Lastly create datamarts for each application
    await datamart_ingest()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Main Ingest Prefect Flow for PRH warehouse."
    )
    parser.add_argument(
        "--drop",
        action="store_true",
        help="Drop and recreate tables before ingesting data",
    )
    args = parser.parse_args()
    DROP_FLAG = "--drop" if args.drop else ""

    asyncio.run(prw_ingest())
