"""
Main ingest workflow for the PRH data warehouse. This flow orchestrates the ingestion of data from various 
source systems into the warehouse.

The flow is deployed and scheduled via Prefect (see prw_deploy.py). It:
1. Sets up the Python environment using pipenv
2. Executes individual ingest subflows for each data source
3. Triggers datamart refresh flows after ingestion is complete

Environment configuration is loaded from .env.* files based on PRW_ENV setting (defaults to prod).
Datamart flows to execute are defined in the DATAMART_DEPLOYMENTS constant
"""

import os
import pathlib
import asyncio
import argparse
from dotenv import load_dotenv
from prefect import flow
from prefect.blocks.system import Secret
from prefect.deployments import run_deployment
from prefect_util import shell_op

# Load env vars from a .env file
# load_dotenv() does NOT overwrite existing env vars that are set before running this script.
# Look for the .env file in this file's directory
# Actual .env file (eg .env.dev) depends on value of PRW_ENV. Default to prod.
PRW_ENV = os.getenv("PRW_ENV", "prod")
ENV_FILES = {
    "dev": ".env.dev",
    "prod": ".env.prod",
}
ENV_PATH = os.path.join(os.path.dirname(__file__), ENV_FILES.get(PRW_ENV))
print(f"Using environment: {ENV_PATH}")
load_dotenv(dotenv_path=ENV_PATH)

# Update path to include pipenv in the worker user's local bin
os.environ["PATH"] = f"{os.environ['PATH']}:{pathlib.Path.home()}/.local/bin"

# Load config from env vars into constants
PRW_ENCOUNTERS_SOURCE_DIR = os.environ.get("PRW_ENCOUNTERS_SOURCE_DIR")
PRW_FINANCE_SOURCE_DIR = os.environ.get("PRW_FINANCE_SOURCE_DIR")
PRW_DB_ODBC = os.environ.get("PRW_DB_ODBC") or Secret.load("prw-db-url").get()
PRW_ID_DB_ODBC = os.environ.get("PRW_ID_DB_ODBC") or Secret.load("prw-id-db-url").get()

# Path to ../ingest/, where actual ingest subflow code is located
INGEST_CODE_ROOT = pathlib.Path(__file__).parent.parent / "ingest"

# Datamart flow deployment names to execute as part of the ingest process
# Format flows as "deployment-name/flow-name"
# TODO: Move this to a Prefect block where deployments are registered in prw_deploy.py
DATAMART_DEPLOYMENTS = [
    "prw-datamart-finance-dash/prh-dash-ingest",
]


# -----------------------------------------
# Ingest source data processes
# -----------------------------------------
@flow
async def prw_ingest_encounters(drop_tables=False):
    drop_flag = "--drop" if drop_tables else ""
    cmd = f'pipenv run python ingest_encounters.py -i "{PRW_ENCOUNTERS_SOURCE_DIR}" -o "{PRW_DB_ODBC}" --id_out "{PRW_ID_DB_ODBC}" {drop_flag}'
    return await shell_op(
        command=cmd,
        cwd=INGEST_CODE_ROOT,
    )


@flow
async def prw_ingest_finance(drop_tables=False):
    drop_flag = "--drop" if drop_tables else ""
    cmd = f'pipenv run python ingest_finance.py -i "{PRW_FINANCE_SOURCE_DIR}" -o "{PRW_DB_ODBC}" {drop_flag}'
    return await shell_op(
        command=cmd,
        cwd=INGEST_CODE_ROOT,
    )


# -----------------------------------------
# Additional transforms to source data
# -----------------------------------------
@flow
async def prw_transform_clean_encounters():
    return await shell_op(
        command=f'pipenv run python transform_clean_encounters.py -db "{PRW_DB_ODBC}"',
        cwd=INGEST_CODE_ROOT,
    )


@flow
async def prw_transform_patient_panel():
    return await shell_op(
        command=f'pipenv run python transform_patient_panel.py -db "{PRW_DB_ODBC}"',
        cwd=INGEST_CODE_ROOT,
    )


# -----------------------------------------
# Datamart ingest
# -----------------------------------------
async def datamart_ingest():
    """Ask Prefect to start all datamart flows by names defined in DATAMART_DEPLOYMENTS"""
    # Kick off flow runs for each deployment.
    # timeout=0 means return immediately without waiting for the flow to complete.
    # as_subflow=False means run the flow as a top-level flow, not as a subflow.
    for deployment_name in DATAMART_DEPLOYMENTS:
        print(f"Triggering datamart flow: {deployment_name}")
        await run_deployment(name=deployment_name, timeout=0, as_subflow=False)


# -----------------------------------------
# Main entry point / parent flow
# -----------------------------------------
def get_flow_name():
    base_name = "prw-ingest"
    env_prefix = f"{PRW_ENV}." if PRW_ENV != "prod" else ""
    return f"{env_prefix}{base_name}"


@flow(name=get_flow_name(), retries=0, retry_delay_seconds=300)
async def prw_ingest(
    run_ingest=True, run_transform=True, run_datamart=True, drop_tables=False
):
    # First, create/update the python virtual environment which is used by all subflows in ../ingest/
    # The PIPENV_IGNORE_VIRTUALENVS env var instructs pipenv to install dependencies from the Pipfile 
    # in the current directory (../ingest) into the current venv (prefect-prh-warehouse).
    await shell_op(
        command="pipenv install -v",
        env={"PIPENV_IGNORE_VIRTUALENVS": "1"},
        cwd=INGEST_CODE_ROOT
    )

    if run_ingest:
        # Run ingest subflows
        ingest_flows = [
            prw_ingest_encounters(drop_tables),
            prw_ingest_finance(drop_tables),
        ]
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


def run_as_script():
    """
    This function is used when executed directly (not via Prefect).
    It provides a CLI for running the ingest flow in different stages.
    """
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
            drop_tables=args.drop,
        )
    )


if __name__ == "__main__":
    # This module is primary executed by Prefect with prw_ingest() as the main entry point.
    # This file can also be executed directly for testing, and run_as_script() provides a CLI
    run_as_script()
