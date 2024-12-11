import os
import pathlib
import asyncio
import argparse
from dotenv import load_dotenv
from prefect import flow, task
from prefect_shell import ShellOperation
from prefect.runner.storage import GitRepository
from prefect_github import GitHubCredentials
from prefect.blocks.system import Secret

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
# Common
# -----------------------------------------
async def ingest_shell_op(cmds: list[str]):
    proc = await ShellOperation(working_dir=INGEST_CODE_DIR, commands=cmds).trigger()
    await proc.wait_for_completion()
    if proc.return_code != 0:
        raise Exception(f"Failed, exit code {proc.return_code}")


@task
async def pipenv_install():
    """
    Creates virtual environment for all subflows defined in ../ingest/. The virtual environment is named
    using PIPENV_CUSTOM_VENV_NAME in the ./.env so it is reused between runs.
    """
    return await ingest_shell_op(["PIPENV_IGNORE_VIRTUALENVS=1 pipenv install"])


# -----------------------------------------
# Ingest source data processes
# -----------------------------------------
@flow
async def ingest_source_encounters():
    return await ingest_shell_op(
        [
            f'pipenv run python ingest_encounters.py -i "{PRW_ENCOUNTERS_SOURCE_DIR}" -o "{PRW_DB_ODBC}" {DROP_FLAG}',
        ],
    )


@flow
async def ingest_source_finance():
    return await ingest_shell_op(
        [
            f'pipenv run python ingest_finance.py -i "{PRW_FINANCE_SOURCE_DIR}" -o "{PRW_DB_ODBC}" {DROP_FLAG}',
        ],
    )


# -----------------------------------------
# Additional transforms to source data
# -----------------------------------------
@flow
async def transform_patient_panel():
    return await ingest_shell_op(
        [f'pipenv run python transform_patient_panel.py -db "{PRW_DB_ODBC}"'],
    )


# -----------------------------------------
# Create datamarts
# -----------------------------------------
@flow
async def create_datamart_finance():
    return await ingest_shell_op(["pipenv run python ingest_finance.py"])


# -----------------------------------------
# Main entry point / parent flow
# -----------------------------------------
@flow(retries=0, retry_delay_seconds=300)
async def prh_prw_ingest():
    # First, create/update the python virtual environment which is used by all subflows in ../ingest/
    await pipenv_install()

    # Run ingest subflows
    ingest_flows = [ingest_source_encounters(), ingest_source_finance()]
    await asyncio.gather(*ingest_flows)

    # After ingest flows are complete, run transform flows, which calculate
    # additional common columns that will be used across multiple applications
    transform_flows = [transform_patient_panel()]
    await asyncio.gather(*transform_flows)

    # Lastly create datamarts for each application
    datamart_flows = [create_datamart_finance()]
    await asyncio.gather(*datamart_flows)


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

    asyncio.run(prh_prw_ingest())
