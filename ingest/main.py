"""
Main ingest workflow for the PRH data warehouse. This flow orchestrates the ingestion of data from various
source systems into the warehouse.

The flow is executed via the ingest.yml workflow. It:
1. Sets up the Python environment using uv
2. Executes individual ingest scripts for each data source
3. Triggers datamart refresh flows after ingestion is complete

Environment configuration is loaded from .env.* files based on PRW_ENV setting (defaults to prod).
Datamart flows to execute are defined in the DATAMART_WORKFLOWS constant
"""

import os
import pathlib
import asyncio
import argparse
from prw_common.db_utils import mask_conn_pw

# Load config from env vars into constants. The caller needs to preload these by running `source .env.${PRW_ENV}`.
PRW_EPIC_SOURCE_DIR = os.environ.get("PRW_EPIC_SOURCE_DIR")
PRW_CHARGES_SOURCE_DIR = os.environ.get("PRW_CHARGES_SOURCE_DIR")
PRW_FINANCE_SOURCE_DIR = os.environ.get("PRW_FINANCE_SOURCE_DIR")
PRW_RVU_MAPPING_SOURCE_DIR = os.environ.get("PRW_RVU_MAPPING_SOURCE_DIR")
PRW_CONN = os.environ.get("PRW_CONN")
PRW_ID_CONN = os.environ.get("PRW_ID_CONN")

# Path to ../ingest/, where actual ingest subflow code is located
INGEST_CODE_ROOT = pathlib.Path(__file__).parent.parent / "ingest"


# -----------------------------------------
# Main ingest pipeline
# -----------------------------------------
async def run_pipeline(run_ingest=True, run_transform=True):
    # Create/update the venv which is used by all scripts in ../ingest/
    await shell_op(
        cmd="uv sync",
        cwd=INGEST_CODE_ROOT,
        cmd_name="uv_sync"
    )

    if run_ingest:
        # Run ingest subflows
        # First ingest patients which creates MRN -> PRW ID mapping
        await ingest_patients()

        # Other
        await run_parallel(
            ingest_encounters,
            ingest_finance,
            ingest_imaging,
            ingest_notes,
            ingest_charges,
        )

    if run_transform:
        # After ingest flows are complete, run transform flows, which calculate
        # additional common columns that will be used across multiple applications
        await run_parallel(transform_patient_panel)


# -----------------------------------------
# Ingest source data processes
# -----------------------------------------
async def ingest_patients():
    cmd = f'uv run python ingest_patients.py -i "{PRW_EPIC_SOURCE_DIR}" --prw "{PRW_CONN}" --prwid "{PRW_ID_CONN}"'
    return await shell_op(
        cmd=cmd,
        cwd=INGEST_CODE_ROOT,
        cmd_name="ingest_patients",
    )


async def ingest_encounters():
    cmd = f'uv run python ingest_encounters.py -i "{PRW_EPIC_SOURCE_DIR}" --prw "{PRW_CONN}" --prwid "{PRW_ID_CONN}"'
    return await shell_op(
        cmd=cmd,
        cwd=INGEST_CODE_ROOT,
        cmd_name="ingest_encounters",
    )


async def ingest_notes():
    cmd = f'uv run python ingest_notes.py -i "{PRW_EPIC_SOURCE_DIR}" --prw "{PRW_CONN}" --prwid "{PRW_ID_CONN}"'
    return await shell_op(
        cmd=cmd,
        cwd=INGEST_CODE_ROOT,
        cmd_name="ingest_notes",
    )


async def ingest_charges():
    cmd = f'uv run python ingest_charges.py -i "{PRW_CHARGES_SOURCE_DIR}" --prw "{PRW_CONN}" --prwid "{PRW_ID_CONN}" --rvu-mapping "{PRW_RVU_MAPPING_SOURCE_DIR}"'
    return await shell_op(
        cmd=cmd,
        cwd=INGEST_CODE_ROOT,
        cmd_name="ingest_charges",
    )


async def ingest_imaging():
    cmd = f'uv run python ingest_imaging.py -i "{PRW_EPIC_SOURCE_DIR}" --prw "{PRW_CONN}"'
    return await shell_op(
        cmd=cmd,
        cwd=INGEST_CODE_ROOT,
        cmd_name="ingest_imaging",
    )


async def ingest_finance():
    cmd = f'uv run python ingest_finance.py -i "{PRW_FINANCE_SOURCE_DIR}" --prw "{PRW_CONN}" --epic-in "{PRW_EPIC_SOURCE_DIR}"'
    return await shell_op(
        cmd=cmd,
        cwd=INGEST_CODE_ROOT,
        cmd_name="ingest_finance",
    )


# -----------------------------------------
# Additional transforms to source data
# -----------------------------------------
async def transform_patient_panel():
    return await shell_op(
        cmd=f'uv run python transform_patient_panel.py --prw "{PRW_CONN}"',
        cwd=INGEST_CODE_ROOT,
        cmd_name="transform_patient_panel",
    )


# -----------------------------------------
# Utility functions
# -----------------------------------------
async def run_parallel(*fns, max_parallel=3):
    """Run async functions in parallel, but limit concurrency to max_parallel"""
    sem = asyncio.Semaphore(max_parallel)

    async def call_with_semaphore(fn):
        async with sem:
            return await fn()

    return await asyncio.gather(*[call_with_semaphore(fn) for fn in fns])


async def shell_op(cmd, env=None, cwd=None, cmd_name="") -> int:
    """
    Run a shell command and return the result. Output from subprocesses
    is combined into main process stdout.
    """
    prefix = f"({cmd_name}) " if cmd_name else ""
    print(f"{prefix}Running command: {mask_conn_pw(cmd)}")

    # Pass current environment into subprocess, and add env if provided
    subprocess_env = os.environ.copy()
    subprocess_env.update(env or {})
    process = await asyncio.create_subprocess_shell(
        cmd,
        env=subprocess_env,
        cwd=cwd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    # Prefix output with process name
    async def prefix_output(stream):
        while line := await stream.readline():
            print(f"{prefix}{line.decode().rstrip()}")

    await asyncio.gather(
        prefix_output(process.stdout),
        prefix_output(process.stderr),
    )

    exit_code = await process.wait()
    if exit_code != 0:
        raise Exception(f"'{cmd_name}' failed with exit code {exit_code}")
    return exit_code


# -----------------------------------------
# Main entry point
# -----------------------------------------
def main():
    """
    This function is used when executed directly (not via GH Actions).
    It provides a CLI for running the ingest flow in different stages.
    """
    parser = argparse.ArgumentParser(
        description="Main source data ingest for PRH warehouse."
    )
    parser.add_argument(
        "--ingest",
        action="store_true",
        help="Ingest data. Ingest, transform stages will run if none are set.",
    )
    parser.add_argument(
        "--transform",
        action="store_true",
        help="Run transforms. Ingest, transform stages will run if none are set.",
    )

    args = parser.parse_args()

    # Determine which subflows to run based on command line args
    run_all = not args.ingest and not args.transform
    run_ingest = run_all or args.ingest
    run_transform = run_all or args.transform

    # Run the main ingest flow
    asyncio.run(
        run_pipeline(
            run_ingest=run_ingest,
            run_transform=run_transform,
        )
    )


if __name__ == "__main__":
    main()
