"""
Common utilities for Prefect flows for Ingest
"""

from prefect import task
from prefect_shell import ShellOperation


async def ingest_shell_op(cmds: list[str], working_dir: str = None):
    proc = await ShellOperation(working_dir=working_dir, commands=cmds).trigger()
    await proc.wait_for_completion()
    if proc.return_code != 0:
        raise Exception(f"Failed, exit code {proc.return_code}")


@task
async def pipenv_install_task(working_dir: str = None):
    """
    Creates virtual environment for all subflows defined in ../ingest/. The virtual environment is named
    using PIPENV_CUSTOM_VENV_NAME in the ./.env so it is reused between runs.
    """
    return await ingest_shell_op(
        ["PIPENV_IGNORE_VIRTUALENVS=1 pipenv install"], working_dir=working_dir
    )
