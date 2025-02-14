"""
Common utilities for Prefect flows for Ingest
"""

from prefect import task
from prefect_shell import ShellOperation


async def shell_op(command: str, env: dict[str, str] = {}, cwd: str = None):
    """
    Run a shell command and return the output. Raise an exception if the command fails.
    This mirrors prefect_shell.shell_run_command, but uses ShellOperation(), which
    correctly handles logging output to prefect.
    """
    proc = await ShellOperation(commands=[command], working_dir=cwd, env=env).trigger()
    await proc.wait_for_completion()
    if proc.return_code != 0:
        raise Exception(f"Failed, exit code {proc.return_code}")
    return await proc.fetch_result()