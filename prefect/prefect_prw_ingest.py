import os
import pathlib
from dotenv import load_dotenv
from prefect import flow, task
from prefect_shell import ShellOperation
from prefect.blocks.system import Secret

# Load environment from .env file, does not overwrite existing env variables
load_dotenv()

# Load config from env vars
PRW_SOURCE_DIR = os.environ.get("PRW_SOURCE_DIR")

# Update path to include worker user's local bin
os.environ["PATH"] = f"{os.environ['PATH']}:{pathlib.Path.home()}/.local/bin"


@task
def run_task():
    db_url = Secret.load("prw-db-url").get()

    with ShellOperation(
        commands=[
            "pipenv install",
            f'pipenv run python prefect/prefect_prw_ingest.py -i "{PRW_SOURCE_DIR}" -o "{db_url}"',
        ],
        stream_output=True,
    ) as op:
        proc = op.trigger()
        proc.wait_for_completion()
        if proc.return_code != 0:
            raise Exception(f"Failed, exit code {proc.return_code}")


@flow(retries=0, retry_delay_seconds=300)
def prw_ingest():
    run_task()


if __name__ == "__main__":
    prw_ingest()
