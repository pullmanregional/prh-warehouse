"""
This module / script executes the Prefect flows defined in DEPLOYMENTS.
Each flow should be stored in an external git repository.

This module is necessary because there is no public API to run a single execution
of a flow currently. However, we still use Prefect utilities to clone the code
and import the flow function.

The main entry point of the script, datamart_ingest(), is not defined as a flow,
so the flows are triggered as top level Prefect flows, not subflows,
unless datamart_ingest() is called within a flow, like prefect_prw_ingest.py does.
"""

import asyncio
import importlib
import os
import pathlib
from prefect import flow
from prefect.utilities import importtools
from prefect.runner.storage import GitRepository
from prefect_github import GitHubCredentials
from prefect_util import ingest_shell_op
from prefect_aws import AwsCredentials

EXTERNAL_CODE_DIR = pathlib.Path.home() / "repos"

# -----------------------------------------
# Datamart ingest flow definitions
# -----------------------------------------
DEPLOYMENTS = [
    flow.from_source(
        source=GitRepository(url="https://github.com/jonjlee-streamlit/prh-dash.git"),
        entrypoint="prefect/prh-dash-ingest.py:prh_dash_ingest",
    ).to_deployment(
        name="prw-finance-dash",
    )
]


# -----------------------------------------
# Flow execution helper
# -----------------------------------------
async def exec_deployment(deployment):
    """
    Accepts a Prefect deployment definition and executes a single run.
    There is currently no public API to run a flow from the deployment object,
    so we perform the steps manually:
    * Clone the code repo
    * Parse the entrypoint definition into the python file and function
    * Import the python file and call entrypoint function
    """
    print(f"Running flow {deployment.name}")

    # Use deployment.storage to clone the code repo
    deployment.storage.set_base_path(EXTERNAL_CODE_DIR)
    await deployment.storage.pull_code()
    repo_path = deployment.storage.destination

    # Set working directory to repo path
    os.chdir(repo_path)

    # Split the deployment.entrypoint into python fileand function to call
    module_name, function_name = deployment.entrypoint.split(":")

    # Convert relative python file to a module and then import it
    module = importtools.load_script_as_module(str(repo_path / module_name))

    # Call target function
    flow_fn = getattr(module, function_name)
    if asyncio.iscoroutinefunction(flow_fn):
        await flow_fn()
    else:
        flow_fn()


async def datamart_ingest():
    deployments = [exec_deployment(deployment) for deployment in DEPLOYMENTS]
    await asyncio.gather(*deployments)


if __name__ == "__main__":
    asyncio.run(datamart_ingest())
