import os
from pathlib import Path
from prefect import flow, deploy
from prefect.runner.storage import GitRepository
from prefect_github import GitHubCredentials
from prefect_github import GitHubCredentials
from dotenv import load_dotenv

# Load environment from .env file, does not overwrite existing env variables
load_dotenv()
PREFECT_DATA_HOME = Path(os.environ.get("PREFECT_DATA_HOME", Path.cwd()))

if __name__ == "__main__":
    flows = []

    # Provider schedules for staff scheduling calendar
    clinic_cal_repo = GitRepository(
        url="https://github.com/jonjlee/clinic-cal.git",
        credentials=GitHubCredentials.load("github-clinic-cal"),
    )
    # Sets destination to checkout repo to
    clinic_cal_repo.set_base_path(PREFECT_DATA_HOME)
    flows.append(
        flow.from_source(
            source=clinic_cal_repo,
            entrypoint="prefect/clinic-cal-epic-ingest.py:clinic_cal_epic_ingest",
        ).to_deployment(
            "clinic-cal-epic-ingest",
            cron="*/15 * * * *",
        )
    )

    # Financial dashboard data
    prh_dash_repo = GitRepository(
        url="https://github.com/jonjlee-streamlit/prh-dash.git"
    )
    prh_dash_repo.set_base_path(PREFECT_DATA_HOME)
    flows.append(
        flow.from_source(
            source=prh_dash_repo,
            entrypoint="prefect/prh-dash-ingest.py:prh_dash_ingest",
        ).to_deployment(
            "prh-dash-ingest",
            cron="*/15 * * * *",
        )
    )

    # Deploy all flows to run by workers in the "ingest" pool
    deploy(*flows, work_pool_name="ingest")
