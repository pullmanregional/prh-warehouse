from prefect import flow, deploy
from prefect.runner.storage import GitRepository
from prefect_github import GitHubCredentials
from dotenv import load_dotenv

# Load environment from .env file, does not overwrite existing env variables
load_dotenv()

if __name__ == "__main__":
    deploy(
        flow.from_source(
            source=GitRepository(
                url="https://github.com/jonjlee/clinic-cal.git",
                credentials=GitHubCredentials.load("github-clinic-cal"),
            ),
            entrypoint="prefect/clinic-cal-epic-ingest.py:clinic_cal_epic_ingest",
        ).to_deployment(
            "clinic-cal-epic-ingest",
            cron="*/15 * * * *",
        ),
        # All deployments are run by workers in the "ingest" pool
        work_pool_name="ingest",
    )
