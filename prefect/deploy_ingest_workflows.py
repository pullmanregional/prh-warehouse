from prefect import flow, deploy
from prefect.runner.storage import GitRepository
from prefect_github import GitHubCredentials

# cron syntax reference:
# * * * * *
# | | | | +----- day of week (0 - 6) (Sunday=0)
# | | | +------- month (1 - 12)
# | | +--------- day of month (1 - 31)
# | +----------- hour (0 - 23)
# +------------- minute (0 - 59)

if __name__ == "__main__":
    flows = []

    # Provider schedules for staff scheduling calendar
    clinic_cal_repo = GitRepository(
        url="https://github.com/jonjlee/clinic-cal.git",
        credentials=GitHubCredentials.load("github-clinic-cal"),
    )
    flows.append(
        flow.from_source(
            source=clinic_cal_repo,
            entrypoint="prefect/clinic-cal-epic-ingest.py:clinic_cal_epic_ingest",
        ).to_deployment(
            "clinic-cal-epic-ingest",
            cron="0 7-18 * * 1-5",  # Every hour, between 07:00 AM and 06:00 PM, Monday through Friday
        )
    )

    # Financial dashboard data
    prh_dash_repo = GitRepository(
        url="https://github.com/jonjlee-streamlit/prh-dash.git"
    )
    flows.append(
        flow.from_source(
            source=prh_dash_repo,
            entrypoint="prefect/prh-dash-ingest.py:prh_dash_ingest",
        ).to_deployment(
            "prh-dash-ingest",
            cron="0 7-18 * * *",  # Every hour, between 07:00 AM and 06:00 PM every day
        )
    )

    # Financial dashboard data
    prh_warehouse_repo = GitRepository(
        url="https://github.com/pullmanregional/prh-warehouse.git"
    )
    flows.append(
        flow.from_source(
            source=prh_warehouse_repo,
            entrypoint="prefect/prh_prw_ingest.py:prh_prw_ingest",
        ).to_deployment(
            "prh-prw-ingest",
            cron="0 9 * * *",  # Daily at 09:00 AM
        )
    )

    # Deploy all flows to run by workers in the "ingest" pool
    deploy(*flows, work_pool_name="ingest")
