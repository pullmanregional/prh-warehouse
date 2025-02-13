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
    # ------------------------------------------------------------------
    # Main ingest flow for warehouse
    # ------------------------------------------------------------------
    prw_ingest_repo = GitRepository(
        url="https://github.com/pullmanregional/prh-warehouse.git",
        include_submodules=True,
    )
    flow.from_source(
        source=prw_ingest_repo,
        entrypoint="prefect/prefect_prw_ingest.py:prw_ingest",
    ).deploy(
        name="prw-ingest",
        work_pool_name="ingest",
    )

    # ------------------------------------------------------------------
    # Datamart flows
    # These flows are triggered by name by prefect_prw_ingest.py
    # ------------------------------------------------------------------
    flow.from_source(
        source="https://github.com/jonjlee-streamlit/prh-dash.git",
        entrypoint="prefect/prh-dash-ingest.py:prh_dash_ingest",
    ).to_deployment(
        name="prw-datamart-finance-dash",
    )

    # ------------------------------------------------------------------
    # Miscellaneous
    # ------------------------------------------------------------------
    # Provider schedules for staff scheduling calendar
    clinic_cal_repo = GitRepository(
        url="https://github.com/jonjlee/clinic-cal.git",
        credentials=GitHubCredentials.load("github-clinic-cal"),
    )
    flow.from_source(
        source=clinic_cal_repo,
        entrypoint="prefect/clinic-cal-epic-ingest.py:clinic_cal_epic_ingest",
    ).deploy(
        name="clinic-cal-epic-ingest",
        cron="0 7-18 * * 1-5",  # Every hour, between 07:00 AM and 06:00 PM, Monday through Friday
        work_pool_name="ingest",
    )

    # Financial dashboard data
    flow.from_source(
        source="https://github.com/jonjlee-streamlit/prh-dash.git",
        entrypoint="prefect/prh-dash-ingest.py:prh_dash_ingest",
    ).deploy(
        name="prh-dash-ingest",
        cron="0 7-18 * * *",  # Every hour, between 07:00 AM and 06:00 PM every day
        work_pool_name="ingest",
    )
