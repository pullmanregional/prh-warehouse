from prefect import flow, deploy
from prefect.runner.storage import GitRepository
from prefect_github import GitHubCredentials
from prefect.schedules import Schedule


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
        entrypoint="prefect/prw_ingest.py:prw_ingest",
    ).deploy(
        name="prw-ingest",
        # Daily at 5:00 AM
        schedule=Schedule(cron="0 5 * * *", timezone="America/Los_Angeles"),
        work_pool_name="ingest",
    )

    # ------------------------------------------------------------------
    # Datamart flows
    # These flows are triggered by name by prw_ingest.py
    # ------------------------------------------------------------------
    # Finance dashboard
    flow.from_source(
        source="https://github.com/jonjlee-streamlit/prh-dash.git",
        entrypoint="prefect/flow.py:prh_datamart_finance",
    ).deploy(
        name="prw-datamart-finance",
        work_pool_name="ingest",
    )

    # Marketing dashboard
    prh_streamlit_repo = GitRepository(
        url="https://github.com/pullmanregional/streamlit.git",
        credentials=GitHubCredentials.load("github-prh-ro"),
        include_submodules=True,
    )
    flow.from_source(
        source=prh_streamlit_repo,
        entrypoint="marketing/prefect/flow.py:prw_datamart_marketing",
    ).deploy(
        name="prw-datamart-marketing",
        work_pool_name="ingest",
    )

    # Patient panel
    flow.from_source(
        source=prh_streamlit_repo,
        entrypoint="panel/prefect/flow.py:prw_datamart_panel",
    ).deploy(
        name="prw-datamart-panel",
        work_pool_name="ingest",
    )


    # SQL reports
    prw_exporter_repo = GitRepository(
        url="https://github.com/pullmanregional/prw-exporter.git",
        credentials=GitHubCredentials.load("github-prh-ro"),
        include_submodules=True,
    )
    flow.from_source(
        source=prw_exporter_repo,
        entrypoint="reports/prefect/flow.py:prh_reports",
    ).deploy(
        name="prh-reports",
        # Daily at 7:00 AM
        schedule=Schedule(cron="0 7 * * *", timezone="America/Los_Angeles"),
        work_pool_name="ingest",
    )

    # ------------------------------------------------------------------
    # Source data extraction flows
    # ------------------------------------------------------------------
    flow.from_source(
        source=prw_exporter_repo,
        entrypoint="sources/epic/prefect/flow.py:prh_sources_epic",
    ).deploy(
        name="prh-sources-epic",
        # At 2:00 AM every day
        schedule=Schedule(cron="0 2 * * *", timezone="America/Los_Angeles"),
        work_pool_name="ingest",
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
        # Every hour, between 07:00 AM and 06:00 PM, Monday through Friday
        schedule=Schedule(cron="0 7-18 * * 1-5", timezone="America/Los_Angeles"),
        work_pool_name="ingest",
    )

    # Financial dashboard data
    flow.from_source(
        source="https://github.com/jonjlee-streamlit/prh-dash.git",
        entrypoint="prefect/prh-dash-ingest.py:prh_dash_ingest",
    ).deploy(
        name="prh-dash-ingest",
        # Every hour, between 07:00 AM and 06:00 PM every day
        schedule=Schedule(cron="0 7-18 * * *", timezone="America/Los_Angeles"),
        work_pool_name="ingest",
    )
