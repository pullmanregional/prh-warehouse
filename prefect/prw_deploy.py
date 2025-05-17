from prefect import flow, deploy
from prefect.runner.storage import GitRepository
from prefect_github import GitHubCredentials
from prefect.schedules import Schedule
import argparse


# cron syntax reference:
# * * * * *
# | | | | +----- day of week (0 - 6) (Sunday=0)
# | | | +------- month (1 - 12)
# | | +--------- day of month (1 - 31)
# | +----------- hour (0 - 23)
# +------------- minute (0 - 59)


def parse_arguments():
    parser = argparse.ArgumentParser(description="Deploy Prefect flows")
    parser.add_argument(
        "deployments",
        nargs="*",
        help="Specific deployment names to deploy. If not specified, all deployments will be deployed.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    deployment_names = args.deployments
    deployments = {}

    # ------------------------------------------------------------------
    # Main ingest flow for warehouse
    # ------------------------------------------------------------------
    prw_ingest_repo = GitRepository(
        url="https://github.com/pullmanregional/prh-warehouse.git",
        include_submodules=True,
    )
    deployment_name = "prw-ingest"
    deployments[deployment_name] = (
        None
        if deployment_names and deployment_name not in deployment_names
        else {
            "flow": flow.from_source(
                source=prw_ingest_repo,
                entrypoint="prefect/prw_ingest.py:prw_ingest",
            ),
            "deploy_params": {
                "name": deployment_name,
                "schedule": Schedule(cron="0 5 * * *", timezone="America/Los_Angeles"),
                "work_pool_name": "ingest",
            },
        }
    )

    # ------------------------------------------------------------------
    # Datamart flows
    # These flows are triggered by name by prw_ingest.py
    # ------------------------------------------------------------------
    # Marketing dashboard
    prh_streamlit_repo = GitRepository(
        url="https://github.com/pullmanregional/streamlit.git",
        credentials=GitHubCredentials.load("github-prh-ro"),
        include_submodules=True,
    )
    deployment_name = "prw-datamart-marketing"
    deployments[deployment_name] = (
        None
        if deployment_names and deployment_name not in deployment_names
        else {
            "flow": flow.from_source(
                source=prh_streamlit_repo,
                entrypoint="marketing/prefect/flow.py:prw_datamart_marketing",
            ),
            "deploy_params": {
                "name": deployment_name,
                "work_pool_name": "ingest",
            },
        }
    )

    # Patient panel
    deployment_name = "prw-datamart-panel"
    deployments[deployment_name] = (
        None
        if deployment_names and deployment_name not in deployment_names
        else {
            "flow": flow.from_source(
                source=prh_streamlit_repo,
                entrypoint="panel/prefect/flow.py:prw_datamart_panel",
            ),
            "deploy_params": {
                "name": deployment_name,
                "work_pool_name": "ingest",
            },
        }
    )

    # Finance dashboard
    deployment_name = "prw-datamart-finance"
    deployments[deployment_name] = (
        None
        if deployment_names and deployment_name not in deployment_names
        else {
            "flow": flow.from_source(
                source=prh_streamlit_repo,
                entrypoint="finance/prefect/flow.py:prw_datamart_finance",
            ),
            "deploy_params": {
                "name": deployment_name,
                "work_pool_name": "ingest",
            },
        }
    )

    # Residency dashboard
    deployment_name = "prw-datamart-residency"
    deployments[deployment_name] = (
        None
        if deployment_names and deployment_name not in deployment_names
        else {
            "flow": flow.from_source(
                source=prh_streamlit_repo,
                entrypoint="residency/prefect/flow.py:prw_datamart_residency",
            ),
            "deploy_params": {
                "name": deployment_name,
                "work_pool_name": "ingest",
            },
        }
    )

    # SQL reports
    prw_exporter_repo = GitRepository(
        url="https://github.com/pullmanregional/prw-exporter.git",
        credentials=GitHubCredentials.load("github-prh-ro"),
        include_submodules=True,
    )
    deployment_name = "prh-reports"
    deployments[deployment_name] = (
        None
        if deployment_names and deployment_name not in deployment_names
        else {
            "flow": flow.from_source(
                source=prw_exporter_repo,
                entrypoint="reports/prefect/flow.py:prh_reports",
            ),
            "deploy_params": {
                "name": deployment_name,
                "schedule": Schedule(cron="0 7 * * *", timezone="America/Los_Angeles"),
                "work_pool_name": "ingest",
            },
        }
    )

    # ------------------------------------------------------------------
    # Source data extraction flows
    # ------------------------------------------------------------------
    deployment_name = "prh-sources-epic"
    deployments[deployment_name] = (
        None
        if deployment_names and deployment_name not in deployment_names
        else {
            "flow": flow.from_source(
                source=prw_exporter_repo,
                entrypoint="sources/epic/prefect/flow.py:prh_sources_epic",
            ),
            "deploy_params": {
                "name": deployment_name,
                # Daily at 4am. Caboodle prod -> QA happens at ~3am.
                "schedule": Schedule(cron="0 4 * * *", timezone="America/Los_Angeles"),
                "work_pool_name": "ingest",
            },
        }
    )

    # ------------------------------------------------------------------
    # Miscellaneous
    # ------------------------------------------------------------------
    # Provider schedules for staff scheduling calendar
    clinic_cal_repo = GitRepository(
        url="https://github.com/jonjlee/clinic-cal.git",
        credentials=GitHubCredentials.load("github-clinic-cal"),
    )
    deployment_name = "clinic-cal-epic-ingest"
    deployments[deployment_name] = (
        None
        if deployment_names and deployment_name not in deployment_names
        else {
            "flow": flow.from_source(
                source=clinic_cal_repo,
                entrypoint="prefect/clinic-cal-epic-ingest.py:clinic_cal_epic_ingest",
            ),
            "deploy_params": {
                "name": deployment_name,
                "schedule": Schedule(
                    # Daily at 6am. Report is generated by prh-sources-epic at 4am.
                    cron="0 6 * * *",
                    timezone="America/Los_Angeles",
                ),
                "work_pool_name": "ingest",
            },
        }
    )

    # Deploy flows based on command line arguments
    if deployment_names:
        # Deploy only specified deployments
        for name in deployment_names:
            if name in deployments and deployments[name] is not None:
                print(f"Deploying {name}")
                deployments[name]["flow"].deploy(**deployments[name]["deploy_params"])
            else:
                print(f"Warning: Deployment '{name}' not found")
    else:
        # Deploy all flows
        for name, deployment in deployments.items():
            if deployment is not None:
                print(f"Deploying {name}")
                deployment["flow"].deploy(**deployment["deploy_params"])
