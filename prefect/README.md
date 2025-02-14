This subproject contains definitions for Prefect workflows and their deployments for the PRH Data Warehouse.

See https://orion-docs.prefect.io/latest/concepts/ for definitions.

- Prefect deployments define the schedule for the Prefect server to execute Prefect flows. Think of:

  - **Flows** as processes to be executed
  - **Deployments** as the cron schedule for those processes

  `prw_deploy.py` contains the deployment definitions. Running it will update the schedules on the Prefect server, each of which is identified by a flow name in the script.

- The main Prefect flow is `prw_ingest.py`, which contains a number of subflows.
  - It is run by the Prefect Worker process, which polls the Prefect server for flows to execute.
  - Each subflow executes a different ETL process using shell.
