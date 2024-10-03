# Overview

Here is a diagram to understand the relationship between Prefect Flows, Deployments, and Workers from their 2.10.15 documentation (https://docs-2.prefect.io/2.10.15/concepts/work-pools/):

![Prefect Deployment End-to-End](prefect-flow-deployment-end-to-end.png "Prefect Deployment End-to-End")

# Prefect Flows

This are the individual processes that do the ingest. Typically, they are simple files that wrap a shell process. Key points:

* The Prefect flows are defined in individual projects, such as `prh-dash` and `clinic-cal`.
* Each project has a `prefect` dir containing the defined flows
* Each project has a `prefect/.env` file that provides configuration parameters that will take effect in production
* To run a flow locally, export the appropriate env vars prior to executing it. For example:
  ```
  cd clinic-cal
  CLINIC_CAL_EPIC_SOURCE_FILE=/path/to/local.xlsx python prefect/clinic-cal-epic-ingest.py
  ```

### Special cases:

For flows that need `pipenv` to install dependencies, like `prh-dash`:

* All imports by the **flow script itself** need to be pip installed in the Worker's environment. For example:
  ```
  # In prh-dash/prefect/prh-dash-ingest.py:

  from prefect_shell import ShellOperation
  from prefect_aws import AwsCredentials, S3Bucket
  ```

  Before this will execute, do this on the Worker VM:
  ```
  su - deploy
  pip install "prefect[shell,aws]"
  ```
* Then, the actual code, like `prh-dash/ingest.py`, that is called in `ShellOperation` requires `pipenv install` to run. This is the first step in `prh-dash-ingest.py`:
  ```
  with ShellOperation(
    commands=[
        "pipenv install",
        ...
  ```

  If we don't reuse a single virtualenv for all runs, the disk will be littered with a >800MB venv for each run. To avoid, set the venv name:

  ```
  # In prh-dash/prefect/.env:

  PIPENV_CUSTOM_VENV_NAME=prefect-prh-dash
  ```
  This makes all `ShellOperation` `pipenv ...` statements use this specific venv. 


# Prefect Deployments

* These are named groups of flows that are defined by python files in `prh-warehouse/prefect`
* Deployments can be seen as *code as configuration*. For example, see the contents of `prefect/deploy_ingest_workflows.py` to see the source repository and cron schedule for the `clinic-cal-epic-ingest` flow.
* Each deployment can have a set schedule for executing and be assigned to a **Prefect Work Pool**
* A deployment can be manually started as well using:
  ```
  prefect deployment run <deployment name>/<flow name>
  ``` 
* Multiple flows can be included in a single deployment

# Prefect Work Pool and Worker

Starting Prefect Worker creates an actual process that polls the Prefect Server for work to do, and spawns subprocesses to run flows. Each deployment (see above) is assigned to a work pool.

Start a long running worker in the pool to activate it:
```
prefect work-pool create ingest
prefect worker start --pool "ingest"
```

# Prefect Worker Systemd Service

In production our worker runs as a systemd daemon by defining `/etc/systemd/system/prefect-worker.service`. The worker will by default concurrently spawn one subprocess per CPU to run any scheduled flows in parallel.

Install this service to `/etc/systemd/system/prefect-worker-ingest.service`

Set permissions:
```
sudo chmod 644 /etc/systemd/system/prefect-worker-ingest.service
```

Then start the service with:
````
sudo systemctl daemon-reload
sudo systemctl enable prefect-worker-ingest.service
sudo systemctl start prefect-worker-ingest.service
sudo systemctl status prefect-worker-ingest.service
```

Follow logs with:
```
journalctl -fu prefect-worker-ingest.service
```
