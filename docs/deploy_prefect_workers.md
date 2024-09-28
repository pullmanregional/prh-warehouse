# Prefect Flows

This are the individual processes that do the ingest. Typically, they are simple files that wrap a shell process.. Key points:

* The Prefect flows are defined in individual projects, such as `prh-dash` and `clinic-cal`.
* Each project has a `prefect` dir containing the defined flows
* Each project has a `prefect/.env` file that provides configuration parameters that will take effect in production
* To run a flow locally, export the appropriate env vars prior to executing it. For example:
  ```
  cd clinic-cal
  CLINIC_CAL_EPIC_SOURCE_FILE=/path/to/local.xlsx python prefect/clinic-cal-epic-ingest.py
  ```

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

This spawns an actual process that polls the Prefect Server for work to do, and spawns subprocesses to run flows. Each deployment (see above) is assigned to a work pool.

Start a long running worker in the pool to activate it:
```
prefect work-pool create ingest
prefect worker start --pool "ingest"
```

We start our worker as a systemd process by defining `/etc/systemd/system/prefect-worker.service`. The worker will by default concurrently spawn one subprocess per CPU to run any scheduled flows in parallel.

