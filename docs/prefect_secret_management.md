# Secrets

Third party secrets, like API tokens, are stored in encrypted Prefect Blocks on the server.

* Official tutorial at https://docs.prefect.io/3.0/resources/secrets
* The preferred way to update a secret for our deployment is interactively via the web UI: Dashboard > Configuration > Blocks
* There are a couple different block types that we use for secrets including:
  * GitHub credentials
  * CloudFlare R2 credentials (S3 compatible, uses boto3 client internally)

# Development

In order to use the Blocks defined above in code, you will need to install the corresponding client library. For example:
```
pip install "prefect[github,aws]"
```

Remember to set the Python interpreter in VSCode after doing `pip install`, since this project doesn't use pipenv yet.

Then you can import and use the blocks to retrieve a secret by name. For example, see `prefect/prw_deploy.py`:
```
from prefect import flow, deploy
from prefect.runner.storage import GitRepository
from prefect_github import GitHubCredentials

flow.from_source(
    source=GitRepository(
        url="https://github.com/jonjlee/clinic-cal.git",
        credentials=GitHubCredentials.load("github-clinic-cal"),
    ))
```

For secrets used by specific flows, which are checked into separate repos, remember to `pip install` the block there as well. Look at the docs for the `pip install prefect[github]` style statements, which are the most current (rather than `pip install prefect_github`). For example, see https://docs.prefect.io/integrations/prefect-github/index.