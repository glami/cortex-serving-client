# Python-Cortex-Serving-Client

Python ML Serving Client for [Cortex.dev](https://cortex.dev) with garbage API collection.

- Control your Cortex cluster from Python. 
- Prevent accidental charges caused by forgotten APIs by setting a timeout.
- Supported operations: deploy, delete, get, get all.
- Supported Cortex Version: 0.17

[Sample](/integration_test/integration_test.py):
```python
deployment = dict(
    name='dummy-api',
    predictor=dict(
        type='python',
        path='dummy_predictor.py',
    ),
    compute=dict(
        cpu=1,
    )
)

with cortex.deploy_temporarily(
        deployment,
        dir="./",
        api_timeout_sec=10 * 60,
        print_logs=True,
) as get_result:
    result = post(get_result.endpoint, json={}).json()

assert result['yes']
```

### Garbage API Collection
Garbage API collection auto-removes forgotten APIs to reduce costs.

Each deployed API a timeout configured during deployment when it definitely should not exist in the cluster anymore.
This timeout is stored in a Postgres database table.
Cortex client periodically checks currently deployed APIs and removes expired APIs from the cluster.

## Our Use Case
We use this project to automate deployment to auto-scalable AWS instances.
The deployment management is part of application-specific Flask applications,
which call to Python-Cortex-Serving-Client to command environment-dedicated Cortex cluster.

In cases where multiple environments share single cluster, a shared Cortex database Postgres instance is required.

## Try it out
This tutorial will help you to get [the basic example](/integration_test/integration_test.py) running under 15 minutes.

### Pre-requisites
- Linux OS
- Docker
- Postgres

### Example

Install supported [Cortex.dev](https://cortex.dev) version:
```bash
bash -c "$(curl -sS https://raw.githubusercontent.com/cortexlabs/cortex/0.17/get-cli.sh)";
```


Follow below instuctions to configure local database,
or configure cluster database,
and re-configure db in [the test script](/integration_test/integration_test.py). 

```bash
sudo su postgres;
psql postgres postgres;
CREATE DATABASE cortex_test;
create role cortex_test login password 'cortex_test';
GRANT ALL PRIVILEGES ON DATABASE cortext_test to cortex_test;
```

This example runs in a simulated local cluster in docker. 
You may be required to add your user into docker group and then re-login.

Create a Python virtual environment, install requirements `requirements.txt` and `requirements-dev.txt`, 
and run [the test script](/integration_test/integration_test.py). 

Please wait couple a minutes as Docker images of the Cortex instances need to be downloaded and cached at least once.

