# Python Cortex Serving Client

Python Cortex Serving Client makes serving automation simple.
It is a Python wrapper around [Cortex's command-line client](https://cortex.dev) that provides garbage API collection.

- Control your Cortex cluster from Python.
- Prevent accidental charges by auto-removing deployments that exceeded a timeout.
- Supported operations: deploy, delete, get, get all.
- Supported Cortex Version: 0.17.

## How it works

After implementing your predictor project in a folder `dir`,
you can deploy it to your Cortex cluster,
and execute a prediction via a POST request.

[Working example](/integration_test/integration_test.py):
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

Each deployed API has a timeout period configured during deployment after which it definitely should not exist in the cluster anymore.
This timeout is stored in a Postgres database table.
Cortex client periodically checks currently deployed APIs and removes expired APIs from the cluster.

## Our Use Case
We use this project to automate deployment to auto-scalable AWS instances.
The deployment management is part of application-specific Flask applications,
which call to Python-Cortex-Serving-Client to command environment-dedicated Cortex cluster.

In cases where multiple environments share a single cluster, a shared Cortex database Postgres instance is required.

## Get started 
This tutorial will help you to get [the basic example](/integration_test/integration_test.py) running under 15 minutes.

Pre-requisites:
- Linux OS
- Docker
- Postgres

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
create database cortex_test;
create role cortex_test login password 'cortex_test';
grant all privileges on database cortext_test to cortex_test;
```

This example runs in a simulated local cluster in docker. 
You may be required to add your user into docker group and then re-login.

Create a Python virtual environment, install requirements `requirements.txt` and `requirements-dev.txt`, 
and run [the test script](/integration_test/integration_test.py). 

Please wait a couple minutes as Docker images of the Cortex instances need to be downloaded and cached at least once.

## Contact us
Submit an issue or a pull request if you have any problems or need an extra feature.