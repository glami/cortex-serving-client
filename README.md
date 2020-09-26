# Cortex Serving Client

<img src="https://raw.githubusercontent.com/glami/cortex-serving-client/master/cortex-serving-client-logo-2.svg" alt="Cortex Serving Client" style="max-width: 200px">

Cortex Serving Client makes Python serving automation simple.
It is a Python wrapper around [Cortex's command-line client](https://cortex.dev) that provides garbage API collection. 

- Automate your Cortex AWS cluster from Python.
- Prevent accidental charges by auto-removing deployments that exceeded a timeout.
- Execute operations: deploy, delete, get, get all.
- Stream remote logs into the local log with thread name set to the API name.
- Supported Cortex Version: 0.18

Here is [a video about the package](https://youtu.be/aU95dBAspr0?t=510).

## How It Works?

After implementing your predictor module in a folder (see `dummy_dir` in below),
you can deploy it to your Cortex cluster,
and execute a prediction via a POST request.

Here is [a video of the demo below](https://youtu.be/aU95dBAspr0?t=1261).

### Working Example
Below is a snippet from [example.py](/example/example.py):
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
        dir="dummy_dir",
        api_timeout_sec=10 * 60,
        print_logs=True,
) as get_result:
    result = post(get_result.endpoint, json={}).json()
```

### Garbage API Collection
Garbage API collection auto-removes forgotten APIs to reduce costs.

Each deployed API has a timeout period configured during deployment after which it definitely should not exist in the cluster anymore.
This timeout is stored in a Postgres database table.
Cortex client periodically checks currently deployed APIs and removes expired APIs from the cluster.

### Can You Rollback?
How do you deal with new model failure in production?
Do you have the ability to return to your model's previous working version?
There is no generic solution for everybody.
But you can implement the best suiting your needs using the Python API for Cortex.
Having a plan B is a good idea.

## Our Use Case
We use this project to automate deployment to auto-scalable AWS instances.
The deployment management is part of application-specific Flask applications,
which call to Python-Cortex-Serving-Client to command environment-dedicated Cortex cluster.

In cases where multiple environments share a single cluster, a shared Cortex database Postgres instance is required.

Read more about our use case in [Cortex Client release blog post](https://medium.com/@aiteamglami/serve-your-ml-models-in-aws-using-python-9908a4127a13).
Or you can [watch a video about our use case](https://youtu.be/aU95dBAspr0?t=1164).

## Get Started
This tutorial will help you to get [the basic example](/example/example.py) running under 15 minutes.

### Pre-requisites
- Linux OS
- Docker
- Postgres



### Setup Database
Follow below instuctions to configure local database,
or configure cluster database,
and re-configure db in [the example script](/example/example.py). 

```bash
sudo su postgres;
psql postgres postgres;
create database cortex_test;
create role cortex_test login password 'cortex_test';
grant all privileges on database cortext_test to cortex_test;
```

### Install Cortex
Install supported [Cortex.dev](https://cortex.dev) version:
```bash
bash -c "$(curl -sS https://raw.githubusercontent.com/cortexlabs/cortex/0.18/get-cli.sh)";
```

This example runs in a simulated local cluster in local docker. To execute it in an AWS cluster, follow [this Cortex tutorial](https://docs.cortex.dev/install#running-at-scale-on-aws),
and then change `cortex_env` from `'local'` with `'aws'` in the [the test script](/example/example.py).
You may be required to add your user into docker group and then re-login.

### Deploy Your First Model

The deployment and prediction example resides in [the example script](/example/example.py).
Make sure you have created a virtual environment, and installed requirements in `requirements.txt` and `requirements-dev.txt`, 
before you execute it. Please be ready to wait a couple of minutes during the first run as Cortex Docker instance images need to be downloaded and cached.

## Contact Us
[Submit an issue](https://github.com/glami/cortex-serving-client/issues) or a pull request if you have any problems or need an extra feature.
