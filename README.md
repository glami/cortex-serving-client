# Cortex Serving Client

<img src="https://raw.githubusercontent.com/glami/cortex-serving-client/master/cortex-serving-client-logo-2.svg" alt="Cortex Serving Client" style="max-width: 200px">

Cortex Serving Client makes Python serving automation simple.
It is a Python wrapper around [Cortex's command-line client](https://cortex.dev) that provides garbage API collection.
Cortex has [official Python client now](https://pypi.org/project/cortex/) ([source](https://github.com/cortexlabs/cortex/blob/e22985f8516fe8db930aaecd05269da99d5e7a93/pkg/cortex/client/cortex/client.py)), but this project offers advanced features (GC, temporary deployment, timeouts) not present in the vanilla.

Main feature of this package is that you can use it on top of your codebase created for Cortex Version <= 0.34, meaning that:
 - deployment directory is automatically zipped and uploaded to S3 bucket
 - we prepared a base docker image that downloads this zipped code, unzips it and runs it in an Uvicorn worker
 - => you can simply deploy your `PythonPredictor` using Cortex 0.40 without having to wrap it inside your own docker image 

Additional features:
 - Automate your Cortex AWS cluster from Python.
 - Prevent accidental charges by auto-removing deployments that exceeded a timeout.
 - Execute operations: deploy, delete, get, get all.
 - Stream remote logs into the local log with thread name set to the API name.
 - Supported Cortex Version: 0.40.0 (See requirements.txt)

Here is [a video about the package (version for Cortex 0.33 = before the big changes)](https://youtu.be/aU95dBAspr0?t=510).

## How Does It Work?

After implementing your predictor module in a folder (see `example/dummy_dir`),
you can deploy it to your Cortex cluster,
and execute a prediction via a POST request.

Here is [a video of the demo below](https://youtu.be/aU95dBAspr0?t=1261).

### Working Example
Below is a snippet from [example.py](/example/example.py):

The deployment dict has these additional fields compared to Cortex docs:
 - `"project_name":<string>` in deployment root
   - name of the project, zipped source code is going to be uploaded to S3 path: `<project_name>/<api_name>.zip`
 - `predictor_path`: Module containing your predictor: `cls.__module__` = e.g. `predictors.my_predictor`
 - Optional `predictor_class_name`: `cls.__name__` of your predictor class, default is `PythonPredictor`
 - `"config":<dict>` in `container` specification
   - config dict that will be saved to `predictor_config.json` in the root of deployment dir
   - this file can then be loaded in `main.py` and passed to the PythonPredictor constructor = can be seen in `resources/main.py`


```python
deployment = {
        "name": "dummy-a",
        "project_name": "test",
        "kind": "RealtimeAPI",
        "predictor_path": "dummy_predictor",
        "pod": {
            "containers": [
                {
                    "config": {"geo": "cz", "model_name": "CoolModel", "version": "000000-000000"},
                    "env": {
                        "SECRET_ENV_VAR": "secret",
                    },
                    "compute": {"cpu": '200m', "mem": f"{0.1}Gi"},
                }
            ],
        },
    }

# Deploy
with cortex.deploy_temporarily(
        deployment,
        deploy_dir="dummy_dir",
        api_timeout_sec=30 * 60,
        verbose=True,
) as get_result:
    # Predict
    response = post(get_result.endpoint, json={}).json()
```

### Required changes for projects using Cortex version <= 0.34
 - optionally add `main.py` to the root of your cortex deployment folder
   - if there is no `main.py` in the root of the deployment folder, the default one from `resources/main.py` will be used
 - restructure your deployment dict to look like the one in `example.py`

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
Follow instructions below to configure local database,
or configure cluster database,
and re-configure db in [the example script](/example/example.py).

```bash
sudo su postgres;
psql postgres postgres;
create database cortex_test;
create role cortex_test login password 'cortex_test';
grant all privileges on database cortex_test to cortex_test;
```

You may need to configure also
```bash

vi /etc/postgresql/11/main/pg_hba.conf
# change a matching line into following to allow localhost network access
# host    all             all             127.0.0.1/32            trust

sudo systemctl restart postgresql;
```

### Install Cortex
Supported [Cortex.dev](https://cortex.dev) version is a Python dependency version installed through `requirements.txt`.

Cortex requires having [Docker](https://docs.docker.com/get-docker/) installed on your machine.

### Deploy Your First Model

The deployment and prediction example resides in [the example script](/example/example.py).
Make sure you have created a virtual environment, and installed requirements in `requirements.txt` and `requirements-dev.txt`,
before you execute it.

## Contact Us
[Submit an issue](https://github.com/glami/cortex-serving-client/issues) or a pull request if you have any problems or need an extra feature.
