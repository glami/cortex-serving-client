# Python-Cortex-Serving-Client

Python ML Serving Client for [Cortex.dev](https://cortex.dev) with garbage API collection.

- Control your Cortex cluster from Python. 
- Prevent accidental charges caused by forgotten APIs by setting a timeout.
- Supported operations: deploy, delete, get, get all.
- Supported Cortex Version: 0.17

Sample:
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

## Try it out
This tutorial will help you to get [the basic example](/integration_test/integration_test.py) running under 15 minutes.

### Pre-requisites
- Linux OS
- Docker
- Postgres

### How To
```bash
sudo su postgres;
psql postgres postgres;
CREATE DATABASE cortex_test;
create role cortex_test login password 'cortex_test';
GRANT ALL PRIVILEGES ON DATABASE cortext_test to cortex_test;
```

```bash
bash -c "$(curl -sS https://raw.githubusercontent.com/cortexlabs/cortex/0.17/get-cli.sh)";
```

During execution of below you may be required to add your user into docker group and then re-login.
```bash
sudo gpasswd -a $USER docker;
```

Create virtual environment, install requrements, 
and run [test script](/integration_test/integration_test.py). 
Please wait couple a minutes at first as Docker images need to be downloaded for the first time.
Main part of the code is:

