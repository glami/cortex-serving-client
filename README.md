# Python-Cortex-Serving-Client

Python ML Serving Client for [Cortex.dev](https://cortex.dev) with garbage API collection.

- Control your Cortex cluster from Python. 
- Prevent accidental charges caused by forgotten APIs by setting a timeout.

Supported operations: deploy, delete, get, get all.

## TODO: Example

```python
import os
from requests import post
from cortex_serving_client.cortex_client import CortexClient
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import NamedTupleCursor

deployment = dict(
    name='text-generator',
    predictor=dict(
        type='python',
        path='predictor.py',
        model='s3://cortex-examples/tensorflow/text-generator/gpt-2/124M',
    ),
    compute=dict(
        cpu=1,
        gpu=1,
    )
)

cortex = CortexClient(
    ThreadedConnectionPool(
        1,
        3,
        user=os.environ.get("POSTGRES_USERNAME"),
        password=os.environ.get("POSTGRES_PASSWORD"),
        host=os.environ.get("POSTGRES_HOSTNAME"),
        port=os.environ.get("POSTGRES_PORT"),
        database=os.environ.get("POSTGRES_DATABASE"),
        cursor_factory=NamedTupleCursor,
    ),
    cortex_env="local",
)

with cortex.deploy_temporarily(
    deployment,
    dir="text-generator",
    api_timeout_sec=10 * 60,
    print_logs=True,
) as get_result:
    stats = post(get_result.endpoint, json={})).json()
```
