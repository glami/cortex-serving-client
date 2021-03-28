# Basic logging config
import logging
logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(threadName)-10s : %(name)s : %(message)s", level=logging.INFO,
)

from requests import post
from cortex_serving_client.cortex_client import get_cortex_client_instance


# Instantiate Cortex Client
cortex = get_cortex_client_instance(
    cortex_env='local',
    pg_user='cortex_test',
    pg_password='cortex_test',
    pg_db='cortex_test',
    )


# Deployment config
deployment = dict(
    name='dummy-api',
    kind='RealtimeAPI',
    predictor=dict(
        type='python',
        path='dummy_predictor.py',
    ),
    compute=dict(
        cpu='290m',
    )
)

# Deploy
with cortex.deploy_temporarily(
        deployment,
        dir="dummy_dir",
        api_timeout_sec=30 * 60,
        print_logs=True,
) as get_result:
    # Predict
    result = post(get_result.endpoint, json={"question": "Do you love ML?"}).json()

# Check the response
assert result['yes']
print(result)
