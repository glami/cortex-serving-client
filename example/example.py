import logging

logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(threadName)-10s : %(name)s : %(message)s", level=logging.INFO,
)

logger = logging.getLogger(__name__)

from requests import post
from cortex_serving_client.cortex_client import get_cortex_client_instance


# Instantiate Cortex Client
cortex = get_cortex_client_instance(
    cortex_env='cortex-serving-client-test',
    pg_user='cortex_test',
    pg_password='cortex_test',
    pg_db='cortex_test',
    )

# Deployment config
deployment = {
        "name": "dummy-a",
        "project_name": "test",
        "predictor_path": "dummy_predictor",
        "kind": "RealtimeAPI",
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
        "autoscaling": {
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
    response = post(get_result.endpoint, json={"question": "Do you love ML?"}).json()

# Print the response
logger.info(f"API Response: {response}")
# assert result['yes']
