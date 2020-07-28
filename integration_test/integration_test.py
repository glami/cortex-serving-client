from requests import post

from cortex_serving_client.cortex_client import get_cortex_client_instance


cortex = get_cortex_client_instance(pg_user='cortex_test', pg_password='cortex_test', pg_db='cortex_test', cortex_env='local')

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

assert result['yes']
print(result)