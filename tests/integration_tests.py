import unittest

from requests import post

from cortex_serving_client.cortex_client import get_cortex_client_instance


class IntegrationTests(unittest.TestCase):
    """
    These tests require Docker and Cortex.
    """

    cortex = get_cortex_client_instance(
        pg_user='cortex_test',
        pg_password='cortex_test',
        pg_db='cortex_test',
        cortex_env='local')

    def test_deploy_yes(self):
        deployment = dict(
            name='yes-api',
            predictor=dict(
                type='python',
                path='yes_predictor.py',
            ),
            compute=dict(
                cpu=1,
            )
        )

        with self.cortex.deploy_temporarily(
                deployment,
                dir="./",
                api_timeout_sec=10 * 60,
                print_logs=True,
        ) as get_result:
            result = post(get_result.endpoint, json={}).json()

        self.assertTrue(result['yes'])

    def test_deploy_fail(self):
        deployment = dict(
            name='fail-api',
            predictor=dict(
                type='python',
                path='fail_predictor.py',
            ),
            compute=dict(
                cpu=1,
            )
        )

        try:
            with self.cortex.deploy_temporarily(
                    deployment,
                    dir="./",
                    api_timeout_sec=10 * 60,
                    print_logs=True,
            ) as get_result:
                self.fail(f'Deployment should fail but {get_result.status}.')

        except ValueError as e:
            self.assertIn('Deployment failed with status error', str(e))







