import logging

logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(threadName)-10s : %(name)s : %(message)s", level=logging.INFO
)


from unittest.mock import patch
from requests import post
import unittest

from cortex_serving_client.cortex_client import get_cortex_client_instance
from cortex_serving_client.deployment_failed import DeploymentFailed, DEPLOYMENT_TIMEOUT_FAIL_TYPE, \
    DEPLOYMENT_ERROR_FAIL_TYPE


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
            # extra delete can occur, should not cause failure. Non-force deletes are tested in other cases.
            self.cortex.delete(deployment['name'], force=True)

        self.assertTrue(result['yes'])
        self.assertEqual(self.cortex.get(deployment['name']).status, 'not_deployed')

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

        except DeploymentFailed as e:
            self.assertEqual(e.failure_type, DEPLOYMENT_ERROR_FAIL_TYPE)

        self.assertEqual(self.cortex.get(deployment['name']).status, 'not_deployed')

    def test_deploy_timeout_fail(self):
        deployment = dict(
            name='timeout-api',
            predictor=dict(
                type='python',
                path='yes_predictor.py',
            ),
            compute=dict(
                cpu=1,
            )
        )

        try:
            with patch(target="cortex_serving_client.cortex_client.CORTEX_DEFAULT_DEPLOYMENT_TIMEOUT", new=0):
                with self.cortex.deploy_temporarily(
                    deployment,
                    dir="./",
                    api_timeout_sec=10 * 60,
                    print_logs=True,
                    deployment_timeout_sec=0
                ) as get_result:
                    self.fail(f'Deployment should fail but {get_result.status}.')

        except DeploymentFailed as e:
            self.assertEqual(e.failure_type, DEPLOYMENT_TIMEOUT_FAIL_TYPE)

        self.assertEqual(self.cortex.get(deployment['name']).status, 'not_deployed')





