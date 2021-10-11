import io
import logging
import os
import pickle
from concurrent.futures import ThreadPoolExecutor
from time import sleep

from cortex_serving_client import s3

logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(threadName)-10s : %(name)s : %(message)s", level=logging.INFO
)


from unittest.mock import patch
from requests import post
import unittest

from cortex_serving_client.cortex_client import get_cortex_client_instance, NOT_DEPLOYED_STATUS, JOB_STATUS_SUCCEEDED, \
    KIND_BATCH_API
from cortex_serving_client.deployment_failed import DeploymentFailed, DEPLOYMENT_TIMEOUT_FAIL_TYPE, \
    DEPLOYMENT_JOB_NOT_DEPLOYED_FAIL_TYPE, DEPLOYMENT_ERROR_FAIL_TYPE

logger = logging.getLogger(__name__)


class IntegrationTests(unittest.TestCase):
    """
    These tests require Docker and Cortex.
    """

    cortex = get_cortex_client_instance(
        cortex_env='cortex-serving-client-test',  # if you create cluster by using example/cluster.yaml
        pg_user='cortex_test',
        pg_password='cortex_test',
        pg_db='cortex_test',
    )

    @staticmethod
    def _get_deployment_dict(name, env=None):
        if env is None:
            env = {}
        return {
            "name": name,
            "project_name": "test",
            "predictor_path": "yes_predictor",
            "pod": {
                "containers": [
                    {
                        "compute": {"cpu": '200m', "mem": f"{0.1}Gi"},
                        "env": env,
                    }
                ],
            },
            "node_groups": ['ng-spot-1']
        }

    def test_thread_safety(self):
        def get_deploy_dict(a):
            deployment = IntegrationTests._get_deployment_dict("yes-api")
            cortex_ = get_cortex_client_instance(
                cortex_env='cortex-serving-client-test',  # if you create cluster by using example/cluster.yaml
                pg_user='cortex_test',
                pg_password='cortex_test',
                pg_db='cortex_test',
            )
            x = cortex_._prepare_deployment(deployment, "./data/yes_deployment")
            return len(x)

        with ThreadPoolExecutor(max_workers=40) as e:
            futures = []
            for _ in range(100):
                futures.append(e.submit(get_deploy_dict, 1))

            for f in futures:
                logger.info(f.result())  # necessary to get the exceptions to the main thread

    def test_deploy_yes(self):
        deployment = self._get_deployment_dict("yes-api")

        with self.cortex.deploy_temporarily(
                deployment,
                deploy_dir="./data/yes_deployment",
                print_logs=True,
                api_timeout_sec=10 * 60,
        ) as get_result:
            for _ in range(10):
                logger.info(f"Sending request to test logging terminated correctly ...")
                result = post(get_result.endpoint, json={}).json()
                sleep(1)

            # extra delete can occur, should not cause failure. Non-force deletes are tested in other cases.
            self.cortex.delete(deployment['name'], force=True)

        self.assertTrue(result['yes'])
        self.assertEqual(self.cortex.get(deployment['name']).status, NOT_DEPLOYED_STATUS)

    def test_redeploy(self):
        deployment = self._get_deployment_dict("yes-api")

        with self.cortex.deploy_temporarily(
                deployment,
                deploy_dir="./data/yes_deployment",
                print_logs=True,
                api_timeout_sec=10 * 60,
        ) as get_result:
            result = post(get_result.endpoint, json={}).json()
            self.assertTrue(result['yes'])
            sleep(1)

            # redeploy test
            logger.info(f"\n --- Testing redeploy --- ")
            with open("./data/yes_deployment/yes_predictor.py", 'rt') as f:
                orig_code = f.readlines()
            try:
                with open("./data/yes_deployment/redeploy_yes_predictor.py", 'rt') as f:
                    redeploy_code = f.readlines()
                with open("./data/yes_deployment/yes_predictor.py", 'wt') as f:
                    f.writelines(redeploy_code)
                    logger.info(f"Changed code in yes_predictor.py, calling deploy again ...")

                deployment = self._get_deployment_dict("yes-api")
                self.cortex.deploy_single(
                    deployment,
                    deploy_dir="./data/yes_deployment",
                    print_logs=True,
                    api_timeout_sec=10 * 60,
                )

                for _ in range(10):
                    logger.info(f"Sending request to test logging terminated and code has been updated ...")
                    result = post(get_result.endpoint, json={}).json()
                    self.assertEqual(result['yes'], False)
                    sleep(1)
            finally:
                with open("./data/yes_deployment/yes_predictor.py", 'wt') as f:
                    f.writelines(orig_code)
                    logger.info(f"Reset code in yes_predictor.py to original.")

            # extra delete can occur, should not cause failure. Non-force deletes are tested in other cases.
            self.cortex.delete(deployment['name'], force=True)

        self.assertEqual(self.cortex.get(deployment['name']).status, NOT_DEPLOYED_STATUS)

    def test_deploy_no_predictor(self):
        deployment = self._get_deployment_dict("no-predictor-api")

        with self.assertRaises(RuntimeError):
            with self.cortex.deploy_temporarily(
                    deployment,
                    deploy_dir="data/no_predictor_deployment",
                    api_timeout_sec=10 * 60,
            ) as get_result:
                self.fail(f'Deployment should fail but {get_result.status}.')

    def test_deploy_no_app(self):
        # also tests that if main.py is present it will be used instead of the default one
        deployment = self._get_deployment_dict("no-app-api")

        with self.assertRaises(AssertionError):
            with self.cortex.deploy_temporarily(
                    deployment,
                    deploy_dir="./data/no_app_deployment",
                    api_timeout_sec=10 * 60,
            ) as get_result:
                self.fail(f'Deployment should fail but {get_result.status}.')

    def test_deploy_fail(self):
        deployment = self._get_deployment_dict("fail-api")
        deployment["predictor_path"] = "fail_predictor"

        try:
            with patch(target="cortex_serving_client.cortex_client.CORTEX_DEFAULT_DEPLOYMENT_TIMEOUT", new=5*60):
                with self.cortex.deploy_temporarily(
                        deployment,
                        deploy_dir="./data/fail_deployment",
                        api_timeout_sec=5 * 60,
                        print_logs=True,
                        deployment_timeout_sec=5 * 60,
                ) as get_result:
                    self.fail(f'Deployment should fail but {get_result.status}.')

        except DeploymentFailed as e:
            self.assertEqual(e.failure_type, DEPLOYMENT_ERROR_FAIL_TYPE)

        self.assertEqual(self.cortex.get(deployment['name']).status, NOT_DEPLOYED_STATUS)

    def test_deploy_timeout_fail(self):
        deployment = self._get_deployment_dict("timeout-api")

        try:
            with patch(target="cortex_serving_client.cortex_client.CORTEX_DEFAULT_DEPLOYMENT_TIMEOUT", new=0):
                with self.cortex.deploy_temporarily(
                    deployment,
                    deploy_dir="./data/yes_deployment",
                    api_timeout_sec=10 * 60,
                    deployment_timeout_sec=0
                ) as get_result:
                    self.fail(f'Deployment should fail but {get_result.status}.')

        except DeploymentFailed as e:
            self.assertEqual(e.failure_type, DEPLOYMENT_TIMEOUT_FAIL_TYPE)

        self.assertEqual(self.cortex.get(deployment['name']).status, NOT_DEPLOYED_STATUS)

    def test_deploy_job_yes(self):
        env = {
            'CSC_BUCKET_NAME': os.environ["CSC_BUCKET_NAME"],
            'CSC_S3_SSE_KEY': os.environ['CSC_S3_SSE_KEY']
        }
        deployment = self._get_deployment_dict("job-yes", env)
        deployment['kind'] = KIND_BATCH_API
        deployment["predictor_path"] = "batch_yes_predictor"

        job_spec = {
            "workers": 1,
            "item_list": {"items": [1, 2, 3, 4], "batch_size": 2},
        }
        job_result = self.cortex.deploy_batch_api_and_run_job(
            deployment,
            job_spec,
            deploy_dir="./data/batch_yes_deployment",
            api_timeout_sec=10 * 60,
            print_logs=True,
            verbose=True,
        )
        assert job_result.status == JOB_STATUS_SUCCEEDED
        self.assertEqual(self.cortex.get(deployment['name']).status, NOT_DEPLOYED_STATUS)

        for batch in [[1, 2], [3, 4]]:
            s3_path = f'test/batch-yes/{sum(batch)}.json'
            with io.BytesIO() as fp:
                s3.download_fileobj(s3_path, fp)
                fp.seek(0)
                result = pickle.load(fp)

            logger.info(f"{s3_path}: {result}")
            assert result['yes']

            with io.BytesIO() as fp:
                pickle.dump({'yes': False, 'info': 'this should be rewritten!'}, fp)
                fp.seek(0)
                s3.upload_fileobj(fp, s3_path)
                logger.info(f"Replaced {s3_path} with dummy file.")

    def test_deploy_job_worker_error(self):
        deployment = self._get_deployment_dict("job-worker-err")
        deployment['kind'] = KIND_BATCH_API
        deployment["predictor_path"] = "fail_batch_predictor"

        job_spec = {
            "workers": 1,
            "item_list": {"items": [1, 2, 3, 4], "batch_size": 2},
        }
        try:
            with patch(target="cortex_serving_client.cortex_client.N_RETRIES_BATCH_JOB", new=2):
                job_result = self.cortex.deploy_batch_api_and_run_job(
                    deployment,
                    job_spec,
                    deploy_dir="./data/batch_fail_deployment",
                    api_timeout_sec=10 * 60,
                    print_logs=True,
                    verbose=True,
                )
        except DeploymentFailed as e:
            self.assertEqual(e.failure_type, DEPLOYMENT_JOB_NOT_DEPLOYED_FAIL_TYPE)

        self.assertEqual(self.cortex.get(deployment['name']).status, NOT_DEPLOYED_STATUS)




