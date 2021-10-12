import contextlib
import json
import logging
import multiprocessing
import os
import subprocess
import tempfile
import time
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timedelta
from json import JSONDecodeError
from math import ceil
from pathlib import Path
from threading import Thread, Lock
from typing import NamedTuple, Optional, Dict, List, Callable

import cortex
import yaml
from cortex.binary import get_cli_path
from psycopg2._psycopg import DatabaseError
from psycopg2.extras import NamedTupleCursor
from psycopg2.pool import ThreadedConnectionPool

from cortex_serving_client import s3
from cortex_serving_client.command_line_wrapper import _verbose_command_wrapper
from cortex_serving_client.deployment_failed import DeploymentFailed, COMPUTE_UNAVAILABLE_FAIL_TYPE, \
    DEPLOYMENT_TIMEOUT_FAIL_TYPE, DEPLOYMENT_ERROR_FAIL_TYPE, DEPLOYMENT_JOB_NOT_DEPLOYED_FAIL_TYPE
from cortex_serving_client.hash_utils import get_file_hash
from cortex_serving_client.printable_chars import remove_non_printable
from cortex_serving_client.retry_utils import create_always_retry_session
from cortex_serving_client.s3 import BUCKET_NAME, BUCKET_SSE_KEY
from cortex_serving_client.shell_utils import kill_process_with_children
from cortex_serving_client.zip_utils import zip_dir, add_file_to_zip

ENV_CSC_DEFAULT_DOCKER_IMAGE = "CSC_DEFAULT_BASE_DOCKER_IMAGE"

KIND_REALTIME_API = 'RealtimeAPI'
KIND_BATCH_API = 'BatchAPI'
KIND_ASYNC_API = 'AsyncAPI'
KIND_TASK_API = 'TaskAPI'
KIND_DEFAULT = KIND_REALTIME_API

NOT_DEPLOYED = "not deployed"

"""
Source: https://github.com/cortexlabs/cortex/blob/0.31/pkg/types/status/code.go
On some places custom "not_deployed" status may be used.
"""

LIVE_STATUS = "status_live"
UPDATING_STATUS = "status_updating"
COMPUTE_UNAVAILABLE_STATUS = "status_stalled"
CORTEX_STATUSES = [LIVE_STATUS, UPDATING_STATUS, "status_error", "status_oom", COMPUTE_UNAVAILABLE_STATUS]  #
NOT_DEPLOYED_STATUS = "status_not_deployed"

# returned if cortex describe says Failed
API_FAILED_STATUS = "api_failed_status"

JOB_STATUS_SUCCEEDED = 'succeeded'
JOB_STATUS_ENQUEUING = 'enqueuing'
JOB_STATUS_RUNNING = 'running'
JOB_STATUS_ENQUEUED_FAILED = 'failed_while_enqueuing'
JOB_STATUS_COMPLETED_WITH_FAILURES = 'completed_with_failures'
JOB_STATUS_WORKER_ERROR = 'worker_error'
JOB_STATUS_OOM = 'out_of_memory'
JOB_STATUS_TIMED_OUT = 'timed_out'
JOB_STATUS_STOPPED = 'stopped'
JOB_STATUS_UNEXPECTED_STATUS = 'unexpected_status'

JOB_FAIL_STATUSES = [
    JOB_STATUS_ENQUEUED_FAILED,
    JOB_STATUS_WORKER_ERROR,
    JOB_STATUS_OOM,
    JOB_STATUS_TIMED_OUT,
    JOB_STATUS_STOPPED,
    API_FAILED_STATUS
]

JOB_VALID_STATUSES = [
    JOB_STATUS_SUCCEEDED,
    JOB_STATUS_ENQUEUING,
    JOB_STATUS_RUNNING,
    JOB_STATUS_COMPLETED_WITH_FAILURES,
] + JOB_FAIL_STATUSES


CORTEX_DELETE_TIMEOUT_SEC = 10 * 60
CORTEX_DEPLOY_REPORTED_TIMEOUT_SEC = 60
CORTEX_DEFAULT_DEPLOYMENT_TIMEOUT = 20 * 60
CORTEX_DEFAULT_API_TIMEOUT = CORTEX_DEFAULT_DEPLOYMENT_TIMEOUT
CORTEX_MIN_API_TIMEOUT_SEC = CORTEX_DELETE_TIMEOUT_SEC
CORTEX_DEPLOY_RETRY_BASE_SLEEP_SEC = 5 * 60
CORTEX_STATUS_CHECK_SLEEP_SEC = 15
INFINITE_TIMEOUT_SEC = 30 * 365 * 24 * 60 * 60  # 30 years
WAIT_BEFORE_JOB_GET = int(os.environ.get('CORTEX_WAIT_BEFORE_JOB_GET', str(30)))
N_RETRIES_BATCH_JOB = 5

CORTEX_PATH = get_cli_path()

logger = logging.getLogger('cortex_client')
__cortex_client_instance = None

# insert your base image id
DEFAULT_DOCKER_IMAGE = os.environ.get(ENV_CSC_DEFAULT_DOCKER_IMAGE)
DEFAULT_PORT = os.environ.get("CSC_DEFAULT_UVICORN_PORT", 8080)
DEFAULT_PREDICTOR_CLASS_NAME = "PythonPredictor"


class CortexClient:
    """
    An object used to execute commands on Cortex, maintain API state in the db to collect garbage.

    Is thread-safe but not process-safe.
    """

    def __init__(self, db_connection_pool: ThreadedConnectionPool, gc_interval_sec=30 * 60, cortex_env="aws"):
        self.db_connection_pool = db_connection_pool
        self._init_garbage_api_collector(gc_interval_sec)
        self.cortex_env = cortex_env
        self.cortex_vanilla = cortex.client(self.cortex_env)
        self.lock = Lock()
        logger.info(f'Constructing CortexClient for {CORTEX_PATH}.')

    def _prepare_deployment(self, deployment, deploy_dir):
        deployment = deepcopy(deployment)
        name = deployment["name"]

        if "kind" not in deployment:
            deployment["kind"] = KIND_DEFAULT

        assert len(deployment["pod"]['containers']) == 1, f"Number of containers must be 1 for simplicity! " \
                                                          f"Not {len(deployment['pod']['containers'])}"
        container = deployment["pod"]['containers'][0]

        docker_image = container.get("image", DEFAULT_DOCKER_IMAGE)
        if docker_image is None:
            raise ValueError(f'Image needs to be provided either in the container configuration or as environmental variable {ENV_CSC_DEFAULT_DOCKER_IMAGE}.')

        container['image'] = docker_image

        port = deployment["pod"].get('port', DEFAULT_PORT)
        deployment["pod"]["port"] = port

        if 'name' not in container:
            container['name'] = 'api'

        project_name = deployment.pop("project_name")
        predictor_path = deployment.pop("predictor_path")
        predictor_class_name = deployment.pop("predictor_class_name", DEFAULT_PREDICTOR_CLASS_NAME)
        bucket_name = deployment.pop("bucket_name", BUCKET_NAME)
        bucket_sse_key = deployment.pop("bucket_sse_key", BUCKET_SSE_KEY)
        config = container.pop("config", {})
        with self.lock:
            with tempfile.TemporaryDirectory() as tmp_dir_name:
                fname = f"{name}.zip"
                local_zip_path = str(Path(tmp_dir_name) / fname)
                s3_path = f"{project_name}/{fname}"
                zip_dir(deploy_dir, local_zip_path)

                # dump config and add it to zipped deploy dir
                config_path = str(Path(tmp_dir_name) / 'predictor_config.json')
                with open(config_path, 'w') as f:
                    json.dump(config, f)
                add_file_to_zip(local_zip_path, config_path)

                # add empty requirements.txt if it does not exist - it is mandatory
                requirements_path = Path(deploy_dir) / "requirements.txt"
                if not requirements_path.exists():
                    tmp_req_path = str(Path(tmp_dir_name) / 'requirements.txt')
                    open(tmp_req_path, 'w').close()
                    add_file_to_zip(local_zip_path, tmp_req_path)

                # add or check main.py
                main_path = Path(deploy_dir) / "main.py"
                if main_path.exists():
                    with open(main_path, 'rt') as f:
                        code = ''.join(f.readlines())
                        assert '@app.post' in code, f"API {name} failed to deploy: @app.post not found in {main_path}! " \
                                                    f"main:app must be runnable by Uvicorn!"
                else:
                    # add default main.py if it was not supplied
                    add_file_to_zip(local_zip_path, Path(__file__).parent / 'resources' / 'main.py')

                    predictor_filepath = Path(deploy_dir) / f"{predictor_path.replace('.', '/')}.py"
                    try:
                        with open(predictor_filepath, 'rt') as f:
                            code = ''.join(f.readlines())
                            assert predictor_class_name in code, f"{predictor_class_name} not found in {predictor_filepath}!"
                    except Exception as e:
                        raise RuntimeError(f"API {name} failed to deploy:") from e

                s3.upload_file(local_zip_path, s3_path, bucket_name, bucket_sse_key, verbose=True)

                # add necessary env vars
                if 'env' not in container:
                    container['env'] = {}
                container["env"]['CSC_BUCKET_NAME'] = bucket_name
                container["env"]['CSC_S3_SSE_KEY'] = bucket_sse_key
                container["env"]['CSC_S3_SOURCE_ZIP_PATH'] = s3_path
                container["env"]['CSC_UVICORN_PORT'] = port
                container["env"]['CSC_PREDICTOR_PATH'] = predictor_path
                container["env"]['CSC_PREDICTOR_CLASS_NAME'] = predictor_class_name
                # so redeploy actually deploys the new code
                container["env"]['CSC_HASH_OF_THE_CODE_DIR'] = get_file_hash(local_zip_path)

                # env must be string -> string map
                container['env'] = {str(k): str(v) for k, v in container['env'].items()}
        return deployment

    @staticmethod
    def _kill_log_worker(log_worker):
        if log_worker is not None and log_worker.is_alive():
            logger.info(f"Killing process printing logs with pid: {log_worker.pid} ...")
            # log_worker.terminate() will not actually terminate the subprocess streaming logs :(
            kill_process_with_children(log_worker.pid)

    def deploy_single(
        self,
        deployment: Dict,
        deploy_dir,
        deployment_timeout_sec=CORTEX_DEFAULT_DEPLOYMENT_TIMEOUT,
        api_timeout_sec=CORTEX_DEFAULT_API_TIMEOUT,
        print_logs=False,
        n_retries=0
    ) -> 'CortexGetResult':
        """
        Deploy an API until timeouts. Cortex docs https://docs.cortex.dev/deployments/deployment.

        Parameters
        ----------

        deployment
            Cortex deployment config. See https://docs.cortex.dev/deployments/api-configuration
        deploy_dir
            Base directory of code to deploy to Cortex.
        deployment_timeout_sec
            Time to keep the API deploying. Including execution of predictor's `__init__`,
            which can be used to e.g. train a model.
        api_timeout_sec
            Time until API will be auto-deleted. Use `INFINITE_TIMEOUT_SEC` for infinite.
        print_logs
            Subscribe to Cortex logs of the API and print random pod's logs to stdout.
        n_retries
            Number of attempts to deploy until raising the failure. Retries only if the failure is not an application error.

        Returns
        -------
        get_result
            Deployed API get result information.

        """
        deployment = self._prepare_deployment(deployment, deploy_dir)
        name = deployment["name"]

        predictor_yaml_str = yaml.dump([deployment], default_style='"')

        if api_timeout_sec < CORTEX_MIN_API_TIMEOUT_SEC:
            logger.info(f'API timeout {api_timeout_sec} is be smaller than minimal API timeout {CORTEX_MIN_API_TIMEOUT_SEC}. Setting it to {CORTEX_MIN_API_TIMEOUT_SEC} to avoid excessive GC.')
            api_timeout_sec = CORTEX_MIN_API_TIMEOUT_SEC

        if deployment_timeout_sec < CORTEX_DEFAULT_DEPLOYMENT_TIMEOUT:
            logger.info(f'Deployment timeout {deployment_timeout_sec} is be smaller than default deployment timeout {CORTEX_DEFAULT_DEPLOYMENT_TIMEOUT}. Setting it to {CORTEX_DEFAULT_DEPLOYMENT_TIMEOUT}.')
            deployment_timeout_sec = CORTEX_DEFAULT_DEPLOYMENT_TIMEOUT

        # file has to be created the dir to which python predictors are described in yaml
        filename = f"{name}.yaml"
        filepath = f"{deploy_dir}/{filename}"

        log_worker = None
        for retry in range(n_retries + 1):
            try:
                self._collect_garbage()
                with str_to_public_temp_file(predictor_yaml_str, filepath, self.lock):
                    gc_timeout_sec = deployment_timeout_sec + CORTEX_DELETE_TIMEOUT_SEC
                    logger.info(f"Deployment {name} has deployment timeout {deployment_timeout_sec}sec and GC timeout {gc_timeout_sec}sec.")
                    self._insert_or_update_gc_timeout(name, gc_timeout_sec)
                    _verbose_command_wrapper([CORTEX_PATH, "deploy", filename, f"--env={self.cortex_env}", "--yes", "-o=json"], cwd=deploy_dir)

                if print_logs and deployment['kind'] == KIND_REALTIME_API:
                    assert log_worker is None or not log_worker.is_alive(), f"Should be None or dead!"
                    log_worker = self._cortex_logs_print_async(name)

                start_time = time.time()
                while True:
                    if log_worker is not None and not log_worker.is_alive():
                        # necessary because cortex logs interrupts when pod is spun up and has to be run again
                        log_worker = self._cortex_logs_print_async(name)

                    get_result = self.get(name)
                    time_since_start = ceil(time.time() - start_time)

                    if get_result.status == LIVE_STATUS:
                        self._kill_log_worker(log_worker)
                        gc_timeout_sec = api_timeout_sec + CORTEX_DELETE_TIMEOUT_SEC
                        logger.info(f"Deployment {name} successful. Setting API timeout {api_timeout_sec}sec and GC timeout {gc_timeout_sec}sec.")
                        self._insert_or_update_gc_timeout(name, gc_timeout_sec)
                        return get_result

                    elif get_result.status == COMPUTE_UNAVAILABLE_STATUS:
                        self.delete(name)
                        raise DeploymentFailed(
                            f'Deployment of {name} API could not start due to insufficient memory, CPU, GPU or Inf in the cluster after {time_since_start} secs.',
                            COMPUTE_UNAVAILABLE_FAIL_TYPE, name, time_since_start)

                    elif get_result.status != UPDATING_STATUS:
                        self.delete(name)
                        raise DeploymentFailed(f"Deployment of {name} failed with status {get_result.status} after {time_since_start} secs.",
                                               DEPLOYMENT_ERROR_FAIL_TYPE, name, time_since_start)

                    if time_since_start > deployment_timeout_sec:
                        self.delete(name)
                        raise DeploymentFailed(f"Deployment of {name} timed out after {deployment_timeout_sec} secs.",
                                               DEPLOYMENT_TIMEOUT_FAIL_TYPE, name, time_since_start)

                    logger.info(f"Sleeping during deployment of {name} until next status check. Current status: {get_result.status}.")
                    time.sleep(CORTEX_STATUS_CHECK_SLEEP_SEC)

            except DeploymentFailed as e:
                if retry == n_retries or e.failure_type == DEPLOYMENT_ERROR_FAIL_TYPE:
                    self._kill_log_worker(log_worker)
                    raise e

                else:
                    sleep_secs = ceil(CORTEX_DEPLOY_RETRY_BASE_SLEEP_SEC * 2 ** retry)
                    logger.warning(f'Retrying {retry + 1} time after sleep of {sleep_secs} secs due to deployment failure: {e}')
                    time.sleep(sleep_secs)

        raise RuntimeError('Execution should never reach here.')


    @contextmanager
    def deploy_temporarily(
        self,
        deployment: Dict,
        deploy_dir: str,
        deployment_timeout_sec=CORTEX_DEFAULT_DEPLOYMENT_TIMEOUT,
        api_timeout_sec=CORTEX_DEFAULT_API_TIMEOUT,
        print_logs=False,
        n_retries=0
    ) -> 'CortexGetResult':
        """
        Deploy an API until timeouts. Cortex docs https://docs.cortex.dev/deployments/deployment.

        Parameters
        ----------

        deployment
            Cortex deployment config. See https://docs.cortex.dev/deployments/api-configuration
        deploy_dir
            Base directory of code to deploy to Cortex.
        deployment_timeout_sec
            Time to keep the API deploying. Including execution of predictor's `__init__`,
            which can be used to e.g. train a model.
        api_timeout_sec
            Time until API will be auto-deleted. Use `INFINITE_TIMEOUT_SEC` for infinite.
        print_logs
            Subscribe to Cortex logs of the API and print random pod's logs to stdout.
        n_retries
            Number of attempts to deploy until raising the failure. Retries only if the failure is not an application error.

        Returns
        -------
        get_result
            Deployed API get result information.

        """

        try:
            yield self.deploy_single(deployment, deploy_dir, deployment_timeout_sec, api_timeout_sec, print_logs, n_retries)

        finally:
            self.delete(deployment["name"])

    def deploy_batch_api_and_run_job(self,
                                     deployment: dict,
                                     job_spec: dict,
                                     deploy_dir: str,
                                     deployment_timeout_sec=CORTEX_DEFAULT_DEPLOYMENT_TIMEOUT,
                                     api_timeout_sec=CORTEX_DEFAULT_API_TIMEOUT,
                                     print_logs=False,
                                     verbose=False,
                                     n_retries=0) -> "CortexGetResult":

        deployment['kind'] = deployment.get('kind', KIND_BATCH_API)
        assert deployment['kind'] == KIND_BATCH_API, f"Deployment['kind'] must be {KIND_BATCH_API}!"

        # has to be in BatchAPI, no idea why ...
        deployment['pod']['containers'][0]['command'] = ["python", "/tmp/download_source_code.py"]

        with self.deploy_temporarily(
                deployment,
                deploy_dir=deploy_dir,
                deployment_timeout_sec=deployment_timeout_sec,
                api_timeout_sec=api_timeout_sec,
                print_logs=print_logs,
                n_retries=n_retries
        ) as get_result:

            logger.info(f'BatchAPI {deployment["name"]} deployed. Starting a job.')
            with create_always_retry_session() as session:
                job_id = None
                for retry_job_get in range(N_RETRIES_BATCH_JOB):
                    for retry_job_submit in range(3):
                        try:
                            job_json = session.post(get_result.endpoint, json=job_spec, timeout=10 * 60).json()

                        except JSONDecodeError as e:
                            logger.warning(f'Job submission response could not be decoded: {e}.')
                            job_json = None

                        if job_json is not None and 'job_id' in job_json:
                            job_id = job_json['job_id']
                            break

                        else:
                            sleep_time = 3 * 2 ** retry_job_submit
                            logger.info(f'Retrying job creation request after {sleep_time}.')
                            time.sleep(sleep_time)
                            continue

                    if job_id is None:
                        raise ValueError(f'job_id not in job_json {json.dumps(job_json)}')

                    log_worker = None
                    if print_logs:
                        log_worker = self._cortex_logs_print_async(deployment['name'], job_id)

                    # Don't call job too early: https://gitter.im/cortexlabs/cortex?at=5f7fe4c01cbba72b63cb745f
                    time.sleep(WAIT_BEFORE_JOB_GET * 2 ** retry_job_get)

                    job_status = JOB_STATUS_ENQUEUING
                    while job_status in (JOB_STATUS_ENQUEUING, JOB_STATUS_RUNNING):
                        if log_worker is not None and not log_worker.is_alive():
                            # necessary because cortex logs interrupts when pod is spun up and has to be run again
                            log_worker = self._cortex_logs_print_async(deployment['name'], job_id)

                        job_result = self.get(deployment["name"], job_id)
                        job_status = job_result.status
                        if verbose:
                            logger.info(f"Job {job_id} has status: {job_status}")
                        time.sleep(30)

                    if log_worker is not None:
                        logger.info(f"Terminating process printing logs from {deployment['name']} job_id={job_id} ...")
                        log_worker.terminate()

                    if job_status in [NOT_DEPLOYED_STATUS, JOB_STATUS_UNEXPECTED_STATUS] + JOB_FAIL_STATUSES:
                        # TODO request fixes improvements https://gitter.im/cortexlabs/cortex?at=5f7fe4c01cbba72b63cb745f
                        # Sleep in after job submission.
                        logger.warning(f'Job unexpectedly undeployed or failed with status {job_status}. Retrying with sleep.')
                        logger.info(f"Finished retry {retry_job_get+1}/{N_RETRIES_BATCH_JOB}")
                        continue

                    else:
                        logger.info(f'BatchAPI {deployment["name"]} job {job_id} ended with status {job_status}. Deleting the BatchAPI.')
                        return job_result

                raise DeploymentFailed(f'Job unexpectedly undeployed or failed with status {job_status}.', DEPLOYMENT_JOB_NOT_DEPLOYED_FAIL_TYPE,
                                       deployment['name'], -1)

    def postpone_api_timeout(self, name: str, timeout_timestamp: datetime):
        ultimate_timeout = timeout_timestamp + timedelta(seconds=CORTEX_DELETE_TIMEOUT_SEC)
        with self._open_db_cursor() as cursor:
            cursor.execute(
                "update cortex_api_timeout set ultimate_timeout = %s, modified = transaction_timestamp() where api_name = %s",
                [ultimate_timeout, name],
            )

    def raise_on_cluster_down(self):
        try:
            self.get_all()

        except (ValueError, JSONDecodeError) as e:
            raise ValueError(f"Cluster is likely down: {e}.") from e

    def get(self, name, job_id=None) -> "CortexGetResult":
        job_id_or_empty = [job_id] if job_id is not None else []
        cmd = [CORTEX_PATH, "get", name] + job_id_or_empty + [f"--env={self.cortex_env}", "-o=json"]
        out = _verbose_command_wrapper(cmd, allow_non_0_return_code_on_stdout_sub_strs=[NOT_DEPLOYED])
        try:
            json_dict = json.loads(out.strip())

        except JSONDecodeError as e:
            logger.debug(f'Encountered {e} but ignoring.')
            json_dict = None

        lines = out.splitlines()
        first_line = lines[0] if len(lines) > 0 else ''
        if json_dict is not None:
            if job_id is not None:
                job_status = json_dict['job_status']["status"]
                if job_status not in JOB_VALID_STATUSES:
                    logger.warning(f"Unexpected status: {job_status} of job: {job_id}!")
                    job_status = JOB_STATUS_UNEXPECTED_STATUS
                return CortexGetResult(job_status, None, json_dict)

            first_json_dict = json_dict[0]
            if first_json_dict['spec']['kind'] in [KIND_BATCH_API, KIND_TASK_API]:
                # BatchAPI has no status
                return CortexGetResult(LIVE_STATUS, first_json_dict['endpoint'], first_json_dict)

            else:
                # check RealtimeAPI or AsyncAPI
                cmd_describe = [CORTEX_PATH, "describe", name, f"--env={self.cortex_env}"]
                describe_out = _verbose_command_wrapper(cmd_describe,
                                                        allow_non_0_return_code_on_stdout_sub_strs=[NOT_DEPLOYED])
                if 'Failed' in describe_out:
                    # Failed appears in the output only if there is >0 of Failed replicas
                    return CortexGetResult(API_FAILED_STATUS, None, describe_out)
                else:
                    return CortexGetResult(status_from_dict(first_json_dict['status']), first_json_dict['endpoint'], first_json_dict)

        elif NOT_DEPLOYED in first_line:
            return CortexGetResult(NOT_DEPLOYED_STATUS, None, dict())

        else:
            raise ValueError(f"For api {name} with job_id {job_id} got unsupported Cortex output:\n{out}")

    def get_all(self) -> List["CortexGetAllStatus"]:
        input_in_case_endpoint_prompt = b"n\n"
        out = _verbose_command_wrapper([CORTEX_PATH, "get", f"--env={self.cortex_env}", "-o=json"], input=input_in_case_endpoint_prompt)
        json_dict = json.loads(out.strip())
        return [CortexGetAllStatus(e['metadata']['name'], status_from_dict(e['status']) if e['metadata']['kind'] == KIND_REALTIME_API else LIVE_STATUS, e) for e in json_dict]

    def delete(self, name, force=False, timeout_sec=CORTEX_DELETE_TIMEOUT_SEC, cursor=None) -> "CortexGetResult":
        """
        Executes delete and checks that the API was deleted. If not tries force delete. If that fails, raises exception.
        """

        delete_cmd = [CORTEX_PATH, "delete", name, f"--env={self.cortex_env}", "-o=json"]
        if force:
            delete_cmd.append("-f")

        accept_deletion_if_asked_input = b"y\n"
        try:
            self._open_cursor_if_none(cursor, self._modify_ultimate_timeout_to_delete_timeout, name)
            start_time = time.time()
            while True:
                get_result = self.get(name)
                if get_result.status == NOT_DEPLOYED_STATUS:
                    return get_result

                else:
                    _verbose_command_wrapper(delete_cmd, input=accept_deletion_if_asked_input,
                                             allow_non_0_return_code_on_stdout_sub_strs=[NOT_DEPLOYED])

                if start_time + timeout_sec < time.time():
                    if force:
                        raise ValueError(f'Timeout of force delete {delete_cmd} after {timeout_sec}.')

                    else:
                        logger.error(f'Timeout of delete cmd {delete_cmd} after {timeout_sec} seconds. Will attempt to force delete now.')
                        return self.delete(name, force=True, timeout_sec=timeout_sec, cursor=cursor)

                logger.info(f"During delete of {name} sleeping until next status check. Current result: {get_result.status}.")
                time.sleep(CORTEX_STATUS_CHECK_SLEEP_SEC)

        finally:
            self._open_cursor_if_none(cursor, self._del_db_api_row, name)

    def _cortex_logs_print_async(self, name, job_id=None):
        def listen_on_logs(cmd_arr):
            logger.debug(f"Started log watch process pid: {os.getpid()} ...")
            with subprocess.Popen(cmd_arr, stdout=subprocess.PIPE) as logs_sp:
                while True:
                    # returns None while subprocess is running
                    retcode = logs_sp.poll()
                    line = logs_sp.stdout.readline().decode(encoding='utf-8')
                    print_line = remove_non_printable(line.rstrip('\n'))
                    if len(print_line) > 0:
                        logger.info(print_line)
                    if retcode is not None:
                        break

        if job_id is None:
            cmd_arr = [CORTEX_PATH, "logs", "--yes", "--random-pod", name, f"--env={self.cortex_env}"]

        else:
            cmd_arr = [CORTEX_PATH, "logs", "--yes", "--random-pod", name, job_id, f"--env={self.cortex_env}"]

        worker = multiprocessing.Process(target=listen_on_logs, args=(cmd_arr,), daemon=False, name=f'api_{name}')
        worker.start()
        return worker

    @staticmethod
    def _modify_ultimate_timeout_to_delete_timeout(cur, name):
        """ Will update modified time to prevent GC raise conditions and shorten timeout to the amount needed to delete the api."""
        try:
            cur.execute("""
                update cortex_api_timeout
                set ultimate_timeout = transaction_timestamp() + %s * interval '1 second',
                    modified = transaction_timestamp()
                where api_name = %s""",
                [CORTEX_DELETE_TIMEOUT_SEC, name])

        except DatabaseError:
            logger.warning(f"Ignoring exception during cortex_api_timeout record for {name} modifying ultimate_timeout to delete the api", exc_info=True)

    def _insert_or_update_gc_timeout(self, name: str, gc_timeout_seconds_from_now: float):
        with self._open_db_cursor() as cursor:
            cursor.execute(
                """
                insert into cortex_api_timeout(api_name, ultimate_timeout) values (%s, transaction_timestamp() + %s * interval '1 second')
                on conflict (api_name) do update
                    set ultimate_timeout = transaction_timestamp() + %s * interval '1 second', modified = transaction_timestamp()
            """,
                [name, gc_timeout_seconds_from_now, gc_timeout_seconds_from_now],
            )

    @staticmethod
    def _del_db_api_row(cur, name):
        try:
            cur.execute("delete from cortex_api_timeout where api_name = %s", [name])

        except DatabaseError:
            logger.warning(f"Ignoring exception during cortex_api_timeout record for {name} deletion", exc_info=True)

    def _open_db_cursor(self):
        return open_pg_cursor(self.db_connection_pool)

    def _open_cursor_if_none(self, cursor, fun: Callable, *args):
        if cursor is not None:
            fun(cursor, *args)

        else:
            with self._open_db_cursor() as cursor:
                fun(cursor, *args)

    def _collect_garbage(self):
        logger.info(f"Starting garbage collection.")
        try:
            with self._open_db_cursor() as cur:
                # remove timed out - deployed and recorded
                cur.execute(
                    "select api_name from cortex_api_timeout where ultimate_timeout < transaction_timestamp() for update"
                )
                for api_row in cur.fetchall():
                    api_name = api_row.api_name
                    if self.get(api_name).status != NOT_DEPLOYED_STATUS:
                        logger.warning(f"Collecting Cortex garbage - timed-out deployed API: {api_name}")
                        self.delete(api_name, cursor=cur)

                    else:
                        logger.warning(f"Collecting Cortex garbage - timed-out db row: {api_name}")
                        self._del_db_api_row(cur, api_name)

                cur.execute("commit")

                deployed_api_names = [row.api for row in self.get_all()]

                # Remove recorded but not deployed - fast to avoid conflicts
                cur.execute(
                    "select api_name from cortex_api_timeout where modified + %s * interval '1 second' < transaction_timestamp() and not (api_name = ANY(%s))",
                    [CORTEX_DELETE_TIMEOUT_SEC, deployed_api_names],
                )
                if cur.rowcount > 0:
                    apis = [r.api_name for r in cur.fetchall()]
                    logger.warning(f"Collecting Cortex garbage - recorded but not deployed: {apis}")
                    cur.execute(
                        "delete from cortex_api_timeout where modified + %s * interval '1 second' < transaction_timestamp() and not (api_name = ANY(%s))",
                        [CORTEX_DELETE_TIMEOUT_SEC, deployed_api_names],
                    )
                    cur.execute("commit")

                # Remove deployed but not recorded
                cur.execute("select api_name from cortex_api_timeout")
                recorded_apis_set = set([r.api_name for r in cur.fetchall()])
                for deployed_not_recorded_name in set(deployed_api_names).difference(recorded_apis_set):
                    logger.warning(f"Collecting Cortex garbage - deployed not recorded: {deployed_not_recorded_name}")
                    self.delete(deployed_not_recorded_name, cursor=cur)

        except Exception as e:
            logger.info(f"Ignoring unexpected exception that occurred during Garbage Collection: {e}", exc_info=e)

    def _init_garbage_api_collector(self, interval_sec):
        with self._open_db_cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS (
                   SELECT FROM pg_catalog.pg_class c
                   JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                   WHERE  n.nspname = current_schema()
                   AND    c.relname = 'cortex_api_timeout'
                   );
            """
            )
            if not cur.fetchone().exists:
                cur.execute(
                    """
                    create table cortex_api_timeout (
                        api_name varchar(255) primary key,
                        ultimate_timeout timestamp not null,
                        modified timestamp default transaction_timestamp() not null,
                        created timestamp default transaction_timestamp() not null
                    )
                """
                )

        self.looping_thread = Thread(target=lambda: self._start_gc_loop(interval_sec), daemon=True)
        self.looping_thread.start()

    def _start_gc_loop(self, interval_sec):
        while True:
            time.sleep(interval_sec)
            self._collect_garbage()


@contextmanager
def open_pg_cursor(db_connection_pool, key=None):
    try:
        with db_connection_pool.getconn(key) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                yield cur

    finally:
        db_connection_pool.putconn(conn, key)


def status_from_dict(status_dict):
    ready = status_dict['ready']
    requested = status_dict['requested']
    up_to_date = status_dict['up_to_date']

    if ready == requested and ready == up_to_date and ready > 0:
        return LIVE_STATUS
    else:
        return UPDATING_STATUS


class CortexGetAllStatus(NamedTuple):
    api: str
    status: str
    response: dict


class CortexGetResult(NamedTuple):
    status: str
    endpoint: Optional[str]
    response: dict


def file_to_str(path: str) -> str:
    with open(path, "r") as f:
        return f.read()


@contextlib.contextmanager
def str_to_public_temp_file(string: str, filepath: str, lock: Lock) -> str:
    with lock:
        with open(filepath, "w+") as f:
            f.write(string)

        yield filepath

        os.remove(filepath)


def get_cortex_client_instance_with_pool(db_connection_pool: ThreadedConnectionPool, gc_interval_sec=15 * 60, cortex_env="aws"):
    global __cortex_client_instance
    if __cortex_client_instance is not None:
        return __cortex_client_instance

    else:
        __cortex_client_instance = CortexClient(db_connection_pool, gc_interval_sec, cortex_env)
        return __cortex_client_instance


def get_cortex_client_instance(pg_user, pg_password, pg_db, pg_host='127.0.0.1', pg_port='5432', min_conn=0, max_conn=3, gc_interval_sec=15 * 60, cortex_env="aws"):
    global __cortex_client_instance
    if __cortex_client_instance is not None:
        return __cortex_client_instance

    else:
        pg_user = os.environ.get("CORTEX_CLIENT_USERNAME", pg_user)
        pg_password = os.environ.get("CORTEX_CLIENT_PASSWORD", pg_password)
        pg_host = os.environ.get("CORTEX_CLIENT_HOSTNAME", pg_host)
        pg_port = os.environ.get("CORTEX_CLIENT_PORT", pg_port)
        pg_db = os.environ.get("CORTEX_CLIENT_DATABASE", pg_db)
        db_connection_pool = ThreadedConnectionPool(minconn=min_conn, maxconn=max_conn, user=pg_user, password=pg_password,
                                                    host=pg_host, port=pg_port,
                                                    database=pg_db, cursor_factory=NamedTupleCursor)

        __cortex_client_instance = CortexClient(db_connection_pool, gc_interval_sec, cortex_env)
        return __cortex_client_instance
