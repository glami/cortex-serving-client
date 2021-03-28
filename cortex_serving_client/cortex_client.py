import contextlib
import io
import json
import logging
import os
import subprocess
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from json import JSONDecodeError
from math import ceil
from threading import Thread
from typing import NamedTuple, Optional, Dict, List, Callable

import cortex
import yaml
from cortex.binary import get_cli_path
from psycopg2._psycopg import DatabaseError
from psycopg2.extras import NamedTupleCursor
from psycopg2.pool import ThreadedConnectionPool

from cortex_serving_client.command_line_wrapper import _verbose_command_wrapper
from cortex_serving_client.deployment_failed import DeploymentFailed, COMPUTE_UNAVAILABLE_FAIL_TYPE, \
    DEPLOYMENT_TIMEOUT_FAIL_TYPE, DEPLOYMENT_ERROR_FAIL_TYPE, DEPLOYMENT_JOB_NOT_DEPLOYED_FAIL_TYPE
from cortex_serving_client.printable_chars import remove_non_printable
from cortex_serving_client.retry_utils import create_always_retry_session

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

JOB_STATUS_SUCCEEDED = 'status_succeeded'
JOB_STATUS_ENQUEUING = 'status_enqueuing'
JOB_STATUS_RUNNING = 'status_running'
JOB_STATUS_UNEXPECTED_ERROR = 'status_unexpected_error'
JOB_STATUS_ENQUEUED_FAILED = 'status_enqueue_failed'


CORTEX_DELETE_TIMEOUT_SEC = 10 * 60
CORTEX_DEPLOY_REPORTED_TIMEOUT_SEC = 60
CORTEX_DEFAULT_DEPLOYMENT_TIMEOUT = 20 * 60
CORTEX_DEFAULT_API_TIMEOUT = CORTEX_DEFAULT_DEPLOYMENT_TIMEOUT
CORTEX_MIN_API_TIMEOUT_SEC = CORTEX_DELETE_TIMEOUT_SEC
CORTEX_DEPLOY_RETRY_BASE_SLEEP_SEC = 5 * 60
CORTEX_STATUS_CHECK_SLEEP_SEC = 15
INFINITE_TIMEOUT_SEC = 30 * 365 * 24 * 60 * 60  # 30 years
WAIT_BEFORE_JOB_GET = int(os.environ.get('CORTEX_WAIT_BEFORE_JOB_GET', str(30)))

CORTEX_PATH = get_cli_path()

logger = logging.getLogger('cortex_client')
__cortex_client_instance = None


class CortexClient:
    """
    An object used to execute commands on Cortex, maintain API state in the db to collect garbage.
    """

    def __init__(self, db_connection_pool: ThreadedConnectionPool, gc_interval_sec=30 * 60, cortex_env="aws"):
        self.db_connection_pool = db_connection_pool
        self._init_garbage_api_collector(gc_interval_sec)
        self.cortex_env = cortex_env
        self.cortex_vanilla = cortex.client(self.cortex_env)
        logger.info(f'Constructing CortexClient for {CORTEX_PATH}.')

    def deploy_single(
        self,
        deployment: Dict,
        dir,
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
        dir
            Base directory of code to deploy to Cortex.
        deployment_timeout_sec
            Time to keep the API deploying. Including execution of predictor's `__init__`,
            which can be used to e.g. train a model.
        api_timeout_sec
            Time until API will be auto-deleted. Use `INFINITE_TIMEOUT_SEC` for infinite.
        print_logs
            Subscribe to Cortex logs of the API and print them to stdout.
        n_retries
            Number of attempts to deploy until raising the failure. Retries only if the failure is not an application error.

        Returns
        -------
        get_result
            Deployed API get result information.

        """

        name = deployment["name"]
        if "kind" not in deployment:
            deployment["kind"] = KIND_DEFAULT

        predictor_yaml_str = yaml.dump([deployment], default_flow_style=False)

        if api_timeout_sec < CORTEX_MIN_API_TIMEOUT_SEC:
            logger.info(f'API timeout {api_timeout_sec} is be smaller than minimal API timeout {CORTEX_MIN_API_TIMEOUT_SEC}. Setting it to {CORTEX_MIN_API_TIMEOUT_SEC} to avoid excessive GC.')
            api_timeout_sec = CORTEX_MIN_API_TIMEOUT_SEC

        if deployment_timeout_sec < CORTEX_DEFAULT_DEPLOYMENT_TIMEOUT:
            logger.info(f'Deployment timeout {deployment_timeout_sec} is be smaller than default deployment timeout {CORTEX_DEFAULT_DEPLOYMENT_TIMEOUT}. Setting it to {CORTEX_DEFAULT_DEPLOYMENT_TIMEOUT}.')
            deployment_timeout_sec = CORTEX_DEFAULT_DEPLOYMENT_TIMEOUT

        # file has to be created the dir to which python predictors are described in yaml
        filename = f"{name}.yaml"
        filepath = f"{dir}/{filename}"

        for retry in range(n_retries + 1):
            try:
                self._collect_garbage()
                with str_to_public_temp_file(predictor_yaml_str, filepath):
                    gc_timeout_sec = deployment_timeout_sec + CORTEX_DELETE_TIMEOUT_SEC
                    logger.info(f"Deployment {name} has deployment timeout {deployment_timeout_sec}sec and GC timeout {gc_timeout_sec}sec.")
                    self._insert_or_update_gc_timeout(name, gc_timeout_sec)
                    _verbose_command_wrapper([CORTEX_PATH, "deploy", filename, f"--env={self.cortex_env}", "--yes", "-o=json"], cwd=dir)

                if print_logs and deployment['kind'] == KIND_REALTIME_API:
                    self._cortex_logs_print_async(name)

                start_time = time.time()
                while True:
                    get_result = self.get(name)
                    time_since_start = ceil(time.time() - start_time)
                    if get_result.status == LIVE_STATUS:
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
        dir,
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
        dir
            Base directory of code to deploy to Cortex.
        deployment_timeout_sec
            Time to keep the API deploying. Including execution of predictor's `__init__`,
            which can be used to e.g. train a model.
        api_timeout_sec
            Time until API will be auto-deleted. Use `INFINITE_TIMEOUT_SEC` for infinite.
        print_logs
            Subscribe to Cortex logs of the API and print them to stdout.
        n_retries
            Number of attempts to deploy until raising the failure. Retries only if the failure is not an application error.

        Returns
        -------
        get_result
            Deployed API get result information.

        """

        try:
            yield self.deploy_single(deployment, dir, deployment_timeout_sec, api_timeout_sec, print_logs, n_retries)

        finally:
            self.delete(deployment["name"])

    def deploy_batch_api_and_run_job(self,
                                     deployment,
                                     job_spec,
                                     dir,
                                     deployment_timeout_sec=CORTEX_DEFAULT_DEPLOYMENT_TIMEOUT,
                                     api_timeout_sec=CORTEX_DEFAULT_API_TIMEOUT,
                                     print_logs=False,
                                     n_retries=0) -> "CortexGetResult":

        with self.deploy_temporarily(
                deployment,
                dir=dir,
                deployment_timeout_sec=deployment_timeout_sec,
                api_timeout_sec=api_timeout_sec,
                print_logs=print_logs,
                n_retries=n_retries
        ) as get_result:

            logger.info(f'BatchAPI {deployment["name"]} deployed. Starting a job.')
            with create_always_retry_session() as session:
                job_id = None
                for retry_job_get in range(5):
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

                    time.sleep(WAIT_BEFORE_JOB_GET * 2 ** retry_job_get)  # Don't call job too early: https://gitter.im/cortexlabs/cortex?at=5f7fe4c01cbba72b63cb745f

                    if print_logs:
                        self._cortex_logs_print_async(deployment['name'], job_id)

                    job_status = JOB_STATUS_ENQUEUING
                    while job_status in (JOB_STATUS_ENQUEUING, JOB_STATUS_RUNNING):
                        job_result = self.get(deployment["name"], job_id)
                        job_status = job_result.status
                        time.sleep(30)

                    if job_status in (NOT_DEPLOYED_STATUS, JOB_STATUS_UNEXPECTED_ERROR, JOB_STATUS_ENQUEUED_FAILED):
                        # TODO request fixes improvements https://gitter.im/cortexlabs/cortex?at=5f7fe4c01cbba72b63cb745f
                        # Sleep in after job submission.
                        logger.warning(f'Job unexpectedly undeployed or failed with status {job_status}. Retrying with sleep.')
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
                return CortexGetResult(job_status, None, json_dict)

            first_json_dict = json_dict[0]
            if first_json_dict['spec']['kind'] == KIND_BATCH_API:
                # BatchAPI has no status
                return CortexGetResult(LIVE_STATUS, first_json_dict['endpoint'], first_json_dict)

            else:
                return CortexGetResult(first_json_dict['status']['status_code'], first_json_dict['endpoint'], first_json_dict)

        elif NOT_DEPLOYED in first_line:
            return CortexGetResult(NOT_DEPLOYED_STATUS, None, dict())

        else:
            raise ValueError(f"For api {name} with job_id {job_id} got unsupported Cortex output:\n{out}")

    def get_all(self) -> List["CortexGetAllStatus"]:
        input_in_case_endpoint_prompt = b"n\n"
        out = _verbose_command_wrapper([CORTEX_PATH, "get", f"--env={self.cortex_env}", "-o=json"], input=input_in_case_endpoint_prompt)
        json_dict = json.loads(out.strip())
        return [CortexGetAllStatus(e['spec']['name'], e['status']['status_code'] if e['spec']['kind'] == KIND_REALTIME_API else LIVE_STATUS, e) for e in json_dict]

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
            with subprocess.Popen(cmd_arr, stdout=subprocess.PIPE) as logs_sp:
                with io.TextIOWrapper(logs_sp.stdout, encoding="utf-8") as logs_out:
                    for line in logs_out:
                        print_line = remove_non_printable(line.rstrip('\n'))
                        if len(print_line) > 0:
                            logger.info(print_line)

        if job_id is None:
            cmd_arr = [CORTEX_PATH, "logs", name, f"--env={self.cortex_env}"]

        else:
            cmd_arr = [CORTEX_PATH, "logs", name, job_id, f"--env={self.cortex_env}"]

        worker = Thread(target=listen_on_logs, args=(cmd_arr,), daemon=True, name=f'api_{name}')
        worker.start()

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
def str_to_public_temp_file(string: str, filepath: str) -> str:
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
