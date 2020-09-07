import contextlib
import json
import logging
import os
import re
import subprocess
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from threading import Thread
from typing import NamedTuple, Optional, Dict, List, Callable

import yaml
from psycopg2._psycopg import DatabaseError
from psycopg2.extras import NamedTupleCursor
from psycopg2.pool import ThreadedConnectionPool

"""
Details: https://www.cortex.dev/deployments/statuses
On some places custom "not_deployed" status may be used.
"""

CORTEX_STATUSES = ["live", "updating", "error", "error (out of memory)", "compute unavailable"]  #
CORTEX_DELETE_TIMEOUT_SEC = 10 * 60
CORTEX_DEPLOY_REPORTED_TIMEOUT_SEC = 60
CORTEX_DEFAULT_DEPLOYMENT_TIMEOUT = 20 * 60
CORTEX_DEFAULT_API_TIMEOUT = CORTEX_DEFAULT_DEPLOYMENT_TIMEOUT
INFINITE_TIMEOUT_SEC = 30 * 365 * 24 * 60 * 60  # 30 years
CORTEX_DEFAULT_COMMAND_SYSTEM_PROCESS_TIMEOUT = 3 * 60


logger = logging.getLogger(__name__)
__cortex_client_instance = None


class CortexClient:
    """
    An object used to execute commands on Cortex, maintain API state in the db to collect garbage.
    """

    def __init__(self, db_connection_pool: ThreadedConnectionPool, gc_interval_sec=30 * 60, cortex_env="aws"):
        self.db_connection_pool = db_connection_pool
        self._init_garbage_api_collector(gc_interval_sec)
        self.cortex_env = cortex_env

    def deploy_single(
        self,
        deployment: Dict,
        dir,
        deployment_timeout_sec=CORTEX_DEFAULT_DEPLOYMENT_TIMEOUT,
        api_timeout_sec=CORTEX_DEFAULT_API_TIMEOUT,
        print_logs=False,
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

        Returns
        -------
        get_result
            Deployed API get result information.

        """

        name = deployment["name"]
        predictor_yaml_str = yaml.dump([deployment], default_flow_style=False)

        if api_timeout_sec < deployment_timeout_sec:
            logger.warning(f'API timeout for {name} of {api_timeout_sec} is shorter than deployment timeout of {deployment_timeout_sec}. This may cause unintended garbage collection! Setting API timeout to deployment timeout.')
            api_timeout_sec = deployment_timeout_sec

        # file has to be created the dir to which python predictors are described in yaml
        filename = f"{name}.yaml"
        filepath = f"{dir}/{filename}"
        with str_to_public_temp_file(predictor_yaml_str, filepath):
            self._collect_garbage()
            gc_timeout_sec = api_timeout_sec + CORTEX_DELETE_TIMEOUT_SEC
            logger.info(f"Deployment {name} has deployment timeout: {deployment_timeout_sec} sec, garbage collection timeout: {gc_timeout_sec} seconds.")
            with self._open_db_cursor() as cursor:
                cursor.execute(
                    """
                    insert into cortex_api_timeout(api_name, ultimate_timeout) values (%s, transaction_timestamp() + %s * interval '1 second')
                    on conflict (api_name) do update
                        set ultimate_timeout = transaction_timestamp() + %s * interval '1 second', modified = transaction_timestamp()
                """,
                    [name, gc_timeout_sec, gc_timeout_sec],
                )

            _verbose_command_wrapper(["cortex", "deploy", filename, f"--env={self.cortex_env}"], cwd=dir)

        if print_logs:
            self._cortex_logs_print_async(name)

        start_time = time.time()
        while True:
            get_result = self.get(name)
            if get_result.status not in ("live", "updating"):
                # avoids boot loop
                self.delete(name)
                raise ValueError(f"Deployment {name} failed with status {get_result.status}.")

            elif get_result.status == "live":
                break

            if start_time + deployment_timeout_sec < time.time():
                # avoids boot loop
                self.delete(name)
                raise ValueError(f"Deployment {name} timeout after {deployment_timeout_sec} seconds.")

            logger.info(f"Sleeping during deployment of {name} until next retry. Current get_result: {get_result}.")
            time.sleep(10)

        return get_result

    @contextmanager
    def deploy_temporarily(
        self,
        deployment: Dict,
        dir,
        deployment_timeout_sec=CORTEX_DEFAULT_DEPLOYMENT_TIMEOUT,
        api_timeout_sec=CORTEX_DEFAULT_API_TIMEOUT,
        print_logs=False,
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

        Returns
        -------
        get_result
            Deployed API get result information.

        """

        try:
            yield self.deploy_single(deployment, dir, deployment_timeout_sec, api_timeout_sec, print_logs)

        finally:
            self.delete(deployment["name"])

    def postpone_api_timeout(self, name: str, timeout_timestamp: datetime):
        ultimate_timeout = timeout_timestamp + timedelta(seconds=CORTEX_DELETE_TIMEOUT_SEC)
        with self._open_db_cursor() as cursor:
            cursor.execute(
                "update cortex_api_timeout set ultimate_timeout = %s, modified = transaction_timestamp() where api_name = %s",
                [ultimate_timeout, name],
            )

    def raise_on_cluster_down(self):
        try:
            input_in_case_endpoint_prompt = b"n\n"
            _verbose_command_wrapper(["cortex", "get", f"--env={self.cortex_env}"], input=input_in_case_endpoint_prompt)

        except ValueError as e:
            raise ValueError("Cluster is likely down! Check the exception text") from e

    def get(self, name) -> "CortexGetResult":
        out = _verbose_command_wrapper(["cortex", "get", name, f"--env={self.cortex_env}"])
        lines = out.splitlines()
        if lines[1].split(" ")[0] == "status":
            return self._parse_get_deployed(out)

        elif "not deployed" in lines[1]:
            return CortexGetResult("not_deployed", None)

        else:
            raise ValueError(f"For api: {name} got unsupported Cortex output:\n{out}")

    @staticmethod
    def _parse_get_deployed(cmd_out: str):
        lines = cmd_out.splitlines()
        status = lines[2].split("  ")[0].strip()
        if status == "live":
            for line in lines:
                line_split = line.split(" ")
                if line_split[0] == "endpoint:":
                    url = line_split[1]
                    return CortexGetResult(status, url)

            raise ValueError(f"Unsupported Cortex output:\n{cmd_out}")

        return CortexGetResult(status, None)

    def get_all(self) -> List["CortexGetAllStatus"]:
        out = _verbose_command_wrapper(["cortex", "get", f"--env={self.cortex_env}"])
        return cortex_parse_get_all(out)

    def delete(self, name, force=False, timeout_sec=CORTEX_DELETE_TIMEOUT_SEC, cursor=None) -> "CortexGetResult":
        delete_arr = ["cortex", "delete", name, f"--env={self.cortex_env}"]
        if force:
            delete_arr.append("-f")

        accept_deletion_if_asked_input = b"y\n"
        self._open_cursor_if_none(cursor, lambda c: self._del_db_api_row(name, c))
        start_time = time.time()
        while True:
            get_result = self.get(name)
            if get_result.status == "not_deployed":
                return get_result

            else:
                _verbose_command_wrapper(delete_arr, input=accept_deletion_if_asked_input,
                                         allow_non_0_return_code_on_stdout_sub_strs=['not deployed'])

            if start_time + timeout_sec < time.time():
                raise ValueError(
                    f"Timeout after {timeout_sec} seconds. Attempted force delete, but not waiting for results."
                )

            logger.info(f"During delete of {name} sleeping until next retry. Current get_result: {get_result}.")
            time.sleep(10)

    def _cortex_logs_print_async(self, name):
        def listen_on_logs():
            os.system("cortex logs " + name + " " + f"--env={self.cortex_env}")

        worker = Thread(target=listen_on_logs, daemon=True)
        worker.start()

    @staticmethod
    def _del_db_api_row(name, cur):
        try:
            cur.execute("delete from cortex_api_timeout where api_name = %s", [name])

        except DatabaseError:
            logger.warning(f"Ignoring exception during cortex_api_timeout record for {name} deletion", exc_info=True)

    def _open_db_cursor(self):
        return open_pg_cursor(self.db_connection_pool)

    def _open_cursor_if_none(self, cursor, fun: Callable):
        if cursor is not None:
            fun(cursor)

        else:
            with self._open_db_cursor() as cursor:
                fun(cursor)

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
                    if self.get(api_name).status != "not_deployed":
                        logger.warning(f"Collecting Cortex garbage - timed-out deployed API: {api_name}")
                        self.delete(api_name, cursor=cur)

                    else:
                        logger.warning(f"Collecting Cortex garbage - timed-out db row: {api_name}")
                        self._del_db_api_row(api_name, cur)

                cur.execute("commit")

                deployed_api_names = [row.api for row in self.get_all()]

                # Remove recorded but not deployed - fast to avoid conflicts
                cur.execute(
                    "select api_name from cortex_api_timeout where modified + %s * interval '1 second' < transaction_timestamp() and not (api_name = ANY(%s))",
                    [CORTEX_DEPLOY_REPORTED_TIMEOUT_SEC, deployed_api_names],
                )
                if cur.rowcount > 0:
                    apis = [r.api_name for r in cur.fetchall()]
                    logger.warning(f"Collecting Cortex garbage - recorded but not deployed: {apis}")
                    cur.execute(
                        "delete from cortex_api_timeout where modified + %s * interval '1 second' < transaction_timestamp() and not (api_name = ANY(%s))",
                        [CORTEX_DEPLOY_REPORTED_TIMEOUT_SEC, deployed_api_names],
                    )
                    cur.execute("commit")

                # Remove deployed but not recorded
                cur.execute(
                    "select api_name from cortex_api_timeout", [CORTEX_DEPLOY_REPORTED_TIMEOUT_SEC],
                )
                recorded_apis_set = set([r.api_name for r in cur.fetchall()])
                for deployed_not_recorded_name in set(deployed_api_names).difference(recorded_apis_set):
                    logger.warning(f"Collecting Cortex garbage - deployed not recorded: {deployed_not_recorded_name}")
                    self.delete(deployed_not_recorded_name, cursor=cur)

        except Exception as e:
            logger.warning(f"Ignoring unexpected exception that occurred during Garbage Collection: {e}", exc_info=e)

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


def _verbose_command_wrapper(
        cmd_arr: List[str], cwd: str = None, timeout: int = CORTEX_DEFAULT_COMMAND_SYSTEM_PROCESS_TIMEOUT,
        input: bytes = None, retry_count=3, allow_non_0_return_code_on_stdout_sub_strs=None
):
    cmd_str = " ".join(cmd_arr)
    message = ""
    for retry in range(retry_count):
        try:
            p = subprocess.run(cmd_arr, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=cwd, timeout=timeout, input=input)
            stdout = p.stdout.decode()
            stderr = p.stderr.decode()
            # check_returncode() does not print process output to the console automatically so custom below
            if p.returncode == 0:
                logger.debug(f"Successful command {cmd_arr} stdout is {json.dumps(stdout)}")
                return stdout

            elif allow_non_0_return_code_on_stdout_sub_strs is not None and any([s in stdout or s in stderr for s in allow_non_0_return_code_on_stdout_sub_strs]):
                logger.warning(f"Allowed unsuccessful command {cmd_arr} execution. Stdout {json.dumps(stdout)} or stderr {json.dumps(stderr)} matches one of {allow_non_0_return_code_on_stdout_sub_strs}")
                return stdout

            message = f"Non zero return code for command {cmd_str}! Stdout:\n{json.dumps(stdout)}"
            if retry <= retry_count:
                logger.info(message)

        except subprocess.TimeoutExpired as e:
            timeout_message = f'{e} with stdout: "{e.output}" and stderr: "{e.stderr}"'
            if retry <= retry_count:
                logger.info(f"Retrying command {cmd_str} ignoring exception: " + timeout_message)

            else:
                raise ValueError(f"Retry count for command {cmd_str} exceeded: " + timeout_message) from e

        time.sleep(3)

    raise ValueError(f"Retry count for command {cmd_str} exceeded: " + message)


class CortexGetAllStatus(NamedTuple):
    api: str
    status: str


def cortex_parse_get_all(out: str) -> List[CortexGetAllStatus]:
    if "no apis are deployed" in out:
        return []

    lines = out.splitlines()

    header_split = re.split(r"\s+", lines[1])
    if header_split[0] == "api" and header_split[1] == "status":
        result = []
        for row_line in lines[2:]:
            row = re.split(r"\s{2,}", row_line)
            result.append(CortexGetAllStatus(row[0].strip(), row[1].strip()))

        return result

    else:
        raise ValueError(f"Cannot parse output:\n{out}")


class CortexGetResult(NamedTuple):
    status: str
    endpoint: Optional[str]


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
