import json
import logging
import subprocess
import time
from math import ceil
from typing import List

CORTEX_DEFAULT_COMMAND_SYSTEM_PROCESS_TIMEOUT = 3 * 60
CORTEX_CMD_BASE_RETRY_SEC = 5


logger = logging.getLogger(__name__)


def _verbose_command_wrapper(
        cmd_arr: List[str], cwd: str = None, timeout: int = CORTEX_DEFAULT_COMMAND_SYSTEM_PROCESS_TIMEOUT,
        input: bytes = None, retry_count=3, allow_non_0_return_code_on_stdout_sub_strs=None, sleep_base_retry_sec=CORTEX_CMD_BASE_RETRY_SEC
):
    cmd_str = " ".join(cmd_arr)
    message = ""
    for retry in range(retry_count + 1):
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

            else:
                message = f"Non zero return code for command {cmd_str}! Output: {json.dumps(dict(stdout=stdout, stderr=stderr))}"

        except subprocess.TimeoutExpired as e:
            message = f'Timed out command  {cmd_str}: {e} with stdout: "{e.output}" and stderr: "{e.stderr}"'

        if retry < retry_count:
            sleep_secs = ceil(sleep_base_retry_sec * 2 ** retry)
            logger.info(f"Retrying after {sleep_secs} sec: {message}")
            time.sleep(sleep_secs)

        else:
            raise ValueError(f"Retry count for command {cmd_str} exceeded: {message}")

    raise RuntimeError(f'Execution should never reach here: {message}')


