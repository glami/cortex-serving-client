import subprocess
import unittest

from cortex_serving_client.cortex_client import _verbose_command_wrapper, \
    NOT_DEPLOYED_STATUS


class CortexClientTest(unittest.TestCase):

    def test_subprocess_timeout(self):
        try:
            p = subprocess.run(['grep', 'test'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=1)
            print(p)

        except subprocess.TimeoutExpired as e:
            print(f'{e} with stdout: "{e.output}" and stderr: "{e.stderr}"')

    def test_not_deployed(self):
        _verbose_command_wrapper(['cat', NOT_DEPLOYED_STATUS], allow_non_0_return_code_on_stdout_sub_strs=[NOT_DEPLOYED_STATUS], timeout=1, sleep_base_retry_sec=0.1)
        try:
            _verbose_command_wrapper(['cat', NOT_DEPLOYED_STATUS], timeout=1, sleep_base_retry_sec=0.1)
            self.fail()

        except ValueError as e:
            pass
