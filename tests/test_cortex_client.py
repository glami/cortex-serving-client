import subprocess
import unittest

from cortex_serving_client.cortex_client import cortex_parse_get_all, CortexGetAllStatus, _verbose_command_wrapper, \
    NOT_DEPLOYED_STATUS


class CortexClientTest(unittest.TestCase):
    def test_get_all_parsing(self):
        out = "\nno apis are deployed\n"
        self.assertEqual(cortex_parse_get_all(out), [])

        out = (
            "\n"
            "api              status     up-to-date   requested   last update   avg request   2XX  \n"
            "cat-trainer-27   updating   0            1           21s  \n"
        )
        self.assertEqual(cortex_parse_get_all(out), [CortexGetAllStatus("cat-trainer-27", "updating")])

        out = (
            "\n"
            "api              status                 up-to-date   requested   last update   avg request   2XX  \n"
            "cat-trainer-27   error (out of memory)  0            1           21s  \n"
        )
        self.assertEqual(cortex_parse_get_all(out), [CortexGetAllStatus("cat-trainer-27", "error (out of memory)")])

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
