import subprocess
import textwrap
import unittest

# from cortex_client import cortex_parse_get_all, CortexGetAllStatus, CortexClient
from cortex_serving_client.cortex_client import cortex_parse_get_all, CortexGetAllStatus, CortexClient


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

    def test_parse_get_deployed(self):
        out = textwrap.dedent(
            """
                status                 up-to-date   requested   last update   avg request   2XX   
                error (out of memory)  1            1           4m            -             -     

                metrics dashboard: https://xxxxxx.com/cloudwatch/home#dashboards:name=xxx-xx

                endpoint: http://xxxxysdyfasdf.elb.us-east-1.amazonaws.com/xxxljsdf
                curl: curl http://xxxxysdyfasdf.elb.us-east-1.amazonaws.com/xxxljsdf -X POST -H "Content-Type: application/json" -d @sample.json
        """
        )
        resutl = CortexClient._parse_get_deployed(out)
        self.assertEqual(resutl.status, "error (out of memory)")
        self.assertEqual(resutl.endpoint, None)

        out = textwrap.dedent(
            """
                status   up-to-date   requested   last update   avg request   2XX   
                live     1            1           4m            -             -     

                metrics dashboard: https://xxxxxx.com/cloudwatch/home#dashboards:name=xxx-xx

                endpoint: http://xxxxysdyfasdf.elb.us-east-1.amazonaws.com/xxxljsdf
                curl: curl http://xxxxysdyfasdf.elb.us-east-1.amazonaws.com/xxxljsdf -X POST -H "Content-Type: application/json" -d @sample.json
        """
        )
        resutl = CortexClient._parse_get_deployed(out)
        self.assertEqual(resutl.status, "live")
        self.assertEqual(resutl.endpoint, "http://xxxxysdyfasdf.elb.us-east-1.amazonaws.com/xxxljsdf")

    def test_subprocess_timeout(self):
        try:
            p = subprocess.run(['grep', 'test'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=1)
            print(p)

        except subprocess.TimeoutExpired as e:
            print(f'{e} with stdout: "{e.output}" and stderr: "{e.stderr}"')
