COMPUTE_UNAVAILABLE_FAIL_TYPE = 'compute_unavailable'
DEPLOYMENT_TIMEOUT_FAIL_TYPE = 'deployment_timeout'
DEPLOYMENT_ERROR_FAIL_TYPE = 'deployment_error'
DEPLOYMENT_JOB_NOT_DEPLOYED_FAIL_TYPE = 'deployment_job_not_deployed'

FAILURE_TYPES = (COMPUTE_UNAVAILABLE_FAIL_TYPE, DEPLOYMENT_TIMEOUT_FAIL_TYPE, DEPLOYMENT_ERROR_FAIL_TYPE, DEPLOYMENT_JOB_NOT_DEPLOYED_FAIL_TYPE)


class DeploymentFailed(RuntimeError):

    def __init__(self, message: str, failure_type: str, deployment_name: dict, time_to_fail_sec: float):
        super().__init__(message)
        self.failure_type = failure_type
        self.deployment_name = deployment_name
        self.time_to_fail_sec = time_to_fail_sec
