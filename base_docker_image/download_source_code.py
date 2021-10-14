import logging
import os
import subprocess
import sys
from pathlib import Path

import boto3
import zipfile

BUCKET_NAME = os.environ["CSC_BUCKET_NAME"]
S3_PATH = os.environ["CSC_S3_SOURCE_ZIP_PATH"]
S3_SSE_KEY = os.environ["CSC_S3_SSE_KEY"]
UVICORN_PORT = os.environ["CSC_UVICORN_PORT"]  # CORTEX_* env vars are reserved for cortex
DEPLOYMENT_DIR = "cortex_deployment"
EXIT_CODE_ERROR = 4

ADDITIONAL_BUCKET_KWARGS = {
    BUCKET_NAME: {"SSECustomerAlgorithm": "AES256", "SSECustomerKey": S3_SSE_KEY},
}

logging.basicConfig(
        format="%(asctime)s : %(levelname)s : %(threadName)-10s : %(name)s : %(message)s", level=logging.INFO,
)

logger = logging.getLogger(__name__)


def download_file(s3_path, local_filepath, bucket_name, overwrite=True):
    local_filepath = str(local_filepath)
    s3_path = str(s3_path)

    if Path(local_filepath).exists() and not overwrite:
        logger.info(f"{local_filepath} exists and overwrite=False, skipping")
    else:
        Path(local_filepath).parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"Downloading file from {bucket_name}:{s3_path} -> {local_filepath}")

        session = boto3.session.Session()
        session.client("s3").download_file(
            bucket_name, s3_path, local_filepath, ExtraArgs=ADDITIONAL_BUCKET_KWARGS[bucket_name]
        )


def unzip(filepath, targetdir):
    logger.info(f"Unzipping {filepath} -> {targetdir} ...")
    with zipfile.ZipFile(filepath, "r") as zip_ref:
        zip_ref.extractall(targetdir)


def run_cmd(cmd):
    logger.info(f"Running {cmd} ...")
    os.system(cmd)


def run_with_error_on_completion(cmd):
    logger.info(f"Running {cmd} ...")
    try:
        proc = subprocess.Popen(cmd, shell=True)
        proc.wait()
    finally:
        logger.info(f"Uvicorn process ended, exiting with code {EXIT_CODE_ERROR}!")
        sys.exit(EXIT_CODE_ERROR)


def main():
    # download code.zip
    local_path = "code.zip"
    download_file(S3_PATH, local_path, BUCKET_NAME, overwrite=True)
    # unzip it into cortex_deployment dir
    unzip(local_path, DEPLOYMENT_DIR)
    # set workdir to cortex_deployment
    os.chdir(str(Path.cwd() / DEPLOYMENT_DIR))
    # set PYTHONPATH=/cortex_deployment
    os.environ["PYTHONPATH"] = str(Path.cwd())
    # install requirements
    run_cmd(f"pip install -r requirements.txt")
    # run app
    run_with_error_on_completion(f"uvicorn --no-access-log --workers 1 --host 0.0.0.0 --port {UVICORN_PORT} "
                                 f"--proxy-headers --forwarded-allow-ips '*' main:app")


if __name__ == "__main__":
    main()
