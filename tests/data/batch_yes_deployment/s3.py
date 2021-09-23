import logging
import os
from pathlib import Path

import boto3

# insert Cortex Serving Client bucket name
BUCKET_NAME = os.environ["CSC_BUCKET_NAME"]
BUCKET_SSE_KEY = os.environ['CSC_S3_SSE_KEY']

logger = logging.getLogger(__name__)


def _get_extra_args(sse_key):
    return {"SSECustomerAlgorithm": "AES256", "SSECustomerKey": sse_key}


def upload_file(local_filepath, s3_path, bucket_name=BUCKET_NAME, sse_key=BUCKET_SSE_KEY, verbose=True):
    local_filepath = str(local_filepath)
    s3_path = str(s3_path)

    if verbose:
        logger.info(f"Uploading local file {local_filepath} -> {bucket_name}:{s3_path}")

    session = boto3.session.Session()
    session.client("s3").upload_file(
        local_filepath, bucket_name, s3_path, ExtraArgs=_get_extra_args(sse_key)
    )


def upload_fileobj(fp, s3_path, bucket_name=BUCKET_NAME, sse_key=BUCKET_SSE_KEY, verbose=True):
    s3_path = str(s3_path)

    if verbose:
        logger.info(f"Uploading local file object -> {bucket_name}:{s3_path}")

    session = boto3.session.Session()
    session.client("s3").upload_fileobj(fp, bucket_name, s3_path, ExtraArgs=_get_extra_args(sse_key))


def download_fileobj(s3_path, fp, bucket_name=BUCKET_NAME, sse_key=BUCKET_SSE_KEY, verbose=True):
    s3_path = str(s3_path)

    if verbose:
        logger.info(f"Downloading file object from {bucket_name}:{s3_path}")

    session = boto3.session.Session()
    session.client("s3").download_fileobj(bucket_name, s3_path, fp, ExtraArgs=_get_extra_args(sse_key))


def download_file(s3_path, local_filepath, bucket_name=BUCKET_NAME, sse_key=BUCKET_SSE_KEY, overwrite=True):
    local_filepath = str(local_filepath)
    s3_path = str(s3_path)

    if Path(local_filepath).exists() and not overwrite:
        logger.info(f"{local_filepath} exists and overwrite=False, skipping")
    else:
        Path(local_filepath).parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"Downloading file from {bucket_name}:{s3_path} -> {local_filepath}")

        session = boto3.session.Session()
        session.client("s3").download_file(
            bucket_name, s3_path, local_filepath, ExtraArgs=_get_extra_args(sse_key)
        )
