from time import sleep
from typing import Callable, Tuple

import logging

import inspect

logger = logging.getLogger(__name__)


def retry_on_exception(
    fun: Callable, catched_exceptions: Tuple = (Exception,), max_retries: int = 3, starting_sleep_secs: float = 3.0
):
    ex = None
    fun_logger = None
    for retry in range(max_retries):
        try:
            return fun()

        except Exception as e:
            if isinstance(e, catched_exceptions):
                if not fun_logger:
                    fun_logger = logging.getLogger(inspect.getmodule(fun).__name__)

                ex = e
                sleep_secs = starting_sleep_secs * 2 ** retry
                fun_logger.info(f"Retrying {retry+1} time with sleep {sleep_secs} secs.")
                sleep(sleep_secs)

    raise RuntimeError(f"Too many retries ({max_retries}). Last exception: {ex}.") from ex
