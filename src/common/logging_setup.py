"""Structured logging with a run id.

Required by R2 platform-engineering scope: error handling, logging,
monitoring and alerting at the engineering layer. Every pipeline run logs
through a single configured logger that tags every record with a run id, so
a single run's log lines can be filtered out of a shared log stream.
"""

from __future__ import annotations

import logging
import sys
import time

LOGGER_NAME = "northgate"
LOG_FORMAT = "%(asctime)sZ %(levelname)s run_id=%(run_id)s %(name)s: %(message)s"
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"


class _RunIdFilter(logging.Filter):
    """Stamps every record with the run id."""

    def __init__(self, run_id: str) -> None:
        super().__init__()
        self.run_id = run_id

    def filter(self, record: logging.LogRecord) -> bool:
        record.run_id = self.run_id
        return True


def get_logger(run_id: str) -> logging.Logger:
    """Return the pipeline logger, writing UTC timestamps and the run id to stderr.

    Calling it again replaces the handler, so records are never duplicated and
    always carry the latest run id. Child loggers (`northgate.bronze`, ...)
    inherit the handler.
    """
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    for handler in list(logger.handlers):
        logger.removeHandler(handler)

    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)
    formatter.converter = time.gmtime

    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(formatter)
    handler.addFilter(_RunIdFilter(run_id))
    logger.addHandler(handler)
    return logger
