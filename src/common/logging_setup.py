"""Structured logging with a run id.

Required by R2 platform-engineering scope: error handling, logging,
monitoring and alerting at the engineering layer. Every pipeline run must log
through a single configured logger that tags every record with a run id, so
a single run's log lines can be filtered out of a shared log stream.
"""

from __future__ import annotations

import logging


def get_logger(run_id: str) -> logging.Logger:
    raise NotImplementedError
