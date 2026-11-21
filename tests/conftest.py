"""Shared pytest fixtures.

Working code, not a stub. Provides a `spark` fixture backed by a local Delta
-enabled SparkSession (see src/common/spark_session.py) so every test file in
this repo can request `spark` without any per-test setup.
"""

from __future__ import annotations

import shutil
import tempfile
from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession

from src.common.spark_session import get_spark


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """Session-scoped local Spark session with Delta Lake enabled.

    Uses a temporary warehouse directory so test runs never write into the
    candidate's real ./lakehouse/ and never leave state behind.
    """
    warehouse_dir = tempfile.mkdtemp(prefix="northgate-funnel-test-warehouse-")
    session = get_spark(app_name="northgate-funnel-tests", warehouse_dir=warehouse_dir)
    try:
        yield session
    finally:
        session.stop()
        shutil.rmtree(warehouse_dir, ignore_errors=True)
