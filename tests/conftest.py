"""Shared pytest fixtures.

Working code, not a stub. Provides a `spark` fixture backed by a local Delta
-enabled SparkSession (see src/common/spark_session.py) so every test file in
this repo can request `spark` without any per-test setup.
"""

from __future__ import annotations

import shutil
import tempfile
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

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


# --------------------------------------------------------------------------- real data
# Full-data fixtures are built once per test session and shared by every test
# file. Tests that use them are marked `slow` (deselect with `-m "not slow"`).

REPO_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class BronzeRun:
    config: dict
    first: list
    second: list


def real_data_config(lakehouse_root: Path, run_id: str) -> dict:
    """The committed run config, pointed at a temp lakehouse, absolute source paths."""
    from src.common.config import load_config

    config = load_config(str(REPO_ROOT / "config" / "config.yaml"))
    config["lakehouse"] = {"local_root": str(lakehouse_root)}
    config["run_id"] = run_id
    config["sources"] = {
        name: {**src, "local_path": str(REPO_ROOT / src["local_path"])}
        for name, src in config["sources"].items()
    }
    return config


@pytest.fixture(scope="session")
def real_bronze(spark, tmp_path_factory) -> BronzeRun:
    """All 12 real files landed in bronze, then ingested a second time (idempotency)."""
    from src.bronze.ingest import ingest_all

    config = real_data_config(tmp_path_factory.mktemp("real") / "lakehouse", "test-real")
    first = ingest_all(spark, config)
    second = ingest_all(spark, config)
    return BronzeRun(config, first, second)


@pytest.fixture(scope="session")
def real_reference(spark, real_bronze):
    """Step 6 silver tables built once on the real bronze, with their own run id."""
    from src.silver.reference import build_reference

    config = {**real_bronze.config, "run_id": "test-silver"}
    results = {r.table: r for r in build_reference(spark, config)}
    return config, results


@pytest.fixture(scope="session")
def real_leads(spark, real_reference):
    """Step 7 silver leads and consent, built once on the real bronze (shared by step 8)."""
    from src.silver.leads import build_consent, build_leads

    config, _ = real_reference
    return config, build_leads(spark, config), build_consent(spark, config)
