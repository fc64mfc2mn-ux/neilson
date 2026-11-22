"""One command runs the whole pipeline: python -m src.run_pipeline --config config/config.yaml.

Reads the config file named on the command line (see src/common/config.py),
runs bronze ingestion for all twelve input files, builds silver, then builds
gold, in that order.

Every stage must run. A stage that is not built (NotImplementedError) or fails
stops the run with exit code 1; nothing is skipped (CLAUDE.md rule 11).
"""

from __future__ import annotations

import argparse
import logging
import sys
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import SparkSession

from src.bronze.ingest import ingest_all
from src.common.config import (
    ensure_secrets_file,
    load_config,
    load_secrets,
    resolve_lakehouse_root,
    resolve_source_path,
)
from src.common.logging_setup import get_logger
from src.common.spark_session import get_spark, spark_settings, stop_spark
from src.gold.apply_pii_policy import build_dim_lead
from src.gold.build_dimensions import build_dimensions
from src.gold.build_facts import build_facts
from src.gold.build_lead_funnel import build_lead_funnel
from src.silver.build_silver import build_silver

Stage = Callable[[SparkSession, dict], None]

STAGES: list[tuple[str, Stage]] = [
    ("bronze.ingest_all", ingest_all),
    ("silver.build_silver", build_silver),
    ("gold.build_dimensions", build_dimensions),
    ("gold.build_dim_lead", build_dim_lead),
    ("gold.build_facts", build_facts),
    ("gold.build_lead_funnel", build_lead_funnel),
]


class StageFailedError(RuntimeError):
    """A stage raised: the run stops there."""


def run_stages(
    spark: SparkSession, config: dict, stages: list[tuple[str, Stage]], log: logging.Logger
) -> None:
    """Run every stage in order; the first that is unbuilt or fails stops the run."""
    for name, stage in stages:
        log.info("Stage %s: start", name)
        try:
            stage(spark, config)
        except NotImplementedError as exc:
            raise StageFailedError(f"Stage {name} is not implemented") from exc
        except Exception as exc:
            raise StageFailedError(f"Stage {name} failed: {exc}") from exc
        log.info("Stage %s: done", name)


def new_run_id() -> str:
    """UTC timestamp plus a short random suffix, e.g. 20261121T170501Z-3f9a1c2b."""
    return f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Northgate Life funnel pipeline.")
    parser.add_argument("--config", required=True, help="Path to config/config.yaml")
    return parser.parse_args(argv)


def check_local_sources(config: dict) -> list[str]:
    """Return the names of local sources whose file does not exist."""
    if config["environment"] != "local":
        return []
    return [
        name for name in config["sources"] if not Path(resolve_source_path(config, name)).is_file()
    ]


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    run_id = new_run_id()
    log = get_logger(run_id)
    log.info("Pipeline start, config=%s", args.config)

    try:
        config = load_config(args.config)
        if ensure_secrets_file(config):
            log.info("Generated %s with a random PII hash salt", config["secrets_file"])
        load_secrets(config)  # fail fast on a missing or placeholder salt
    except Exception:
        log.exception("Could not load config or secrets")
        return 1

    missing = check_local_sources(config)
    if missing:
        log.error("Source files not found: %s", missing)
        return 1
    log.info("Environment=%s, %d sources configured", config["environment"], len(config["sources"]))

    config["run_id"] = run_id
    spark = get_spark(warehouse_dir=resolve_lakehouse_root(config), **spark_settings(config))
    spark.sparkContext.setLogLevel("WARN")
    log.info(
        "Spark %s started, session timezone=%s",
        spark.version,
        spark.conf.get("spark.sql.session.timeZone"),
    )

    try:
        run_stages(spark, config, STAGES, log)
    except Exception:
        log.exception("Pipeline failed")
        return 1
    finally:
        stop_spark(spark)

    log.info("Pipeline finished: all %d stages run", len(STAGES))
    return 0


if __name__ == "__main__":
    sys.exit(main())
