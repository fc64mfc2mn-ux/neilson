"""Data-quality exception handling shared by silver builders.

Every rejected row lands in `silver.dq_exception` with `check_name`,
`source_table`, `key_value`, `severity` and `detected_at_utc`, written by the
one function every silver builder calls, so the five columns are populated the
same way every time and the CI data-quality gate has one table to read.

Four columns are added after the contract five (`check_name` is the rule name):
- `defect_id`: the defect's number in docs/DEFECT_REGISTER.md. Required and
  validated, so every exception traces to a registered defect; a new kind of
  problem is added to the register before a check can report it.
- `run_id`: the pipeline run that raised the exception. Silver is rebuilt on
  every run, so the table keeps history and readers filter to one run.
- `action`: `quarantined` (row removed from silver), `flagged` (row kept, issue
  recorded) or `corrected` (row kept with a documented fix). Only quarantined
  rows count in the reconciliation rows in = rows out + rows quarantined.
- `detail`: optional free text per row (for example the conflicting values).
"""

from __future__ import annotations

import logging
import re
from functools import cache
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.tables import append_table, read_table, table_exists

log = logging.getLogger("northgate.dq")

DQ_TABLE = "silver.dq_exception"
CONTRACT_COLUMNS = ("check_name", "source_table", "key_value", "severity", "detected_at_utc")
SEVERITIES = ("info", "warning", "error")
ACTIONS = ("quarantined", "flagged", "corrected")
NULL_KEY = "<null>"
KEY_SEPARATOR = "|"
DQ_SCHEMA = (
    "check_name string, source_table string, key_value string, severity string, "
    "detected_at_utc timestamp, defect_id int, run_id string, action string, detail string"
)
DEFECT_REGISTER = Path(__file__).resolve().parents[2] / "docs" / "DEFECT_REGISTER.md"


class ReconciliationError(AssertionError):
    """Rows in do not equal rows out plus rows quarantined."""


@cache
def defect_ids() -> frozenset[int]:
    """Defect numbers listed in the table in docs/DEFECT_REGISTER.md."""
    text = DEFECT_REGISTER.read_text(encoding="utf-8")
    return frozenset(int(n) for n in re.findall(r"^\| (\d+) \|", text, flags=re.MULTILINE))


def _key_expr(key_column: str | list[str]) -> Column:
    columns = [key_column] if isinstance(key_column, str) else list(key_column)
    parts = [F.coalesce(F.col(c).cast("string"), F.lit(NULL_KEY)) for c in columns]
    return parts[0] if len(parts) == 1 else F.concat_ws(KEY_SEPARATOR, *parts)


def ensure_dq_table(spark: SparkSession, config: dict) -> None:
    """Create an empty silver.dq_exception if it does not exist yet."""
    if not table_exists(spark, config, DQ_TABLE):
        append_table(spark, spark.createDataFrame([], DQ_SCHEMA), config, DQ_TABLE)


def write_dq_exception(
    df: DataFrame,
    check_name: str,
    source_table: str,
    key_column: str | list[str],
    severity: str,
    *,
    defect_id: int,
    config: dict,
    action: str = "quarantined",
    detail_col: str | None = None,
) -> int:
    """Append one exception row per row of `df`. Returns the number written.

    `defect_id` must be a defect number in docs/DEFECT_REGISTER.md.
    `key_column` may be a list for composite keys (values joined with `|`);
    a null key is written as `<null>`. `detail_col` names an optional column of
    `df` copied into `detail`.
    """
    if severity not in SEVERITIES:
        raise ValueError(f"severity must be one of {SEVERITIES}, got {severity!r}")
    if defect_id not in defect_ids():
        raise ValueError(f"defect_id {defect_id!r} is not a defect in DEFECT_REGISTER.md")
    if action not in ACTIONS:
        raise ValueError(f"action must be one of {ACTIONS}, got {action!r}")
    keys = [key_column] if isinstance(key_column, str) else list(key_column)
    needed = keys + ([detail_col] if detail_col else [])
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise ValueError(f"{source_table}: columns {missing} not present for check {check_name}")

    spark = df.sparkSession
    ensure_dq_table(spark, config)
    exceptions = df.select(
        F.lit(check_name).alias("check_name"),
        F.lit(source_table).alias("source_table"),
        _key_expr(key_column).alias("key_value"),
        F.lit(severity).alias("severity"),
        F.current_timestamp().alias("detected_at_utc"),
        F.lit(defect_id).cast("int").alias("defect_id"),
        F.lit(config.get("run_id", "")).alias("run_id"),
        F.lit(action).alias("action"),
        (F.col(detail_col).cast("string") if detail_col else F.lit(None).cast("string")).alias(
            "detail"
        ),
    ).cache()
    count = exceptions.count()
    if count:
        append_table(spark, exceptions, config, DQ_TABLE)
        log.info(
            "DQ defect %d %s on %s: %d rows %s (%s)",
            defect_id,
            check_name,
            source_table,
            count,
            action,
            severity,
        )
    exceptions.unpersist()
    return count


def read_dq_exceptions(spark: SparkSession, config: dict, all_runs: bool = False) -> DataFrame:
    """Exceptions for the current run (`config['run_id']`), or every run."""
    ensure_dq_table(spark, config)
    df = read_table(spark, config, DQ_TABLE)
    return df if all_runs else df.where(F.col("run_id") == config.get("run_id", ""))


def quarantined_count(spark: SparkSession, config: dict, source_table: str) -> int:
    """Rows of `source_table` quarantined in the current run."""
    return (
        read_dq_exceptions(spark, config)
        .where((F.col("source_table") == source_table) & (F.col("action") == "quarantined"))
        .count()
    )


def assert_reconciled(
    source_table: str, rows_in: int, rows_out: int, rows_quarantined: int
) -> None:
    """Raise unless rows in = rows out + rows quarantined."""
    if rows_in != rows_out + rows_quarantined:
        raise ReconciliationError(
            f"{source_table}: {rows_in} in != {rows_out} out + {rows_quarantined} quarantined"
        )
    log.info(
        "Reconciled %s: %d in = %d out + %d quarantined",
        source_table,
        rows_in,
        rows_out,
        rows_quarantined,
    )
