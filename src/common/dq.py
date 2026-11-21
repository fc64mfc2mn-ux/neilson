"""Data-quality exception handling shared by silver builders.

Every rejected row lands in `silver.dq_exception` with `check_name`,
`source_table`, `key_value`, `severity` and `detected_at_utc`. This module
must provide the one function every silver builder calls to write a rejected
row to that table, so the same five columns are populated the same way every
time, and the CI data-quality gate has one table to read.
"""

from __future__ import annotations

from pyspark.sql import DataFrame


def write_dq_exception(
    df: DataFrame,
    check_name: str,
    source_table: str,
    key_column: str,
    severity: str,
) -> None:
    raise NotImplementedError
