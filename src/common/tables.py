"""Path-based Delta tables under the lakehouse root, registered by name.

Spark's default catalog is in-memory, so a table saved only by name would be
forgotten by the next process. Every table is therefore stored at
`<lakehouse root>/<layer>/<table>` and registered in the session catalog as
`<layer>.<table>` on each write or read, so both `spark.table("bronze.x")` and
path reads work across runs.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from src.common.config import resolve_lakehouse_root


def _split(name: str) -> tuple[str, str]:
    layer, _, table = name.partition(".")
    if not layer or not table or "." in table:
        raise ValueError(f"Table name must be <layer>.<table>, got {name!r}")
    return layer, table


def table_path(config: dict, name: str) -> str:
    layer, table = _split(name)
    root = resolve_lakehouse_root(config)
    if "://" in root:
        return f"{root.rstrip('/')}/{layer}/{table}"
    return str(Path(root).resolve() / layer / table)


def table_exists(spark: SparkSession, config: dict, name: str) -> bool:
    from delta.tables import DeltaTable

    return DeltaTable.isDeltaTable(spark, table_path(config, name))


def register_table(spark: SparkSession, config: dict, name: str) -> None:
    """(Re)point the catalog entry `<layer>.<table>` at the Delta path."""
    layer, _ = _split(name)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {layer}")
    location = table_path(config, name)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {name} USING DELTA LOCATION '{location}'")


def append_table(
    spark: SparkSession, df: DataFrame, config: dict, name: str, merge_schema: bool = False
) -> None:
    """Append to a Delta table, creating it if needed. Never overwrites."""
    writer = df.write.format("delta").mode("append")
    if merge_schema:
        writer = writer.option("mergeSchema", "true")
    writer.save(table_path(config, name))
    register_table(spark, config, name)


def overwrite_table(spark: SparkSession, df: DataFrame, config: dict, name: str) -> None:
    """Replace a Delta table's contents (silver and gold rebuilds, not bronze)."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(table_path(config, name))
    )
    register_table(spark, config, name)


def read_table(spark: SparkSession, config: dict, name: str) -> DataFrame:
    register_table(spark, config, name)
    return spark.read.format("delta").load(table_path(config, name))
