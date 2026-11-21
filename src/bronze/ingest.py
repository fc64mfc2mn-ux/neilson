"""Land all twelve input files into bronze.

Bronze is append-only, all columns typed as string, plus `_ingest_ts`,
`_source_file`, `_row_hash` and `_source_row` (the record's position in the
file, from 1: data row n of a CSV is line n + 1, after the header). No
cleansing in bronze. Produces
`bronze.leads_raw`, `bronze.consent_events_raw`, `bronze.calls_raw`,
`bronze.policies_raw`, `bronze.marketing_spend_raw`, `bronze.campaigns_raw`,
`bronze.agents_raw`, `bronze.fx_rates_raw`, `bronze.ref_channel_raw`,
`bronze.ref_region_raw` and `bronze.sec_user_region_raw`. `bronze.calls_raw`
carries both call files, with `_source_file` distinguishing them.

Idempotent reruns: each landing is a batch with one `_ingest_ts`. A content
fingerprint of every batch is recorded in `bronze.ingest_log`. If a file's
fingerprint equals the one most recently landed for that file, nothing is
appended. A changed file appends a new batch. Silver reads the latest batch
per source file with `latest_batch`. Bronze is never dropped or overwritten.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, LongType, MapType, StructField, StructType

from src.common.config import resolve_source_path
from src.common.tables import append_table, read_table, table_exists

log = logging.getLogger("northgate.bronze")

# Source name -> bronze table. The two call files share bronze.calls_raw.
BRONZE_TABLES = {
    "dim_campaign": "bronze.campaigns_raw",
    "marketing_spend_daily": "bronze.marketing_spend_raw",
    "leads": "bronze.leads_raw",
    "consent_events": "bronze.consent_events_raw",
    "calls_uk_au": "bronze.calls_raw",
    "calls_ca_us": "bronze.calls_raw",
    "dim_agent": "bronze.agents_raw",
    "policies": "bronze.policies_raw",
    "fx_rates_monthly": "bronze.fx_rates_raw",
    "ref_channel": "bronze.ref_channel_raw",
    "ref_region": "bronze.ref_region_raw",
    "sec_user_region": "bronze.sec_user_region_raw",
}
INGEST_LOG = "bronze.ingest_log"
NULL_SENTINEL = "\\N"
FIELD_SEPARATOR = "\x1f"  # ASCII unit separator: cannot appear in the source text


@dataclass(frozen=True)
class IngestResult:
    source_name: str
    table: str
    rows_read: int
    rows_appended: int
    skipped: bool


def to_all_strings(df: DataFrame) -> DataFrame:
    """Cast every column to string; nested JSON values are kept as JSON text."""
    cols = []
    for field in df.schema.fields:
        col = F.col(f"`{field.name}`")
        if isinstance(field.dataType, ArrayType | MapType | StructType):
            col = F.to_json(col)
        cols.append(col.cast("string").alias(field.name))
    return df.select(*cols)


def add_bronze_metadata(
    df: DataFrame, source_file: str, ingest_ts: datetime | None = None
) -> DataFrame:
    """Add `_ingest_ts`, `_source_file` and `_row_hash`.

    `_row_hash` is SHA-256 over every source column, in file order, joined with a
    separator that cannot occur in the data and with nulls encoded as a sentinel.
    So identical rows hash identically, and a null and an empty string differ.
    `_source_row` is not hashed, so a row's hash does not depend on its position.
    """
    ts = ingest_ts or datetime.now(UTC).replace(tzinfo=None)
    source_cols = [c for c in df.columns if c != "_source_row"]
    values = [F.coalesce(F.col(f"`{c}`"), F.lit(NULL_SENTINEL)) for c in source_cols]
    return (
        df.withColumn("_ingest_ts", F.lit(ts).cast("timestamp"))
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_row_hash", F.sha2(F.concat_ws(FIELD_SEPARATOR, *values), 256))
    )


def read_source(spark: SparkSession, config: dict, source_name: str) -> DataFrame:
    """Read one source file with its configured options, every column a string."""
    spec = config["sources"][source_name]
    path = resolve_source_path(config, source_name)
    options = {str(k): str(v).lower() if isinstance(v, bool) else str(v)
               for k, v in spec.get("options", {}).items()}  # fmt: skip
    fmt = spec["format"]
    if fmt == "csv":
        options["inferSchema"] = "false"  # bronze never types values
        return spark.read.options(**options).csv(path)
    if fmt == "json":
        options["primitivesAsString"] = "true"
        return to_all_strings(spark.read.options(**options).json(path))
    raise ValueError(f"Unsupported format {fmt!r} for source {source_name!r}")


def add_source_row(df: DataFrame) -> DataFrame:
    """Add `_source_row`: 1-based record position in file order.

    Must be applied straight after the read, before any shuffle. `zipWithIndex`
    numbers rows by partition, then within partition, and a single file's
    partitions follow its byte order, so the number is the file order. The tests
    check it against an independent read of the file.
    """
    schema = StructType([*df.schema.fields, StructField("_source_row", LongType(), False)])
    numbered = df.rdd.zipWithIndex().map(lambda pair: (*pair[0], pair[1] + 1))
    return df.sparkSession.createDataFrame(numbered, schema)


def batch_fingerprint(df: DataFrame) -> str:
    """Order-insensitive SHA-256 of a batch's row hashes (duplicates count)."""
    row = df.agg(
        F.sha2(F.concat_ws(",", F.sort_array(F.collect_list("_row_hash"))), 256).alias("fp")
    ).first()
    return row.fp


def latest_batch(df: DataFrame) -> DataFrame:
    """Rows from the most recent landing of each source file."""
    w = Window.partitionBy("_source_file")
    return (
        df.withColumn("_latest_ts", F.max("_ingest_ts").over(w))
        .where(F.col("_ingest_ts") == F.col("_latest_ts"))
        .drop("_latest_ts")
    )


def _last_fingerprint(spark: SparkSession, config: dict, source_file: str) -> str | None:
    if not table_exists(spark, config, INGEST_LOG):
        return None
    row = (
        read_table(spark, config, INGEST_LOG)
        .where(F.col("source_file") == source_file)
        .orderBy(F.col("ingest_ts").desc())
        .select("fingerprint")
        .first()
    )
    return row.fingerprint if row else None


def ingest_source(spark: SparkSession, config: dict, source_name: str) -> IngestResult:
    """Land one source file into its bronze table, unless it is unchanged."""
    table = BRONZE_TABLES[source_name]
    source_file = Path(resolve_source_path(config, source_name)).name
    ingest_ts = datetime.now(UTC).replace(tzinfo=None)

    raw = add_source_row(read_source(spark, config, source_name))
    batch = add_bronze_metadata(raw, source_file, ingest_ts)
    batch = batch.cache()
    rows_read = batch.count()
    fingerprint = batch_fingerprint(batch)

    if fingerprint == _last_fingerprint(spark, config, source_file):
        batch.unpersist()
        log.info("%s: unchanged (%d rows), nothing appended to %s", source_file, rows_read, table)
        return IngestResult(source_name, table, rows_read, 0, True)

    append_table(spark, batch, config, table, merge_schema=True)
    batch.unpersist()
    entry = spark.createDataFrame(
        [
            (
                source_name,
                source_file,
                table,
                fingerprint,
                rows_read,
                ingest_ts,
                config.get("run_id", ""),
            )
        ],  # fmt: skip
        "source_name string, source_file string, bronze_table string, fingerprint string, "
        "row_count long, ingest_ts timestamp, run_id string",
    )
    append_table(spark, entry, config, INGEST_LOG)
    log.info("%s: appended %d rows to %s", source_file, rows_read, table)
    return IngestResult(source_name, table, rows_read, rows_read, False)


def ingest_all(spark: SparkSession, config: dict) -> list[IngestResult]:
    """Land every configured source. Call files go in a fixed order (UK/AU first)."""
    unknown = sorted(set(config["sources"]) - set(BRONZE_TABLES))
    if unknown:
        raise ValueError(f"No bronze table mapped for sources: {unknown}")
    return [ingest_source(spark, config, name) for name in BRONZE_TABLES]
