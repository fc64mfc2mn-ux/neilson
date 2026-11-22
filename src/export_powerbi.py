"""Export gold to CSV for Power BI: python -m src.export_powerbi --config config/config.yaml

Also the last stage of `python -m src.run_pipeline`. Writes one file per gold
table to `export.powerbi_dir` (default `export/powerbi/`, gitignored), named as
the table: `gold.dim_lead` -> `dim_lead.csv`. Every dimension, fact and bridge
is exported, plus `dim_lead_nopii` (`EXPORT_TABLES`, the same list the PII scan
covers).

Format: UTF-8 without BOM, comma-delimited, a header row in gold column order,
`\\n` line endings, minimal quoting. Nulls are empty. Dates `yyyy-MM-dd`;
timestamps `yyyy-MM-dd HH:mm:ss`, UTC (formatted in the UTC Spark session,
never through host-local Python datetimes). Integers and keys as plain digits;
decimals as plain text (never scientific notation); booleans `true`/`false`.
Rows are sorted on every column, so a second export is byte-identical.

No column the data classification keeps out of gold (`RAW_PII_COLUMNS`) may
be exported: the export stops if one appears. Health flags and hashes do reach
`dim_lead.csv` (classified tier 3 and 2); `dim_lead_nopii.csv` has neither.
"""

from __future__ import annotations

import argparse
import csv
import io
import logging
import sys
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.common.dq import ReconciliationError
from src.common.tables import read_table, table_exists
from src.gold.apply_pii_policy import GOLD_TABLES

log = logging.getLogger("northgate.export")

EXPORT_TABLES = GOLD_TABLES
DEFAULT_DIR = "export/powerbi"
# Kept out of gold by the PII policy (docs/DATA_CLASSIFICATION.md, "Not in gold at all").
RAW_PII_COLUMNS = (
    "first_name", "last_name", "email", "phone", "postcode", "date_of_birth",
    "consent_timestamp_utc", "consent_source", "call_notes", "recording_url",
    "underwriting_decision", "agent_name", "agent_email", "manager_name", "hire_date", "fte",
)  # fmt: skip


def export_dir(config: dict) -> Path:
    return Path(config.get("export", {}).get("powerbi_dir", DEFAULT_DIR))


def _as_text(name: str, dtype: T.DataType) -> Column:
    c = F.col(name)
    if isinstance(dtype, T.DateType):
        return F.date_format(c, "yyyy-MM-dd")
    if isinstance(dtype, T.TimestampType):
        return F.date_format(c, "yyyy-MM-dd HH:mm:ss")  # session timezone is UTC
    if isinstance(dtype, T.BooleanType):
        return F.when(c, F.lit("true")).when(~c, F.lit("false"))
    if isinstance(dtype, T.FloatType | T.DoubleType):
        # Fixed point, then trailing zeros off: 87.5 -> "87.5", 90.0 -> "90".
        fixed = c.cast("decimal(38,10)").cast("string")
        return F.regexp_replace(F.regexp_replace(fixed, r"(\.\d*?)0+$", "$1"), r"\.$", "")
    return c.cast("string")  # strings, integers (plain digits), decimals (plain text)


def to_csv_text(df: DataFrame) -> str:
    """The whole table as deterministic CSV text (header, sorted rows, nulls empty)."""
    text_cols = [_as_text(f.name, f.dataType).alias(f.name) for f in df.schema.fields]
    rows = [tuple("" if v is None else v for v in r) for r in df.select(*text_cols).collect()]
    rows.sort()
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
    writer.writerow(df.columns)
    writer.writerows(rows)
    return buf.getvalue()


def export_powerbi(spark: SparkSession, config: dict) -> dict[str, int]:
    """Write every table in EXPORT_TABLES; return rows written per table."""
    missing = [t for t in EXPORT_TABLES if not table_exists(spark, config, t)]
    if missing:
        raise ReconciliationError(f"Power BI export: gold tables not built: {missing}")
    out = export_dir(config)
    out.mkdir(parents=True, exist_ok=True)
    wanted = {f"{t.split('.')[1]}.csv" for t in EXPORT_TABLES}
    for stale in out.glob("*.csv"):
        if stale.name not in wanted:
            stale.unlink()  # one file per gold table, nothing left from older layouts
    written: dict[str, int] = {}
    for table in EXPORT_TABLES:
        df = read_table(spark, config, table)
        raw = sorted(set(df.columns) & set(RAW_PII_COLUMNS))
        if raw:
            raise ReconciliationError(f"Power BI export: {table} has raw PII columns {raw}")
        text = to_csv_text(df)
        rows = text.count("\n") - 1
        expected = df.count()
        if rows != expected:  # a value with an embedded newline would break this count
            raise ReconciliationError(f"Power BI export: {table} wrote {rows} != {expected}")
        target = out / f"{table.split('.')[1]}.csv"
        tmp = target.with_suffix(".csv.tmp")
        tmp.write_text(text, encoding="utf-8", newline="")
        tmp.replace(target)
        written[table] = rows
    size = sum(p.stat().st_size for p in out.glob("*.csv"))
    log.info("Exported %d gold tables to %s (%d bytes)", len(written), out, size)
    return written


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Export gold to CSV for Power BI.")
    parser.add_argument("--config", required=True)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    from src.common.config import load_config, resolve_lakehouse_root
    from src.common.spark_session import get_spark, spark_settings, stop_spark

    try:
        config = load_config(args.config)
        spark = get_spark(warehouse_dir=resolve_lakehouse_root(config), **spark_settings(config))
    except Exception:
        log.exception("Power BI export could not start")
        return 1
    try:
        spark.sparkContext.setLogLevel("ERROR")
        written = export_powerbi(spark, config)
        print(f"Exported {len(written)} tables to {export_dir(config).resolve()}")
        return 0
    except Exception:
        log.exception("Power BI export failed")
        return 1
    finally:
        stop_spark(spark)


if __name__ == "__main__":
    sys.exit(main())
