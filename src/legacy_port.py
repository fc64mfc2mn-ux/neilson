"""Run the legacy report two ways: the naive translation and the corrected port.

- `naive_summary`: src/sql/legacy_sp_marketing_lead_summary_naive.sql over raw-shaped
  sources (`raw_sources`: bronze, latest batch, typed as the SQL Server tables and
  otherwise untouched: no trimming, no dedup, no FX, no minor-unit fix).
- `ported_summary`: src/sql/port_sp_marketing_lead_summary.sql over gold.

GETDATE() is the `as_of` parameter (a timestamp, default 2026-06-30 00:00:00) so both
are reproducible; `region` is @RegionCode (None = all). `register_results` makes the
temporary views the named queries in src/sql/findings_legacy.sql read. The behaviour
differences are in docs/LEGACY_PORT.md.
"""

from __future__ import annotations

import uuid
from pathlib import Path
from string import Template

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.bronze.ingest import latest_batch
from src.common.tables import read_table

SQL_DIR = Path(__file__).resolve().parent / "sql"
NAIVE_SQL = SQL_DIR / "legacy_sp_marketing_lead_summary_naive.sql"
PORTED_SQL = SQL_DIR / "port_sp_marketing_lead_summary.sql"
DEFAULT_AS_OF = "2026-06-30 00:00:00"
ISO_UTC = "yyyy-MM-dd'T'HH:mm:ssX"
RAW_NAMES = ("leads", "calls", "policies", "spend", "ref_region")
GOLD_NAMES = ("fact_marketing_spend_daily", "fact_lead", "fact_policy", "bridge_lead_call",
              "dim_date", "dim_region", "dim_channel")  # fmt: skip


def _blank(c: str):
    """Empty CSV field -> NULL, as a SQL Server load would; no trimming."""
    return F.when(F.col(c) == "", None).otherwise(F.col(c))


def raw_sources(spark: SparkSession, config: dict) -> dict[str, DataFrame]:
    """The procedure's five tables, from bronze (latest batch), SQL Server types."""
    b = lambda t: latest_batch(read_table(spark, config, t))  # noqa: E731
    return {
        "leads": b("bronze.leads_raw").select(
            _blank("lead_id").alias("LeadID"),
            _blank("region_code").alias("RegionCode"),
            _blank("channel_code").alias("ChannelCode"),
            _blank("lead_status").alias("LeadStatus"),
            F.to_timestamp("created_at_utc", ISO_UTC).alias("CreatedAt"),
        ),  # fmt: skip
        "calls": b("bronze.calls_raw").select(
            _blank("call_id").alias("CallID"), _blank("lead_id").alias("LeadID")
        ),
        "policies": b("bronze.policies_raw").select(
            _blank("policy_id").alias("PolicyID"),
            _blank("lead_id").alias("LeadID"),
            _blank("policy_status").alias("PolicyStatus"),
        ),  # fmt: skip
        "spend": b("bronze.marketing_spend_raw").select(
            F.to_date("spend_date").alias("SpendDate"),
            _blank("region_code").alias("RegionCode"),
            _blank("channel_code").alias("ChannelCode"),
            F.col("spend_local").cast("decimal(18,2)").alias("SpendLocal"),
        ),
        "ref_region": b("bronze.ref_region_raw").select(_blank("region_code").alias("RegionCode")),
    }


def gold_sources(spark: SparkSession, config: dict) -> dict[str, DataFrame]:
    return {n: read_table(spark, config, f"gold.{n}") for n in GOLD_NAMES}


def _run(spark: SparkSession, sql_path: Path, sources: dict[str, DataFrame], names, as_of,
         region) -> DataFrame:  # fmt: skip
    tag = uuid.uuid4().hex[:8]
    views = {}
    for name in names:
        view = f"legacy_{name}_{tag}"
        sources[name].createOrReplaceTempView(view)
        views[name] = view
    sql = Template(sql_path.read_text()).substitute(views)
    return spark.sql(sql, args={"as_of": as_of, "region_code": region})


def naive_summary(spark: SparkSession, sources: dict[str, DataFrame], as_of: str = DEFAULT_AS_OF,
                  region: str | None = None) -> DataFrame:  # fmt: skip
    """The literal translation, on raw-shaped sources."""
    return _run(spark, NAIVE_SQL, sources, RAW_NAMES, as_of, region)


def ported_summary(spark: SparkSession, sources: dict[str, DataFrame], as_of: str = DEFAULT_AS_OF,
                   region: str | None = None) -> DataFrame:  # fmt: skip
    """The corrected port, on gold-shaped sources."""
    return _run(spark, PORTED_SQL, sources, GOLD_NAMES, as_of, region)


def register_results(spark: SparkSession, config: dict, as_of: str = DEFAULT_AS_OF) -> None:
    """Temporary views for src/sql/findings_legacy.sql: legacy_naive, legacy_ported and
    the raw sources as legacy_raw_<name>."""
    raw = raw_sources(spark, config)
    for name, df in raw.items():
        df.createOrReplaceTempView(f"legacy_raw_{name}")
    naive_summary(spark, raw, as_of).createOrReplaceTempView("legacy_naive")
    ported_summary(spark, gold_sources(spark, config), as_of).createOrReplaceTempView(
        "legacy_ported"
    )
