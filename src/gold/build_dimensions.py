"""Build the gold dimension tables.

Must produce `gold.dim_date` (one row per date, 2026-01-01 to 2026-12-31,
with `year_month`, `fiscal_quarter`, `is_weekday`), `gold.dim_region` (from
`ref_region.csv`, carrying `timezone` and `gos_target_seconds`),
`gold.dim_channel` (plus an `UNKNOWN` member), `gold.dim_campaign` (SCD1,
plus an `UNKNOWN` member), `gold.dim_agent` (SCD2, one row per agent per
validity window, with `valid_from`, `valid_to`, `is_current`, plus an
`UNKNOWN` member), `gold.dim_product` (`TERM_LIFE`, `OVER50`, `CI`) and
`gold.dim_lead` (one row per deduplicated lead, PII removed or hashed via
apply_pii_policy, `age_band` in place of `date_of_birth`).
"""

from __future__ import annotations

from pyspark.sql import SparkSession


def build_dimensions(spark: SparkSession, config: dict) -> None:
    raise NotImplementedError
