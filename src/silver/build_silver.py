"""Build the silver layer from bronze.

Silver is typed, conformed and deduplicated. Every rejected row lands in
`silver.dq_exception` with `check_name`, `source_table`, `key_value`,
`severity` and `detected_at_utc`. Must produce `silver.lead`,
`silver.lead_dedup_map`, `silver.consent_current`, `silver.call`,
`silver.policy`, `silver.marketing_spend`, `silver.agent`, `silver.campaign`,
`silver.fx_rate` and `silver.dq_exception`. Orchestrates
dedupe_leads, normalise_calls and resolve_consent.
"""

from __future__ import annotations

from pyspark.sql import SparkSession


def build_silver(spark: SparkSession, config: dict) -> None:
    raise NotImplementedError
