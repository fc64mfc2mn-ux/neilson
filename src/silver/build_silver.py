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

from src.common.dq import StageResult, ensure_dq_table
from src.silver.leads import build_consent, build_leads
from src.silver.reference import build_reference


def build_silver(spark: SparkSession, config: dict) -> list[StageResult]:
    """Build silver in dependency order. Later plan steps add calls, policy and spend."""
    ensure_dq_table(spark, config)
    results = build_reference(spark, config)
    leads = build_leads(spark, config)
    results.append(
        StageResult(
            "silver.lead_dedup_map",
            "bronze.leads_raw",
            leads.rows_in,
            leads.map_rows,
            leads.rows_quarantined,
        )  # fmt: skip
    )
    results.append(build_consent(spark, config))
    return results
