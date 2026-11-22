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
from src.silver.calls import build_calls
from src.silver.leads import build_consent, build_leads
from src.silver.policy import build_policy
from src.silver.reference import build_reference
from src.silver.spend import build_spend


def build_silver(spark: SparkSession, config: dict) -> list[StageResult]:
    """Build silver in dependency order. Steps 6 to 9."""
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
        )
    )
    results.append(build_consent(spark, config))
    calls = build_calls(spark, config)
    results.append(
        StageResult(
            "silver.call", "bronze.calls_raw", calls.rows_in, calls.rows_out, calls.rows_quarantined
        )
    )
    policy = build_policy(spark, config)
    results.append(
        StageResult(
            "silver.policy",
            "bronze.policies_raw",
            policy.rows_in,
            policy.rows_out,
            policy.rows_quarantined,
        )
    )
    spend = build_spend(spark, config)
    results.append(
        StageResult(
            "silver.marketing_spend",
            "bronze.marketing_spend_raw",
            spend.rows_in,
            spend.rows_out,
            spend.rows_quarantined,
        )
    )
    return results
