"""Build the gold fact tables other than the lead funnel.

Must produce `gold.fact_marketing_spend_daily` (date x campaign grain, with
`spend_gbp`, `spend_local`, `impressions`, `clicks`), `gold.fact_lead` (one
row per deduplicated lead, with `lead_created_date_key`, `campaign_key`,
`channel_key`, `region_key`), `gold.fact_call` (one row per deduplicated
call, seconds throughout, UTC throughout) and `gold.fact_policy` (one row per
deduplicated policy, with `annual_premium_gbp` derived where null).
"""

from __future__ import annotations

from pyspark.sql import SparkSession


def build_facts(spark: SparkSession, config: dict) -> None:
    raise NotImplementedError
