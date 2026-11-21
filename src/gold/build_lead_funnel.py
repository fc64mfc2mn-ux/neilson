"""Build gold.fact_lead_funnel, the integration table, plus its bridges.

Build `gold.fact_lead_funnel` at one row per deduplicated lead, carrying the
full lead to call to policy path. Required columns: `lead_key`,
`lead_created_ts_utc`, `region_key`, `campaign_key`, `channel_key`,
`product_key`, `first_call_ts_utc`, `first_call_agent_key`,
`last_call_agent_key`, `call_attempt_count`, `connected_call_count`,
`minutes_to_first_call`, `within_first_call_sla_flag`, `quoted_flag`,
`application_flag`, `policy_key`, `policy_start_date_key`,
`annual_premium_gbp`, `days_lead_to_policy`, `consent_phone_current_flag`,
`dedup_cluster_size`. Also builds `gold.bridge_lead_call` (one row per lead x
call, supporting the call-grain denominator) and `gold.bridge_user_region`
(one row per user x region, supporting dynamic RLS).
"""

from __future__ import annotations

from pyspark.sql import SparkSession


def build_lead_funnel(spark: SparkSession, config: dict) -> None:
    raise NotImplementedError
