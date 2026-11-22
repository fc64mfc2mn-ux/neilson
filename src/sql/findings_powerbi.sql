-- Findings for step 16, the Power BI model.
-- Executed by tests/test_powerbi_model.py, which asserts the quoted numbers, and by
-- src/powerbi_expected.py, which writes powerbi/expected_values.csv from the
-- expected_* queries. Tables are read as gold.<table>; the fixture tests swap the
-- prefix for global_temp. (see src/powerbi_expected.py: run_query).
--
-- The expected_* queries mirror powerbi/measures.dax, one query per core measure plus
-- Cost Per Policy (GBP), each returning (slice, slice_value, value). Where DAX returns
-- BLANK (DIVIDE by zero or blank, no rows), SQL returns NULL.

-- name: conversion_by_status_rule
-- Assumption 3. Lead to Policy Conversion Rate under three status rules: all policies,
-- excluding NTU (chosen), and also excluding CANCELLED and LAPSED. Numerator: policies
-- linked to a surviving lead (is_lead_attributed); denominator: surviving leads. Region
-- is the lead's region (equal to the policy's for every linked policy; tested).
WITH leads AS (
    SELECT r.region_code, COUNT(*) AS leads
    FROM gold.fact_lead l JOIN gold.dim_region r ON r.region_key = l.region_key
    GROUP BY GROUPING SETS ((), (r.region_code))
),
policies AS (
    SELECT r.region_code,
           COUNT(*)                                                    AS policies_all,
           COUNT_IF(p.policy_status <> 'NTU')                          AS policies_excl_ntu,
           COUNT_IF(p.policy_status NOT IN ('NTU', 'CANCELLED', 'LAPSED')) AS policies_inforce_only
    FROM gold.fact_policy p
    JOIN gold.fact_lead l   ON l.lead_key = p.lead_key
    JOIN gold.dim_region r  ON r.region_key = l.region_key
    WHERE p.is_lead_attributed
    GROUP BY GROUPING SETS ((), (r.region_code))
)
SELECT COALESCE(l.region_code, 'ALL') AS region_code,
       l.leads,
       p.policies_all,
       p.policies_excl_ntu,
       p.policies_inforce_only,
       ROUND(100 * p.policies_all / l.leads, 2)          AS rate_all_pct,
       ROUND(100 * p.policies_excl_ntu / l.leads, 2)     AS rate_excl_ntu_pct,
       ROUND(100 * p.policies_inforce_only / l.leads, 2) AS rate_inforce_only_pct,
       ROUND(100 * (p.policies_excl_ntu - p.policies_inforce_only) / l.leads, 2)
                                                          AS stricter_rule_drop_pp,
       ROUND(100 * (p.policies_excl_ntu - p.policies_inforce_only) / p.policies_excl_ntu, 1)
                                                          AS stricter_rule_relative_drop_pct
FROM leads l
JOIN policies p ON p.region_code <=> l.region_code
ORDER BY l.region_code IS NULL DESC, l.region_code

-- name: premium_gbp_coverage
-- Assumption 27. Coverage of Average Annual Premium (GBP): of the policies the measure
-- counts (not NTU), the share with a GBP premium. Quoted in the measure's description.
SELECT COUNT(*)                                                AS policies_counted,
       COUNT(annual_premium_gbp)                               AS with_gbp_premium,
       ROUND(100 * COUNT(annual_premium_gbp) / COUNT(*), 1)    AS coverage_pct
FROM gold.fact_policy
WHERE COALESCE(policy_status, '') <> 'NTU'

-- Slices (GROUPING SETS): slice 'all' / slice_value 'ALL', or slice 'region', 'month',
-- 'channel', 'seniority' or 'first_call_bucket' with that column's value ('(blank)' when
-- the fact row has no match, Power BI's blank member). Region, channel and seniority
-- are the fact's own keys; month is the year_month of the measure's active date. All
-- keys are gold columns (step 16b): the model needs no Power Query merge.
-- Seniority (gold.dim_agent_seniority) reaches leads through last_call_agent_key and
-- policies through the selling agent (fact_policy.agent_key); see
-- seniority_attribution_agreement.

-- name: expected_marketing_spend_gbp
-- DAX: SUM ( fact_marketing_spend_daily[spend_gbp] ). Spend date, region, channel.
SELECT CASE
            WHEN GROUPING(r.region_code) = 0 THEN 'region'
            WHEN GROUPING(d.year_month) = 0 THEN 'month'
            WHEN GROUPING(ch.channel_code) = 0 THEN 'channel'
            ELSE 'all' END AS slice,
       CASE
            WHEN GROUPING(r.region_code) = 0 THEN COALESCE(CAST(r.region_code AS STRING), '(blank)')
            WHEN GROUPING(d.year_month) = 0 THEN COALESCE(CAST(d.year_month AS STRING), '(blank)')
            WHEN GROUPING(ch.channel_code) = 0 THEN COALESCE(CAST(ch.channel_code AS STRING), '(blank)')
            ELSE 'ALL' END AS slice_value,
       SUM(f.spend_gbp)                                              AS value
FROM gold.fact_marketing_spend_daily f
LEFT JOIN gold.dim_region r  ON r.region_key = f.region_key
LEFT JOIN gold.dim_date d    ON d.date_key = f.date_key
LEFT JOIN gold.dim_channel ch ON ch.channel_key = f.channel_key
GROUP BY GROUPING SETS ((), (r.region_code), (d.year_month), (ch.channel_code))

-- name: expected_leads_deduplicated
-- DAX: DISTINCTCOUNT ( fact_lead_funnel[lead_key] ). Lead-created date. The
-- first_call_bucket rows are the expected counts for the distribution visual.
SELECT CASE
            WHEN GROUPING(r.region_code) = 0 THEN 'region'
            WHEN GROUPING(d.year_month) = 0 THEN 'month'
            WHEN GROUPING(ch.channel_code) = 0 THEN 'channel'
            WHEN GROUPING(f.first_call_bucket) = 0 THEN 'first_call_bucket'
            ELSE 'all' END AS slice,
       CASE
            WHEN GROUPING(r.region_code) = 0 THEN COALESCE(CAST(r.region_code AS STRING), '(blank)')
            WHEN GROUPING(d.year_month) = 0 THEN COALESCE(CAST(d.year_month AS STRING), '(blank)')
            WHEN GROUPING(ch.channel_code) = 0 THEN COALESCE(CAST(ch.channel_code AS STRING), '(blank)')
            WHEN GROUPING(f.first_call_bucket) = 0 THEN COALESCE(CAST(f.first_call_bucket AS STRING), '(blank)')
            ELSE 'ALL' END AS slice_value,
       COUNT(DISTINCT f.lead_key)                                    AS value
FROM gold.fact_lead_funnel f
LEFT JOIN gold.dim_region r  ON r.region_key = f.region_key
LEFT JOIN gold.dim_date d    ON d.date_key = f.lead_created_date_key
LEFT JOIN gold.dim_channel ch ON ch.channel_key = f.channel_key
LEFT JOIN gold.dim_agent_seniority sa ON sa.agent_key = f.last_call_agent_key
GROUP BY GROUPING SETS ((), (r.region_code), (d.year_month), (ch.channel_code), (f.first_call_bucket))

-- name: expected_cost_per_lead_gbp
-- DAX: DIVIDE ( [Marketing Spend (GBP)], [Leads (Deduplicated)] ). NULL when either
-- side is missing or there are no leads, as DIVIDE returns BLANK.
WITH spend AS (
    SELECT CASE
                WHEN GROUPING(r.region_code) = 0 THEN 'region'
                WHEN GROUPING(d.year_month) = 0 THEN 'month'
                WHEN GROUPING(ch.channel_code) = 0 THEN 'channel'
                ELSE 'all' END AS slice,
           CASE
                WHEN GROUPING(r.region_code) = 0 THEN COALESCE(CAST(r.region_code AS STRING), '(blank)')
                WHEN GROUPING(d.year_month) = 0 THEN COALESCE(CAST(d.year_month AS STRING), '(blank)')
                WHEN GROUPING(ch.channel_code) = 0 THEN COALESCE(CAST(ch.channel_code AS STRING), '(blank)')
                ELSE 'ALL' END AS slice_value,
           SUM(f.spend_gbp)                                              AS spend_gbp
    FROM gold.fact_marketing_spend_daily f
    LEFT JOIN gold.dim_region r  ON r.region_key = f.region_key
    LEFT JOIN gold.dim_date d    ON d.date_key = f.date_key
    LEFT JOIN gold.dim_channel ch ON ch.channel_key = f.channel_key
    GROUP BY GROUPING SETS ((), (r.region_code), (d.year_month), (ch.channel_code))
),
leads AS (
    SELECT CASE
                WHEN GROUPING(r.region_code) = 0 THEN 'region'
                WHEN GROUPING(d.year_month) = 0 THEN 'month'
                WHEN GROUPING(ch.channel_code) = 0 THEN 'channel'
                ELSE 'all' END AS slice,
           CASE
                WHEN GROUPING(r.region_code) = 0 THEN COALESCE(CAST(r.region_code AS STRING), '(blank)')
                WHEN GROUPING(d.year_month) = 0 THEN COALESCE(CAST(d.year_month AS STRING), '(blank)')
                WHEN GROUPING(ch.channel_code) = 0 THEN COALESCE(CAST(ch.channel_code AS STRING), '(blank)')
                ELSE 'ALL' END AS slice_value,
           COUNT(DISTINCT f.lead_key)                                    AS leads
    FROM gold.fact_lead_funnel f
    LEFT JOIN gold.dim_region r  ON r.region_key = f.region_key
    LEFT JOIN gold.dim_date d    ON d.date_key = f.lead_created_date_key
    LEFT JOIN gold.dim_channel ch ON ch.channel_key = f.channel_key
    LEFT JOIN gold.dim_agent_seniority sa ON sa.agent_key = f.last_call_agent_key
    GROUP BY GROUPING SETS ((), (r.region_code), (d.year_month), (ch.channel_code))
)
SELECT COALESCE(s.slice, l.slice)               AS slice,
       COALESCE(s.slice_value, l.slice_value)   AS slice_value,
       s.spend_gbp / NULLIF(l.leads, 0)         AS value
FROM spend s
FULL OUTER JOIN leads l ON l.slice = s.slice AND l.slice_value = s.slice_value

-- name: expected_lead_to_policy_conversion_rate
-- DAX: DIVIDE ( [Policies Linked to Surviving Leads], [Leads (Deduplicated)] ), the
-- helper counting fact_policy rows with is_lead_attributed = TRUE and
-- policy_status <> "NTU" (a blank status is kept in DAX, hence the COALESCE). Policies
-- use their lead_created_date_key (the lead's date), their own region and their selling
-- agent's seniority. NULL where a slice has leads but no linked policy (DAX: BLANK).
WITH leads AS (
    SELECT CASE
                WHEN GROUPING(r.region_code) = 0 THEN 'region'
                WHEN GROUPING(d.year_month) = 0 THEN 'month'
                WHEN GROUPING(sa.seniority_band) = 0 THEN 'seniority'
                ELSE 'all' END AS slice,
           CASE
                WHEN GROUPING(r.region_code) = 0 THEN COALESCE(CAST(r.region_code AS STRING), '(blank)')
                WHEN GROUPING(d.year_month) = 0 THEN COALESCE(CAST(d.year_month AS STRING), '(blank)')
                WHEN GROUPING(sa.seniority_band) = 0 THEN COALESCE(CAST(sa.seniority_band AS STRING), '(blank)')
                ELSE 'ALL' END AS slice_value,
           COUNT(DISTINCT f.lead_key)                                    AS leads
    FROM gold.fact_lead_funnel f
    LEFT JOIN gold.dim_region r  ON r.region_key = f.region_key
    LEFT JOIN gold.dim_date d    ON d.date_key = f.lead_created_date_key
    LEFT JOIN gold.dim_channel ch ON ch.channel_key = f.channel_key
    LEFT JOIN gold.dim_agent_seniority sa ON sa.agent_key = f.last_call_agent_key
    GROUP BY GROUPING SETS ((), (r.region_code), (d.year_month), (sa.seniority_band))
),
policies AS (
    SELECT CASE
                WHEN GROUPING(r.region_code) = 0 THEN 'region'
                WHEN GROUPING(d.year_month) = 0 THEN 'month'
                WHEN GROUPING(sa.seniority_band) = 0 THEN 'seniority'
                ELSE 'all' END AS slice,
           CASE
                WHEN GROUPING(r.region_code) = 0 THEN COALESCE(CAST(r.region_code AS STRING), '(blank)')
                WHEN GROUPING(d.year_month) = 0 THEN COALESCE(CAST(d.year_month AS STRING), '(blank)')
                WHEN GROUPING(sa.seniority_band) = 0 THEN COALESCE(CAST(sa.seniority_band AS STRING), '(blank)')
                ELSE 'ALL' END AS slice_value,
           COUNT(*)                                                      AS policies
    FROM gold.fact_policy p
    LEFT JOIN gold.dim_region r  ON r.region_key = p.region_key
    LEFT JOIN gold.dim_date d    ON d.date_key = p.lead_created_date_key
    LEFT JOIN gold.dim_channel ch ON ch.channel_key = p.channel_key
    LEFT JOIN gold.dim_agent_seniority sa ON sa.agent_key = p.agent_key
    WHERE p.is_lead_attributed
      AND COALESCE(p.policy_status, '') <> 'NTU'
    GROUP BY GROUPING SETS ((), (r.region_code), (d.year_month), (sa.seniority_band))
)
SELECT l.slice,
       l.slice_value,
       p.policies / NULLIF(l.leads, 0) AS value
FROM leads l
LEFT JOIN policies p ON p.slice = l.slice AND p.slice_value = l.slice_value

-- name: expected_average_handle_time_seconds
-- DAX: (SUM talk + SUM hold) / COUNTROWS over fact_call with is_connected = TRUE. Two
-- SUMs, not SUM(talk + hold), so a null in one column does not drop the other, as in
-- DAX. Call date, call region.
SELECT CASE
            WHEN GROUPING(r.region_code) = 0 THEN 'region'
            WHEN GROUPING(d.year_month) = 0 THEN 'month'
            ELSE 'all' END AS slice,
       CASE
            WHEN GROUPING(r.region_code) = 0 THEN COALESCE(CAST(r.region_code AS STRING), '(blank)')
            WHEN GROUPING(d.year_month) = 0 THEN COALESCE(CAST(d.year_month AS STRING), '(blank)')
            ELSE 'ALL' END AS slice_value,
       (SUM(c.talk_time_seconds) + SUM(c.hold_time_seconds)) / COUNT(*) AS value
FROM gold.fact_call c
LEFT JOIN gold.dim_region r ON r.region_key = c.region_key
LEFT JOIN gold.dim_date d   ON d.date_key = c.call_date_key
WHERE c.is_connected
GROUP BY GROUPING SETS ((), (r.region_code), (d.year_month))

-- name: expected_time_to_first_call_median_minutes
-- DAX: MEDIAN ( fact_lead_funnel[minutes_to_first_call] ): blanks (never called) are
-- ignored and an even count averages the middle two, as Spark's exact percentile does
-- (not percentile_approx). Lead-created date, lead region.
SELECT CASE
            WHEN GROUPING(r.region_code) = 0 THEN 'region'
            WHEN GROUPING(d.year_month) = 0 THEN 'month'
            ELSE 'all' END AS slice,
       CASE
            WHEN GROUPING(r.region_code) = 0 THEN COALESCE(CAST(r.region_code AS STRING), '(blank)')
            WHEN GROUPING(d.year_month) = 0 THEN COALESCE(CAST(d.year_month AS STRING), '(blank)')
            ELSE 'ALL' END AS slice_value,
       percentile(f.minutes_to_first_call, 0.5)                      AS value
FROM gold.fact_lead_funnel f
LEFT JOIN gold.dim_region r  ON r.region_key = f.region_key
LEFT JOIN gold.dim_date d    ON d.date_key = f.lead_created_date_key
LEFT JOIN gold.dim_channel ch ON ch.channel_key = f.channel_key
LEFT JOIN gold.dim_agent_seniority sa ON sa.agent_key = f.last_call_agent_key
GROUP BY GROUPING SETS ((), (r.region_code), (d.year_month))

-- name: expected_cost_per_policy_gbp
-- Stretch measure 10, for the channel CPL against cost per policy visual. DAX:
-- DIVIDE ( [Marketing Spend (GBP)], [Policies Linked to Surviving Leads] ). Spend by
-- spend date; policies by the lead's date and channel. NULL as DIVIDE returns BLANK.
WITH spend AS (
    SELECT CASE
                WHEN GROUPING(r.region_code) = 0 THEN 'region'
                WHEN GROUPING(d.year_month) = 0 THEN 'month'
                WHEN GROUPING(ch.channel_code) = 0 THEN 'channel'
                ELSE 'all' END AS slice,
           CASE
                WHEN GROUPING(r.region_code) = 0 THEN COALESCE(CAST(r.region_code AS STRING), '(blank)')
                WHEN GROUPING(d.year_month) = 0 THEN COALESCE(CAST(d.year_month AS STRING), '(blank)')
                WHEN GROUPING(ch.channel_code) = 0 THEN COALESCE(CAST(ch.channel_code AS STRING), '(blank)')
                ELSE 'ALL' END AS slice_value,
           SUM(f.spend_gbp)                                              AS spend_gbp
    FROM gold.fact_marketing_spend_daily f
    LEFT JOIN gold.dim_region r  ON r.region_key = f.region_key
    LEFT JOIN gold.dim_date d    ON d.date_key = f.date_key
    LEFT JOIN gold.dim_channel ch ON ch.channel_key = f.channel_key
    GROUP BY GROUPING SETS ((), (r.region_code), (d.year_month), (ch.channel_code))
),
policies AS (
    SELECT CASE
                WHEN GROUPING(r.region_code) = 0 THEN 'region'
                WHEN GROUPING(d.year_month) = 0 THEN 'month'
                WHEN GROUPING(ch.channel_code) = 0 THEN 'channel'
                ELSE 'all' END AS slice,
           CASE
                WHEN GROUPING(r.region_code) = 0 THEN COALESCE(CAST(r.region_code AS STRING), '(blank)')
                WHEN GROUPING(d.year_month) = 0 THEN COALESCE(CAST(d.year_month AS STRING), '(blank)')
                WHEN GROUPING(ch.channel_code) = 0 THEN COALESCE(CAST(ch.channel_code AS STRING), '(blank)')
                ELSE 'ALL' END AS slice_value,
           COUNT(*)                                                      AS policies
    FROM gold.fact_policy p
    LEFT JOIN gold.dim_region r  ON r.region_key = p.region_key
    LEFT JOIN gold.dim_date d    ON d.date_key = p.lead_created_date_key
    LEFT JOIN gold.dim_channel ch ON ch.channel_key = p.channel_key
    LEFT JOIN gold.dim_agent_seniority sa ON sa.agent_key = p.agent_key
    WHERE p.is_lead_attributed
      AND COALESCE(p.policy_status, '') <> 'NTU'
    GROUP BY GROUPING SETS ((), (r.region_code), (d.year_month), (ch.channel_code))
)
SELECT COALESCE(s.slice, p.slice)               AS slice,
       COALESCE(s.slice_value, p.slice_value)   AS slice_value,
       s.spend_gbp / NULLIF(p.policies, 0)      AS value
FROM spend s
FULL OUTER JOIN policies p ON p.slice = s.slice AND p.slice_value = s.slice_value

-- name: seniority_attribution_agreement
-- The seniority visual: leads follow the last-call agent, policies the selling agent.
-- How often they are the same agent, for lead-linked policies.
SELECT COUNT(*)                                                    AS linked_policies,
       COUNT_IF(p.agent_key = f.last_call_agent_key)               AS same_agent,
       COUNT_IF(f.last_call_agent_key IS NULL)                     AS lead_never_called,
       ROUND(100 * COUNT_IF(p.agent_key = f.last_call_agent_key) / COUNT(*), 1) AS same_agent_pct
FROM gold.fact_policy p
JOIN gold.fact_lead_funnel f ON f.lead_key = p.lead_key
WHERE p.is_lead_attributed

-- name: agent_seniority_group_sizes
-- Small groups behind the seniority visual: current agents per region and band.
SELECT a.region_code, s.seniority_band, COUNT(*) AS agents
FROM gold.dim_agent a
JOIN gold.dim_agent_seniority s ON s.agent_key = a.agent_key
WHERE a.is_current AND a.agent_id <> 'UNKNOWN'
GROUP BY a.region_code, s.seniority_band
ORDER BY a.region_code, s.seniority_band

-- name: aht_numerator_scope
-- Why AHT sums handle time of connected calls only: calls that did not connect carry
-- talk time too. Quoted in powerbi/MODEL_DESIGN.md.
SELECT COUNT_IF(is_connected)                                          AS connected_calls,
       COUNT_IF(NOT is_connected AND talk_time_seconds > 0)            AS unconnected_with_talk,
       ROUND((SUM(IF(is_connected, talk_time_seconds + hold_time_seconds, 0)))
             / COUNT_IF(is_connected), 1)                              AS aht_connected_only,
       ROUND(SUM(talk_time_seconds + hold_time_seconds) / COUNT_IF(is_connected), 1)
                                                                       AS aht_all_calls_numerator
FROM gold.fact_call
