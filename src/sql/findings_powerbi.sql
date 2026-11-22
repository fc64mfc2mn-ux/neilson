-- Findings for step 16, the Power BI model.
-- Executed by tests/test_powerbi_model.py, which asserts the quoted numbers, and by
-- src/powerbi_expected.py, which writes powerbi/expected_values.csv from the
-- expected_* queries. Tables are read as gold.<table>; the fixture tests swap the
-- prefix for global_temp. (see src/powerbi_expected.py: run_query).
--
-- The expected_* queries mirror powerbi/measures.dax, one query per core measure,
-- each returning (slice, slice_value, value) for the whole dataset ('all'), by region
-- and by month. Region is the fact's own region_key; month is the year_month of the
-- date the measure's active relationship uses (spend date, lead-created date, call
-- date). Where DAX returns BLANK (DIVIDE by zero or blank, no rows), SQL returns NULL.

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

-- Slices, shared by every expected_* query (GROUPING SETS ((), (region), (month))):
--   slice 'all'    slice_value 'ALL'
--   slice 'region' slice_value region_code
--   slice 'month'  slice_value year_month of the measure's date ('(blank)' if unmatched)
-- Dimensions are LEFT JOINed: in Power BI a fact row with no matching dimension row
-- still counts in the total, under the blank member.

-- name: expected_marketing_spend_gbp
-- DAX: SUM ( fact_marketing_spend_daily[spend_gbp] ). Spend date, spend region.
SELECT CASE WHEN GROUPING(r.region_code) = 0 THEN 'region'
            WHEN GROUPING(d.year_month) = 0 THEN 'month' ELSE 'all' END AS slice,
       CASE WHEN GROUPING(r.region_code) = 0 THEN COALESCE(r.region_code, '(blank)')
            WHEN GROUPING(d.year_month) = 0 THEN COALESCE(d.year_month, '(blank)')
            ELSE 'ALL' END                                           AS slice_value,
       SUM(f.spend_gbp)                                              AS value
FROM gold.fact_marketing_spend_daily f
LEFT JOIN gold.dim_region r ON r.region_key = f.region_key
LEFT JOIN gold.dim_date d   ON d.date_key = f.date_key
GROUP BY GROUPING SETS ((), (r.region_code), (d.year_month))

-- name: expected_leads_deduplicated
-- DAX: DISTINCTCOUNT ( fact_lead_funnel[lead_key] ). Lead-created date: the model
-- merges fact_lead.lead_created_date_key into fact_lead_funnel in Power Query.
SELECT CASE WHEN GROUPING(r.region_code) = 0 THEN 'region'
            WHEN GROUPING(d.year_month) = 0 THEN 'month' ELSE 'all' END AS slice,
       CASE WHEN GROUPING(r.region_code) = 0 THEN COALESCE(r.region_code, '(blank)')
            WHEN GROUPING(d.year_month) = 0 THEN COALESCE(d.year_month, '(blank)')
            ELSE 'ALL' END                                           AS slice_value,
       COUNT(DISTINCT f.lead_key)                                    AS value
FROM gold.fact_lead_funnel f
LEFT JOIN gold.fact_lead l  ON l.lead_key = f.lead_key
LEFT JOIN gold.dim_region r ON r.region_key = f.region_key
LEFT JOIN gold.dim_date d   ON d.date_key = l.lead_created_date_key
GROUP BY GROUPING SETS ((), (r.region_code), (d.year_month))

-- name: expected_cost_per_lead_gbp
-- DAX: DIVIDE ( [Marketing Spend (GBP)], [Leads (Deduplicated)] ). NULL when either
-- side is missing or there are no leads, as DIVIDE returns BLANK.
WITH spend AS (
    SELECT CASE WHEN GROUPING(r.region_code) = 0 THEN 'region'
                WHEN GROUPING(d.year_month) = 0 THEN 'month' ELSE 'all' END AS slice,
           CASE WHEN GROUPING(r.region_code) = 0 THEN COALESCE(r.region_code, '(blank)')
                WHEN GROUPING(d.year_month) = 0 THEN COALESCE(d.year_month, '(blank)')
                ELSE 'ALL' END                                           AS slice_value,
           SUM(f.spend_gbp)                                              AS spend_gbp
    FROM gold.fact_marketing_spend_daily f
    LEFT JOIN gold.dim_region r ON r.region_key = f.region_key
    LEFT JOIN gold.dim_date d   ON d.date_key = f.date_key
    GROUP BY GROUPING SETS ((), (r.region_code), (d.year_month))
),
leads AS (
    SELECT CASE WHEN GROUPING(r.region_code) = 0 THEN 'region'
                WHEN GROUPING(d.year_month) = 0 THEN 'month' ELSE 'all' END AS slice,
           CASE WHEN GROUPING(r.region_code) = 0 THEN COALESCE(r.region_code, '(blank)')
                WHEN GROUPING(d.year_month) = 0 THEN COALESCE(d.year_month, '(blank)')
                ELSE 'ALL' END                                           AS slice_value,
           COUNT(DISTINCT f.lead_key)                                    AS leads
    FROM gold.fact_lead_funnel f
    LEFT JOIN gold.fact_lead l  ON l.lead_key = f.lead_key
    LEFT JOIN gold.dim_region r ON r.region_key = f.region_key
    LEFT JOIN gold.dim_date d   ON d.date_key = l.lead_created_date_key
    GROUP BY GROUPING SETS ((), (r.region_code), (d.year_month))
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
-- take the lead-created date (merged from fact_lead in Power Query) and the policy's
-- own region. NULL where a slice has leads but no linked policy (DAX: BLANK).
WITH leads AS (
    SELECT CASE WHEN GROUPING(r.region_code) = 0 THEN 'region'
                WHEN GROUPING(d.year_month) = 0 THEN 'month' ELSE 'all' END AS slice,
           CASE WHEN GROUPING(r.region_code) = 0 THEN COALESCE(r.region_code, '(blank)')
                WHEN GROUPING(d.year_month) = 0 THEN COALESCE(d.year_month, '(blank)')
                ELSE 'ALL' END                                           AS slice_value,
           COUNT(DISTINCT f.lead_key)                                    AS leads
    FROM gold.fact_lead_funnel f
    LEFT JOIN gold.fact_lead l  ON l.lead_key = f.lead_key
    LEFT JOIN gold.dim_region r ON r.region_key = f.region_key
    LEFT JOIN gold.dim_date d   ON d.date_key = l.lead_created_date_key
    GROUP BY GROUPING SETS ((), (r.region_code), (d.year_month))
),
policies AS (
    SELECT CASE WHEN GROUPING(r.region_code) = 0 THEN 'region'
                WHEN GROUPING(d.year_month) = 0 THEN 'month' ELSE 'all' END AS slice,
           CASE WHEN GROUPING(r.region_code) = 0 THEN COALESCE(r.region_code, '(blank)')
                WHEN GROUPING(d.year_month) = 0 THEN COALESCE(d.year_month, '(blank)')
                ELSE 'ALL' END                                           AS slice_value,
           COUNT(*)                                                      AS policies
    FROM gold.fact_policy p
    LEFT JOIN gold.fact_lead l  ON l.lead_key = p.lead_key
    LEFT JOIN gold.dim_region r ON r.region_key = p.region_key
    LEFT JOIN gold.dim_date d   ON d.date_key = l.lead_created_date_key
    WHERE p.is_lead_attributed
      AND COALESCE(p.policy_status, '') <> 'NTU'
    GROUP BY GROUPING SETS ((), (r.region_code), (d.year_month))
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
SELECT CASE WHEN GROUPING(r.region_code) = 0 THEN 'region'
            WHEN GROUPING(d.year_month) = 0 THEN 'month' ELSE 'all' END AS slice,
       CASE WHEN GROUPING(r.region_code) = 0 THEN COALESCE(r.region_code, '(blank)')
            WHEN GROUPING(d.year_month) = 0 THEN COALESCE(d.year_month, '(blank)')
            ELSE 'ALL' END                                           AS slice_value,
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
SELECT CASE WHEN GROUPING(r.region_code) = 0 THEN 'region'
            WHEN GROUPING(d.year_month) = 0 THEN 'month' ELSE 'all' END AS slice,
       CASE WHEN GROUPING(r.region_code) = 0 THEN COALESCE(r.region_code, '(blank)')
            WHEN GROUPING(d.year_month) = 0 THEN COALESCE(d.year_month, '(blank)')
            ELSE 'ALL' END                                           AS slice_value,
       percentile(f.minutes_to_first_call, 0.5)                      AS value
FROM gold.fact_lead_funnel f
LEFT JOIN gold.fact_lead l  ON l.lead_key = f.lead_key
LEFT JOIN gold.dim_region r ON r.region_key = f.region_key
LEFT JOIN gold.dim_date d   ON d.date_key = l.lead_created_date_key
GROUP BY GROUPING SETS ((), (r.region_code), (d.year_month))

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
