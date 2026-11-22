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
