-- Findings for step 14 (legacy port): the numbers in docs/LEGACY_PORT.md.
-- Run after src/legacy_port.register_results(spark, config), which makes the temporary
-- views legacy_naive, legacy_ported and legacy_raw_<leads|calls|policies|spend|ref_region>,
-- all at as_of 2026-06-30 00:00:00. Asserted in tests/test_legacy_parity.py.

-- name: legacy_row_counts
-- Output rows, and spend groups that have no lead under an exact join (the WHERE on the
-- LEFT JOIN would drop them).
WITH spend_groups AS (
    SELECT DISTINCT RegionCode, ChannelCode FROM legacy_raw_spend
    WHERE SpendDate >= TIMESTAMP'2026-06-30 00:00:00' - INTERVAL 90 DAYS
)
SELECT
    (SELECT COUNT(*) FROM legacy_naive) AS naive_rows,
    (SELECT COUNT(*) FROM legacy_ported) AS ported_rows,
    (SELECT COUNT(*) FROM spend_groups) AS spend_groups,
    (SELECT COUNT(*) FROM spend_groups g WHERE NOT EXISTS (
        SELECT 1 FROM legacy_raw_leads l
        WHERE l.RegionCode = g.RegionCode AND l.ChannelCode = g.ChannelCode
          AND l.LeadStatus <> 'DEAD')) AS groups_without_leads;

-- name: legacy_fan_out
-- Joining calls and policies before aggregating: TotalCalls counts lead x call x policy
-- rows, ConvertedLeads counts a converted lead once per joined row.
SELECT
    (SELECT SUM(TotalLeads) FROM legacy_naive) AS naive_total_leads,
    (SELECT SUM(TotalLeads) FROM legacy_ported) AS ported_total_leads,
    (SELECT SUM(TotalCalls) FROM legacy_naive) AS naive_total_calls,
    (SELECT SUM(TotalCalls) FROM legacy_ported) AS ported_total_calls,
    (SELECT SUM(ConvertedLeads) FROM legacy_naive) AS naive_converted,
    (SELECT SUM(ConvertedLeads) FROM legacy_ported) AS ported_converted,
    (SELECT ROUND(MAX(ConvertedLeads / TotalLeads), 3) FROM legacy_naive)
        AS max_naive_rate_unbounded,
    (SELECT ROUND(MAX(ConversionRate), 3) FROM legacy_ported) AS max_ported_rate,
    (SELECT COUNT(*) FROM legacy_naive WHERE ConvertedLeads > TotalLeads) AS groups_over_100_pct,
    (SELECT COUNT(*) FROM legacy_naive WHERE ConvertedLeads > 999) AS groups_cast_overflow,
    (SELECT COUNT(*) FROM legacy_naive WHERE ConversionRate IS NULL) AS naive_null_rates;

-- name: legacy_cpl_factor
-- CPL: the naive version divides spend by join rows, not leads. For UK (GBP, so no FX
-- effect) the ratio naive CPL / ported CPL per channel, and the divisor effect alone
-- (TotalLeads / TotalCalls in the naive result).
SELECT
    ROUND(MIN(n.CPL / p.CPL), 3) AS uk_min_cpl_ratio,
    ROUND(MAX(n.CPL / p.CPL), 3) AS uk_max_cpl_ratio,
    ROUND(SUM(n.TotalLeads) / SUM(n.TotalCalls), 3) AS uk_divisor_factor
FROM legacy_naive n
JOIN legacy_ported p ON p.RegionCode = n.RegionCode AND p.ChannelCode = n.ChannelCode
WHERE n.RegionCode = 'UK';

-- name: legacy_channel_collation
-- Codes that SQL Server's collation (case-insensitive, trailing spaces ignored) would
-- match but Spark would not, in the procedure's inputs and in the campaign file.
SELECT
    (SELECT COUNT(*) FROM legacy_raw_spend
     WHERE ChannelCode <> UPPER(RTRIM(ChannelCode))) AS spend_rows_not_canonical,
    (SELECT COUNT(*) FROM legacy_raw_leads
     WHERE ChannelCode <> UPPER(RTRIM(ChannelCode))) AS lead_rows_not_canonical,
    (SELECT COUNT(*) FROM bronze.campaigns_raw
     WHERE _ingest_ts = (SELECT MAX(_ingest_ts) FROM bronze.campaigns_raw)
       AND channel_code <> UPPER(TRIM(channel_code))) AS campaign_rows_not_canonical,
    (SELECT COUNT(*) FROM (
        SELECT DISTINCT s.RegionCode, s.ChannelCode FROM legacy_raw_spend s
        JOIN legacy_raw_leads l ON UPPER(RTRIM(l.ChannelCode)) = UPPER(RTRIM(s.ChannelCode))
                               AND l.RegionCode = s.RegionCode
        WHERE NOT EXISTS (SELECT 1 FROM legacy_raw_leads x
                          WHERE x.ChannelCode = s.ChannelCode AND x.RegionCode = s.RegionCode)
    )) AS groups_lost_to_exact_join;

-- name: legacy_spend_and_period
-- Spend: 90 days back from as_of, raw rows (copies, minor units, CAD June, negatives).
-- Leads: all time. And what a run on the submission date (2026-11-25) would see.
SELECT
    (SELECT COUNT(*) FROM legacy_raw_spend
     WHERE SpendDate >= TIMESTAMP'2026-06-30 00:00:00' - INTERVAL 90 DAYS) AS naive_spend_rows,
    (SELECT COUNT(*) FROM gold.fact_marketing_spend_daily) AS gold_spend_rows,
    (SELECT COUNT(*) FROM legacy_raw_spend
     WHERE SpendDate >= TIMESTAMP'2026-11-25 00:00:00' - INTERVAL 90 DAYS)
        AS spend_rows_in_window_on_2026_11_25,
    (SELECT COUNT(*) FROM legacy_raw_spend
     WHERE SpendDate >= TIMESTAMP'2026-06-30 07:00:00' - INTERVAL 90 DAYS)
        AS spend_rows_when_run_at_0700,
    (SELECT COUNT(DISTINCT LeadID) FROM legacy_raw_leads
     WHERE LeadStatus <> 'DEAD'
       AND (CAST(CreatedAt AS DATE) < DATE'2026-04-01' OR CAST(CreatedAt AS DATE) > DATE'2026-06-30'))
        AS naive_leads_outside_window,
    (SELECT COUNT(*) FROM legacy_raw_leads WHERE LeadStatus = 'DEAD') AS dead_leads,
    (SELECT COUNT(*) FROM legacy_raw_leads WHERE LeadStatus IS NULL) AS null_status_leads;

-- name: legacy_spend_by_region
-- TotalSpend per region: naive (raw local currency) against gold (local, cleaned) and
-- gold GBP. Regions differ in currency, so naive CPLs cannot be compared across them.
WITH naive AS (SELECT RegionCode, SUM(TotalSpend) AS naive_local FROM legacy_naive GROUP BY 1),
gold AS (
    SELECT r.region_code AS RegionCode, SUM(f.spend_local) AS gold_local,
           SUM(f.spend_gbp) AS gold_gbp, MIN(f.currency_code) AS currency
    FROM gold.fact_marketing_spend_daily f
    JOIN gold.dim_region r ON r.region_key = f.region_key
    GROUP BY 1
)
SELECT g.RegionCode, g.currency, n.naive_local, g.gold_local, g.gold_gbp,
       ROUND(n.naive_local / g.gold_local, 3) AS naive_over_gold_local
FROM gold g LEFT JOIN naive n ON n.RegionCode = g.RegionCode
ORDER BY g.RegionCode;

-- name: legacy_duplicates
-- Raw duplicates the naive version counts: leads (same person), calls, policies.
SELECT
    (SELECT COUNT(DISTINCT LeadID) FROM legacy_raw_leads) AS raw_distinct_lead_ids,
    (SELECT COUNT(*) FROM gold.fact_lead) AS surviving_leads,
    (SELECT COUNT(*) FROM legacy_raw_calls) AS raw_call_rows,
    (SELECT COUNT(DISTINCT CallID) FROM legacy_raw_calls) AS raw_distinct_calls,
    (SELECT COUNT(*) FROM legacy_raw_policies
     WHERE LeadID IS NOT NULL AND PolicyStatus <> 'NTU') AS raw_linked_policy_rows,
    (SELECT COUNT(DISTINCT PolicyID) FROM legacy_raw_policies
     WHERE LeadID IS NOT NULL AND PolicyStatus <> 'NTU') AS raw_linked_distinct_policies;
