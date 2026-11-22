-- src/sql/legacy_sp_marketing_lead_summary_naive.sql
--
-- NAIVE translation of data/legacy/sp_MarketingLeadSummary.sql: a literal Spark SQL
-- port that keeps the procedure's logic, quirks included. Kept only as the baseline
-- for tests/test_legacy_parity.py and docs/LEGACY_PORT.md; do not report from it.
--
-- Run by src/legacy_port.py (naive_summary). The dollar placeholders are temporary views the
-- runner registers over the raw (bronze, latest batch) data, typed as the SQL Server
-- tables would be and otherwise untouched. Parameters:
--   :as_of        GETDATE(), as a timestamp, so the result is reproducible
--                 (default 2026-06-30 00:00:00)
--   :region_code  @RegionCode, NULL = all regions
--
-- Line by line:
--   @RegionList from RefRegion, or the one @RegionCode   -> region_list
--   #ChannelSpend, last 90 days from GETDATE()          -> channel_spend
--   the region cursor, one INSERT per region            -> one set-based query filtered
--                                                          on region_list (same rows)
--   WITH (NOLOCK)                                       -> dropped: Delta reads a
--                                                          consistent snapshot
--   INSERT into @Results (DECIMAL(18,4) CPL,            -> the same casts on output
--   DECIMAL(9,6) ConversionRate)
-- Spark compares strings exactly (case and trailing spaces count); SQL Server's default
-- collation does not. Spark (ANSI off) returns NULL where SQL Server raises an overflow.

WITH region_list AS (
    SELECT DISTINCT RegionCode FROM ${ref_region} WHERE :region_code IS NULL
    UNION ALL
    SELECT :region_code AS RegionCode WHERE :region_code IS NOT NULL
),
channel_spend AS (
    SELECT s.RegionCode, s.ChannelCode, CAST(SUM(s.SpendLocal) AS DECIMAL(18,2)) AS SpendLocal
    FROM ${spend} s
    WHERE s.SpendDate >= CAST(:as_of AS TIMESTAMP) - INTERVAL 90 DAYS
    GROUP BY s.RegionCode, s.ChannelCode
)
SELECT
    cs.RegionCode,
    cs.ChannelCode,
    cs.SpendLocal                                                    AS TotalSpend,
    COUNT(DISTINCT l.LeadID)                                         AS TotalLeads,
    COUNT(l.LeadID)                                                  AS TotalCalls,
    SUM(CASE WHEN p.PolicyID IS NOT NULL THEN 1 ELSE 0 END)          AS ConvertedLeads,
    CAST(CASE WHEN COUNT(l.LeadID) = 0 THEN NULL
              ELSE cs.SpendLocal / COUNT(l.LeadID)
         END AS DECIMAL(18,4))                                       AS CPL,
    CAST(CASE WHEN COUNT(DISTINCT l.LeadID) = 0 THEN NULL
              ELSE CAST(SUM(CASE WHEN p.PolicyID IS NOT NULL THEN 1 ELSE 0 END) AS DECIMAL(9,6))
                   / COUNT(DISTINCT l.LeadID)
         END AS DECIMAL(9,6))                                        AS ConversionRate
FROM channel_spend cs
LEFT JOIN ${leads} l
    ON l.ChannelCode = cs.ChannelCode
   AND l.RegionCode = cs.RegionCode
LEFT JOIN ${calls} c
    ON c.LeadID = l.LeadID
LEFT JOIN ${policies} p
    ON p.LeadID = l.LeadID
   AND p.PolicyStatus <> 'NTU'
WHERE cs.RegionCode IN (SELECT RegionCode FROM region_list)
  AND l.LeadStatus <> 'DEAD'
GROUP BY cs.RegionCode, cs.ChannelCode, cs.SpendLocal
ORDER BY cs.RegionCode, cs.ChannelCode
