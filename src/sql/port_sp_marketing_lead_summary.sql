-- src/sql/port_sp_marketing_lead_summary.sql
--
-- CORRECTED port of data/legacy/sp_MarketingLeadSummary.sql (the channel CPL and
-- conversion report) on the gold star schema. One row per region and channel.
-- Run by src/legacy_port.py (ported_summary); dollar placeholders are gold tables (or test views).
-- Parameters, as the naive version:
--   :as_of        GETDATE() made a parameter (default 2026-06-30 00:00:00)
--   :region_code  @RegionCode, NULL = all regions
--
-- Fixes, one per behaviour difference in docs/LEGACY_PORT.md:
--   * Spend and leads use the same window: the 91 days as_of - 90 to as_of (date of
--     as_of, inclusive), so the time of day of the run no longer moves it.
--   * Spend is GBP at the month's rate, deduplicated, minor units rescaled (gold).
--   * Leads are deduplicated surviving leads; channel is the campaign channel
--     (assumption 4); codes are conformed, so no join depends on case or spaces.
--   * Calls and policies are aggregated per lead before joining: TotalCalls is
--     calls, ConvertedLeads is leads with a non-NTU policy (each lead once), so
--     ConversionRate <= 100% and CPL = spend / leads.
--   * Spend with no leads keeps its row (FULL OUTER JOIN); no status filter turns
--     a left join into an inner one.
--   * Set-based, no cursor; Delta snapshot reads instead of NOLOCK.

WITH win AS (
    SELECT date_sub(CAST(CAST(:as_of AS TIMESTAMP) AS DATE), 90) AS start_date,
           CAST(CAST(:as_of AS TIMESTAMP) AS DATE)               AS end_date
),
spend AS (
    SELECT r.region_code, ch.channel_code, SUM(f.spend_gbp) AS spend_gbp
    FROM ${fact_marketing_spend_daily} f
    JOIN ${dim_date} d     ON d.date_key = f.date_key
    JOIN ${dim_region} r   ON r.region_key = f.region_key
    JOIN ${dim_channel} ch ON ch.channel_key = f.channel_key
    CROSS JOIN win
    WHERE d.date BETWEEN win.start_date AND win.end_date
    GROUP BY r.region_code, ch.channel_code
),
calls_per_lead AS (
    SELECT lead_key, COUNT(*) AS calls FROM ${bridge_lead_call} GROUP BY lead_key
),
converted AS (
    SELECT DISTINCT lead_key
    FROM ${fact_policy}
    WHERE is_lead_attributed AND counts_as_sale
),
leads AS (
    SELECT r.region_code,
           ch.channel_code,
           COUNT(*)                                       AS leads,
           COALESCE(SUM(c.calls), 0)                      AS calls,
           SUM(CASE WHEN v.lead_key IS NOT NULL THEN 1 ELSE 0 END) AS converted
    FROM ${fact_lead} l
    JOIN ${dim_region} r   ON r.region_key = l.region_key
    JOIN ${dim_channel} ch ON ch.channel_key = l.channel_key
    LEFT JOIN calls_per_lead c ON c.lead_key = l.lead_key
    LEFT JOIN converted v      ON v.lead_key = l.lead_key
    CROSS JOIN win
    WHERE CAST(l.lead_created_ts_utc AS DATE) BETWEEN win.start_date AND win.end_date
    GROUP BY r.region_code, ch.channel_code
)
SELECT
    COALESCE(s.region_code, l.region_code)                   AS RegionCode,
    COALESCE(s.channel_code, l.channel_code)                 AS ChannelCode,
    CAST(COALESCE(s.spend_gbp, 0) AS DECIMAL(18,2))          AS TotalSpendGBP,
    COALESCE(l.leads, 0)                                     AS TotalLeads,
    COALESCE(l.calls, 0)                                     AS TotalCalls,
    COALESCE(l.converted, 0)                                 AS ConvertedLeads,
    CAST(CASE WHEN COALESCE(l.leads, 0) = 0 THEN NULL
              ELSE COALESCE(s.spend_gbp, 0) / l.leads END AS DECIMAL(18,4)) AS CPL,
    CAST(CASE WHEN COALESCE(l.leads, 0) = 0 THEN NULL
              ELSE l.converted / l.leads END AS DECIMAL(9,6))              AS ConversionRate
FROM spend s
FULL OUTER JOIN leads l
    ON l.region_code = s.region_code AND l.channel_code = s.channel_code
WHERE :region_code IS NULL OR COALESCE(s.region_code, l.region_code) = :region_code
ORDER BY RegionCode, ChannelCode
