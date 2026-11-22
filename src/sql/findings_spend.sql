-- Findings for step 9, marketing spend (defects 9 to 12, and 28).
-- Executed by tests/test_silver_spend.py, which asserts the quoted numbers.
-- Bronze is append-only: each query reads the latest batch.

-- name: spend_row_reconciliation
-- Every bronze row: 2,224 = kept + exact copies + CAD June without a rate + negatives.
WITH raw AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY _row_hash ORDER BY _source_row) AS copy
    FROM bronze.marketing_spend_raw
    WHERE _ingest_ts = (SELECT MAX(_ingest_ts) FROM bronze.marketing_spend_raw)
),
cad_june AS (SELECT * FROM raw WHERE currency_code = 'CAD' AND spend_date LIKE '2026-06-%')
SELECT
    (SELECT COUNT(*) FROM raw) AS bronze_rows,
    (SELECT COUNT(*) FROM raw WHERE copy > 1) AS exact_copies,
    (SELECT COUNT(*) FROM cad_june) AS cad_june_rows,
    (SELECT COUNT(*) FROM cad_june WHERE copy > 1) AS cad_june_copies,
    (SELECT COUNT(*) FROM cad_june WHERE copy = 1) AS cad_june_distinct,
    (SELECT COUNT(*) FROM raw
     WHERE copy = 1 AND CAST(spend_local AS DOUBLE) < 0
       AND NOT (currency_code = 'CAD' AND spend_date LIKE '2026-06-%')) AS negatives,
    (SELECT COUNT(*) FROM silver.marketing_spend) AS silver_rows;

-- name: minor_unit_evidence
-- Defect 11. Per campaign, on distinct bronze rows with positive spend: median CPM and
-- median click-through, each as a ratio to its currency's median.
WITH rows AS (
    SELECT campaign_id, currency_code,
        CAST(spend_local AS DOUBLE) / CAST(impressions AS DOUBLE) * 1000 AS cpm,
        CAST(clicks AS DOUBLE) / CAST(impressions AS DOUBLE) AS ctr
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY _row_hash ORDER BY _source_row) AS copy
        FROM bronze.marketing_spend_raw
        WHERE _ingest_ts = (SELECT MAX(_ingest_ts) FROM bronze.marketing_spend_raw)
    )
    WHERE copy = 1 AND CAST(spend_local AS DOUBLE) > 0
      AND NOT (CAST(clicks AS DOUBLE) > CAST(impressions AS DOUBLE))
),
ccy AS (
    SELECT currency_code, percentile(cpm, 0.5) AS ccy_cpm, percentile(ctr, 0.5) AS ccy_ctr
    FROM rows GROUP BY currency_code
)
SELECT r.campaign_id, r.currency_code, COUNT(*) AS rows,
    percentile(r.cpm, 0.5) / FIRST(c.ccy_cpm) AS median_cpm_ratio,
    percentile(r.ctr, 0.5) / FIRST(c.ccy_ctr) AS median_ctr_ratio
FROM rows r JOIN ccy c USING (currency_code)
GROUP BY r.campaign_id, r.currency_code
ORDER BY median_cpm_ratio DESC;

-- name: minor_unit_row_bands
-- Defect 11. Row CPM ratio (source spend) on silver rows the rule evaluates: positive
-- spend, clicks not above impressions. Normal and minor-unit rows, and anything between.
WITH rows AS (
    SELECT currency_code,
        CAST(spend_local_raw AS DOUBLE) / impressions * 1000 AS cpm
    FROM silver.marketing_spend
    WHERE spend_local_raw > 0 AND impressions > 0
      AND NOT COALESCE(clicks > impressions, FALSE)
),
ratio AS (
    SELECT r.cpm / c.ccy_cpm AS ratio
    FROM rows r
    JOIN (SELECT currency_code, percentile(cpm, 0.5) AS ccy_cpm FROM rows
          GROUP BY currency_code) c USING (currency_code)
)
SELECT
    SUM(CASE WHEN ratio < 2 THEN 1 ELSE 0 END) AS normal_rows,
    SUM(CASE WHEN ratio >= 20 THEN 1 ELSE 0 END) AS minor_rows,
    SUM(CASE WHEN ratio >= 2 AND ratio < 20 THEN 1 ELSE 0 END) AS between_rows,
    MAX(CASE WHEN ratio < 2 THEN ratio END) AS normal_max,
    MIN(CASE WHEN ratio >= 20 THEN ratio END) AS minor_min,
    MAX(CASE WHEN ratio >= 20 THEN ratio END) AS minor_max
FROM ratio;

-- name: cpl_minor_unit_effect
-- Defect 11. Cost per lead in GBP before (source spend) and after the rescale, for the
-- two rescaled campaigns and all campaigns with spend. Leads: silver.lead by campaign.
WITH spend AS (
    SELECT campaign_id,
        SUM(CAST(spend_local_raw AS DOUBLE) * CAST(rate_to_gbp AS DOUBLE)) AS before_gbp,
        SUM(CAST(spend_gbp AS DOUBLE)) AS after_gbp
    FROM silver.marketing_spend GROUP BY campaign_id
),
leads AS (SELECT campaign_id, COUNT(*) AS leads FROM silver.lead GROUP BY campaign_id),
joined AS (SELECT s.*, l.leads FROM spend s JOIN leads l USING (campaign_id))
SELECT campaign_id AS scope, before_gbp, after_gbp, leads,
    before_gbp / leads AS cpl_before_gbp, after_gbp / leads AS cpl_after_gbp
FROM joined WHERE campaign_id IN ('CMP-1061', 'CMP-1062')
UNION ALL
SELECT 'ALL', SUM(before_gbp), SUM(after_gbp), SUM(leads),
    SUM(before_gbp) / SUM(leads), SUM(after_gbp) / SUM(leads)
FROM joined
ORDER BY scope;

-- name: spend_gbp_by_currency_month
-- Local to GBP by currency and month: sum(local) x the month's rate against sum(GBP).
SELECT currency_code, date_format(spend_date, 'yyyy-MM') AS month, COUNT(*) AS rows,
    SUM(spend_local) AS spend_local, FIRST(rate_to_gbp) AS rate_to_gbp,
    COUNT(DISTINCT rate_to_gbp) AS rates,
    CAST(SUM(spend_local) * FIRST(rate_to_gbp) AS DECIMAL(18,2)) AS local_x_rate,
    SUM(spend_gbp) AS spend_gbp
FROM silver.marketing_spend
GROUP BY currency_code, date_format(spend_date, 'yyyy-MM')
ORDER BY currency_code, month;

-- name: negative_zero_null_evidence
-- Defect 10. Negatives on distinct bronze rows: matched by a positive of the same
-- campaign and amount, and size against the campaign median. Zeros and null clicks in
-- silver.
WITH raw AS (
    SELECT campaign_id, spend_date, CAST(spend_local AS DOUBLE) AS sp
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY _row_hash ORDER BY _source_row) AS copy
        FROM bronze.marketing_spend_raw
        WHERE _ingest_ts = (SELECT MAX(_ingest_ts) FROM bronze.marketing_spend_raw)
    ) WHERE copy = 1
),
neg AS (SELECT * FROM raw WHERE sp < 0),
med AS (SELECT campaign_id, percentile(sp, 0.5) AS med FROM raw WHERE sp > 0 GROUP BY 1)
SELECT
    (SELECT COUNT(*) FROM neg) AS negatives,
    (SELECT COUNT(*) FROM neg n
     WHERE EXISTS (SELECT 1 FROM raw p WHERE p.campaign_id = n.campaign_id AND p.sp = -n.sp))
        AS negatives_with_offset,
    (SELECT ROUND(MAX(-n.sp / m.med), 1) FROM neg n JOIN med m USING (campaign_id))
        AS max_negative_vs_median,
    (SELECT COUNT(*) FROM silver.marketing_spend WHERE spend_local = 0) AS zero_rows,
    (SELECT COUNT(*) FROM silver.marketing_spend
     WHERE spend_local = 0 AND channel_code = 'ORGANIC') AS zero_organic,
    (SELECT MAX(impressions) FROM silver.marketing_spend WHERE spend_local = 0)
        AS zero_max_impressions,
    (SELECT COUNT(*) FROM silver.marketing_spend WHERE clicks IS NULL) AS null_click_rows,
    (SELECT COUNT(*) FROM silver.marketing_spend
     WHERE clicks IS NULL AND (impressions IS NULL OR impressions = 0)) AS null_click_no_impressions;

-- name: clicks_exceed_impressions
-- Defect 28. Rows with clicks above impressions, and their spend against the campaign's
-- median spend in silver.
WITH med AS (
    SELECT campaign_id, percentile(CAST(spend_local AS DOUBLE), 0.5) AS med
    FROM silver.marketing_spend WHERE spend_local > 0 GROUP BY campaign_id
)
SELECT COUNT(*) AS rows,
    ROUND(MAX(CAST(s.spend_local AS DOUBLE) / m.med), 1) AS max_spend_vs_campaign_median
FROM silver.marketing_spend s JOIN med m USING (campaign_id)
WHERE s.clicks > s.impressions;
