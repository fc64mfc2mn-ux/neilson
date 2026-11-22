-- Findings for step 6 (silver reference data and dimensions).
-- Run against the lakehouse after `python -m src.run_pipeline`; each query is
-- executed by tests/test_silver_reference.py, which asserts the quoted numbers.
-- Bronze is append-only, so each query reads the latest batch of its file.

-- name: null_region_agent_call_regions
-- Defect 18: where do the agents with no roster region actually take calls?
WITH calls AS (
    SELECT * FROM (
        SELECT c.*, MAX(_ingest_ts) OVER (PARTITION BY _source_file) AS latest
        FROM bronze.calls_raw c
    ) WHERE _ingest_ts = latest
),
agents AS (
    SELECT * FROM bronze.agents_raw
    WHERE _ingest_ts = (SELECT MAX(_ingest_ts) FROM bronze.agents_raw)
)
SELECT a.agent_id, c.region_code AS call_region, COUNT(*) AS calls
FROM agents a
JOIN calls c ON c.agent_id = a.agent_id
WHERE a.region_code IS NULL OR TRIM(a.region_code) = ''
GROUP BY a.agent_id, c.region_code
ORDER BY a.agent_id, c.region_code;

-- name: team_prefix_region_agreement
-- Defect 18 and 27: how often does the team prefix agree with the roster region?
SELECT
    SUM(CASE WHEN SUBSTRING(team, 1, 2) = region_code THEN 1 ELSE 0 END) AS matching,
    COUNT(*) AS with_region
FROM bronze.agents_raw
WHERE _ingest_ts = (SELECT MAX(_ingest_ts) FROM bronze.agents_raw)
  AND region_code IS NOT NULL AND TRIM(region_code) <> '';

-- name: lead_campaign_channel_disagreement
-- Defect 24: lead channel vs campaign channel, before and after label normalisation.
-- silver.campaign.channel_code is the normalised label; channel_code_raw the source.
WITH leads AS (
    SELECT * FROM bronze.leads_raw
    WHERE _ingest_ts = (SELECT MAX(_ingest_ts) FROM bronze.leads_raw)
),
lead_norm AS (
    SELECT l.lead_id, l.channel_code AS lead_raw,
           CASE UPPER(REGEXP_REPLACE(l.channel_code, '[^A-Za-z]', ''))
               WHEN 'SOCIALPAID' THEN 'PAIDSOCIAL'
               ELSE UPPER(REGEXP_REPLACE(l.channel_code, '[^A-Za-z]', ''))
           END AS lead_key,
           l.campaign_id
    FROM leads l
)
SELECT
    COUNT(*) AS leads_with_known_campaign,
    SUM(CASE WHEN ln.lead_raw <> c.channel_code_raw THEN 1 ELSE 0 END) AS disagree_raw,
    SUM(CASE WHEN ln.lead_key <> REPLACE(c.channel_code, '_', '') THEN 1 ELSE 0 END)
        AS disagree_normalised
FROM lead_norm ln
JOIN silver.campaign c ON c.campaign_id = ln.campaign_id;

-- name: region_codes_by_source
-- Open question (f): region codes present in each source. No Ireland code anywhere,
-- although the Marketing director's brief covers UK and Ireland.
SELECT 'ref_region' AS source, region_code, COUNT(*) AS rows FROM bronze.ref_region_raw
WHERE _ingest_ts = (SELECT MAX(_ingest_ts) FROM bronze.ref_region_raw) GROUP BY region_code
UNION ALL
SELECT 'leads', region_code, COUNT(*) FROM bronze.leads_raw
WHERE _ingest_ts = (SELECT MAX(_ingest_ts) FROM bronze.leads_raw) GROUP BY region_code
UNION ALL
SELECT 'campaigns', region_code, COUNT(*) FROM bronze.campaigns_raw
WHERE _ingest_ts = (SELECT MAX(_ingest_ts) FROM bronze.campaigns_raw) GROUP BY region_code
UNION ALL
SELECT 'spend', region_code, COUNT(*) FROM bronze.marketing_spend_raw
WHERE _ingest_ts = (SELECT MAX(_ingest_ts) FROM bronze.marketing_spend_raw) GROUP BY region_code
ORDER BY source, region_code;

-- name: ref_region_business_hours
-- Open question (c): ref_region gives daily start and end times per region, but no
-- weekend or holiday rule; assumption 9 fills that gap.
SELECT region_code, timezone, business_hours_start, business_hours_end
FROM bronze.ref_region_raw
WHERE _ingest_ts = (SELECT MAX(_ingest_ts) FROM bronze.ref_region_raw)
ORDER BY region_code;
