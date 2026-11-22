-- Findings for step 7 (silver leads and consent).
-- Executed by tests/test_silver_leads.py, which asserts the quoted numbers.
-- Bronze is append-only: each query reads the latest batch of its file.

-- name: lead_id_reconciliation
-- Distinct lead_id: bronze = surviving + mapped duplicates + unidentifiable + PK conflicts.
WITH leads AS (
    SELECT * FROM bronze.leads_raw
    WHERE _ingest_ts = (SELECT MAX(_ingest_ts) FROM bronze.leads_raw)
),
pk AS (SELECT lead_id FROM leads GROUP BY lead_id HAVING COUNT(*) > 1),
unid AS (
    SELECT DISTINCT lead_id FROM leads
    WHERE (email IS NULL OR TRIM(email) = '') AND (phone IS NULL OR TRIM(phone) = '')
      AND lead_id NOT IN (SELECT lead_id FROM pk)
)
SELECT
    (SELECT COUNT(DISTINCT lead_id) FROM leads) AS distinct_ids,
    (SELECT COUNT(*) FROM pk) AS pk_conflict_ids,
    (SELECT COUNT(*) FROM unid) AS unidentifiable_ids,
    (SELECT COUNT(*) FROM silver.lead) AS surviving,
    (SELECT COUNT(*) FROM silver.lead_dedup_map WHERE source_lead_id <> surviving_lead_id)
        AS mapped_duplicates;

-- name: source_flag_vs_rule
-- Defect 3: the source duplicate_of_lead_id flag against the 48h email-or-phone rule.
WITH leads AS (
    SELECT * FROM bronze.leads_raw
    WHERE _ingest_ts = (SELECT MAX(_ingest_ts) FROM bronze.leads_raw)
),
flagged AS (
    SELECT l.lead_id, l.duplicate_of_lead_id AS flagged_original,
           m.surviving_lead_id AS my_survivor,
           mo.surviving_lead_id AS original_survivor
    FROM leads l
    LEFT JOIN silver.lead_dedup_map m ON m.source_lead_id = l.lead_id
    LEFT JOIN silver.lead_dedup_map mo ON mo.source_lead_id = l.duplicate_of_lead_id
    WHERE l.duplicate_of_lead_id IS NOT NULL AND TRIM(l.duplicate_of_lead_id) <> ''
),
rule_dups AS (
    SELECT source_lead_id FROM silver.lead_dedup_map WHERE source_lead_id <> surviving_lead_id
)
SELECT
    COUNT(*) AS source_flagged,
    SUM(CASE WHEN my_survivor = flagged_original THEN 1 ELSE 0 END) AS flag_matches_rule,
    SUM(CASE WHEN my_survivor = lead_id AND original_survivor = lead_id THEN 1 ELSE 0 END)
        AS flag_reversed,
    SUM(CASE WHEN my_survivor = lead_id AND original_survivor = flagged_original THEN 1 ELSE 0 END)
        AS flag_no_shared_key,
    (SELECT COUNT(*) FROM rule_dups
     WHERE source_lead_id NOT IN (SELECT lead_id FROM flagged WHERE my_survivor = flagged_original)
       AND source_lead_id NOT IN (SELECT flagged_original FROM flagged
                                  WHERE my_survivor = lead_id AND original_survivor = lead_id))
        AS rule_only
FROM flagged;

-- name: dob_implausible_groups
-- Defect 7: implausible dates of birth by group (age at lead creation).
WITH leads AS (
    SELECT lead_id,
           TO_DATE(date_of_birth) AS dob,
           TO_DATE(SUBSTRING(created_at_utc, 1, 10)) AS created
    FROM bronze.leads_raw
    WHERE _ingest_ts = (SELECT MAX(_ingest_ts) FROM bronze.leads_raw)
      AND lead_id IN (SELECT source_lead_id FROM silver.lead_dedup_map)
),
aged AS (SELECT lead_id, dob, created, FLOOR(MONTHS_BETWEEN(created, dob) / 12) AS age FROM leads)
SELECT dob_group, COUNT(*) AS leads FROM (
    SELECT CASE
               WHEN dob > created THEN 'born_after_creation'
               WHEN age < 18 THEN 'age_under_18'
               WHEN age > 100 THEN 'age_over_100'
           END AS dob_group
    FROM aged
) WHERE dob_group IS NOT NULL
GROUP BY dob_group ORDER BY dob_group;

-- name: born_2008_ages
-- Defect 7: the register's "after 2008" count included 2008 births; they are all 18.
SELECT COUNT(*) AS leads,
       MIN(FLOOR(MONTHS_BETWEEN(TO_DATE(SUBSTRING(created_at_utc, 1, 10)),
                                TO_DATE(date_of_birth)) / 12)) AS min_age,
       MAX(FLOOR(MONTHS_BETWEEN(TO_DATE(SUBSTRING(created_at_utc, 1, 10)),
                                TO_DATE(date_of_birth)) / 12)) AS max_age
FROM bronze.leads_raw
WHERE _ingest_ts = (SELECT MAX(_ingest_ts) FROM bronze.leads_raw)
  AND date_of_birth LIKE '2008-%';

-- name: withdrawn_consent_still_y
-- Defect 25: latest consent event WITHDRAWN, yet the lead record says consent_marketing = Y.
SELECT
    COUNT(*) AS latest_withdrawn,
    SUM(CASE WHEN EXISTS (
            SELECT 1 FROM bronze.leads_raw l
            WHERE l.lead_id = c.lead_id AND l.consent_marketing = 'Y'
              AND l._ingest_ts = (SELECT MAX(_ingest_ts) FROM bronze.leads_raw)
        ) THEN 1 ELSE 0 END) AS lead_still_y
FROM silver.consent_current c
WHERE c.current_event_type = 'WITHDRAWN';

-- name: july_leads
-- Defect 6: leads created after the reporting period, and where they end up.
WITH leads AS (
    SELECT * FROM bronze.leads_raw
    WHERE _ingest_ts = (SELECT MAX(_ingest_ts) FROM bronze.leads_raw)
      AND created_at_utc >= '2026-07-01'
)
SELECT
    COUNT(*) AS july_leads,
    SUM(CASE WHEN m.source_lead_id IS NOT NULL THEN 1 ELSE 0 END) AS in_dedup_map,
    SUM(CASE WHEN s.created_at_utc < TIMESTAMP '2026-07-01 00:00:00' THEN 1 ELSE 0 END)
        AS mapped_to_june
FROM leads l
LEFT JOIN silver.lead_dedup_map m ON m.source_lead_id = l.lead_id
LEFT JOIN silver.lead s ON s.lead_id = m.surviving_lead_id;

-- name: dob_register_bands
-- Defect 7: the register's bands (before 1920; 2008 to today; future dates), with how
-- many each band has nulled by the rule (age at creation under 18 or over 100).
-- "Today" is the run date. All 48,000 bronze rows are counted, as the register did.
WITH leads AS (
    SELECT TO_DATE(date_of_birth) AS dob,
           FLOOR(MONTHS_BETWEEN(TO_DATE(SUBSTRING(created_at_utc, 1, 10)),
                                TO_DATE(date_of_birth)) / 12) AS age
    FROM bronze.leads_raw
    WHERE _ingest_ts = (SELECT MAX(_ingest_ts) FROM bronze.leads_raw)
),
banded AS (
    SELECT CASE
               WHEN dob < DATE '1920-01-01' THEN '1_before_1920'
               WHEN dob > CURRENT_DATE() THEN '3_future'
               WHEN dob >= DATE '2008-01-01' THEN '2_2008_to_today'
           END AS band,
           CASE WHEN age < 18 OR age > 100 THEN 1 ELSE 0 END AS nulled
    FROM leads
)
SELECT band, COUNT(*) AS leads, SUM(nulled) AS nulled
FROM banded WHERE band IS NOT NULL
GROUP BY band ORDER BY band;

-- name: postcode_region_format
-- Defect 29 (step 11): surviving leads whose postcode is not in its region's format, by
-- region and shape (letters as A, digits as 9). Rules: POSTCODE_RULES in
-- src/gold/apply_pii_policy.py. Asserted in tests/test_pii_policy.py.
WITH pc AS (
    SELECT region_code, UPPER(REGEXP_REPLACE(TRIM(postcode), ' +', ' ')) AS p
    FROM silver.lead
    WHERE postcode IS NOT NULL
)
SELECT region_code,
       REGEXP_REPLACE(REGEXP_REPLACE(p, '[A-Z]', 'A'), '[0-9]', '9') AS shape,
       COUNT(*) AS leads
FROM pc
WHERE NOT (
       (region_code = 'UK' AND p RLIKE '^[A-Z]{1,2}[0-9][A-Z0-9]? [0-9][A-Z]{2}$')
    OR (region_code = 'CA' AND p RLIKE '^[A-Z][0-9][A-Z] [0-9][A-Z][0-9]$')
    OR (region_code = 'US' AND p RLIKE '^[0-9]{5}$')
    OR (region_code = 'AU' AND p RLIKE '^[0-9]{4}$')
)
GROUP BY region_code, shape
ORDER BY region_code, shape;
