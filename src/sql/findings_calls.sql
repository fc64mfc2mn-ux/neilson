-- Findings for step 8 (silver calls, defects 13 to 17 and 26).
-- Executed by tests/test_silver_calls.py, which asserts the quoted numbers.
-- Bronze is append-only: each query reads the latest batch of each file.
-- "Distinct calls" = bronze rows with exact duplicate copies collapsed (defect 13).

-- name: first_call_gap_by_hypothesis
-- Assumption 5, defect 15. Median minutes from lead creation to the lead's first call,
-- reading call timestamps as UTC and as the region's local time (DST-aware).
WITH raw AS (
    SELECT *, MAX(_ingest_ts) OVER (PARTITION BY _source_file) AS _latest FROM bronze.calls_raw
),
calls AS (
    SELECT DISTINCT call_id, lead_id, region_code,
        CASE WHEN _source_file = 'calls_uk_au.csv' THEN to_timestamp(call_start_ts_utc)
             ELSE to_timestamp(call_start_ts, 'dd/MM/yyyy HH:mm') END AS start_ts
    FROM raw WHERE _ingest_ts = _latest
),
leads AS (
    SELECT lead_id, MIN(to_timestamp(created_at_utc)) AS created_ts
    FROM bronze.leads_raw
    WHERE _ingest_ts = (SELECT MAX(_ingest_ts) FROM bronze.leads_raw)
    GROUP BY lead_id
),
first_call AS (
    SELECT c.region_code, c.lead_id, l.created_ts,
        MIN(c.start_ts) AS first_as_utc,
        MIN(to_utc_timestamp(c.start_ts, CASE c.region_code
            WHEN 'UK' THEN 'Europe/London' WHEN 'AU' THEN 'Australia/Sydney'
            WHEN 'CA' THEN 'America/Toronto' WHEN 'US' THEN 'America/New_York' END))
            AS first_as_local
    FROM calls c JOIN leads l ON l.lead_id = c.lead_id
    GROUP BY c.region_code, c.lead_id, l.created_ts
)
SELECT region_code,
    percentile((unix_timestamp(first_as_utc) - unix_timestamp(created_ts)) / 60, 0.5)
        AS median_min_as_utc,
    percentile((unix_timestamp(first_as_local) - unix_timestamp(created_ts)) / 60, 0.5)
        AS median_min_as_local,
    COUNT(*) AS leads
FROM first_call
GROUP BY region_code
ORDER BY region_code;

-- name: au_first_call_gap_in_silver
-- AU on silver.call: rows whose start differs from the stored bronze value (0 while AU is
-- kept as stored), and the first-call median (near the as-UTC figure; silver has already
-- dropped quarantined calls, so it differs by a few minutes).
WITH leads AS (
    SELECT lead_id, MIN(to_timestamp(created_at_utc)) AS created_ts
    FROM bronze.leads_raw
    WHERE _ingest_ts = (SELECT MAX(_ingest_ts) FROM bronze.leads_raw)
    GROUP BY lead_id
),
first_call AS (
    SELECT c.lead_id, l.created_ts, MIN(c.call_start_ts_utc) AS first_ts
    FROM silver.call c JOIN leads l ON l.lead_id = c.lead_id
    WHERE c.region_code = 'AU'
    GROUP BY c.lead_id, l.created_ts
)
SELECT percentile((unix_timestamp(first_ts) - unix_timestamp(created_ts)) / 60, 0.5)
    AS median_min,
    (SELECT COUNT(*) FROM silver.call c
     JOIN bronze.calls_raw b
       ON b.call_id = c.call_id AND b._source_file = c.source_file
      AND b._source_row = c.source_row
     WHERE c.region_code = 'AU'
       AND b._ingest_ts = (SELECT MAX(_ingest_ts) FROM bronze.calls_raw
                           WHERE _source_file = 'calls_uk_au.csv')
       AND c.call_start_ts_utc <> to_timestamp(b.call_start_ts_utc)) AS au_rows_changed,
    (SELECT COUNT(*) FROM silver.call WHERE region_code = 'AU') AS au_rows
FROM first_call;

-- name: policy_date_vs_call_date_by_offset
-- Which side is shifted. Finance's application_date against the date of the linked call,
-- with the call moved back 0 to 24 hours. Counts policy rows (bronze, latest batch).
WITH raw AS (
    SELECT *, MAX(_ingest_ts) OVER (PARTITION BY _source_file) AS _latest FROM bronze.calls_raw
),
calls AS (
    SELECT DISTINCT call_id, region_code,
        CASE WHEN _source_file = 'calls_uk_au.csv' THEN to_timestamp(call_start_ts_utc)
             ELSE to_timestamp(call_start_ts, 'dd/MM/yyyy HH:mm') END AS start_ts
    FROM raw WHERE _ingest_ts = _latest
),
policies AS (
    SELECT call_id, to_date(application_date) AS application_date
    FROM bronze.policies_raw
    WHERE _ingest_ts = (SELECT MAX(_ingest_ts) FROM bronze.policies_raw)
),
offsets AS (SELECT explode(sequence(0, 24)) AS offset_hours)
SELECT c.region_code, o.offset_hours,
    SUM(CASE WHEN p.application_date
                  = to_date(c.start_ts - make_interval(0, 0, 0, 0, o.offset_hours, 0, 0))
             THEN 1 ELSE 0 END) AS matching,
    COUNT(*) AS policies
FROM policies p
JOIN calls c ON c.call_id = p.call_id
CROSS JOIN offsets o
GROUP BY c.region_code, o.offset_hours
ORDER BY c.region_code, o.offset_hours;

-- name: lead_internal_timing
-- Lead side: minutes from created_at_utc to the lead's own ingested_at_utc and consent
-- timestamp, by region. A shifted AU created_at would show as a different lag.
SELECT region_code,
    percentile((unix_timestamp(to_timestamp(ingested_at_utc))
                - unix_timestamp(to_timestamp(created_at_utc))) / 60, 0.5)
        AS median_ingest_lag_min,
    percentile((unix_timestamp(to_timestamp(consent_timestamp_utc))
                - unix_timestamp(to_timestamp(created_at_utc))) / 60, 0.5)
        AS median_consent_lag_min,
    COUNT(*) AS leads
FROM bronze.leads_raw
WHERE _ingest_ts = (SELECT MAX(_ingest_ts) FROM bronze.leads_raw)
GROUP BY region_code
ORDER BY region_code;

-- name: register_vs_distinct
-- Defects 16 and 17: the register counts bronze rows (duplicates included); silver counts
-- distinct calls. Both, side by side.
WITH raw AS (
    SELECT *, MAX(_ingest_ts) OVER (PARTITION BY _source_file) AS _latest FROM bronze.calls_raw
),
typed AS (
    SELECT call_id, lead_id, agent_id, _source_file,
        CASE WHEN _source_file = 'calls_uk_au.csv' THEN to_timestamp(call_start_ts_utc)
             ELSE to_timestamp(call_start_ts, 'dd/MM/yyyy HH:mm') END AS start_ts,
        CASE WHEN _source_file = 'calls_uk_au.csv' THEN to_timestamp(call_end_ts_utc)
             ELSE to_timestamp(call_end_ts, 'dd/MM/yyyy HH:mm') END AS end_ts
    FROM raw WHERE _ingest_ts = _latest
),
calls AS (SELECT DISTINCT * FROM typed),
leads AS (
    SELECT lead_id, MIN(to_timestamp(created_at_utc)) AS created_ts
    FROM bronze.leads_raw
    WHERE _ingest_ts = (SELECT MAX(_ingest_ts) FROM bronze.leads_raw)
    GROUP BY lead_id
),
roster AS (SELECT DISTINCT agent_id FROM silver.agent)
SELECT
    (SELECT COUNT(*) FROM typed WHERE end_ts < start_ts AND _source_file = 'calls_uk_au.csv')
        AS end_before_start_raw_uk_au,
    (SELECT COUNT(*) FROM typed WHERE end_ts < start_ts) AS end_before_start_raw,
    (SELECT COUNT(*) FROM calls WHERE end_ts < start_ts) AS end_before_start,
    (SELECT COUNT(*) FROM typed t JOIN leads l USING (lead_id) WHERE t.start_ts < l.created_ts)
        AS before_lead_raw,
    (SELECT COUNT(*) FROM calls c JOIN leads l USING (lead_id) WHERE c.start_ts < l.created_ts)
        AS before_lead,
    (SELECT COUNT(*) FROM typed WHERE agent_id NOT IN (SELECT agent_id FROM roster))
        AS ghost_agent_raw,
    (SELECT COUNT(*) FROM calls WHERE agent_id NOT IN (SELECT agent_id FROM roster))
        AS ghost_agent,
    (SELECT COUNT(DISTINCT lead_id) FROM calls WHERE lead_id NOT IN (SELECT lead_id FROM leads))
        AS ghost_lead_ids,
    (SELECT COUNT(*) FROM typed WHERE lead_id NOT IN (SELECT lead_id FROM leads))
        AS ghost_lead_raw,
    (SELECT COUNT(*) FROM calls WHERE lead_id NOT IN (SELECT lead_id FROM leads))
        AS ghost_lead;

-- name: ghost_lead_evidence
-- Defect 17. Call lead ids with no surviving or mapped lead, checked against the leads
-- removed or set aside at step 7 (PK conflicts, unidentifiable, outside the period).
WITH raw AS (
    SELECT *, MAX(_ingest_ts) OVER (PARTITION BY _source_file) AS _latest FROM bronze.calls_raw
),
calls AS (SELECT DISTINCT call_id, lead_id, region_code FROM raw WHERE _ingest_ts = _latest),
leads AS (
    SELECT * FROM bronze.leads_raw
    WHERE _ingest_ts = (SELECT MAX(_ingest_ts) FROM bronze.leads_raw)
),
pk AS (SELECT lead_id FROM leads GROUP BY lead_id HAVING COUNT(*) > 1),
unid AS (
    SELECT DISTINCT lead_id FROM leads
    WHERE (email IS NULL OR TRIM(email) = '') AND (phone IS NULL OR TRIM(phone) = '')
),
outside AS (
    SELECT DISTINCT lead_id FROM leads
    WHERE to_date(to_timestamp(created_at_utc)) NOT BETWEEN '2026-04-01' AND '2026-06-30'
),
unmatched AS (
    SELECT DISTINCT lead_id FROM calls
    WHERE lead_id NOT IN (SELECT source_lead_id FROM silver.lead_dedup_map)
),
ghost AS (SELECT lead_id FROM unmatched WHERE lead_id NOT IN (SELECT lead_id FROM leads)),
ghost_calls AS (SELECT c.* FROM calls c JOIN ghost g USING (lead_id))
SELECT
    (SELECT COUNT(*) FROM unmatched) AS unmatched_ids,
    (SELECT COUNT(*) FROM unmatched JOIN pk USING (lead_id)) AS match_pk_conflict,
    (SELECT COUNT(*) FROM unmatched JOIN unid USING (lead_id)) AS match_unidentifiable,
    (SELECT COUNT(*) FROM unmatched JOIN outside USING (lead_id)) AS match_outside_period,
    (SELECT COUNT(*) FROM ghost) AS ghost_ids,
    (SELECT COUNT(*) FROM ghost WHERE lead_id RLIKE '^LD-GHOST-(UK|AU|CA|US)-[0-9]+$')
        AS ghost_pattern_ids,
    (SELECT COUNT(*) FROM ghost_calls) AS ghost_calls,
    (SELECT COUNT(*) FROM ghost_calls WHERE split(lead_id, '-')[2] = region_code)
        AS prefix_matches_call_region,
    (SELECT COUNT(*) FROM bronze.policies_raw
     WHERE _ingest_ts = (SELECT MAX(_ingest_ts) FROM bronze.policies_raw)
       AND (call_id IN (SELECT call_id FROM ghost_calls)
            OR lead_id IN (SELECT lead_id FROM ghost))) AS linked_policies;

-- name: start_before_lead_gap
-- Defect 16. How far before its lead each quarantined call starts (minutes).
WITH q AS (
    SELECT CAST(regexp_extract(detail, '([0-9.]+)$', 1) AS DOUBLE) AS minutes
    FROM silver.dq_exception
    WHERE check_name = 'call_start_before_lead'
      AND run_id = (SELECT run_id FROM silver.dq_exception
                    WHERE check_name = 'call_start_before_lead'
                    ORDER BY detected_at_utc DESC LIMIT 1)
)
SELECT COUNT(*) AS calls, MIN(minutes) AS min_minutes,
    percentile(minutes, 0.5) AS median_minutes, MAX(minutes) AS max_minutes
FROM q;

-- name: calls_after_do_not_call
-- Defect 26. Calls starting after the lead's first DO_NOT_CALL disposition, by region:
-- on distinct calls (the register's base) and on silver.call.
WITH raw AS (
    SELECT *, MAX(_ingest_ts) OVER (PARTITION BY _source_file) AS _latest FROM bronze.calls_raw
),
calls AS (
    SELECT DISTINCT call_id, lead_id, region_code, disposition,
        CASE WHEN _source_file = 'calls_uk_au.csv' THEN to_timestamp(call_start_ts_utc)
             ELSE to_timestamp(call_start_ts, 'dd/MM/yyyy HH:mm') END AS start_ts
    FROM raw WHERE _ingest_ts = _latest
),
flagged AS (
    SELECT * FROM (
        SELECT *, MIN(CASE WHEN disposition = 'DO_NOT_CALL' THEN start_ts END)
                  OVER (PARTITION BY lead_id) AS first_dnc
        FROM calls
    ) WHERE start_ts > first_dnc
),
silver AS (
    SELECT region_code, COUNT(*) AS calls_silver, COUNT(DISTINCT lead_id) AS leads_silver
    FROM silver.call WHERE after_do_not_call GROUP BY region_code
)
SELECT f.region_code, COUNT(*) AS calls_distinct, COUNT(DISTINCT f.lead_id) AS leads_distinct,
    FIRST(s.calls_silver) AS calls_silver, FIRST(s.leads_silver) AS leads_silver
FROM flagged f LEFT JOIN silver s USING (region_code)
GROUP BY f.region_code
ORDER BY f.region_code;
