-- Findings for step 9, policies (defects 19 to 22).
-- Executed by tests/test_silver_policy.py, which asserts the quoted numbers.
-- Bronze is append-only: each query reads the latest batch.

-- name: conflicting_duplicate_fields
-- Defect 19. For the policy ids whose copies differ, how many differ on each field:
-- one copy null and the other not (null_fill), or two different values (value_conflict).
WITH raw AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY _row_hash ORDER BY _source_row) AS copy
    FROM bronze.policies_raw
    WHERE _ingest_ts = (SELECT MAX(_ingest_ts) FROM bronze.policies_raw)
),
distinct_rows AS (SELECT * FROM raw WHERE copy = 1),
conflicting AS (
    SELECT policy_id FROM distinct_rows GROUP BY policy_id HAVING COUNT(*) > 1
),
pairs AS (
    SELECT policy_id,
        min_by(lead_id, _source_row) AS e_lead_id, max_by(lead_id, _source_row) AS l_lead_id,
        min_by(call_id, _source_row) AS e_call_id, max_by(call_id, _source_row) AS l_call_id,
        min_by(agent_id, _source_row) AS e_agent_id, max_by(agent_id, _source_row) AS l_agent_id,
        min_by(region_code, _source_row) AS e_region_code, max_by(region_code, _source_row) AS l_region_code,
        min_by(product_code, _source_row) AS e_product_code, max_by(product_code, _source_row) AS l_product_code,
        min_by(application_date, _source_row) AS e_application_date, max_by(application_date, _source_row) AS l_application_date,
        min_by(policy_start_date, _source_row) AS e_policy_start_date, max_by(policy_start_date, _source_row) AS l_policy_start_date,
        min_by(annual_premium_local, _source_row) AS e_annual_premium_local, max_by(annual_premium_local, _source_row) AS l_annual_premium_local,
        min_by(monthly_premium_local, _source_row) AS e_monthly_premium_local, max_by(monthly_premium_local, _source_row) AS l_monthly_premium_local,
        min_by(payment_frequency, _source_row) AS e_payment_frequency, max_by(payment_frequency, _source_row) AS l_payment_frequency,
        min_by(currency_code, _source_row) AS e_currency_code, max_by(currency_code, _source_row) AS l_currency_code,
        min_by(sum_assured_local, _source_row) AS e_sum_assured_local, max_by(sum_assured_local, _source_row) AS l_sum_assured_local,
        min_by(policy_status, _source_row) AS e_policy_status, max_by(policy_status, _source_row) AS l_policy_status,
        min_by(cancellation_date, _source_row) AS e_cancellation_date, max_by(cancellation_date, _source_row) AS l_cancellation_date,
        min_by(underwriting_decision, _source_row) AS e_underwriting_decision, max_by(underwriting_decision, _source_row) AS l_underwriting_decision
    FROM distinct_rows JOIN conflicting USING (policy_id)
    GROUP BY policy_id
),
long AS (
    SELECT policy_id, stack(15,
        'lead_id', e_lead_id, l_lead_id,
        'call_id', e_call_id, l_call_id,
        'agent_id', e_agent_id, l_agent_id,
        'region_code', e_region_code, l_region_code,
        'product_code', e_product_code, l_product_code,
        'application_date', e_application_date, l_application_date,
        'policy_start_date', e_policy_start_date, l_policy_start_date,
        'annual_premium_local', e_annual_premium_local, l_annual_premium_local,
        'monthly_premium_local', e_monthly_premium_local, l_monthly_premium_local,
        'payment_frequency', e_payment_frequency, l_payment_frequency,
        'currency_code', e_currency_code, l_currency_code,
        'sum_assured_local', e_sum_assured_local, l_sum_assured_local,
        'policy_status', e_policy_status, l_policy_status,
        'cancellation_date', e_cancellation_date, l_cancellation_date,
        'underwriting_decision', e_underwriting_decision, l_underwriting_decision) AS (field, earliest, later)
    FROM pairs
)
SELECT field,
    SUM(CASE WHEN (earliest IS NULL) <> (later IS NULL) THEN 1 ELSE 0 END) AS null_fill,
    SUM(CASE WHEN earliest <> later THEN 1 ELSE 0 END) AS value_conflict
FROM long
GROUP BY field
HAVING SUM(CASE WHEN NOT (earliest <=> later) THEN 1 ELSE 0 END) > 0
ORDER BY field;

-- name: survivorship_evidence
-- Defect 19. Value-vs-value conflicts against independent evidence: the agent on the
-- linked call, and annual = monthly x 12 (to 0.01).
WITH raw AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY _row_hash ORDER BY _source_row) AS copy
    FROM bronze.policies_raw
    WHERE _ingest_ts = (SELECT MAX(_ingest_ts) FROM bronze.policies_raw)
),
distinct_rows AS (SELECT * FROM raw WHERE copy = 1),
pairs AS (
    SELECT policy_id,
        min_by(agent_id, _source_row) AS e_agent, max_by(agent_id, _source_row) AS l_agent,
        COALESCE(min_by(call_id, _source_row), max_by(call_id, _source_row)) AS call_id,
        CAST(min_by(annual_premium_local, _source_row) AS DOUBLE) AS e_annual,
        CAST(max_by(annual_premium_local, _source_row) AS DOUBLE) AS l_annual,
        CAST(min_by(monthly_premium_local, _source_row) AS DOUBLE) AS monthly
    FROM distinct_rows
    GROUP BY policy_id
    HAVING COUNT(*) > 1
),
calls AS (
    SELECT DISTINCT call_id, agent_id FROM bronze.calls_raw
    WHERE _ingest_ts = (SELECT MAX(_ingest_ts) FROM bronze.calls_raw)
)
SELECT
    SUM(CASE WHEN e_agent <> l_agent THEN 1 ELSE 0 END) AS agent_conflicts,
    SUM(CASE WHEN e_agent <> l_agent AND c.agent_id = e_agent THEN 1 ELSE 0 END)
        AS agent_earliest_matches_call,
    SUM(CASE WHEN e_annual <> l_annual THEN 1 ELSE 0 END) AS premium_conflicts,
    SUM(CASE WHEN e_annual <> l_annual AND ABS(e_annual - monthly * 12) <= 0.01 * 12
             THEN 1 ELSE 0 END) AS premium_earliest_x12,
    SUM(CASE WHEN e_annual <> l_annual AND ABS(l_annual - monthly * 12) <= 0.01 * 12
             THEN 1 ELSE 0 END) AS premium_later_x12
FROM pairs p LEFT JOIN calls c ON c.call_id = p.call_id;

-- name: annual_to_monthly_ratio
-- Defect 21. Annual / monthly premium on silver policies that arrived with both values.
WITH r AS (
    SELECT CAST(annual_premium_local AS DOUBLE) / CAST(monthly_premium_local AS DOUBLE) AS ratio
    FROM silver.policy
    WHERE NOT annual_premium_derived
)
SELECT COUNT(*) AS with_both,
    SUM(CASE WHEN ABS(ratio - 12) <= 0.01 THEN 1 ELSE 0 END) AS at_12,
    percentile(ratio, 0.5) AS median_ratio,
    MIN(ratio) AS min_ratio, MAX(ratio) AS max_ratio,
    MIN(CASE WHEN ABS(ratio - 12) > 0.01 THEN ratio END) AS min_other
FROM r;

-- name: declined_yet_inforce
-- Defect 22. Underwriting DECLINED but status INFORCE: bronze rows and silver policies.
SELECT
    (SELECT COUNT(*) FROM bronze.policies_raw
     WHERE _ingest_ts = (SELECT MAX(_ingest_ts) FROM bronze.policies_raw)
       AND underwriting_decision = 'DECLINED' AND policy_status = 'INFORCE') AS raw_rows,
    (SELECT COUNT(*) FROM silver.policy
     WHERE underwriting_decision = 'DECLINED' AND policy_status = 'INFORCE') AS policies;

-- name: lead_missing_trace
-- Defect 20. Null lead_id: bronze rows, after removing duplicate copies, in silver.
WITH raw AS (
    SELECT * FROM bronze.policies_raw
    WHERE _ingest_ts = (SELECT MAX(_ingest_ts) FROM bronze.policies_raw)
),
removed AS (
    SELECT CAST(regexp_extract(detail, 'at source row ([0-9]+)', 1) AS BIGINT) AS source_row
    FROM silver.dq_exception
    WHERE check_name IN ('policy_exact_duplicate', 'policy_conflicting_duplicate')
      AND run_id = (SELECT run_id FROM silver.dq_exception
                    WHERE check_name = 'policy_conflicting_duplicate'
                    ORDER BY detected_at_utc DESC LIMIT 1)
)
SELECT
    (SELECT COUNT(*) FROM raw WHERE lead_id IS NULL) AS raw_null,
    (SELECT COUNT(*) FROM raw WHERE lead_id IS NULL
       AND CAST(_source_row AS BIGINT) NOT IN (SELECT source_row FROM removed))
        AS after_removing_copies,
    (SELECT COUNT(*) FROM silver.policy WHERE lead_id IS NULL) AS silver_null;

-- name: premium_above_12x_monthly
-- Open question (a): supplied annual premium above 12 x monthly (by more than 0.01 a
-- month), by region and payment frequency. Flagged, values unchanged.
SELECT region_code, payment_frequency, COUNT(*) AS policies,
    MIN(CAST(annual_premium_local AS DOUBLE) / CAST(monthly_premium_local AS DOUBLE))
        AS min_ratio,
    MAX(CAST(annual_premium_local AS DOUBLE) / CAST(monthly_premium_local AS DOUBLE))
        AS max_ratio
FROM silver.policy
WHERE NOT annual_premium_derived AND annual_premium_local > monthly_premium_local * 12.01
GROUP BY region_code, payment_frequency
ORDER BY region_code, payment_frequency;

-- name: policy_gbp_rate_missing
-- Step 12 (assumption 27): policies whose currency has no Treasury rate for the
-- application month, by currency and month. Asserted in tests/test_gold_facts.py.
SELECT p.currency_code,
       DATE_FORMAT(p.application_date, 'yyyy-MM') AS application_month,
       COUNT(*) AS policies
FROM silver.policy p
LEFT JOIN silver.fx_rate f
  ON f.currency_code = p.currency_code
 AND f.rate_month = DATE_FORMAT(p.application_date, 'yyyy-MM')
WHERE f.rate_to_gbp IS NULL
GROUP BY p.currency_code, DATE_FORMAT(p.application_date, 'yyyy-MM')
ORDER BY p.currency_code, application_month;
