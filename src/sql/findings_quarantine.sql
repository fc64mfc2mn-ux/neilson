-- Findings: what the latest pipeline run sent to silver.dq_exception, by check.
-- Quoted in README.md ("What is quarantined and why") and docs/RUNBOOK.md.

-- name: quarantine_by_check
-- One row per check in the latest run: severity, action, defect and row count.
SELECT check_name, severity, action, defect_id, source_table, COUNT(*) AS exception_rows
FROM silver.dq_exception
WHERE run_id = (SELECT run_id FROM silver.dq_exception ORDER BY detected_at_utc DESC LIMIT 1)
GROUP BY check_name, severity, action, defect_id, source_table
ORDER BY severity, action, exception_rows DESC, check_name
