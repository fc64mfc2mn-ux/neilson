# Runbook

## Schedule and upstream dependencies

<!-- fill in -->

## Pipeline stages with expected runtime

<!-- fill in -->

## Failure modes

<!-- fill in -->

## Data-quality gates and thresholds

The gate runs after every pipeline run, in CI and in production:
`python -m src.dq_gate --config config/config.yaml`. It reads `silver.dq_exception`
for that run only (the latest `run_id`, or `--run-id`).

**Threshold rule.** Error-severity rows are gated per check against an expected count
held in config (`dq_gate.expected_errors`), one line per check with its register defect
and the test that proves it. The lines come from `docs/DEFECT_REGISTER.md`, not from a
run. The build fails (exit 1) when:

1. an error check exceeds its expected count;
2. an error check appears with no expected line;
3. the run's error total exceeds the sum of the lines (602).

Below a line is a warning, not a failure: look at the source before lowering the line.
Warning and info rows are monitored, not gated. A new error check needs a register row,
a `CHECK_DEFECTS` entry and a gate line before it can pass.

| Check | Defect | Expected per run | Register evidence |
| --- | --- | --- | --- |
| `lead_pk_conflict` | 2 | 6 | 3 ids twice; both rows quarantined |
| `lead_unidentifiable` | 4 | 35 | No email and no phone |
| `fx_rate_missing` | 12 | 1 | No CAD rate for June 2026 |
| `spend_fx_rate_missing` | 12 | 180 | CAD June spend rows (187 with copies, removed first) |
| `call_timestamp_unparseable` | 14 | 0 | None found |
| `call_end_before_start` | 16 | 380 | Calls ending before they start |
| **Total** | | **602** | Matches a full run at step 12 |

Two figures differ from the register's wording: defect 2 counts 3 ids, but the gate
counts rows, so 6; defect 12 counts 187 spend rows with copies, but the exact copies
are removed first (`spend_exact_duplicate`), so 180 reach the rate check.

## Reprocessing and backfill procedure

<!-- fill in -->

## Monitoring and alerting design

<!-- fill in -->

## Orchestration choice, ADF against Databricks Jobs, with the reason

<!-- fill in -->

## Support model and on-call expectations

<!-- fill in -->
