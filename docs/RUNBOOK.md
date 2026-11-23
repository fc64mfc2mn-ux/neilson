# Runbook

## Schedule and upstream dependencies

Daily at 06:00 UTC, after the upstream extracts have landed (proposed; the pack sets no
schedule). Inputs and owners (`SOURCE_SYSTEM_OWNERS.md`): LeadFlow CRM (leads and
consent), AdHub (campaigns and spend), Aurora Dialler (calls), the HR roster (agents),
PolicyCore (policies), Treasury (monthly FX), and reference data (channel, region and the
security map). If a file is missing the run does not start (exit 1). A month with no FX
rate is quarantined, not filled.

## Pipeline stages with expected runtime

Measured on a second (idempotent) local run on an Apple Silicon Mac
(`run_id` 20261123T115739Z-d8779790, total 5 min 46 s including Spark start-up). On a GitHub
Actions runner a full first run takes about 6 minutes (348 s).

| Stage | Does | Time |
| --- | --- | ---: |
| `bronze.ingest_all` | Land all 12 files as strings; skip unchanged files | 20 s |
| `silver.build_silver` | Type, deduplicate, normalise calls, FX, all DQ checks | 2 min 49 s |
| `gold.build_dimensions` | Dimensions, SCD2 `dim_agent`, bridge to user regions | 30 s |
| `gold.build_dim_lead` | PII policy, `dim_lead`, `dim_lead_nopii` | 15 s |
| `gold.build_facts` | Facts and `bridge_lead_call` | 44 s |
| `gold.build_lead_funnel` | `fact_lead_funnel`, one row per surviving lead | 16 s |
| `gold.scan_pii` | Scan every gold column for raw PII | 30 s |
| `export.powerbi` | CSVs to `export/powerbi/` | 19 s |
| DQ gate (`src.dq_gate`, separate) | Compare error counts with the expected lines | about 20 s |

A stage taking twice its usual time is a warning (see monitoring).

## Failure modes

| Symptom | Likely cause | Action | Escalate to |
| --- | --- | --- | --- |
| Run exits 1 before any stage: "Source files not found" | An upstream extract did not land | Check the landing path; rerun when the file arrives | The source owner |
| Spark fails to start: `UnsupportedClassVersionError`, or a gateway error | Wrong Java version (not 17) | Point `JAVA_HOME` at Java 17 | Platform team |
| DQ gate fails: `spend_fx_rate_missing` above 180 | A new month or currency has no Treasury rate | Load the rate and rerun; never carry a rate forward | Finance Treasury |
| DQ gate fails: a check with no expected line | New kind of bad data, or a code change | Add a register row and a gate line, or fix the code | BI engineering lead |
| Run stops at the PII scan | A raw identifier reached a gold column | Do not publish; find the column, fix the policy, rebuild gold | Data Protection Officer and BI lead |
| Stage fails with a reconciliation error (rows in ≠ rows out + exceptions) | A transformation dropped or doubled rows | Fix and rerun; nothing is published from a failed run | BI engineering lead |
| Hashed keys in gold all change | `config/secrets.json` (the salt) was deleted or rotated | Restore the salt, or plan a full gold rebuild and Power BI refresh | BI engineering lead |
| SCD2 stage fails: three or more agent versions | The roster gained a third version with no dates | Get effective dates from HR (assumption 8) | HR systems owner |

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

- **Rerun.** The one command is idempotent. Bronze appends a new batch only when a file's
  fingerprint changes (`bronze.ingest_log`). Silver and gold are rebuilt from the latest
  batch, and every run writes its exceptions under a new `run_id`.
- **Corrected source file** (for example, Treasury supplies the CAD June rate): land the
  file and rerun. The quarantined rows come back in silver, and the gate's expected line
  for that check is lowered in the same change.
- **Force a reload of one file**: delete that file's rows from `bronze.ingest_log`, then
  rerun.
- **Full rebuild** (after a schema or salt change): delete `lakehouse/` and rerun. Bronze is
  rebuilt from `data/`.
- After any reprocess, refresh the Power BI export and model, and compare the values with
  `powerbi/expected_values.csv` after regenerating it (`python -m src.powerbi_expected`).

## Monitoring and alerting design

- **Per run:** stage start and end times, and the row counts in, out and to exceptions,
  logged with the `run_id`. The DQ gate's result.
- **Alert (page)** on a failed run, a DQ gate failure or a PII scan stop.
- **Warn (ticket)** when a warning check moves more than 20% from its last run (for
  example, `call_after_do_not_call` or `policy_fx_rate_missing`), or when a stage takes
  twice its usual time.
- **Trend:** a small dashboard on `silver.dq_exception` by check and run, so the business
  owners see their data-quality findings over time.

## Orchestration choice, ADF against Databricks Jobs, with the reason

Databricks Jobs. Every stage is Spark on Delta in one workspace, so a job with one task
per stage gives retries, run history and lineage in Unity Catalog without a second tool.
The Asset Bundle (`databricks.yml`) deploys the same job to dev and prod. ADF would add
value only for copying files from systems Databricks cannot reach. If that is needed, ADF
lands the files and triggers the job; it does not orchestrate the stages.

## Support model and on-call expectations

Business hours only (UK), owned by the BI engineering team; this is a daily reporting
pipeline, not a customer-facing one. A failed run is picked up the same morning, and
reports show the previous day's data until it is fixed. A PII scan stop is handled first
and reported to the Data Protection Officer the same day. Source data problems go to the
owners in `SOURCE_SYSTEM_OWNERS.md`, with the evidence query from
`OPEN_QUESTIONS.md`.
