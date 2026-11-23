# Northgate Life funnel

## Purpose

Northgate Life's lead-to-policy funnel: marketing spend, leads, contact-centre calls and
policies, cleaned in a local medallion lakehouse (PySpark and Delta) and served to a Power
BI model. It answers what a lead costs by channel, how leads turn into policies by region,
and how fast and how well the contact centre works them. Two certified conversion
measures replace one disputed one (see `docs/KPI_DECISION_LOG.md`).

## Prerequisites

- **Python 3.11.** Check with `python3.11 --version`.
- **Java 17.** PySpark 3.5.1 and delta-spark 3.2.0 are pinned against it. Check with
  `java -version` (it should say `17`). If it shows another version, point `JAVA_HOME`
  at a Java 17 install. On macOS with Homebrew:

  ```bash
  brew install openjdk@17
  export JAVA_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
  export PATH="$JAVA_HOME/bin:$PATH"
  java -version
  ```

  Put the two `export` lines in your shell profile or `.venv/bin/activate`.
- **Virtual environment and pinned packages** (pyspark, delta-spark, pytest, ruff):

  ```bash
  python3.11 -m venv .venv
  source .venv/bin/activate
  pip install -r requirements.txt
  ```

  Spark downloads the Delta jars from Maven the first time it starts, so that first run
  needs network access.

## How to run, one command

From the repo root, with the venv active:

```bash
python -m src.run_pipeline --config config/config.yaml
```

No other setup is needed. `config/config.yaml` is committed and holds only local `data/`
paths. On the first run the pipeline creates `config/secrets.json` (gitignored, owner-only
permissions) with a random PII hash salt, and reuses it afterwards. Deleting that file
changes the salt, and with it every hashed key in gold.

It runs bronze, silver, gold, the gold PII scan and the Power BI export in order, in a UTC
Spark session; a failed stage stops the run with exit code 1. A second run adds no rows.
Then gate the run on data quality:

```bash
python -m src.dq_gate --config config/config.yaml
```

### Power BI export

The last stage writes one CSV per gold table to `export/powerbi/` (gitignored, like
`lakehouse/`); rerun it alone with `python -m src.export_powerbi --config
config/config.yaml`. The export stops if any raw PII column would leave gold. Operating
the pipeline (failures, reruns, backfill) is in `docs/RUNBOOK.md`.

## Input data

Twelve read-only sources in `data/` (hashes in `data/MANIFEST.md`): leads, consent
events, campaigns and daily spend (marketing); two call files and the agent roster
(contact centre); policies and monthly FX rates (finance); channel, region and the
user-to-region security map (reference). The legacy procedure and the three stakeholder
notes are in `data/legacy/` and `data/stakeholder_notes/`. Problems in the inputs are
findings (`docs/DEFECT_REGISTER.md`), never fixed in place.

## Output tables

- **Bronze** (`lakehouse/bronze/`): one `*_raw` table per source, all strings, with
  `_ingest_ts`, `_source_file`, `_source_row` and `_row_hash`. Append-only: a rerun appends
  nothing unless a file changed.
- **Silver**: typed, deduplicated and conformed: `lead` (47,201 surviving leads),
  `call` (62,829), `policy` (5,172), `marketing_spend` (2,001 rows), plus
  `consent_current`, `lead_dedup_map`, reference tables, and `dq_exception` (every
  rejected or flagged row, with its check, defect, key, severity and action).
- **Gold**: a star schema. Dimensions `dim_date`, `dim_region`, `dim_channel`,
  `dim_campaign`, `dim_product`, `dim_agent` (SCD2), `dim_agent_seniority`, `dim_lead`,
  and `dim_lead_nopii`. Facts `fact_lead`, `fact_call`, `fact_policy`,
  `fact_marketing_spend_daily`, and `fact_lead_funnel` (one row per surviving lead).
  Bridges `bridge_lead_call` and `bridge_user_region`.

### What is quarantined and why

Quarantined rows leave the silver tables but stay in `silver.dq_exception`. Counts are
from the latest run (`src/sql/findings_quarantine.sql: quarantine_by_check`):

| Check | Rows | Severity | Why |
| --- | ---: | --- | --- |
| `call_exact_duplicate` | 540 | info | The same call row twice |
| `call_end_before_start` | 380 | error | Impossible duration |
| `call_start_before_lead` | 251 | warning | Call before its lead exists (after the AU conversion) |
| `spend_fx_rate_missing` | 180 | error | CAD June spend, no rate (open question 5) |
| `spend_exact_duplicate` | 40 | info | Exact copies |
| `lead_unidentifiable` | 35 | error | No email and no phone, so it cannot be deduplicated |
| `policy_conflicting_duplicate` | 16 | warning | Copies merged; earliest wins (assumption 23) |
| `policy_exact_duplicate` | 12 | info | Exact copies |
| `lead_pk_conflict` | 6 | error | 3 `lead_id`s used by two different people; not resolved |
| `spend_negative` | 3 | warning | Negative spend with nothing to offset |

The 758 duplicate leads are merged, not quarantined (`silver.lead_dedup_map`). Other
checks flag or correct rows without removing them: for example, 181 minor-unit spend rows
are divided by 100, and 6,311 calls after a DO_NOT_CALL disposition are flagged. The DQ
gate expects 602 error rows per run (`docs/RUNBOOK.md`).

## Semantic model and measures

`powerbi/northgate_funnel.pbix`: a star schema on the gold CSV export, with a marked date
table, a `Key Measures` table, the four-visual report page (`powerbi/report_page.png`) and
the `Regional_Analyst` RLS role. Every measure's DAX, description and format is in
`powerbi/measures.dax`. The model design and build steps are in
`powerbi/MODEL_DESIGN.md`. `powerbi/expected_values.csv` holds the values each measure
should show, computed independently in SQL; they were checked in Desktop (screenshots in
`powerbi/`). `powerbi/model_walkthrough.mov` is a walkthrough of the report and the model.

## Usage guidelines, who should use this and how

- **Marketing** uses `Cost Per Lead (GBP)` and `Lead to Policy Conversion Rate` (lead
  grain, by lead-created date and first-touch campaign).
- **Sales Operations** uses `Connected Call to Policy Conversion Rate` (call grain,
  business-hours leads, by policy start, credited to the last-call agent).
  `docs/KPI_RECONCILIATION.md` explains why the two rates differ (Q2 2026: 9.53% vs 18.56%).
  Never quote either one as plain "conversion rate".
- **Regional analysts** see only their mapped regions. Anyone not in
  `bridge_user_region` sees nothing (`docs/GOVERNANCE.md`).
- Don't show July for the call-grain rate, or CPL under product or lead slicers
  (`powerbi/MODEL_DESIGN.md`, limits).

## Assumptions

All 28 are in `docs/ASSUMPTIONS.md`. The headline one is entry 13: two certified
conversion measures, not one. Questions for the data owners are in
`docs/OPEN_QUESTIONS.md`.

## Known limitations

- Only `Regional_Analyst` RLS is built in Power BI. The other roles, and the Unity Catalog
  grants, row filter and mask, are designed but not built (`docs/GOVERNANCE.md`).
- 675 policies have no GBP premium, because there is no CAD June rate and no July rate.
  `Average Annual Premium (GBP)` states its coverage.
- The AU first-call gap (median 8.5 hours) remains after the Sydney conversion. It is
  flagged, not corrected.
- Business hours have no holiday calendar. The attribution window is not applied.
- The Power BI model imports the CSV export; it does not connect to a SQL warehouse.

## Performance result

Not captured: the DAX Studio before-and-after artefact for one tuned measure was cut
(stretch; see below). `powerbi/performance/` does not exist yet.

## Key Vault secret-scope path

Locally the PII hash salt comes from `config/secrets.json`, which is gitignored; the
committed `config/secrets.example.json` has placeholders only. In production:
`dbutils.secrets.get(scope="kv-nfs-bi", key="pii-hash-salt")`
(`docs/platform_architecture.md`).

## Time spent and what I cut and why

> **TODO (Jason):** actual hours spent.

Cut, in priority order (correct data, tests and reproducible numbers came first):

1. Unity Catalog grants, row filter and mask (`src/sql/unity_catalog_grants.sql` is a
   stub). The access control that counts here, gold PII handling plus Power BI RLS, is
   built and evidenced.
2. Power BI roles other than `Regional_Analyst`. They need object-level security.
3. The DAX Studio performance artefact.
4. Deploying the Asset Bundle (`databricks.yml` is a stub).
