# CLAUDE.md: Northgate Life funnel (Neilson Staff BI Engineer take-home)

This repo is a take-home case study: a local medallion lakehouse (PySpark + Delta),
a Power BI semantic model, governance, and a written handover for a fictional
insurer, Northgate Life. Read `docs/Candidate_Brief.md` first. It is the source of
truth. Then read `docs/PLAN.md`, `docs/DEFECT_REGISTER.md` and
`docs/KPI_DECISION_LOG.md`.

Deadline: Thursday 26 November 2026. Target submission: Wednesday 25 November evening.

## Priority order

When time is short, work top-down. Never steal time from a higher rung to
polish a lower one.

1. **Correct data.** Every table reconciles to the defect register; every
   rejected row is in `silver.dq_exception` with check name, severity, key.
   No silent fixes, no silent drops.
2. **Tests and reproducible numbers.** The six named tests pass in CI. Every
   number quoted in a document traces to a query in `src/sql/findings_*.sql`
   or a test. If a finding can't be re-run, it isn't a finding.
3. **Executable handover.** A fresh clone on the local stack runs
   `python -m src.run_pipeline --config config/config.yaml` end to end, and a
   second run is idempotent (no doubled rows). The reviewer runs this exact
   command on a Mac — the Databricks validation is evidence, not a substitute.
4. **Real committed history.** Commit after every green check. Never end a
   session with finished work uncommitted. Small commits, each saying what
   changed and why; at least 12 in total; never squash or amend published
   commits.
5. **Power BI core.** `.pbix` star schema with measures 1, 2, 3, 5, 7, 9, the
   marked date table, and the four-visual page; DAX mirrored in
   `powerbi/measures.dax`. This is core scope, not stretch — build it as soon
   as rungs 1 to 4 hold.
6. **Stretch** — only after 1 to 5: legacy port with parity test, UC grants and
   Asset Bundle stub, performance artefact, extra measures, remaining roles.
   SCD2 and both bridges are core work in steps 10 and 12; they aren't stretch, and
   they aren't done in this repo yet.

## Hard rules

1. **Never edit anything under `data/`.** It is read-only and the reviewer rechecks its
   SHA-256 hashes against `data/MANIFEST.md`. Input problems are findings to write up,
   not things to fix in place.
2. **Use the exact names from the brief** for every table, column and measure. They are
   checked. Bronze: `bronze.leads_raw`, `bronze.consent_events_raw`, `bronze.calls_raw`,
   `bronze.policies_raw`, `bronze.marketing_spend_raw`, `bronze.campaigns_raw`,
   `bronze.agents_raw`, `bronze.fx_rates_raw`, `bronze.ref_channel_raw`,
   `bronze.ref_region_raw`, `bronze.sec_user_region_raw`. Silver and gold names are in the
   brief's "Required lakehouse tables" section.
3. **Stack is fixed:** Python 3.11, PySpark 3.5.1, delta-spark 3.2.0, Java 17, local Delta
   tables under `./lakehouse/`, `ruff`, `pytest`. No Databricks workspace, Azure or cloud
   dependency in the core path. `databricks.yml` is a stub only (stretch).
4. **No secrets in code or git.** The PII hash salt comes from `config/secrets.json`
   (gitignored), loaded via `src/common/config.py`. Only `config/secrets.example.json`
   with placeholders is committed. `lakehouse/` is never committed.
5. **Bronze does no cleansing:** all columns strings, plus `_ingest_ts`, `_source_file`, `_source_row`,
   `_row_hash`, append-only.
6. **One command runs everything:** `python -m src.run_pipeline --config config/config.yaml`.
7. **Every rejected row goes to `silver.dq_exception`** through `src/common/dq.py` with
   `check_name`, `defect_id`, `source_table`, `key_value`, `severity`, `action`, `run_id`, `detected_at_utc`.
8. **Every number quoted in a document must come from a query kept in the repo**
   (put them in `src/sql/findings_*.sql` or a test), so findings are reproducible.
9. **Every stage ships with its tests, and reconciliation checks run at every stage.**
10. **Test first.** For each stage, write the tests before the code: small fixture
    DataFrames for the logic, plus a reconciliation check (rows in = rows out + rows in
    `silver.dq_exception`). When something breaks, write the failing test first, then
    fix it.
11. **The skip-if-not-implemented behaviour in the runner is removed by step 12.**

## Cost and speed

- For a small change, run only the touched test files (`pytest tests/test_x.py -q`); never
  the full suite or the full pipeline.
- The slow set (`pytest -m slow`) runs once per step, at the end, in the background; show
  only the last 20 lines.
- Don't `rm -rf lakehouse` unless bronze or the schema changed.
- Don't re-read files already read, or print whole files or logs: use head, tail, grep.
- Reports: what changed, the counts that matter, open decisions.

## Commands

```bash
python3.11 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
java -version                      # must be 17
# On this Mac, activating the venv sets JAVA_HOME to the Homebrew openjdk@17 path.
# Java 17 must be active before running Spark. System default is Java 21.
python -m src.run_pipeline --config config/config.yaml
# config/config.yaml is committed (local paths only). config/secrets.json is gitignored
# and generated with a random salt on the first run. Never commit it.
ruff check .
pytest
```

## Decisions already made (see docs/ for reasoning)

- **KPI conflict: option C.** Two separately certified, explicitly named conversion
  measures: `Lead to Policy Conversion Rate` (lead grain) and
  `Connected Call to Policy Conversion Rate` (call grain), plus a reconciliation view.
  Nothing is ever called plain "Conversion Rate". See `docs/KPI_DECISION_LOG.md`.
- **First access control to implement:** gold PII handling (`src/gold/apply_pii_policy.py`).
  Regional_Analyst RLS second, in Power BI Desktop on the Windows VM.
- **Dedup rule for leads:** normalised email or phone, within 48 hours, earliest lead
  survives; `lead_id` primary-key conflicts are quarantined, not resolved.
- **Spark session timezone is UTC.**
- **Timezones of call files are decided by evidence** (first-call latency under each
  hypothesis) and logged in `docs/ASSUMPTIONS.md`, not assumed.
- **Starter config bug:** `calls_ca_us` is declared `sep: ";"` but the file is
  comma-delimited. Fix it and record it as a finding.
- **SCD2 scope:** only `gold.dim_agent` is SCD2 (one row per agent per validity window:
  `valid_from`, `valid_to`, `is_current`, plus `UNKNOWN`). `silver.agent` keeps every
  version without dates; step 10 builds the windows in `_source_row` order (assumption 8).
- **RLS, one source of truth:** `gold.bridge_user_region`. Power BI `Regional_Analyst`
  filters on `USERPRINCIPALNAME()` against it (core, step 17; view-as evidence for a
  two-region user and an unmapped user). Data layer (step 20, stretch): a Unity Catalog
  row filter on each fact's `region_key` using the bridge, plus a column mask or no-PII
  view for `Marketing_Analyst_NoPII`. Region `ALL` = no restriction; unmapped user = no
  rows. No wide table: keep the star schema.
- **DQ gate expected counts** come from the register, one line per defect with its test,
  and are not copied from a run.
- **Questions for data owners** live in `docs/OPEN_QUESTIONS.md`.
- **Limits to state in `docs/platform_architecture.md`:** UC row filters cannot run in
  local Spark, so they are SQL tested against the entitlements matrix with fixtures only.
  Power BI Import with a shared service account does not pass the user's identity to UC;
  the data-layer filter applies only with DirectQuery and SSO passthrough, so the Power BI
  role stays as defence in depth.
- **Surrogate keys** are 52 bits (below 2^53, exact in Power BI), deterministic,
  collision-tested per dimension.
- **`agent_name` is not in gold** (data minimisation); `agent_id`, team, region and
  seniority band are.
- **`dim_region` and `dim_product` have no UNKNOWN member** (the brief gives UNKNOWN only
  to channel, campaign and agent). At step 12 fact builds must raise an error if any
  region or product key is unresolved. They must never drop rows or set nulls.
- **Step 11 choices (confirmed):** `gold.dim_lead_nopii` is a table here (a view in Unity
  Catalog); `date_of_birth_hash` stays in `dim_lead`; a lead with no consent event keeps
  its capture flag, labelled `lead_capture`; AU postcode keeps the first 3 digits.
- **Consent in measures:** any Power BI measure that uses marketing consent filters on
  `consent_marketing_source`, because `lead_capture` is weaker evidence than a consent
  event.
- **Tier 2 agent columns:** `agent_id` stays visible (it is an operational key, not
  sensitive). Only `seniority_band` / `dim_agent_seniority` is restricted, and the small-group
  risk is handled by the `Agents in Group >= 3` visual filter.
- **Seniority table (step 16b):** `gold.dim_agent_seniority` (`agent_key`,
  `seniority_band`, tier 2) is the separate table for the agent-seniority visual, so OLS
  can hide it whole; `agent_id` stays in `dim_agent`. The model removes `seniority_band`
  from `dim_agent` and imports `dim_lead_nopii`, never `dim_lead`.
- **Lead keys in gold (step 16b):** `gold.fact_policy` carries the surviving lead's
  `campaign_key`, `channel_key` and `lead_created_date_key` (null without a lead), and
  `fact_lead_funnel` carries `lead_created_date_key` and `first_call_bucket`, so the
  model needs no Power Query merge or staging query.
- **Policy status (assumption 3):** exclude NTU only; cancelled and lapsed policies count.
- **`seniority_band` stays in `gold.dim_agent`** (tier 2): the SCD2 versions of AG0008
  and AG0037 differ only by it after the defect 27 fix. Power Query removes it from the
  model's `dim_agent`.
- **Small seniority groups:** the seniority visual has a visual-level filter
  `Agents in Group >= 3` (helper measure, not a KPI; no second conversion measure, one
  certified measure per KPI). Region stays off that visual. The helper counts
  `dim_agent[agent_id]` through `fact_call`, because the two agent tables are not related.

## Reference implementation (read-only)

An earlier attempt built the full pipeline in Databricks at
`/Users/jason/neilson/neilson 2/neilson`. It is reference only: never edit it, and don't
copy files wholesale. Read it, then write a cleaner version here, stage by stage, in the
order of `docs/PLAN.md` steps 4 onward. Known traps in it (each needs a test here):

- **AU call timezone.** UK, CA and US call timestamps behave as UTC (median first-call
  gap 22 to 44 minutes). AU call times are shifted: every region's policy application
  date matches its linked call's date, and for AU that holds for all 426 policies only
  after moving call times back exactly 10 hours. Convert AU with Australia/Sydney,
  set per region in config. About 8.5 hours of first-call gap remains unexplained after
  conversion: keep it flagged, don't correct it further, and test it. The policy-date
  match is a permanent test for every region.
- **The funnel's grain.** `bridge_lead_call` is not the funnel. `fact_lead_funnel` has one
  row per surviving lead (47,201 expected), and a test asserts it.
- **DQ gate.** No zero threshold: 602 error-severity exceptions per run at step 12;
  the step 13 gate builds its expected counts from the register, one line per defect,
  and must reconcile to the run. Gate on the expected
  count per defect, held in config, and fail only on a breach beyond it. Test a seeded
  breach (fails) and clean data (passes).
- **Spend reconciliation.** 2,224 = 2,001 kept + 40 exact copies + 180 CAD June rows + 3
  negatives. The earlier "4 missing FX" claim was wrong.
- **Minor-unit spend.** Record the evidence and the threshold in `ASSUMPTIONS.md`.

## Expected counts (independently verified)

Leads 47,997 distinct ids -> 47,201 surviving (3 PK conflicts, 758 duplicates, 35
unidentifiable). Calls 64,000 rows -> 62,829 (540 exact duplicates, 380 end before
start, 251 start before their lead, after the AU Sydney conversion; by distinct call id
63,460 -> 62,829, confirmed independently; 245 before the conversion). Policies 5,200
-> 5,172. Any difference is explained, never forced to match.

## Git conventions

- Small commits, each with a message saying what changed and why. Aim for at least 12.
- Never squash, amend published commits, or backdate. The history must be real.
- Commit after each plan step that ends in a check passing.

## AI usage log

After each significant task, append to `docs/AI_LOG.md`: what was asked (verbatim
prompt for at least one), what the tool produced, anything it got wrong and how it was
caught, and any task deliberately done by hand. `docs/AI_USAGE.md` is written from it.
Two things are written by the human, not the AI: the final KPI wording and the three
governance risks.

## Working style

- Do one plan step at a time: tests first, implement, run, compare counts to the expected
  figures, commit that slice alone, then stop and report. Wait for "go".
- Ask before deviating from the brief or the decisions above.
- Prefer small, tested functions. Tests use the `spark` fixture in `tests/conftest.py`.
- Keep documents within the brief's word targets and required sections.
- Where evidence is ambiguous (timezones, minor-unit spend, channel source of truth),
  measure first, decide second, and write the choice and its reversal route into
  `docs/ASSUMPTIONS.md`.

## Where things stand

Steps 1 to 15 done (see `git log`): scaffold, config and runner, bronze, DQ framework,
silver, gold dimensions (SCD2 `dim_agent`), PII policy, facts, bridges and
`fact_lead_funnel` (47,201), the six named tests and DQ gate (602 error lines) in CI, the
legacy port with its parity test, and the CSV export to `export/powerbi/`.
Step 16 done: `powerbi/northgate_funnel.pbix` (star schema, 'Key Measures' table, marked
date table), measure values checked against `powerbi/expected_values.csv` (screenshots in
`powerbi/`).
Step 17 done: `Regional_Analyst` RLS on `USERPRINCIPALNAME()` against
`bridge_user_region`, View as evidence for `anz.manager` (AU+US), `uk.analyst` (UK) and an
unmapped user (zero rows); see `powerbi/MODEL_DESIGN.md`.
Step 18 done: `Connected Call to Policy Conversion Rate` in `powerbi/measures.dax` and the
.pbix (Q2 18.56%, all dates 24.01%), expected values, and the bridge in
`docs/KPI_RECONCILIATION.md` / `powerbi/kpi_reconciliation.csv`.
Step 21 done: the documents. Entry 13, the governance risks, `REFLECTION.md` and
`AI_USAGE.md` are the human's own. Step 22 is the final submission review.
