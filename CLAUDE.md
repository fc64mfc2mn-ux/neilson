# CLAUDE.md: Northgate Life funnel (Neilson Staff BI Engineer take-home)

This repo is a take-home case study: a local medallion lakehouse (PySpark + Delta),
a Power BI semantic model, governance, and a written handover for a fictional
insurer, Northgate Life. Read `docs/Candidate_Brief.md` first. It is the source of
truth. Then read `docs/PLAN.md`, `docs/DEFECT_REGISTER.md` and
`docs/KPI_DECISION_LOG.md`.

Deadline: Thursday 26 November 2026. Target submission: Wednesday 25 November evening.

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
5. **Bronze does no cleansing:** all columns strings, plus `_ingest_ts`, `_source_file`,
   `_row_hash`, append-only.
6. **One command runs everything:** `python -m src.run_pipeline --config config/config.yaml`.
7. **Every rejected row goes to `silver.dq_exception`** through `src/common/dq.py` with
   `check_name`, `source_table`, `key_value`, `severity`, `detected_at_utc`.
8. **Every number quoted in a document must come from a query kept in the repo**
   (put them in `src/sql/findings_*.sql` or a test), so findings are reproducible.

## Commands

```bash
python3.11 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
java -version                      # must be 17
cp config/config.example.yaml config/config.yaml
cp config/secrets.example.json config/secrets.json   # then set a local salt
python -m src.run_pipeline --config config/config.yaml
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

- Do one plan step at a time. State the check, run it, show the result, then commit.
- Ask before deviating from the brief or the decisions above.
- Prefer small, tested functions. Tests use the `spark` fixture in `tests/conftest.py`.
- Keep documents within the brief's word targets and required sections.
- Where evidence is ambiguous (timezones, minor-unit spend, channel source of truth),
  measure first, decide second, and write the choice and its reversal route into
  `docs/ASSUMPTIONS.md`.

## Where things stand

Repo scaffold and input data are committed. Next: step 1 of `docs/PLAN.md`.
