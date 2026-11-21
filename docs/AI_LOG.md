# AI log (working notes, not the submitted document)

`docs/AI_USAGE.md` is written from this. Keep entries short and honest. The brief asks for:
tool and version; three named tasks; one verbatim prompt; one thing the tool got wrong and
how it was caught; one task deliberately not delegated.

## Tools

- Claude (chat, claude.ai): discovery, requirements breakdown, plan, defect profiling.
- Claude Code (VS Code extension): build. Record the version shown in the Extensions view.

## Entry template

```
### YYYY-MM-DD HH:MM  <task>
Tool:
Prompt (verbatim if notable):
What it produced:
What was wrong or changed, and how I caught it:
Kept by hand (not delegated):
```

## Entries

### 2026-11-21  Discovery and planning
Tool: Claude (chat)
Task: read the case study pack, profile the data, draft the requirements breakdown, KPI
analysis and build plan.
Wrong or changed: to be filled in as errors are caught.
Kept by hand: final KPI wording; the three governance risks.

### 2026-11-21  Step 1: scaffold checks (venv, data hashes, Java, starter fixture)
Tool: Claude Code (VS Code extension), model Claude Opus 5.5
Prompt (verbatim): "Read CLAUDE.md and docs/PLAN.md. Then do step 1 only: set up the venv,
verify the data/ hashes against MANIFEST.md, check java -version is 17, and run the starter
test fixture. Show me the result of the check before doing anything else. Log the task in
docs/AI_LOG.md."
What it produced:
- `.venv` (Python 3.11.2) with pyspark 3.5.1, delta-spark 3.2.0, pytest 8.2.2, ruff 0.5.0.
- A read-only check of all 12 files in `data/MANIFEST.md`: SHA-256, row count and column
  count all match. Not covered by the manifest (no hash to check against):
  `legacy/sp_MarketingLeadSummary.sql` and the three `stakeholder_notes/*.md`.
- `java -version` gave OpenJDK 21.0.1 (Homebrew), the only JDK installed, so the check
  failed at first. With approval: `brew install openjdk@17` (17.0.21.1). `JAVA_HOME` set to
  `/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home`, exported in
  `.venv/bin/activate`, so the system default is still 21.
- Check passed: a throwaway test using the `spark` fixture started a local Spark 3.5.1
  session with the Delta extension and catalog on Java 17, wrote a 3-row Delta table to the
  temp warehouse and read it back. The test file was deleted afterwards. Full `pytest`:
  6 skipped (stubs). `ruff check .`: clean.
What was wrong or changed, and how I caught it: the first throwaway test also asserted that
`spark.sql.session.timeZone` was set. That isn't a step 1 requirement, and it failed: the
starter `spark_session.py` sets no timezone (it shows as None, so the JVM local zone is
used). I caught it in the test output, dropped the assertion and noted the gap. Setting UTC
is part of step 3.
Kept by hand (not delegated): approving the Java 17 install and choosing the JAVA_HOME path.

### 2026-11-21  Step 2: draft docs/ASSUMPTIONS.md
Tool: Claude Code (VS Code extension), model Claude Opus 5.5
Prompt (verbatim): "Do step 2 of docs/PLAN.md. Draft docs/ASSUMPTIONS.md from the 12
assumptions in PLAN.md, one numbered entry each, in this format: the ambiguity, options
considered, choice made, reason, how to reverse it. Mark entries that depend on a data check
(timezones, minor-unit spend, channel source of truth, attribution window) as "pending
evidence". Do not write the KPI recommendation to the directors yet. Show me the draft
before committing."
What it produced: a 12-entry draft built from PLAN.md, DEFECT_REGISTER.md,
KPI_DECISION_LOG.md, ref_region, ref_channel, the Ops brief (overnight and weekend wording)
and the legacy procedure (`PolicyStatus <> 'NTU'`). Entries 4, 5, 6 and 10 are marked
pending evidence. Counts from the pandas pass were left out, because CLAUDE.md rule 8 needs
each number to come from a query kept in the repo.
What was wrong or changed, and how I caught it: the draft is about 660 words, over the
brief's 300 to 500 word target. Flagged for trimming. Entry 8 (SCD2 proxy) is a proposal
for the human to confirm.
Kept by hand (not delegated): final wording of ASSUMPTIONS.md (per KPI_DECISION_LOG.md).
Revision (human review of the draft): entry 8 now uses Q1 and Q2 validity windows, and
quarantines the UK team values on the later AG0008 and AG0037 rows. The AI checked that
mismatch against `dim_agent.csv` before writing it (lines 44 and 46). Entry 10 now says it
is settled at step 12. Entry 11 now uses a NULL `lead_key` rather than an `UNKNOWN` lead.
Entry 13 (the KPI conflict) was drafted from KPI_DECISION_LOG.md, marked for Jason to
rewrite. Defect 27 was added to DEFECT_REGISTER.md.

### 2026-11-21  Step 2: finish (defect status, KPI decision log)
Tool: Claude Code (VS Code extension), model Claude Opus 5.5
Prompt: add a Status column to DEFECT_REGISTER.md (Decided / Decide at step N with the
measurement / Finding), do not invent decisions that need evidence, and finalise
KPI_DECISION_LOG.md to match ASSUMPTIONS.md entry 13 and the 27 defects.
What it produced: a status for all 27 defects. Defects whose handling was "X or Y"
(7, 10, 17, 18, 19) were split into a decided part and a "decide at step N" part, each
naming the measurement. The KPI log is marked final; its decision section cross-references
assumptions 2, 3, 9 and 11; "Still to settle" became a table of inputs and their status.
What was wrong or changed, and how I caught it: the review found defect 9's handling says
"log assumption" but no ASSUMPTIONS.md entry exists yet. Recorded in its status rather than
invented. Defect 22 is left as their recorded `policy_status` says until PolicyCore
confirms; that default is flagged for the human.
Kept by hand (not delegated): final wording of entry 13 and the directors' recommendation.

### 2026-11-21  Step 3: config, runner, logging, UTC session
Tool: Claude Code (VS Code extension), model Claude Opus 5.5
Prompt (verbatim): "Do step 3 of docs/PLAN.md. Fix the calls_ca_us delimiter in
config/config.example.yaml (comma, not semicolon). Finish src/common/config.py,
spark_session.py (set the session timezone to UTC), logging_setup.py and
src/run_pipeline.py. The one command must run end to end with empty stages. Write a small
test for load_config and resolve_source_path using the spark fixture only where needed.
Show me the check result before committing. Log the task in docs/AI_LOG.md."
What it produced:
- `config.example.yaml`: `calls_ca_us` `sep: ","` (defect 1).
- `config.py`: `load_config` (validates environment and the keys each source needs),
  `resolve_source_path`, `resolve_lakehouse_root`, `load_secrets`. `load_secrets` refuses
  the example file and a placeholder salt.
- `spark_session.py`: `spark.sql.session.timeZone = UTC`.
- `logging_setup.py`: one `northgate` logger; UTC timestamps; `run_id` on every record.
- `run_pipeline.py`: parses `--config`, checks that every local source file exists, starts
  Spark, then runs bronze, silver and the three gold stages in order. A stage that raises
  NotImplementedError is logged as skipped; any other error exits 1.
- `tests/test_config.py`: 12 tests. Only the UTC test uses the `spark` fixture.
Check: `ruff check .` clean; `pytest` 12 passed, 6 skipped (stubs); the one command exits 0
with all 5 stages skipped and the session timezone logged as UTC; a missing config exits 1.
What was wrong or changed, and how I caught it: PyYAML was not in requirements.txt and not
installed, so the YAML config could not be parsed. Found by trying the import before
writing `load_config`. Added `pyyaml==6.0.2` and flagged it for approval. Also, the first
check script read the exit code from `PIPESTATUS`, which zsh does not set, so the exit code
printed blank. Re-ran with a plain `$?`.
Kept by hand (not delegated): approving the new dependency.
Human decisions on the step 3 check, and what followed:
1. PyYAML approved. Logged as ASSUMPTIONS.md entry 14.
2. `config/config.yaml` is committed: local `data/` paths only, no `abfss://` or other cloud
   paths (cloud paths go in a separate `config/config.databricks.yaml` if needed).
   `load_config` was changed to require only the active environment's path. A test checks
   that config.yaml and config.example.yaml agree on paths, formats and options.
3. Salt: if `secrets.json` is missing, the runner generates a random 32-byte hex salt into
   it (mode 0600, exclusive create, never overwrites). It then loads the secrets to fail
   fast on a placeholder salt. README "How to run" says so.
Check after the changes: ruff clean; pytest 17 passed, 6 skipped. First run generated
`config/secrets.json` (-rw-------, gitignored) and exited 0. A second run reused the file.

### 2026-11-21  Way of working change, then step 4: bronze
Tool: Claude Code (VS Code extension), model Claude Opus 5.5
Prompt (summary): priorities in order (correct data, tests at every stage, fresh-clone run,
real history, Power BI, stretch); test first; read the earlier Databricks attempt at
`neilson 2/neilson` as read-only reference without copying; build stage by stage from
step 4, stopping after each.
What it produced:
- CLAUDE.md (commit 1deab17): priority order, test-first rule, the reference repo's
  traps, and the expected counts. Built on the human's own uncommitted draft. One line
  in that draft ("SCD2 and both bridges are already done") described the reference
  repo, not this one, so it was corrected.
- Step 4: `src/common/tables.py` (path-based Delta under `lakehouse/<layer>/<table>`,
  re-registered by name each session, because Spark's default catalog is in-memory) and
  `src/bronze/ingest.py`. Differs from the reference: the reference dropped each bronze
  table on every run (not append-only). Here a content fingerprint per file goes in
  `bronze.ingest_log`; unchanged files append nothing (ASSUMPTIONS entry 15).
- `tests/test_bronze_ingest.py`, written before the code and confirmed failing first:
  10 tests (row-hash determinism and separator safety, raw strings kept, nested JSON
  as text, rerun/changed-file batches, the two call files in one table, all 12 tables
  against MANIFEST rows and columns, exact table names, a second full run appends 0).
Check: bronze counts equal MANIFEST (leads 48,000 with 47,997 distinct ids; calls
41,000 + 23,000; policies 5,200; spend 2,224). A second pipeline run appended 0 rows.
ruff clean.
What was wrong or changed, and how I caught it:
- My `latest_batch` first put a window function inside `where`. Spark rejects that.
  Caught by the failing test; fixed by computing the window as a column first.
- My JSON test expected `{"a":2}`, but `primitivesAsString` gives `{"a":"2"}`. The test
  was wrong: bronze should keep text. The expectation was corrected and commented.
- ruff N812 flags `functions as F`; it is ignored in pyproject.toml as the standard
  PySpark alias.
Open: the bronze tests take about 3 minutes (many small Delta writes). Acceptable for
now; revisit if CI time matters.

### 2026-11-21  Step 5: DQ framework
Tool: Claude Code (VS Code extension), model Claude Opus 5.5
Prompt: "go" (step 5). Mid-task the human added that every exception must carry "a defect_id
that matches the number in DEFECT_REGISTER.md, a severity, the source table and row key,
the rule name and the run id".
What it produced: `src/common/dq.py` with `write_dq_exception` (the brief's five contract
columns, with `check_name` as the rule name, then `defect_id`, `run_id`, `action`
(quarantined, flagged or corrected) and an optional `detail`), plus `read_dq_exceptions`
(current run or all runs), `quarantined_count` and `assert_reconciled`, so every silver
stage uses one reconciliation check. `defect_id` is required and validated against the
register's numbers. `tests/test_dq.py`: 13 tests, written first and confirmed failing.
Differs from the reference: the reference wrote by table name only (lost between
processes) and had no run id, so silver reruns would have doubled the exception count
the DQ gate reads. It also had no defect link and no quarantined/flagged distinction,
which reconciliation needs.
What was wrong or changed, and how I caught it: my first version had no `defect_id`; the
human's mid-task requirement added it, and the tests were updated before the code.
`write_dq_exception` now takes `config` as an extra keyword argument, because tables are
path-based.
Check: pytest (full suite) green; ruff clean.

### 2026-11-21  write_dq_exception: starter call shape restored
Tool: Claude Code (VS Code extension), model Claude Opus 5.5
Prompt (summary): check whether the brief specifies the signature; if anything could call
it with the original five arguments, make that work unchanged.
Finding: the brief does not specify the signature ("exact names, we check" covers tables
and measures), but the starter stub defines five positional arguments, and a reviewer
could call it that way.
Change: `defect_id` and `config` are now optional. `defect_id` comes from `CHECK_DEFECTS`
(rule name -> register number, kept in dq.py next to `defect_ids()`); an unmapped rule
name still fails. `config` defaults to the committed config.yaml, anchored at the repo
root, with run id `adhoc`. 4 new tests, written first and confirmed failing; the
five-argument test redirects the default config to a temp folder, so it never writes
into the real lakehouse.
What was wrong: my step 5 version made two required keyword arguments a condition of
calling the starter function, which broke the starter's call shape. The human caught it
in review.

### 2026-11-21  Step 6: silver reference data and dimensions
Tool: Claude Code (VS Code extension), model Claude Opus 5.5
Prompt (summary): step 6 approved; tests first; decide defects 18 and 24 on evidence; CAD
June as an error; orphan campaigns to UNKNOWN, flagged; keep both versions of the
duplicated agents; assert_reconciled on real data; session-scoped bronze fixture; mark
full-data tests slow.
What it produced:
- `src/silver/reference.py`: `silver.ref_channel`, `silver.ref_region`, `silver.campaign`,
  `silver.fx_rate` and `silver.agent`, each reconciled (8, 4, 24, 11 and 45 rows in and
  out; 0 quarantined). Exceptions on real data: 10 channel labels corrected, 2 campaigns
  ending before they start (flagged), 313 orphan campaign ids on leads (310 on CMP-9999,
  3 on CMP-1099; flagged, mapped with `conform_campaign_id`), 1 missing FX rate
  (`2026-06|CAD`, error), 2 null regions and 2 contradicting teams corrected, and 6
  duplicate-version agent rows flagged.
- `src/sql/findings_reference.sql` plus `src/common/findings.py`; the tests run the named
  queries, so every evidence number traces to SQL.
- `tests/test_silver_reference.py`: 18 tests (7 unit on small fixtures; 11 slow on the
  real data via the shared session bronze fixture in conftest). Confirmed failing first.
- Decisions: defect 24 keeps the campaign channel (600 of 47,687 leads disagree after
  normalisation; 23,644 raw). Defect 18: null region from the team prefix (AG0007 1,302
  and AG0035 1,206 calls, all UK; the prefix matches the region on 41 of 43 rows).
What was wrong or changed, and how I caught it:
- Found my own step 4 bug while planning: `register_table` used CREATE TABLE IF NOT EXISTS,
  so within one Spark session a table name kept its first location. Wrote the failing
  test, fixed it, and committed that alone (a3b4658).
- The register's "8,970 raw" channel disagreement could not be reproduced (the pipeline
  measures 23,644 raw). Recorded in the register rather than forced.
- My ASSUMPTIONS edit script first failed because the human had rewritten entry 5
  meanwhile. Re-read the file and re-applied only entries 4 and 16; entry 5 is untouched.
Open: bronze landing takes about 2 minutes for 12 small files, and the suite is slow
because of it. ASSUMPTIONS.md is about 1,200 words, against a 300 to 500 target.
Bronze does not keep source row order, which assumption 8 (SCD2 by file order) needs at
step 10.
- Mistake found during step 6: my step 4 commit (4d867eb) included the human's own rewrite
  of ASSUMPTIONS entry 5 (timezone evidence), because I staged the whole file to add entry
  15 without checking its diff. The commit message does not mention it. Not rewritten
  (no amending); recorded here. From now on, inspect each file's full diff before staging.
