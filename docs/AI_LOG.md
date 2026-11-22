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

### 2026-11-21  Bronze `_source_row` for SCD2 file order
Tool: Claude Code (VS Code extension), model Claude Opus 5.5
Prompt (summary): add a deterministic `_source_row` (file order from 1) to bronze; test it
against the true line numbers for dim_agent and a larger file; keep idempotency; record
it in ASSUMPTIONS 15; make assumption 8 use it.
What it produced: `add_source_row` (zipWithIndex straight after the read, before any
shuffle), left out of `_row_hash`, so fingerprints and idempotency are unchanged.
Defined as the data-record number: data row n of a CSV is file line n + 1 (the header is
line 1). Tests, written first and confirmed failing: a unit test, plus real-data
comparisons against an independent Python read for dim_agent, leads (48,000), calls_ca_us
(23,000) and consent_events; AG0008 at data rows 8 and 43 (lines 9 and 44).
`silver.agent` carries `source_row`; a test pins the version order of the three
duplicated agents. ASSUMPTIONS 8 and 15 updated (one line each).
What was wrong: my first unit test used a source name with no bronze table mapping, so it
would have failed for the wrong reason; caught when reviewing it before running. A test
edit script failed against text ruff had reformatted; re-read the file and re-applied.
Note: an existing local lakehouse keeps its old batches without `_source_row`, because
the file fingerprints are unchanged. Delete `lakehouse/` once to pick it up.
Test time is now a real problem: the step 6 test file takes about 5.5 minutes.

### 2026-11-21  Step 7: silver leads and consent
Tool: Claude Code (VS Code extension), model Claude Opus 5.5
Prompt (summary): tests first; reconcile by distinct lead_id
(47,997 = 47,201 + 758 + 35 + 3); quarantine PK conflicts and unidentifiable leads
(error); dedup map by the 48h rule compared with the source flag in findings SQL; decide
defect 7 on evidence; flag July leads, channel mismatches and the consent findings.
What it produced:
- `src/silver/dedupe_leads.py` (chained 48h links across email and digits-only phone;
  union-find over about 800 links on the driver; earliest survives; one map row per lead,
  survivors self-mapped, which the brief's named test requires). `resolve_consent.py`
  (latest event per lead; phone flag from the latest PHONE event). `leads.py` (checks in
  order, `LeadReconciliation`, both identities asserted in code).
- `findings_leads.sql`: id reconciliation, source flag vs rule (480 flagged: 478 agree,
  1 reversed, 1 shares no key; 279 found only by the rule), DOB groups, 2008 births,
  withdrawn consent (299 / 284), July leads.
- Tests: the named `test_dedupe_leads_collapses_clusters`, plus 4 dedup unit tests, plus
  `test_silver_leads.py` (4 unit, 8 slow on real data). Confirmed failing first.
Numbers: 48,000 rows / 47,997 ids -> 47,201 survivors + 758 mapped + 35 unidentifiable +
3 PK ids; 41 rows quarantined (35 + 6, both rows of each PK id); 60 DOBs nulled; 10 July
leads flagged; 600 channel mismatches flagged; consent 2,900 leads, 299 latest WITHDRAWN,
284 still Y on the lead record.
What was wrong or changed, and how I caught it:
- My test assumed the 10 July leads survive into silver.lead. The run showed 0: all 10
  are duplicates of late-June leads within 48h, so they map to June survivors. The test
  was wrong, not the code; it now asserts the mapping, with a findings query.
- The register's "292 after 2008" DOB count was misleading: 252 of them are 2008 births
  aged 18 and valid. The real junk is 60. Register corrected.
- Probing before coding showed that anchoring each 48h window on the cluster's first lead
  gives 757 and chaining gives 758, which explains the "757 to 758" in the brief. Digits-only
  and E.164 phone normalisation gave the same clusters in the probe; digits-only was kept
  as the simpler rule. (The probe was a scratch script, so no quoted document number
  depends on it.)
- Caught before running: `F.array()` has element type null, so the consent phone filter
  needed a cast.
Open: the suite is slow (the step 7 file alone takes about 9 minutes). ASSUMPTIONS.md is
about 1,330 words.
Additions before commit (human review): ASSUMPTIONS 18 now states the exact rule and why
60 are nulled, not the register's ~312, with counts per register band from the named
query `dob_register_bands`: before 1920, 20 of 20; 2008 to today, 20 of 272; future,
20 of 20. Nulling was extracted into `null_implausible_dob`, with a unit test written
first: a lead aged 17 at creation keeps its row with DOB nulled, and an 18th birthday on
the creation day keeps its DOB. Register rows 6 and 7 rewritten to one current
statement each.
More mistakes:
- Step 7 was built, but not committed, before a later human message ("docs update
  before step 7 ... do not start step 7") arrived. The docs commit was made from staged
  hunks only, and step 7 waited for "go".
- The RLS entry the human asked for took number 17, so my uncommitted step 7 entries
  were renumbered to 18 and 19, with their references in the register and in
  `leads.py`. Checked with grep.

### 2026-11-21  Docs: SCD2 scope and RLS design (commit 94dea39)
Tool: Claude Code (VS Code extension), model Claude Opus 5.5
Prompt (summary): docs only; record the SCD2 scope (gold.dim_agent only; silver.agent
undated, windows at step 10 in `_source_row` order) and the RLS design
(gold.bridge_user_region as the single source; Power BI role at step 17; UC row filter
on region_key plus a no-PII mask at step 20; ALL = unrestricted; unmapped = no rows;
the limits to state in platform_architecture.md) in PLAN.md and CLAUDE.md, plus one
ASSUMPTIONS entry of at most 60 words.
What it produced: three short bullets in CLAUDE.md's decisions section; a short
"design decisions" block in PLAN.md; ASSUMPTIONS 17 (53 words). Because ASSUMPTIONS.md
also held uncommitted step 7 hunks, the commit was staged with `git apply --cached` and
a one-hunk patch (HEAD plus entry 17), and the staged diff was reviewed before
committing. Pushed 4d867eb..94dea39, which also published the earlier unpushed local
commits (steps 5 and 6, the register and DQ fixes, `_source_row`). `git ls-files`
showed no secrets.json and no lakehouse/ paths.
Not done: platform_architecture.md itself (step 21); the limits are recorded as
something it must state.
Mistake: `git status` was not clean after the commit, as requested, because the step 7
work was still in the tree. Reported rather than hidden.

### 2026-11-22  Cost and speed: CLAUDE.md rules and Spark session settings
Tool: Claude Code (VS Code extension), model Claude Opus 5.5
Prompt (summary): write the cost and speed rules into CLAUDE.md, short; set
shuffle.partitions=8, default.parallelism=8, ui.enabled=false and adaptive.enabled=true
(configurable, UTC unchanged); test the session first; time the slow suite after.
What it produced: CLAUDE.md "Cost and speed" section (90a1457), replacing the human's
committed note (d7a22e9) with five short standing rules. `spark_session.py`:
`DEFAULT_SETTINGS` plus `spark_settings(config)`, with an optional `spark:` block in
config.yaml overriding them; the runner passes them to `get_spark`.
`tests/test_spark_session.py` (2 tests), written first and confirmed failing.
Timing, slow set (`pytest -m slow`, 30 tests): before 116 s (human's baseline),
after 106.7 s. About 8% faster, from a single run each, so treat it as indicative.
Most of the remaining time is bronze landing (many small Delta writes), not shuffles.

### 2026-11-22  Step 8: silver calls (defects 13 to 17 and 26)
Tool: Claude Code (VS Code extension), model Claude Opus 5.5
Prompt (summary): union both call files, seconds and UTC; timezone by evidence with the
queries in findings_calls.sql and a test pinning the AU gap; check whether AU leads are
the shifted side; reconcile 64,000 rows rule by rule; quarantine impossible times with a
severity; decide defect 17; flag defect 26; tests first.
What it produced: `src/silver/normalise_calls.py`, `src/silver/calls.py`,
`src/sql/findings_calls.sql` (8 named queries), 7 fast and 9 slow tests written first
and confirmed failing; a `calls` block in config; register, assumption 5, new
assumption 20. Result: 64,000 = 62,835 + 540 + 380 + 245.
Findings beyond the prompt: the register counts raw rows (262 = UK/AU end-before-start
rows with duplicates; 380 distinct across both files). CLAUDE.md's 62,830 assumed a
rounded 250 before-lead calls; the query gives 245. Policy dates show AU calls carry a
+10 h offset, while AU lead ingestion lag matches other regions; 8.5 h remains
unexplained, so AU stays as stored pending a human decision. DO_NOT_CALL also affects
CA and US (1,966 calls).
Mistakes caught: (1) region counts in a test were typed before being derived; caught
on review and replaced with the profiled figures before the first run. (2) Timestamp
asserts failed because `collect()` converts to the Mac's local time; now formatted in
Spark. (3) A Spark `raise_error` for conflicting call ids surfaced as a Spark exception,
not `ReconciliationError`; the test caught it and the check became eager. (4) The AU
silver test expected the bronze median exactly; silver drops quarantined calls (1,113
vs 1,111), so it now checks row-level equality with bronze instead.
Reference trap found: the earlier attempt nulled CA/US wrap-up code, SLA flag,
compliance flag and queue wait, which the file does contain; a test now guards it.

### 2026-11-22  Step 8 follow-up: AU converted from Sydney time; DO_NOT_CALL reconciled
Tool: Claude Code (VS Code extension), model Claude Opus 5.5
Prompt (verbatim, first line): "Step 8 follow-up, then push. CLAUDE.md was already
updated by the human (expected counts, AU timezone rule, where things stand, rules 5
and 7): do not edit it again, just check its diff and include it in this slice's commit."
What it produced: AU set to `Australia/Sydney` in config and code default; tests first
for the policy-date match in every region (426 of 426 AU on distinct calls, 422 of 422
on silver), the bronze-vs-silver shift (351 AU calls 11 h, 14,411 10 h, others 0) and
the residual AU first-call gap (512 min), flagged as `call_au_residual_offset`. The
DO_NOT_CALL gap 6,427 vs 6,311 is explained in a query: 38 flagged calls quarantined
themselves, 63 lost their only DO_NOT_CALL anchor, 15 kept only a later one; 4,286 vs
4,223 leads across all regions. Assumption 5, register rows 15, 16, 26 updated.
What it found that the human's CLAUDE.md does not say: converting AU moves 6 more AU
calls before their lead, so silver is 62,829 (251 before-lead), not 62,835. Reported,
CLAUDE.md left untouched as instructed. The CLAUDE.md change was already committed by
the human (194e524), so it could not be included in this slice.
Mistakes caught: the before-lead gap test kept the as-stored minimum (40 min); after
conversion it is 120 min (that call was AU, so it moved 10 h). The slow test failed
and was corrected from the query; the register text was fixed with it.

### 2026-11-22  Step 9: silver policy and spend (defects 9 to 12, 19 to 23, new 28)
Tool: Claude Code (VS Code extension), model Claude Opus 5.5
Prompt (summary): policy dedupe and coalesce with logged conflicts, null leads flagged,
annual premium derived, DECLINED-yet-INFORCE as a finding; spend: account for all 223
removed rows, test the minor-unit evidence and set a rule, decide negatives, zeros and
null clicks, quarantine CAD June, flag campaign-date errors, GBP by month; explain the
ambiguous DST hour on 5 April with a test.
What it produced: `src/silver/policy.py`, `src/silver/spend.py`, findings_policy.sql
(5 queries), findings_spend.sql (7), 9 fast and 16 slow tests written first and
confirmed failing; a DST unit test and query; a `spend.minor_units` config block;
assumption 6 rewritten, 21 to 23 added; register rows 9 to 12 and 19 to 22; defect 28.
Results: policy 5,200 = 5,172 + 12 + 16. Spend 2,224 = 2,001 + 40 copies + 180 CAD
June + 3 negatives. 181 minor-unit rows rescaled (two whole campaigns); CPL 319.95 ->
34.39 GBP. The full pipeline ran end to end once (all silver tables reconcile).
Findings beyond the prompt: "4 missing FX rows" in the notes is not supported (180
distinct CAD June rows; 7 copies). Minor units are whole campaigns, so a per-campaign
median test finds nothing; the rule uses CPM against the currency median. Six rows
have clicks above impressions (defect 28): the impressions are wrong, the spend is not.
CLAUDE.md's calls line was already fixed and committed by the human (30de5ff).
Mistakes caught: (1) a test comment claimed 4 of the 40 copies were CAD June; the
profile said 7; corrected before running. (2) A guessed band count (1,784) was typed
into a test; replaced with the derived 1,614 before running. (3) Bounds of 0.9 to 1.1
and 95 to 105 on the campaign CPM evidence were guesses; the slow test failed (0.878;
CMP-1061 92x) and the bounds were set from the query. (4) A register phrase claiming
the first campaign-date counts included copies was unverified; removed. (5) The
register quoted 190 off-ratio premiums from raw rows; silver gives 187, now pinned.

### 2026-11-22  Step 9 follow-up: defect 28 guard, premium flag, open questions
Tool: Claude Code (VS Code extension), model Claude Opus 5.5
Prompt (summary): test that no clicks-above-impressions row is rescaled; flag the 187
policies above 12 x monthly (warning, values unchanged); create docs/OPEN_QUESTIONS.md
(six questions, each with its evidence query); update CLAUDE.md (status, spend result,
DQ gate counts from the register, open questions file).
What it produced: two tests and the `policy_premium_above_12x_monthly` check; three new
evidence queries (`premium_above_12x_monthly`, `region_codes_by_source`,
`ref_region_business_hours`) with tests; OPEN_QUESTIONS.md; CLAUDE.md and register row
21 updated.
Corrected on the way: question (c) was worded as "not in the pack", but `ref_region`
does carry daily hours; the gap is the weekend and holiday rule, so the question says
that. Owner names were first written loosely ("PolicyCore owner"); replaced with the
brief's names (Finance Systems Manager, Operations Resource Planning Manager, Finance
Treasury).

### 2026-11-22  Step 10: gold dimensions
Tool: Claude Code (VS Code extension), model Claude Opus 5.5
Prompt (summary): dim_date, dim_region, dim_channel, dim_campaign, dim_product, and
dim_agent as SCD2 from `_source_row` order; deterministic keys; key coverage for every
future fact foreign key; reconcile each dimension to silver; no dim_lead.
What it produced: `src/gold/build_dimensions.py`; `tests/test_gold_dimensions.py` (7
fast, 5 slow, written first and confirmed failing). Keys: first 60 bits of SHA-256 of
the natural key, `UNKNOWN` = -1, `date_key` yyyymmdd; a rerun returns identical keys.
dim_agent: 45 versions for 42 agents plus `UNKNOWN`; windows contiguous, one current
row each. Coverage: 304 orphan-campaign leads already `UNKNOWN`; 615 calls on AG9001 to
AG9004 -> `UNKNOWN` agent; 1,139 call leads unmatched (1,094 ghost + 45 quarantined);
1 policy lead quarantined at step 7 -> `UNKNOWN`; 404 null policy leads keep NULL.
Agent email, manager name, hire date and FTE are left out of gold.
Mistakes caught: (1) the coverage test first expected 0 unmatched agents; AG9001 to
AG9004 are not agent ids in the dimension, so 615 unmatched (to `UNKNOWN`) was right,
fixed before running. (2) The call-resolution test built timestamps from Python
datetimes, which the host reads as BST, so 00:00 on 1 April became 31 March; the test
now builds UTC timestamps in Spark. The same trap hit step 8.

### 2026-11-22  Step 11: 52-bit keys, PII policy and gold.dim_lead
Tool: Claude Code (VS Code extension), model Claude Opus 5.5
Prompt (verbatim, opening): "Step 11 is approved: PII policy and gold.dim_lead (PLAN.md
step 11). Same method: tests first, confirm they fail, implement, reconcile, commit the
slice, stop and report."
What it produced: slice 11a: surrogate keys cut from 60 to 52 bits (below 2^53, exact in
Power BI), with Python parity, a collision test per dimension and a rebuild from empty
gold; `agent_name` dropped from `dim_agent`. Slice 11b: `src/gold/apply_pii_policy.py`
(salted SHA-256 in a UDF so the salt never enters a Spark plan; E.164 phones; decade age
bands; outward postcode per region; consent from `consent_current`), `gold.dim_lead`
(47,201) and `gold.dim_lead_nopii`, a leak scan over every gold column against real
silver and bronze values that also runs in the pipeline, defect 29 (24 postcodes in the
wrong format for their region), assumption 25, `docs/DATA_CLASSIFICATION.md`.
Mistakes caught: (1) the first full scan stopped the run: `dim_date` month labels "May"
and "June" equal real surnames and first names. Exempted calendar labels from name
checks only, with a test that a raw email in the same column is still caught. (2) The
consent fixtures used 'Y'/'N' strings, but `consent_current` holds booleans; caught
reading the silver code before implementing. (3) Integer columns (the year 2026) equal
four-digit AU postcodes, so non-string columns are compared with phone values only;
designed in before the first run, with a test. (4) Twice ran Python without the venv
active. (5) Misread an overlapping print as a duplicated assumption 23; an assertion in
the edit script stopped the change.
Open: the brief names no no-PII view, so `gold.dim_lead_nopii` is our name. Dedup at
step 7 compares digits-only phones, while the gold hash uses E.164, so the same number
written in national and international form is not linked by dedup. Not measured yet.

### 2026-11-22  Step 12: gold facts, bridges and fact_lead_funnel
Tool: Claude Code (VS Code extension), model Claude Opus 5.5
Prompt (verbatim, opening): "Step 12 is approved: gold facts and fact_lead_funnel (PLAN.md
step 12). Same method: tests first, confirm they fail, implement, reconcile, commit
slices, stop and report."
What it produced: decisions recorded (CLAUDE.md, assumption 26, open question g); the
`phone_normalisation_extra_links` query and test (E.164 finds the same 758 duplicates, 0
extra links); the runner now stops on any unbuilt or failing stage; `build_facts.py`
(four facts), `build_lead_funnel.py` (funnel, both bridges); the PII scan as the last
stage over all 15 gold tables; assumptions 27 (policy GBP) and 28 (funnel definitions);
assumption 20 revised (null `lead_key`, not an `UNKNOWN` lead); every gold column
classified. Two full runs from an empty lakehouse: every table the same size, no
duplicate rows; `dq_exception` keeps one identical slice per run.
Mistakes caught: (1) The first slow run of the fact tests took 47 minutes and died out of
memory. The PII scan built one plan branch per table, each recomputing and joining the
raw value set; rewritten as one pass over distinct cells (30 s). (2) The PII scan first
ran inside `build_dim_lead`, before the facts existed, so a fresh run would never have
scanned them; moved to a final stage that requires every gold table. (3) The open
question for defect 29 first named the AdHub owner; leads come from LeadFlow CRM, so the
Digital Acquisition Lead. (4) A timing probe was run from /tmp without the repo on the
path, and once under a tool timeout with grep buffering, so it showed nothing.
Open: 675 policies have no GBP premium (no CAD June rate, no July rates), assumption 27;
the attribution window (assumption 10) is still not applied to any measure.

### 2026-11-22  Step 13: the six named tests, the DQ gate and CI
Tool: Claude Code (VS Code extension), model Claude Opus 5.5
Prompt (verbatim, opening): "Step 13 is approved: the six named tests, CI, and the DQ
gate (PLAN.md step 13)."
What it produced: `src/dq_gate.py` (`python -m src.dq_gate`): reads the latest run's
error rows, fails above a line, on an error check with no line, or on a total above the
sum; warns below a line. Six expected lines in config, each with its register defect,
evidence and proving test, sum 602; on the last full run all six match (602 = 602).
`test_dq_gates.py` (breach fails, clean passes, unknown check fails, latest run only,
lines match the register); `test_fx_conversion.py` (cents and dollars give the same GBP;
a month with no rate is split out for quarantine), using two helpers extracted from
`build_spend` with no behaviour change (slow spend tests unchanged). RUNBOOK gate
section; CI: lint, fast tests, pipeline, gate on every push; slow set weekly or on demand.
The legacy parity test stays skipped with its reason (step 14).
Mistakes caught: (1) the first small slice of this prompt (675 breakdown, attribution
query, assumptions 10 and 20, exemption tests, 602 in CLAUDE.md) had already been done
and pushed in the previous slice; it was not redone. (2) `gh` is not installed, so action
versions and the CI result came from the public GitHub API. (3) The prompt asked for
setup-java v5 though v6 is the latest; v5 was used as asked and the gap reported.

### 2026-11-22  Step 15: gold exported to CSV for Power BI
Tool: Claude Code (VS Code extension), model Claude Opus 5.5
Prompt (verbatim, opening): "Step 15 is approved, ahead of step 14 (it does not depend
on it): export gold for Power BI (PLAN.md step 15)."
What it produced: `src/export_powerbi.py` (`python -m src.export_powerbi`, also the
pipeline's last stage): one CSV per gold table (15) in `export/powerbi/`, UTF-8, header,
ISO dates, UTC timestamps, plain integer keys, empty nulls, rows sorted so re-exports
are byte-identical; it stops on any raw-PII column name. `export/` is gitignored; README
says how to run it. Tests: formatting on a fixture (big keys, decimals, the one double,
quoting, nulls, order), the export list equals the PII scan list, row counts per file,
byte-identical second export, keys round-trip exactly, and the step 11 PII scan on the
files read back with each gold table's types.
Mistakes caught: (1) reading the CSVs back as plain strings would have made the PII scan
compare the year 2026 with AU postcodes, so the test reads each file with its gold
schema (and FAILFAST, so a value that does not parse back fails). (2) Spark's CSV
writer gives part files in no fixed order, so the export sorts rows and writes one file
itself. (3) An edit script left a mis-sorted import in the runner; ruff caught it.

### 2026-11-22  Step 14: legacy port of sp_MarketingLeadSummary
Tool: Claude Code (VS Code extension), model Claude Opus 5.5
Prompt (verbatim, opening): "Step 14 is approved: the legacy port (PLAN.md step 14)."
What it produced: a naive literal translation on raw data and a corrected port on gold
(`src/sql/`), run by `src/legacy_port.py` with GETDATE() as the `as_of` parameter
(default 2026-06-30 00:00:00); `src/sql/findings_legacy.sql` (7 named queries);
`tests/test_legacy_parity.py`, un-skipped: 8 fixture tests (one per difference) and 7
real-data tests; `docs/LEGACY_PORT.md` with 11 differences, each with cause, query,
number and impact.
Checked against the SQL, not taken from the prompt: (1) the prompt expected naive
conversion above 100%; it can (fixture), but on this data the top is 34.7%. (2) It
suggested the channel join drops "DISPLAY " and "display"; the procedure never reads
campaigns, and spend and lead codes are clean, so 0 rows here (9 campaign rows carry
variants: latent). (3) "Summed across currencies": each region has one currency, so no
group mixes them; the flaw is local currency across regions. (4) DEAD and NULL statuses:
none in the data. Found in addition: the DECIMAL(9,6) cast overflows in 2 groups, so the
real procedure would fail on SQL Server; a run on the submission date sees no spend; a
07:00 run drops 1 April; minor-unit rows inflate UK and AU spend 13x and 16x.
Mistakes caught: a `${...}` in an SQL comment broke `string.Template`; a wait loop
matched ruff's "All checks passed!" instead of pytest's summary.

### 2026-11-22  Step 16 preparation: policy status rule, measures, model design
Tool: Claude Code (VS Code extension), model Claude Opus 5.5
Prompt (verbatim, opening): "Step 16 preparation is approved: the Power BI model design,
measures and expected values (PLAN.md step 16)."
What it produced: one sentence in `docs/LEGACY_PORT.md` (SQL Server behaviour derived, not
executed); the assumption 3 decision (exclude NTU only) from
`findings_powerbi.sql: conversion_by_status_rule`; `powerbi/measures.dax` (12 required
measures plus 2 helpers, in DAX query view syntax with an `EVALUATE` check);
`powerbi/MODEL_DESIGN.md`; `src/powerbi_expected.py` writing
`powerbi/expected_values.csv`; `tests/test_powerbi_model.py` (fixture tests for
denominators, grain and nulls, DAX/SQL rule parity, reproducibility on real data).
The Power BI model itself is built by hand on the VM: Claude Code cannot run Desktop, so
no DAX here has been executed.
Mistakes caught: (1) a KPI log sentence claimed the legacy port's 90-day lead window
explains a difference; the window holds every lead, so it was removed before commit.
(2) A guessed AHT figure (641.5) in a test; the query gives 642.6. (3) The DAX parser
regex used DOTALL, so a header swallowed the whole block; fixture floats failed against
a decimal schema; a SQL comment naming `percentile_approx` tripped the parity check. All
caught by the tests. (4) The first types table listed two columns that are not imported.
Design points found by measuring, not assumed: one policy per surviving lead (why 4,499
policies equal 4,499 converted leads); `fact_policy` has no campaign, channel or lead
date, so Power Query merges them from `fact_lead`; unconnected calls carry talk time.

### 2026-11-22  Step 16b: seniority table, lead keys in gold, visuals
Tool: Claude Code (VS Code extension), model Claude Opus 5.5
Prompt (verbatim, opening): "Step 16b, before the human builds the model."
What it produced: `gold.dim_agent_seniority`; `campaign_key`, `channel_key` and
`lead_created_date_key` on `gold.fact_policy`; `lead_created_date_key` and
`first_call_bucket` on `gold.fact_lead_funnel`; expected values for Cost Per Policy and
CPL by channel, conversion by seniority, and lead counts per first-call bucket; the
steps for the four visuals in `powerbi/MODEL_DESIGN.md`; CLAUDE.md status and decisions.
Found while doing it: the funnel also needed `lead_created_date_key`, or the model would
still need the staging query; the seniority visual mixes two agents (last-call agent for
leads, selling agent for policies), so their agreement is measured and stated.
Mistakes caught: two docstring edits joined lines past the length limit (ruff); fast tests
run alongside the pipeline took 2.5 minutes, so the rest waited for the pipeline. The
first full slow run had 16 failures and 7 errors, and I first read pytest's piped exit
code 0 as a pass. Cause: since step 15, an empty-gold rebuild test deleted the whole
shared test gold directory, after `test_export_powerbi` had built the facts there. Fixed
by deleting only the dimension tables; the rerun has 103 passed.
