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
