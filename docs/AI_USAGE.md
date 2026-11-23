# AI usage

## Tools and what each did
- **Genie in Databricks**: a first draft of the model, and a way to look at the data. It was
  reference only. Nothing from it went into the repo without being rebuilt and tested, and its
  secrets file was never committed.
- **Claude Code (in VS Code)**: wrote the pipeline, tests and documents, one step at a time.
  I chose it as the builder because correctness and testing had to happen at every stage.
- **Claude (chat)**: planning, decisions, step-by-step instructions for the Power BI build
  (which I did by hand in Power BI Desktop), and drafts of some documents.

## Tools and versions
- Claude Code in VS Code, version 2.1.330; model Claude Opus 5.5 (most steps) and Claude Sonnet 5.5 (steps 17 and 18).
- Claude (chat, claude.ai), model Claude Sonnet 5.5.
- Databricks Genie, a workspace feature with no version number; used only for exploration.

## How I worked with it
Each step followed the same loop: tests first and confirmed failing, then the code, then
reconciliation of counts, then a commit, then a stop for me to review. I read each report and
made the decisions myself. I also asked Claude to read the same material and explain it back,
so that what I had read manually and what it understood lined up and I was not mistaken.
Every step is logged in `docs/AI_LOG.md`.

## How the design was decided
I did not hand the design to the AI. The star schema, the fact and dimension tables, and the grain of each were worked out in discussion with Claude: it proposed options with reasons, and I questioned them and made the calls. The decisions I made are recorded in `CLAUDE.md`. Examples are limiting slowly changing history (SCD2) to the agent dimension, where the history is not held in Power BI; removing agent names from gold; and leaving "UNKNOWN" members out of the region and product dimensions so an unresolved value fails loudly. I chose Claude Code as the builder over Genie because correctness and testing had to happen at every stage. I reviewed each step's report before the next began, and I built the Power BI model, measures and report by hand.

## Where AI helped write the documents
The first draft of the assumptions entries (including entry 13, the KPI decision) and of
several other documents was written by Claude from the decision log and the build record. I
read them, confirmed or corrected them, and I am responsible for what they say. The
governance risks, this file and the reflection are written from my own points, with Claude
tidying the wording.

## Mistakes the tools made, and how they were found
- **Genie's funnel was mislabelled**: a figure it presented as the funnel was actually the lead-to-call bridge. I found it when I reconciled counts against the gold tables.
- **"4 missing FX rows" was wrong**: the missing rates were 180 CAD June spend rows.
- **The PII scan took 47 minutes** (and ran out of memory) and had also run before the fact
  tables existed. Rewritten as a single pass and made the last pipeline stage.
- **An exit code was misread**: the test run showed a failure that came from reading the
  wrong exit status, plus a test that deleted a shared gold folder. Both fixed.
- **A timezone test assumed UTC for every region**: Australian call times were shifted, and
  the correction was evidenced by a 426 of 426 policy-date match after a 10-hour shift. A
  residual gap of about 8.5 hours is flagged, not corrected.
- **An approximate expected count**: 62,830 calls was an estimate. The correct figure is
  62,829 after the Australian conversion.
- Others, each fixed and logged: required arguments that should have been optional, a table
  location kept stale within a session, a commit that included an unrelated edit of mine,
  blanked Canada and US fields, and a pasted document that still had instructions inside it.
  The full list is in `docs/AI_LOG.md`.

## One prompt, verbatim (step 18, the second KPI)

~~~
Step 18: second KPI, Connected Call to Policy Conversion Rate, plus the
reconciliation to Lead to Policy Conversion Rate.

Read first: CLAUDE.md, docs/KPI_DECISION_LOG.md, powerbi/MODEL_DESIGN.md,
powerbi/measures.dax, powerbi/expected_values.csv.

Definitions (option C is already decided; nothing may be called plain
"Conversion Rate"):
- Lead to Policy Conversion Rate: policies linked to surviving leads /
  deduplicated leads (existing measure, do not change).
- Connected Call to Policy Conversion Rate: grain is the call. Numerator is
  policies, denominator is connected calls (dispositions CONNECTED, QUOTED,
  APPLICATION_STARTED, SOLD), business-hours leads only, policy-start date as
  the date axis, last-call agent attribution. Check the KPI decision log for
  the exact wording and follow it. If anything in it is ambiguous or conflicts
  with the data, stop and ask me before building.

Method (same as every step): tests first and confirm they fail, implement,
reconcile counts, commit the slice, push, check CI, then stop and report.

Deliver:
1. Expected values for the new measure (overall, by region, by month, and Q2)
   computed independently in pandas from gold, added to
   powerbi/expected_values.csv, with tests that recompute them from the gold
   tables.
2. The DAX measure in powerbi/measures.dax, with description and format, ready
   to paste into the 'Key Measures' table. Use the same 'Key Measures' table
   name and the \£ / percentage format conventions already used.
3. A reconciliation bridge, docs/KPI_RECONCILIATION.md plus a CSV in
   powerbi/, that walks from the Lead to Policy rate to the Connected Call to
   Policy rate in numbered steps. Each step changes one thing (denominator
   grain from lead to connected call, drop non-connected calls, business hours
   only, date axis, agent attribution, and so on), shows the rate and the
   counts before and after, and the steps must sum exactly with no unexplained
   residual. Indicative figures for Q2 were roughly 10% versus 20.5%, so
   explain that gap and flag anything that surprises you.
4. Update docs/KPI_DECISION_LOG.md and powerbi/MODEL_DESIGN.md. Do not edit
   docs/ASSUMPTIONS.md entry 13.
5. A log entry in docs/AI_LOG.md for any mistake or wrong assumption you hit.

Commit in 2 to 3 slices (tests, measure and expected values, reconciliation
docs). Push, confirm CI is green, then report: the three headline numbers, the
bridge table, and any decision you need from me. Do not touch the .pbix.
~~~

I chose it because it shows the pattern I used throughout: definitions stated up front, tests first, a requirement to stop and ask when something is ambiguous, and a ban on touching the .pbix. When Claude Code asked which population to use, I took the option that matched the decision log, and I accepted its finding that the earlier 20.5% figure could not be reproduced, with the gold tables as the source of truth.

## What I checked myself
- I read every step report and made the decisions that are recorded in `CLAUDE.md`.
- I built the Power BI model, measures and report by hand and compared the measure values
  with the independently computed expected values.
- I tested row-level security with "view as" for three users (ANZ manager, UK analyst, and a
  user with no mapping) and kept the screenshots.
- I recorded the model walkthrough myself.
- I reviewed each raw file before and after ingestion into Databricks.
