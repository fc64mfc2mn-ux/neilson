# AI usage

> **TODO (Jason): written by the human, not the AI.** Target 300 to 500 words, with the
> sections below. The facts underneath were drawn from `docs/AI_LOG.md` to write from;
> delete them when the note is written.

## Tool and version

## Three named tasks it was used for

## One verbatim prompt

## One thing the tool got wrong and how I caught it

## One task I chose not to delegate and why

---

## Facts to write from (from `docs/AI_LOG.md`; delete when done)

**Tools and versions**
- Claude (chat, claude.ai): discovery, data profiling, requirements breakdown, the build plan.
- Claude Code, VS Code extension: the build. Models: Claude Opus 5.5 (steps 1 to 16b and
  step 21), Claude Sonnet 5.5 (step 17 wrap-up and step 18). The extension version is
  shown in the VS Code Extensions view: record it.
- `CLAUDE.md` is committed: the priority order, hard rules, decisions and expected counts
  that the agent worked to. Each step ran tests first, then the code, then a comparison
  with the expected counts, then a commit and a stop for "go".

**Candidate named tasks**
- Silver calls (step 8): unioning the two call files, the AU timezone chosen by evidence
  (all 426 AU policy dates match the linked call only after the Sydney conversion), and the
  DO_NOT_CALL reconciliation (6,427 vs 6,311, explained in full).
- The gold PII policy and `dim_lead` (step 11): salted hashes, age bands, postcode rules per
  region, and the gold-wide PII scan.
- The six named tests, the DQ gate and CI (step 13): gate lines from the defect register,
  602 error rows expected.
- Step 18: the Connected Call measure, expected values recomputed in pandas, and the
  reconciliation bridge.

**Verbatim prompts on record (`AI_LOG.md`)**
- Step 13: "Step 13 is approved: the six named tests, CI, and the DQ gate (PLAN.md step 13)."
- Step 18: "Step 18: second KPI, Connected Call to Policy Conversion Rate, plus the
  reconciliation to Lead to Policy Conversion Rate."
- Step 8 follow-up, first line: "Step 8 follow-up, then push. CLAUDE.md was already updated
  by the human (expected counts, AU timezone rule, where things stand, rules 5 and 7): do
  not edit it again, just check its diff and include it in this slice's commit."

**Things it got wrong, and how each was caught**
- Step 16b: it read pytest's piped exit code (0, from `tail`) as a pass. The full slow run
  actually had 16 failures and 7 errors, caused by a test that deleted the shared test gold
  directory. Caught on reading the output; fixed by deleting only the dimension tables.
- Step 12: the PII scan built one plan branch per table. The slow tests ran 47 minutes and
  died out of memory. Rewritten as one pass (30 s).
- Step 17: renaming the `Measures` table to `Key Measures` broke the DAX parser test regex
  (11 failures). Caught by the test run, and fixed in the test.
- Step 18: the indicative 20.5% for the Ops-style rate could not be reproduced (18.56% as
  built). It asked before building, and documented the gap instead of tuning to match.
- Earlier attempt (reference repo): a "4 missing FX" spend claim was wrong. The spend
  reconciliation is 2,224 = 2,001 + 40 + 180 + 3.
- A test comment in step 9 claimed 4 of the 40 exact spend copies were CAD June. Caught in
  review.

**Not delegated (kept by hand)**
- The final KPI wording: `ASSUMPTIONS.md` entry 13 and the recommendation to both directors.
- The three governance risks.
- Building the Power BI model, the RLS role and the view-as evidence in Power BI Desktop on
  the Windows VM. The agent cannot run Desktop, so it prepared `measures.dax`, the model
  design and the expected values, and the human checked the measures against them.
- Approving the Java 17 install and the PyYAML dependency. (pandas was added as a test-only
  dependency at step 18, because the prompt asked for a pandas recomputation.)
- The decisions it asked for: option C for the KPI; the business-hours scope and the date
  axes at step 18; the AU postcode and consent labelling rules.
