# Reflection

## What went well
The coordination between me and Claude. We discussed each step, I reviewed what had been
done, and the decisions were mine and were written down. Every step was logged, so we always
knew what had happened and what was coming, and we could change direction when the data
showed something we had not expected. The documented process is the part I would keep on
any future project.

## What was hard
Waiting. The long pipeline and test runs took a lot of time on a mac mini, and a more powerful machine would have finished sooner. One of the longest waits, the PII scan that took 47 minutes, was a code problem rather than a hardware one: it was rewritten as a single pass and moved to the last stage of the pipeline.

## The KPI disagreement and how I resolved it
The Regional Marketing Director counts policies over all leads, by lead-created month and first-touch campaign. The Sales Operations Director counts policies over connected calls, excluding duplicates and out-of-hours leads, by policy-start month and last-call agent. They are different questions at different grains, so I kept both as separately named measures (9.53% and 18.56% for Q2 2026) and published a six-step reconciliation with no unexplained difference. Details are in `docs/KPI_DECISION_LOG.md`.

## What I got wrong and how I found out
- **Funnel mislabel:** I took a Genie figure as the funnel; reconciling counts showed it was the lead-to-call bridge.
- **"4 missing FX rows":** counting the quarantined spend rows showed 180 CAD June rows.
- **Timezones:** a test assuming UTC for every region failed on Australia; matching policy dates after a 10-hour shift gave 426 of 426.
- **PII scan:** it ran for 47 minutes and before the fact tables existed; I noticed when I ran it and rewrote it as a single pass.
- **Exit code:** a slow-suite failure turned out to be a misread exit status plus a test that deleted shared gold data.

## Two more days, and why in that order
1. **Day one, morning: test the two KPI definitions with the two directors, and take the open questions to their owners** (Finance on CAD rates, Operations on business hours). Everything else depends on these answers, and they are the cheapest to change now.
2. **Day one, afternoon: close the data gaps that follow** (the CAD June and July FX rates, the DECLINED-yet-INFORCE policies, the Australian call-time gap).
3. **Day two, morning: put row-level security in Unity Catalog on a real workspace** and move the Power BI refresh off a shared account, so access control is central, not per report.
4. **Day two, afternoon: scheduling and alerting** on the data-quality gate, then the DAX Studio before and after performance evidence.

That order puts the questions the business must answer first, security second, and polish last.

## What I would do differently
This was a case study. I had no prior experience of the company, its data or the people who wrote the requirements, and I could not ask them questions. In a real role I would be the contact for the project. I would take the time to understand the data and the people who use it, gather the requirements from them directly, and test the results with them at each stage. That would let me refine the model until the end product was what they actually needed. Instead, I recorded my assumptions, set out the open questions in docs/OPEN_QUESTIONS.md, and made each decision reversible, so that those conversations could change the build without a rebuild.