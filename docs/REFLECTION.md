# Reflection

## What went well
The coordination between me and Claude. We discussed each step, I reviewed what had been
done, and the decisions were mine and were written down. Every step was logged, so we always
knew what had happened and what was coming, and we could change direction when the data
showed something we had not expected. The documented process is the part I would keep on
any future project.

## What was hard
Waiting. The long pipeline and test runs took a lot of time on a mac mini, and a more powerful machine would have finished sooner. One of the longest waits, the PII scan that took 47 minutes, was a code problem rather than a hardware one: it was rewritten as a single pass and moved to the last stage of the pipeline.

## What I would do differently
This was a case study. I had no prior experience of the company, its data or the people who wrote the requirements, and I could not ask them questions. In a real role I would be the contact for the project. I would take the time to understand the data and the people who use it, gather the requirements from them directly, and test the results with them at each stage. That would let me refine the model until the end product was what they actually needed. Instead, I recorded my assumptions, set out the open questions in docs/OPEN_QUESTIONS.md, and made each decision reversible, so that those conversations could change the build without a rebuild.

## What I would add with more time
- Run the Unity Catalog row filter and grants on a real workspace, not just describe them.
- Answer the open questions in `docs/OPEN_QUESTIONS.md` with their owners (finance FX rates
  for CAD June and July, the DECLINED-yet-INFORCE policies, the Australia call-time gap).
- Move the Power BI model off a shared service account so identity reaches the data layer.
- Add the DAX Studio before and after performance evidence.
- Schedule the pipeline and alert on a failed data-quality gate.
