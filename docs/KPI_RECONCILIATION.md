# KPI reconciliation: Lead to Policy to Connected Call to Policy

Both measures are certified (option C, `KPI_DECISION_LOG.md`). This page walks from one to
the other so a reader can see why they differ. Every number comes from
`kpi_bridge_steps` in `src/sql/findings_powerbi.sql`, written to
`powerbi/kpi_reconciliation.csv` by `python -m src.kpi_reconciliation --config
config/config.yaml`, and a test recomputes the Q2 steps in pandas.

## The two measures

| | Lead to Policy Conversion Rate | Connected Call to Policy Conversion Rate |
| --- | --- | --- |
| Question | What share of captured leads end in a policy? (acquisition) | How many policies per connected call? (agent productivity) |
| Numerator | Policies linked to a surviving lead, not NTU | Same policies, only for leads created in business hours |
| Denominator | Surviving leads (47,201) | Connected calls (CONNECTED, QUOTED, APPLICATION_STARTED, SOLD) on those business-hours leads |
| Date axis | Lead-created date | Policy-start date for policies, call date for calls |
| Attribution | First-touch campaign | Last-call agent |

The second is a ratio of two counts at different grains, so a lead with three connected
calls adds three to the denominator. It is a productivity ratio, not a share of leads, and it
can pass 100% for a slice with few calls (see the July warning below).

## The bridge, Q2 2026 (all regions)

Each step changes one thing. The rate is `numerator / denominator`; the change is in
percentage points, taken between rounded rates, so the changes add up exactly.

| Step | What changes | Numerator | Denominator | Rate | Change (pp) |
| --- | --- | ---: | ---: | ---: | ---: |
| 0 | Start: Lead to Policy Conversion Rate | 4,499 | 47,201 | 9.5316% | |
| 1 | Policies counted by policy-start date, not lead-created date | 3,392 | 47,201 | 7.1863% | -2.3453 |
| 2 | Denominator grain: leads become all calls on those leads | 3,392 | 61,690 | 5.4985% | -1.6878 |
| 3 | Drop calls that did not connect | 3,392 | 18,851 | 17.9937% | +12.4952 |
| 4 | Calls counted by call date, not by lead-created date | 3,392 | 18,338 | 18.4971% | +0.5034 |
| 5 | Business-hours leads only, on both sides | 1,324 | 7,135 | 18.5564% | +0.0593 |
| 6 | Last-call agent attribution (a slice) | 1,324 | 7,135 | 18.5564% | 0.0000 |

Start 9.5316% plus the six changes (-2.3453 - 1.6878 + 12.4952 + 0.5034 + 0.0593 + 0) is
18.5564%: the sum is exact, with no residual. Step 6 changes only who gets the credit when a
visual slices by agent, not the totals; it is listed so the bridge covers every axis in the
decision log. The same bridge for all dates (no period limit) is in the CSV: it ends at
24.0142% (1,760 policies / 7,329 connected calls). Steps 1 and 4 change nothing there, because
no date limit applies.

## Why the gap is so large

1. **The grain does almost all of it.** Counting calls instead of leads, then keeping only
   the 30.6% of calls that connected (18,851 of 61,690), turns a denominator of 47,201 into
   18,851 and adds +12.5 points. The two measures answer different questions, so a
   gap of this size is expected.
2. **Dates lose policies and calls from each side.** In Q2, 1,107 policies of Q2 leads
   start in July (step 1), and 513 connected calls on Q2 leads happen in July (step 4).
3. **Business hours barely moves the rate (+0.06 points).** It removes about 61% of both
   sides: 1,324 of 3,392 policies and 7,135 of 18,338 connected calls stay. Business-hours
   leads and out-of-hours leads convert at about the same rate per connected call. The
   filter changes the scale of the numbers, not the rate. The Ops director's scope rule has
   little effect on this measure.

## What surprised me

- **The indicative 20.5% was not reproduced.** The measure as decided gives 18.56% for Q2
  (24.01% over all dates). The variants I tried (`kpi_connected_call_variants`, Q2): 3,392
  linked policies / 18,607 connected calls = 18.23%; 3,688 policies, which also keeps 296
  with no lead, / 18,607 = 19.82%; 3,688 / 18,338 (calls of surviving leads only) = 20.11%.
  None gives 20.5%. The 20.5% came from the earlier pandas pass, and its filters were not
  kept, so it cannot be re-run. Treat it as superseded by this bridge.
- **Lead to Policy Conversion Rate for Q2 is 9.53%, not about 10%.** All leads were created
  in April to June, so Q2 and all dates are the same population. The earlier 9.9% and 10.1%
  were raw or partly de-duplicated leads, before the final rules.
- **July is not a usable month for the call-grain measure.** Policies start through 31 July
  but calls stop on 19 July, so July's rate is 224.7% (`expected_values.csv`, month
  2026-07, quarter 2026-Q3). Do not show July or Q3 for this measure. April is also low
  (8.56%), against 20.50% in May and 25.32% in June: the ramp-up month is not typical.
- **Only 1,324 of 3,392 Q2 policies belong to business-hours leads.** The Ops measure uses
  39% of the policies behind the lead-grain headline.

## Date axes are not one axis

The decision log says the measure is "anchored on the policy-start date". Connected calls
have no policy-start date, so policies use that date and calls use the call date, both under
one date slicer (confirmed by the human). A month's rate compares policies that started in
that month with calls made in that month. The two sides cover different leads in a month, which
is why April and July are not reliable alone.

## Reproduce

`python -m src.kpi_reconciliation --config config/config.yaml` rewrites
`powerbi/kpi_reconciliation.csv`. `tests/test_connected_call_kpi.py` checks the fixture
counts, the exact sum, the pandas recomputation of 1,324 / 7,135, and that the committed CSV
matches a fresh run.
