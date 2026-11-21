# Staff Full-Stack BI Engineer: take-home case study

Thank you for taking the time to work through this. It is a single case study
built around a fictional insurer, Northgate Life. You will build a small
lakehouse, integrate five source systems, build a Power BI semantic model,
design governance, and document a handover.

## Contents

- [Summary](#summary)
- [The business scenario](#the-business-scenario)
- [What you're building](#what-youre-building)
- [Required lakehouse tables](#required-lakehouse-tables)
- [Required Power BI measures](#required-power-bi-measures)
- [Governance and access requirements](#governance-and-access-requirements)
- [A KPI definition you'll need to resolve](#a-kpi-definition-youll-need-to-resolve)
- [Repo structure](#repo-structure)
- [Required documents](#required-documents)
- [Core and stretch scope](#core-and-stretch-scope)
- [Local tech stack and setup](#local-tech-stack-and-setup)
- [Using AI tools](#using-ai-tools)
- [Submission](#submission)
- [Format and logistics](#format-and-logistics)

## Summary

- Build a medallion lakehouse (bronze, silver, gold) in PySpark and Delta from
  twelve provided source files, then a Power BI semantic model on top of it.
- Integrate marketing leads through contact-centre calls to policies, design
  governance and access control, and hand the result off in writing as if to
  another engineer.
- Target effort on the core scope is 4 hours. You have a 7-day window to
  submit. Stretch items exist and are welcomed but not required.
- All data is synthetic. Nothing in the pack is a real person or a real
  company record.
- Use whatever AI coding tools you normally use. We want to see how you use
  them, not whether you avoided them.

## The business scenario

Northgate Life sells term life, over-50s and critical-illness cover direct to
consumers in four regions: UK, Canada, Australia and the United States.
Marketing buys leads across eight channels. A contact centre of 42 agents
calls those leads, quotes them, and converts them to policies.

Five source systems feed reporting, each owned by a different team. The data
pack folders mirror these systems:

| Source system | Folder | Contents | Owning role |
| --- | --- | --- | --- |
| AdHub | `data/marketing/` | Campaign metadata and daily spend, impressions, clicks | Marketing Operations Manager |
| LeadFlow CRM | `data/marketing/` | Lead records and consent events | Digital Acquisition Lead, with the Data Protection Officer for consent |
| Aurora Dialler | `data/contact_centre/` | Call-level activity, dispositions, QA scores | Contact Centre Systems Manager |
| PolicyCore | `data/finance/` | Policies, premiums, underwriting decisions | Finance Systems Manager |
| Workforce extract | `data/contact_centre/` | Agent roster, teams, seniority | Operations Resource Planning Manager |

Finance Treasury supplies monthly FX rates as a sixth feed, in
`data/finance/`.

The reporting period is 2026-04-01 to 2026-06-30 inclusive (91 days).
Policy records extend to 2026-07-31, to allow for the lag between a lead
being quoted and a policy going in force.

Two named stakeholders, the Regional Marketing Director (UK and Ireland) and
the Sales Operations Director (International), have each written down how
they want the funnel conversion rate defined. Their definitions don't agree.
The BI Manager has asked for one certified semantic layer that both of them
can use. More on this below, under [A KPI definition you'll need to
resolve](#a-kpi-definition-youll-need-to-resolve).

## What you're building

The case study has six parts. Work through them in this order; each depends
on the one before it.

### 1. Ingest and model

Land all twelve input files into a bronze layer, build a typed and
deduplicated silver layer, and build a gold layer as a star schema. Bronze
should be append-only: every column typed as string, plus columns recording
ingest time, source file, and a row hash. Do no cleansing at bronze.

At silver, type and conform everything, deduplicate where the data calls for
it, and route anything you reject to an exception table that records which
check failed, which table and key it came from, its severity, and when you
caught it. The two call-activity files (one per pair of regions) need to land
in one unified call table, with durations and timestamps normalised to a
single unit and a single timezone.

Your agent dimension needs to track history (SCD2: valid-from, valid-to, and
a current flag), because the source roster changes agents' team and
seniority without telling you when the change happened.

Drive ingestion from a config file, not from hardcoded paths, with one entry
per source file. The whole pipeline should run from a single command:
`python -m src.run_pipeline --config config/config.yaml`.

### 2. Integrate

Build one fact table carrying the full path from lead, to call, to policy, at
one row per deduplicated lead. This is the table that lets you answer
questions that span marketing and sales.

Write a source-system-owners document: one row per source system (six rows,
including Treasury FX), naming what each system feeds, who owns it, a
specific question you'd want to ask that owner, what needs agreeing before
go-live, and what breaks if that conversation doesn't happen.

Then write your resolution of the funnel-conversion-rate disagreement (see
below) as a short recommendation addressed to both directors by title.

### 3. Report

Build a Power BI semantic model on the gold layer with the measures listed
below. Use a star schema: one-to-many relationships from dimensions to
facts, filtering in a single direction, with any exception justified in
writing. Include a marked date table covering the whole of 2026, and a
disconnected parameter table for a target CPL, running £20 to £120 in £5
steps.

Every measure needs a populated description matching its definition in your
KPI documentation. Build one report page with four visuals: channel CPL
against cost per policy, funnel by region, agent seniority against
conversion, and the distribution of time-to-first-call.

Keep the DAX for every measure in a plain-text file so it can be read without
opening the `.pbix`. Capture a before-and-after performance artefact (from
DAX Studio, Tabular Editor, or VertiPaq Analyzer) for one measure you tune,
and state the measured change in your README.

### 4. Govern

Write a governance document and implement at least one access control from
it. See [Governance and access requirements](#governance-and-access-requirements)
below for what's required.

### 5. Document and hand off

Write a README, a runbook, a findings document, and an engineering-standards
document. See [Required documents](#required-documents) for what each needs
to cover.

### 6. Engineer and reflect

Set up CI that lints, tests, and gates on data quality. Write six tests
covering the pipeline's main behaviours (dedup, call normalisation, FX
conversion, funnel grain, DQ gates, and the legacy procedure below). Stub out
a Databricks Asset Bundle with dev and prod targets, and write the Unity
Catalog grants you'd apply in a real workspace, including one row filter or
column mask.

`data/legacy/sp_MarketingLeadSummary.sql` is a T-SQL stored procedure that
currently produces the channel CPL and conversion report. It has known
issues. Port it to PySpark or Spark SQL, and write down the behaviour
differences you find between the original and your port.

Finally, write a short reflection on the exercise, and a short note on how
you used AI tools (see [Using AI tools](#using-ai-tools)).

## Required lakehouse tables

Use these exact names. We check for them.

**Bronze** (append-only, string-typed, plus ingest metadata):
`bronze.leads_raw`, `bronze.consent_events_raw`, `bronze.calls_raw`,
`bronze.policies_raw`, `bronze.marketing_spend_raw`, `bronze.campaigns_raw`,
`bronze.agents_raw`, `bronze.fx_rates_raw`, `bronze.ref_channel_raw`,
`bronze.ref_region_raw`, `bronze.sec_user_region_raw`.

(`bronze.calls_raw` carries both call source files; keep the source file
column so they're distinguishable.)

**Silver** (typed, conformed, deduplicated):
`silver.lead`, `silver.lead_dedup_map`, `silver.consent_current`,
`silver.call`, `silver.policy`, `silver.marketing_spend`, `silver.agent`,
`silver.campaign`, `silver.fx_rate`, `silver.dq_exception`.

**Gold** (star schema):

| Table | Grain |
| --- | --- |
| `gold.dim_date` | One row per date, 2026-01-01 to 2026-12-31 |
| `gold.dim_region` | One row per region |
| `gold.dim_channel` | One row per channel, plus an `UNKNOWN` member |
| `gold.dim_campaign` | One row per campaign, plus an `UNKNOWN` member |
| `gold.dim_agent` | One row per agent per validity window (SCD2), plus an `UNKNOWN` member |
| `gold.dim_product` | One row per product |
| `gold.dim_lead` | One row per deduplicated lead, PII removed or hashed |
| `gold.fact_marketing_spend_daily` | Date by campaign |
| `gold.fact_lead` | One row per deduplicated lead |
| `gold.fact_call` | One row per deduplicated call |
| `gold.fact_policy` | One row per deduplicated policy |
| `gold.fact_lead_funnel` | One row per deduplicated lead: the integration table from Part 2 |
| `gold.bridge_lead_call` | One row per lead by call |
| `gold.bridge_user_region` | One row per user by region, to support row-level security |

## Required Power BI measures

Build these measures, using these exact names. Each should encode the
business logic described.

| # | Measure name | Business logic |
| --- | --- | --- |
| 1 | `Marketing Spend (GBP)` | Sum of local spend, converted to GBP at the month's rate |
| 2 | `Leads (Deduplicated)` | Distinct count of surviving leads after deduplication |
| 3 | `Cost Per Lead (GBP)` | Measure 1 divided by measure 2 |
| 4 | `Contact Rate` | Leads with at least one connected/quoted/application/sold call, divided by measure 2 |
| 5 | `Lead to Policy Conversion Rate` | Policies linked to a surviving lead, divided by measure 2 |
| 6 | `Average Annual Premium (GBP)` | Sum of annual premium in GBP, divided by policy count |
| 7 | `Average Handle Time (Seconds)` | Sum of talk time plus hold time, divided by connected call count |
| 8 | `Grade of Service` | Inbound calls answered within SLA, divided by inbound calls offered, against each region's own target |
| 9 | `Time to First Call (Median Minutes)` | Median minutes from lead creation to first call, at lead grain |
| 10 | `Cost Per Policy (GBP)` | Measure 1 divided by policies attributable to leads |

Two further model objects are required:

| Object | What it should do |
| --- | --- |
| `Cost Per Lead (GBP) MoM %` | Month-over-month change in measure 3, using proper time intelligence against the marked date table. Should return blank for the first month in the data |
| `CPL vs Target Variance (GBP)` | Reads the selected value from the disconnected target-CPL table and compares it to measure 3. Should handle the case where nothing is selected |

Note which of these are core scope and which are stretch under [Core and
stretch scope](#core-and-stretch-scope).

## Governance and access requirements

Write a governance document and implement at least one control from it.

### Access tiers

Classify every column in the pack into one of four tiers:

| Tier | Name | Definition |
| --- | --- | --- |
| 0 | Aggregate | No individual identifiable at any grain |
| 1 | Internal | Business data, no personal data |
| 2 | Confidential | Personal data under UK GDPR |
| 3 | Restricted | Special category data, or data that reveals it |

Every Tier 2 and Tier 3 column in the pack must be named in your governance
document:

| File | Column | Tier | Reason |
| --- | --- | --- | --- |
| `leads.csv` | `first_name` | 2 | Direct identifier |
| `leads.csv` | `last_name` | 2 | Direct identifier |
| `leads.csv` | `email` | 2 | Direct identifier |
| `leads.csv` | `phone` | 2 | Direct identifier |
| `leads.csv` | `postcode` | 2 | Quasi-identifier, re-identifies at full precision |
| `leads.csv` | `date_of_birth` | 2 | Quasi-identifier, re-identifies with postcode |
| `leads.csv` | `smoker_flag` | 3 | Health data |
| `leads.csv` | `health_condition_flag` | 3 | Health data |
| `leads.csv` | `estimated_cover_amount` | 2 | Financial data tied to an individual |
| `leads.csv` | `consent_marketing`, `consent_timestamp_utc`, `consent_source` | 2 | Lawful-basis evidence, integrity-controlled |
| `consent_events.json` | `source.actor`, `source.ref` | 2 | Links to an individual's preference record |
| `calls_uk_au.csv` | `recording_url` | 3 | Recording may contain health disclosure |
| `calls_ca_us.csv` | `recording_url` | 3 | As above |
| `calls_uk_au.csv` | `call_notes` | 3 | Free text containing names and health mentions |
| `calls_*.csv` | `qa_score`, `compliance_fail_flag` | 2 | Employee performance data |
| `dim_agent.csv` | `agent_name`, `agent_email`, `manager_name` | 2 | Employee personal data |
| `dim_agent.csv` | `seniority_band`, `fte`, `hire_date` | 2 | Employee HR data |
| `policies.csv` | `annual_premium_local`, `monthly_premium_local`, `sum_assured_local` | 2 | Financial data tied to an individual |
| `policies.csv` | `underwriting_decision` | 3 | Reveals health status |

### Roles

Document an entitlements matrix, one row per role and one column per gold
table, marked full, filtered, masked or denied, for these four roles:

| Role | Regions | Tier 2 access | Tier 3 access | Notes |
| --- | --- | --- | --- | --- |
| `Regional_Analyst` | Own region(s) only, per the security-mapping file | Denied | Denied | One of the mapped users covers two regions |
| `Marketing_Analyst_NoPII` | All | Denied | Denied | Needs the lead dimension and all facts, with no identifiers |
| `Ops_Team_Manager` | Own region | Agent-performance columns allowed, customer columns denied | Denied | Needs QA score and compliance-fail flag |
| `Underwriting_Reviewer` | All | Allowed | Allowed | Named in the document, not implemented; record it as out of scope with a reason |

Anyone not listed in the security-mapping file should see no rows at all.
Document this default-deny rule, and name
`group.finance@northgatelife.example` as a requester it currently applies to.

### The control you implement

At minimum, implement and evidence one of:

1. A dynamic Power BI row-level-security role for `Regional_Analyst`, filtered
   from the region-mapping bridge table using the signed-in user's principal
   name, propagating to every fact table. It should show two regions for the
   user mapped to two regions, and zero rows for a user with no mapping.
   Capture evidence (the role definition and a "view as role" screenshot for
   two different users) in the repo.
2. PII handling in the silver-to-gold PySpark step for the lead dimension:
   hash email, phone and date of birth with a salted SHA-256 (salt from
   config, never a literal in code), drop first and last name outright,
   replace date of birth with a 10-year age band, truncate postcode to the
   outward code, and exclude health-related flags from anything served to the
   no-PII marketing role. Call notes and recording URLs should never reach
   any gold table.

### Secrets

Keep a secrets template with placeholder values only (including the salt
above), excluded from version control by `.gitignore`. State in your README
the Key Vault-backed secret-scope path a production deployment would use, for
example `dbutils.secrets.get(scope="kv-nfs-bi", key="pii-hash-salt")`.

You'll also need to name three governance or design risks you see in this
data, each with a mitigation. Finding these is part of the exercise; we are
not giving you a list to start from.

## A KPI definition you'll need to resolve

The data pack includes three stakeholder notes, in `data/stakeholder_notes/`:
one from the Regional Marketing Director, one from the Sales Operations
Director, and one from the BI Manager. Read them. The Marketing Director's
and the Sales Operations Director's notes define the funnel conversion rate
differently, and neither one defers to the other. The BI Manager's note sets
a constraint, not a resolution.

The ask, in the BI Manager's words: deliver one certified funnel conversion
rate that the Regional Marketing Director and the Sales Operations Director
can both use in the same report.

There is no tiebreaker in the pack. Resolving this is part of the exercise,
not a blocker to raise with us. We want to see how you reason about it, not
which answer you land on. Document your reasoning in your assumptions log:
what the disagreement actually is, why you chose the resolution you chose,
and how someone would reverse your decision if the business told you to.
Carry the same resolution through into your KPI-definitions document and
your recommendation to both directors.

## Repo structure

Your submission should follow this layout:

```text
/
  README.md
  requirements.txt
  pyproject.toml
  databricks.yml
  .gitignore
  .github/
    workflows/
      ci.yml
  config/
    config.example.yaml
    secrets.example.json
  data/                              (provided, read-only)
    MANIFEST.md
    marketing/
    contact_centre/
    finance/
    reference/
    legacy/
    stakeholder_notes/
  src/
    __init__.py
    run_pipeline.py
    common/
      __init__.py
      config.py
      spark_session.py
      logging_setup.py
      dq.py
    bronze/
      __init__.py
      ingest.py
    silver/
      __init__.py
      build_silver.py
      dedupe_leads.py
      normalise_calls.py
      resolve_consent.py
    gold/
      __init__.py
      build_dimensions.py
      build_facts.py
      build_lead_funnel.py
      apply_pii_policy.py
    sql/
      unity_catalog_grants.sql
      port_sp_marketing_lead_summary.sql
  tests/
    __init__.py
    conftest.py
    test_dedupe_leads.py
    test_normalise_calls.py
    test_fx_conversion.py
    test_lead_funnel_grain.py
    test_dq_gates.py
    test_legacy_parity.py
  powerbi/
    northgate_funnel.pbix
    measures.dax
    rls_roles.md
    performance/
      before.png
      after.png
  docs/
    RUNBOOK.md
    GOVERNANCE.md
    KPI_DEFINITIONS.md
    SOURCE_SYSTEM_OWNERS.md
    ASSUMPTIONS.md
    FINDINGS.md
    REFLECTION.md
    AI_USAGE.md
    STANDARDS.md
    platform_architecture.md
  lakehouse/                         (gitignored, generated by your pipeline)
```

The starter repo we send you already has this structure in place: skeleton
headings in the docs, a working local Spark-with-Delta test fixture so you
don't lose time to environment setup, a pinned `requirements.txt`, and
`config/config.example.yaml` pre-populated with all twelve source entries.
Everything else in `src/` ships as a stub with a docstring describing what it
needs to do.

## Required documents

Word counts below are targets to aim for, not hard gates. Each document needs
the listed sections, at minimum.

| File | Target words | Required sections |
| --- | --- | --- |
| `README.md` | 600-900 | Purpose; Prerequisites (including Java); How to run, one command; Input data; Output tables; Semantic model and measures; Usage guidelines (who should use this and how); Assumptions (linking to `ASSUMPTIONS.md`); Known limitations; Performance result; Key Vault secret-scope path; Time spent and what I cut and why |
| `docs/RUNBOOK.md` | 500-800 | Schedule and upstream dependencies; Pipeline stages with expected runtime; Failure modes (a table of at least six, each with symptom, likely cause, action, escalation target); Data-quality gates and thresholds; Reprocessing and backfill procedure; Monitoring and alerting design; Orchestration choice (ADF vs Databricks Jobs, with reason); Support model and on-call expectations |
| `docs/GOVERNANCE.md` | 600-900 | Access tiers; Column classification (every Tier 2 and Tier 3 column named); Roles and entitlements matrix; Controls implemented and where in the code; Controls designed but not implemented, with reasons; Unity Catalog mapping to the tiers; Secrets handling; Retention and consent handling; Three design risks with mitigations; Residual risk accepted |
| `docs/KPI_DEFINITIONS.md` | 400-700 | One table: measure name, business definition, numerator, denominator, grain, filters applied, period anchor, attribution basis, owning role, where enforced (one row per measure you build). Then a 200-300 word recommendation addressed to both directors |
| `docs/SOURCE_SYSTEM_OWNERS.md` | 300-500 | The six-column, six-row table from Part 2 |
| `docs/ASSUMPTIONS.md` | 300-500 | Numbered list; each entry: the ambiguity, options considered, choice made, reason, how to reverse it |
| `docs/FINDINGS.md` | 300-500 | Exactly three findings, each with the number, the method that produced it, and the business action it implies |
| `docs/REFLECTION.md` | 400-600 | The KPI disagreement and how you resolved it; What you got wrong and how you found out; What you'd do with two more days, and why in that order |
| `docs/AI_USAGE.md` | 300-500 | Tool and version named; three named tasks it was used for; one verbatim prompt; one thing it got wrong and how you caught it; one task you chose not to delegate, and why |
| `docs/STANDARDS.md` | 300-500 | Naming conventions for tables, columns and measures; the reusable pattern in this repo another engineer would follow; what you'd add to make it a team standard |
| `docs/platform_architecture.md` | 500-800 | Unity Catalog three-level namespace for this model; grants design mapped to the four roles; ADLS Gen2 container and path layout, and how config switches local to `abfss://`; Key Vault-backed secret scope; where Microsoft Fabric would sit relative to Databricks for this workload, and where it wouldn't; what changes when this runs on a real workspace |

If you commit an agent configuration file, such as `CLAUDE.md`, `AGENTS.md`,
or `.cursor/rules/`, that counts as positive evidence of how you work with
AI tools. Configuring an agent is a signal we want to see, not something to
leave out.

## Core and stretch scope

Four hours is not enough to build everything above end to end. We'd rather
you finish the core cleanly and name what you cut than half-finish
everything. Prioritisation is itself something we're assessing.

### Core

- Bronze ingestion of all twelve files, parameterised by config.
- Silver: typed, deduplicated, calls unioned and normalised, exceptions
  logged.
- Gold: `dim_date`, `dim_region`, `dim_channel`, `dim_campaign`, `dim_agent`
  (a simple current-state dimension is acceptable here), `dim_lead`,
  `fact_lead`, `fact_call`, `fact_policy`, `fact_marketing_spend_daily`,
  `fact_lead_funnel`.
- Power BI model with measures 1, 2, 3, 5, 7 and 9, each with a description.
- One implemented access control: the `Regional_Analyst` RLS role, or the
  gold-layer PII handling.
- README, runbook, governance doc, KPI definitions, source-system-owners
  doc, assumptions log, findings doc, reflection, and AI-usage note.
- Real git history: at least five commits.

### Stretch

Scored when present, and not penalised when absent, as long as your README
names what you cut and why:

- SCD2 on the agent dimension, with your effective-date assumption stated.
- Measures 4, 6, 8 and 10, plus the month-over-month and target-variance
  objects with the disconnected table.
- The lead-to-call bridge table and the call-grain denominator.
- The `Marketing_Analyst_NoPII` and `Ops_Team_Manager` roles implemented.
- The legacy stored-procedure port, with its parity test.
- CI workflow and the six named tests.
- The Databricks Asset Bundle stub and the Unity Catalog grants script.
- A before-and-after performance artefact with a measured change.
- `docs/STANDARDS.md` and `docs/platform_architecture.md`.

## Local tech stack and setup

We ask for these specific tools, with no substitutes, because they're what
the role uses day to day:

| Layer | Required | Runs without |
| --- | --- | --- |
| Data engineering | Python 3.11, PySpark 3.5, `delta-spark` 3.2 | An Azure subscription or a Databricks workspace |
| Storage | Local Delta tables under `./lakehouse/` | ADLS Gen2 |
| SQL layer | Spark SQL against the Delta tables | SQL Server, Synapse, or a Docker daemon |
| BI | Power BI Desktop, current release | A Power BI Service tenant |
| Tuning | DAX Studio, Tabular Editor 2, or VertiPaq Analyzer | A premium capacity |
| Lint and test | `ruff`, `pytest` (`chispa` optional) | A build agent |
| Version control | Git and a GitHub account | Company GitHub access |

`requirements.txt` in the starter repo is already pinned.

**Java**: PySpark needs Java 17 on your machine. Check what you have before
you start:

- Windows (PowerShell): `java -version` and `$env:JAVA_HOME`
- macOS: `java -version` and `echo $JAVA_HOME`

If neither points at a JDK 17 install, install one (Temurin 17 is a good
choice) and set `JAVA_HOME` before running the pipeline.

**Power BI Desktop is Windows-only.** If you don't have a Windows machine,
submit the `.pbix` file plus a screen recording of the model, the measure
values, and the RLS "view as role" check, recorded from a Windows VM. This is
an accepted alternative, not a workaround you need to ask us about.

## Using AI tools

Use whatever AI coding tools you'd normally use for this kind of work. It's
expected, and encouraged. Document how you used them in
`docs/AI_USAGE.md`: which tool and version, three tasks you used it for, one
verbatim prompt, one thing it got wrong and how you caught it, and one task
you deliberately chose not to delegate.

If you commit an agent configuration file (`CLAUDE.md`, `AGENTS.md`,
`.cursor/rules/`), that's a positive sign of how you set an agent up to work
in an unfamiliar repo, not something to remove before you submit.

## Submission

- Create one private GitHub repository for your submission (not public: a
  public solution would stay searchable by future candidates). Add us as a
  collaborator.
- Build a real commit history: at least five commits, with messages that say
  what changed. A single commit containing the whole solution will read as a
  missing signal, not a technicality.
- `data/` ships read-only. Don't edit the input files; we recheck their
  hashes against `data/MANIFEST.md`. If you find a problem with an input
  file, it's a finding to write up, not something to fix in place.
- `lakehouse/` should stay out of version control (it's generated); we
  regenerate it ourselves from your pipeline.
- Send us the repo URL, plus a one-paragraph email stating how long you
  spent and what you cut.

## Format and logistics

- This is a take-home. There's no live coding stage and no on-site
  component tied to it.
- Target effort on the core scope is 4 hours. Take longer if you want to
  attempt stretch items, but the core scope alone is a complete submission.
- You have 7 calendar days from when you receive this brief to submit.
- All data in the pack is synthetic: no real customers, no real employees,
  no real company data. Treat it as realistic test data, not confidential
  material.

If anything above is unclear, or you hit a genuine blocker unrelated to the
KPI-definition question above, get in touch. We'd rather answer a question
than have you guess at something we didn't mean.
