# Governance

## Access tiers

| Tier | Name | Here |
| --- | --- | --- |
| 0 | Aggregate | Measures in the Power BI model |
| 1 | Internal | Keys, dates, campaigns, channels, spend, call timings, generalised lead attributes (age band, outward postcode) |
| 2 | Confidential | Personal data. Hashes stay tier 2, because they are pseudonymised, not anonymised |
| 3 | Restricted | Health data, or data that reveals it |

## Column classification

Every tier 2 and 3 column in the pack, and what happens to it. Every gold column is listed
in `DATA_CLASSIFICATION.md`, and a test checks that list against the built schemas.

| Source column | Tier | Treatment |
| --- | --- | --- |
| leads `first_name`, `last_name` | 2 | Dropped before gold |
| leads `email`, `phone`, `date_of_birth` | 2 | Salted SHA-256 hashes in `dim_lead`; DOB also becomes a decade `age_band` (tier 1) |
| leads `postcode` | 2 | `postcode_outward` only (AU: first 3 digits) |
| leads `estimated_cover_amount` | 2 | `dim_lead` only |
| leads `smoker_flag`, `health_condition_flag` | 3 | `dim_lead` only, never in `dim_lead_nopii` |
| leads `consent_marketing`, `consent_timestamp_utc`, `consent_source` | 2 | Replaced by the latest consent event (`consent_marketing_current`, `_source`), in `dim_lead` only |
| consent `source.actor`, `source.ref` | 2 | Silver only |
| calls `recording_url`, `call_notes` | 3 | Silver only; never in gold (the PII scan fails the run) |
| calls `qa_score`, `compliance_fail_flag` | 2 | `fact_call`; for Ops roles only |
| agents `agent_name`, `agent_email`, `manager_name`, `fte`, `hire_date` | 2 | Silver only |
| agents `seniority_band` | 2 | `dim_agent`, `dim_agent_seniority` |
| policies `annual_premium_local`, `monthly_premium_local`, `sum_assured_local` | 2 | `fact_policy` |
| policies `underwriting_decision` | 3 | Silver only |

## Roles and entitlements matrix

F = full, R = filtered to the user's regions, M = masked (no tier 2 or 3 columns), D = denied.

| Role | Shared dims¹ | `dim_agent` | `dim_agent_seniority` | `dim_lead` | `dim_lead_nopii` | Facts² | `bridge_user_region` |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `Regional_Analyst` | F | M | D | D | R | R, M | D |
| `Marketing_Analyst_NoPII` | F | M | D | D | F | F, M | D |
| `Ops_Team_Manager` | F | R | R | D | R | R (agent columns allowed) | D |
| `Underwriting_Reviewer` | F | F | F | F | F | F | D |

¹ `dim_date`, `dim_region`, `dim_channel`, `dim_campaign`, `dim_product`.
² `fact_lead`, `fact_call`, `fact_policy`, `fact_lead_funnel`, `bridge_lead_call`. Spend is
not personal data, so it is filtered by region only.

**Default deny.** Anyone with no row in `bridge_user_region` sees no rows. That includes
`group.finance@northgatelife.example` today, which needs a row before it sees anything.
Region `ALL` means no restriction.

**Small groups.** The agent-seniority visual hides any band with fewer than 3 distinct
agents. It uses a visual-level filter on the helper measure `Agents in Group`, which counts
`dim_agent[agent_id]` through `fact_call`. Some region and band groups have only 1 or 2
agents (for example, CA has 1 Trainee), so a region slicer or a one-region analyst would
otherwise see named people's results. Region stays off that visual.

## Controls implemented and where in the code

1. **Gold PII handling.** Implemented in `src/gold/apply_pii_policy.py`: hashes, drops,
   age band, outward postcode, and `dim_lead_nopii`. The run's last stage,
   `scan_gold_for_pii`, checks every gold column for raw values and for email, phone and URL
   patterns, and stops the run on a hit. Tests are in `tests/test_pii_policy.py`.
2. **`Regional_Analyst` RLS** in `powerbi/northgate_funnel.pbix`. The role filters
   `dim_region` on `USERPRINCIPALNAME()` against `bridge_user_region`, and the filter flows
   to every fact. Evidence is in `powerbi/rls_*.png`: `anz.manager` sees AU and US,
   `uk.analyst` sees UK, and an unmapped user sees zero rows.
3. **Data minimisation in the model.** The model imports `dim_lead_nopii`, never
   `dim_lead`. It also removes `seniority_band` from `dim_agent` and leaves `agent_name`
   out of gold.

## Controls designed but not implemented, with reasons

- **Unity Catalog row filter and column mask** (`src/sql/unity_catalog_grants.sql`, stretch).
  They cannot run in local Spark, so they would be tested only as SQL against fixtures, and
  the stub is not written yet.
- **`Ops_Team_Manager` and `Marketing_Analyst_NoPII` Power BI roles.** Only
  `Regional_Analyst` is built. The other two need object-level security (Tabular Editor) to
  hide `qa_score` and the seniority table.
- **`Underwriting_Reviewer`** is out of scope. Underwriting data (`underwriting_decision`,
  tier 3) is not in gold, and no report needs it.

## Unity Catalog mapping to the tiers

Tier 1 columns are granted with `SELECT` on the schema. Tier 2 is masked with a column mask
or served through the `dim_lead_nopii` view; locally that view is a table. Tier 3 is in
`dim_lead` only, granted to `Underwriting_Reviewer` alone. The row filter on each fact's
`region_key` reads `bridge_user_region`, the same source Power BI uses. Details are in
`platform_architecture.md`.

**Stated limits.** Unity Catalog row filters cannot run in local Spark. The plan is to
test them as SQL against the entitlements matrix with fixtures; that is not built yet. Power BI Import mode with a
shared service account does not pass the user's identity to Unity Catalog. The data-layer
filter applies only with DirectQuery and SSO passthrough, so the Power BI role stays as
defence in depth.

## Secrets handling

The PII hash salt is read from `config/secrets.json`, which is gitignored and created with a
random salt on the first run, with owner-only permissions. Only
`config/secrets.example.json`, with placeholders, is committed. The salt is applied inside a
UDF, so it never appears in a Spark plan or a log. In production it would come from
`dbutils.secrets.get(scope="kv-nfs-bi", key="pii-hash-salt")`. Rotating the salt changes
every hash, so it needs a full gold rebuild.

## Retention and consent handling

- **Consent.** The latest consent event in `silver.consent_current` is the source of truth.
  The lead file's flag is used only when a lead has no events, and is labelled
  `lead_capture`. Measures that use consent filter on `consent_marketing_source`.
- **Retention (proposed; the pack sets none).** Bronze keeps raw personal data, so access to
  it is limited to the pipeline identity. A proposed retention period for the Data
  Protection Officer to confirm: bronze and silver personal data 24 months after the last
  contact, gold indefinitely. An erasure request deletes the lead's rows from bronze and
  silver and rebuilds gold.

## Three design risks with mitigations

> **TODO (Jason): written by the human, not the AI.** Three risks found in this data, each
> with a mitigation.

## Residual risk accepted

- `lead_id` in `dim_lead` is pseudonymous but links back to the CRM, so it is tier 2.
- Hashes of email, phone and DOB can be reversed by anyone who holds the salt and a
  candidate list. The salt is the control.
- Calls after a DO_NOT_CALL disposition and withdrawn consent are flagged, not suppressed
  (`FINDINGS.md`). Fixing them is for the source systems.
