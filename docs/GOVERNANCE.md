# Governance

## Access tiers

<!-- fill in -->

## Column classification

<!-- fill in -->

## Roles and entitlements matrix

<!-- fill in -->

## Controls implemented and where in the code

<!-- fill in -->

Note for drafting: the agent-seniority visual hides any band with fewer than 3 distinct
agents (visual-level filter on the helper measure `Agents in Group`, which counts
`dim_agent[agent_id]` through `fact_call`). This stops a region slicer or a one-region
`Regional_Analyst` from seeing one or two named agents' results (tier 2 HR performance
data). Region stays off that visual. `seniority_band` itself is tier 2: it lives in
`gold.dim_agent` (the SCD2 versions of AG0008 and AG0037 differ only by it) and in
`gold.dim_agent_seniority`, which the model uses.

## Controls designed and not implemented, with reasons

<!-- fill in -->

## Unity Catalog mapping to the tiers

<!-- fill in -->

Note for drafting: `gold.dim_lead_nopii` is a Delta table in the local build; in Unity
Catalog it would be a view over `gold.dim_lead` granted to `Marketing_Analyst_NoPII`.

## Secrets handling

<!-- fill in -->

## Retention and consent handling

<!-- fill in -->

## Three design risks with mitigations

<!-- fill in -->

## Residual risk accepted

<!-- fill in -->
