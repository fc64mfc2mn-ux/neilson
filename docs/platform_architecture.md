# Platform architecture

## What runs locally, and the Databricks equivalent

| Concern | Local build (this repo) | Databricks equivalent |
| --- | --- | --- |
| Compute | PySpark 3.5.1 on Java 17, one process | Job cluster or serverless jobs compute |
| Storage | Delta tables under `./lakehouse/{bronze,silver,gold}` | Delta tables in ADLS Gen2, registered in Unity Catalog |
| Tables | `bronze.*`, `silver.*`, `gold.*` in the local metastore | `northgate_<env>.<layer>.<table>` |
| Orchestration | `python -m src.run_pipeline --config config/config.yaml` | A Databricks Job from `databricks.yml` (stub), with dev and prod targets |
| DQ gate | `python -m src.dq_gate`, run in GitHub Actions CI | A job task after the pipeline; failure alerts the owner |
| Secrets | `config/secrets.json` (gitignored) | Key Vault-backed secret scope |
| Access control | Gold PII policy in code, plus Power BI RLS | The same, plus Unity Catalog grants, row filter and column mask |
| Power BI source | CSVs in `export/powerbi/` | A Databricks SQL warehouse (Import or DirectQuery) |

## Unity Catalog three-level namespace for this model

The catalog is per environment and the schema is per layer: `northgate_dev` and
`northgate_prod` (the `catalog` variable in `databricks.yml`), each with the schemas
`bronze`, `silver` and `gold`. Table names are the same as locally, for example
`northgate_prod.gold.fact_lead_funnel` and `northgate_prod.silver.dq_exception`.
`gold.dim_lead_nopii` becomes a view over `gold.dim_lead`; locally it is a table, because
the local build has no grants that a view would enforce.

## Grants design mapped to the four roles

Each role is an account group. The pipeline's service principal owns all three schemas.
Analysts get `USE CATALOG`, plus `USE SCHEMA` on `gold` only; no analyst group can read
bronze or silver.

| Group | Grants |
| --- | --- |
| `Regional_Analyst` | `SELECT` on dimensions, facts and the `dim_lead_nopii` view; row filter on facts |
| `Marketing_Analyst_NoPII` | As above, with no row restriction (`ALL` in the bridge) |
| `Ops_Team_Manager` | `SELECT` on the facts, `dim_agent` and `dim_agent_seniority`; row filter; customer columns masked |
| `Underwriting_Reviewer` | `SELECT` on `gold` including `dim_lead` (designed, not implemented) |

**Row filter.** A SQL function on each fact's `region_key` returns true when
`bridge_user_region` has a row for `current_user()` with that region, or with `ALL`.
Unmapped users get no rows. This is the same bridge the Power BI role reads, so there is
one source of truth for entitlements.

**Column mask.** `qa_score` and `compliance_fail_flag` show only to members of
`Ops_Team_Manager`. The health flags in `dim_lead` show only to `Underwriting_Reviewer`.
These statements belong in `src/sql/unity_catalog_grants.sql`, which is a stub for now
(stretch).

## ADLS Gen2 container and path layout, and how config switches local to `abfss://`

- Raw landing zone: `abfss://raw@nfsdatalakedev.dfs.core.windows.net/{marketing,contact_centre,finance,reference}/`,
  which mirrors `data/`.
- Lakehouse: `abfss://lakehouse@nfsdatalakedev.dfs.core.windows.net/{bronze,silver,gold}/`,
  as managed storage for the catalog.
- Prod uses its own storage account (proposed: `nfsdatalakeprod`), so dev clusters cannot reach prod
  data.

One `environment: local | azure` key in the config chooses each source's `local_path` or
`azure_path`, and `lakehouse.local_root` or `lakehouse.azure_root`
(`src/common/config.py`). Code never holds a path. The committed `config/config.yaml` is
local only; `config/config.example.yaml` shows the `abfss://` paths.

## Key Vault-backed secret scope

The secret scope `kv-nfs-bi` is backed by the Key Vault of the same name, and the salt is
read as `dbutils.secrets.get(scope="kv-nfs-bi", key="pii-hash-salt")`. Only the pipeline's
service principal can read the scope. Databricks redacts secret values in notebook output;
the code also applies the salt inside a UDF, so it never appears in a query plan. Rotating
the salt needs a planned full rebuild of gold, because every hash changes.

## Where Microsoft Fabric would sit, and where it would not

Fabric would sit downstream as the reporting layer. The Power BI semantic model and report
would live in a Fabric workspace, reading gold through a SQL warehouse connection, or
through Direct Lake over OneLake shortcuts to the gold Delta tables. That would remove the
CSV export and the Import refresh.

Fabric would not take over the engineering layer. The pipeline, the DQ gate and the PII
policy stay in Databricks, where Unity Catalog's row filters and masks govern the data
itself. Moving them would split governance across two catalogs. A row filter in Unity
Catalog does not follow a shortcut into OneLake, so the Power BI role would still be
needed.

## What changes when this runs on a real workspace

1. Set `environment: azure`, and take the salt from the secret scope instead of the file.
2. Register the tables in Unity Catalog and apply the grants, the row filter and the masks.
3. Run the job from the Asset Bundle on a schedule, with the DQ gate as a dependent task.
4. Point Power BI at a SQL warehouse instead of CSVs.

## Limits

- Unity Catalog row filters cannot run in local Spark. The plan is to test them as SQL against
  the entitlements matrix with fixtures; that is not built yet (stretch).
- Power BI Import mode with a shared service account does not pass the user's identity to
  Unity Catalog. The data-layer filter applies only with DirectQuery and SSO passthrough,
  so the Power BI `Regional_Analyst` role stays as defence in depth.
- `databricks.yml` is a stub and has not been deployed.
