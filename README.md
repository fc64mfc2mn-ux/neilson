# Northgate Life funnel

## Purpose

<!-- What this repo builds and the business question it answers. -->

## Prerequisites

- **Python 3.11.** Check with `python3.11 --version`.
- **Java 17.** PySpark 3.5.1 and delta-spark 3.2.0 are pinned against it. Check with
  `java -version` (it should say `17`). If it shows another version, point `JAVA_HOME`
  at a Java 17 install. On macOS with Homebrew:

  ```bash
  brew install openjdk@17
  export JAVA_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
  export PATH="$JAVA_HOME/bin:$PATH"
  java -version
  ```

  Add the two `export` lines to your shell profile or to `.venv/bin/activate` so they
  persist.
- **Virtual environment and pinned packages** (pyspark, delta-spark, pytest, ruff):

  ```bash
  python3.11 -m venv .venv
  source .venv/bin/activate
  pip install -r requirements.txt
  ```

  Spark downloads the Delta jars from Maven the first time it starts, so that first run
  needs network access.

## How to run, one command

From the repo root, with the venv active:

```bash
python -m src.run_pipeline --config config/config.yaml
```

No other setup is needed. `config/config.yaml` is committed and holds only local `data/`
paths. On the first run the pipeline creates `config/secrets.json` (gitignored, owner-only
permissions) with a random PII hash salt, and reuses it afterwards. Deleting that file
changes the salt, and with it every hashed key in gold.

The runner checks the config, the input files and the salt, starts a UTC Spark session
and runs every stage in order: bronze, silver, gold dimensions, `dim_lead`, facts and
funnel, the gold PII scan, then the Power BI export. Any stage that fails stops the run
with exit code 1. Then gate the run on data quality:

```bash
python -m src.dq_gate --config config/config.yaml
```

### Power BI export

The export is the **last stage of the pipeline**, so the one command above produces it.
It can also be rerun on its own against an existing lakehouse:

```bash
python -m src.export_powerbi --config config/config.yaml
```

It writes one CSV per gold table to `export/powerbi/` (for example `dim_lead.csv`): every
dimension, fact and bridge plus `dim_lead_nopii`. UTF-8, comma-delimited, header row, ISO
dates, UTC timestamps (`yyyy-MM-dd HH:mm:ss`), keys as plain integers (all below 2^53),
empty for nulls; a second export is byte-identical. `export/` is generated and
gitignored, like `lakehouse/`. No raw PII column can be exported (the export stops);
`dim_lead.csv` carries hashes and the two health flags (tiers 2 and 3), so the model must
hide them from roles not cleared for them, and `dim_lead_nopii.csv` has neither.

## Input data

<!-- What is in data/ and where it comes from. -->

## Output tables

<!-- The bronze, silver and gold tables this pipeline produces. -->

## Semantic model and measures

<!-- What the Power BI model contains and where the measure DAX lives. -->

## Usage guidelines, who should use this and how

<!-- Who should use each measure, and who should not. -->

## Assumptions

<!-- Link to docs/ASSUMPTIONS.md and summarise the headline assumption. -->

## Known limitations

<!-- What is out of scope or known to be incomplete. -->

## Performance result

<!-- The before-and-after number from powerbi/performance/, and which measure it covers. -->

## Key Vault secret-scope path

<!-- The dbutils.secrets.get(scope=..., key=...) path the production equivalent would use. -->

## Time spent and what I cut and why

<!-- Actual hours spent, and the stretch items dropped, in priority order. -->
