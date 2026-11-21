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

<!-- The single command that runs the whole pipeline end to end. -->

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
