"""Expected Power BI measure values: python -m src.powerbi_expected --config config/config.yaml

Runs the expected_* queries in src/sql/findings_powerbi.sql on gold and writes
powerbi/expected_values.csv: one row per core measure and slice (the whole dataset,
each region, each month), so the values in the Power BI model can be compared with
them by hand. The queries mirror powerbi/measures.dax; tests/test_powerbi_model.py
checks the shared logic and that the committed file is reproducible.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import Row, SparkSession

from src.common.findings import load_findings

FINDINGS = Path(__file__).resolve().parent / "sql" / "findings_powerbi.sql"
GOLD_PREFIX = "gold."


def run_query(spark: SparkSession, name: str, prefix: str = GOLD_PREFIX) -> list[Row]:
    """Run a named query from findings_powerbi.sql. `prefix` replaces `gold.`, so the
    fixture tests can point the same SQL at global temp views (`global_temp.`)."""
    queries = load_findings(FINDINGS)
    if name not in queries:
        raise KeyError(f"No query named {name!r} in {FINDINGS}; have {sorted(queries)}")
    return spark.sql(queries[name].replace(GOLD_PREFIX, prefix)).collect()
