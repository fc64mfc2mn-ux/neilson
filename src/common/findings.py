"""Run named queries from src/sql/findings_*.sql.

Each query in a findings file starts with a `-- name: <query_name>` line. Every
number quoted in a document comes from one of these queries, and the tests run
them, so the numbers are reproducible.
"""

from __future__ import annotations

import re
from pathlib import Path

from pyspark.sql import Row, SparkSession

_NAME = re.compile(r"^--\s*name:\s*(\w+)\s*$", re.MULTILINE)


def load_findings(path: Path | str) -> dict[str, str]:
    text = Path(path).read_text(encoding="utf-8")
    parts = _NAME.split(text)
    queries = {}
    for name, body in zip(parts[1::2], parts[2::2], strict=True):
        sql = body.strip().rstrip(";").strip()
        if name in queries:
            raise ValueError(f"Duplicate finding name {name!r} in {path}")
        queries[name] = sql
    return queries


def run_finding(spark: SparkSession, path: Path | str, name: str) -> list[Row]:
    queries = load_findings(path)
    if name not in queries:
        raise KeyError(f"No finding named {name!r} in {path}; have {sorted(queries)}")
    return spark.sql(queries[name]).collect()
