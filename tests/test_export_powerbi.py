"""Step 15: gold exported to CSV for Power BI (export/powerbi/<table>.csv)."""

from __future__ import annotations

import csv
import hashlib
import re
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from src.common.tables import read_table
from src.export_powerbi import (
    EXPORT_TABLES,
    RAW_PII_COLUMNS,
    export_dir,
    export_powerbi,
    to_csv_text,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
INT_TEXT = re.compile(r"^-?\d+$")


def _rows(path: Path) -> list[list[str]]:
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.reader(fh))


# ------------------------------------------------------------------ unit


def test_csv_text_formats_every_type_plainly(spark):
    df = spark.createDataFrame(
        [
            (
                2,
                4_503_599_627_370_495,
                date(2026, 4, 1),
                "2026-04-01 00:02:09",
                Decimal("636.37"),
                87.5,
                True,
                'a, "quoted" value',
            ),  # fmt: skip
            (1, -1, None, None, None, None, None, None),
        ],
        "n int, k bigint, d date, ts string, amount decimal(14,2), score double, flag boolean, "
        "s string",
    ).withColumn("ts", F.to_timestamp("ts"))  # a UTC string, not a host-local datetime
    text = to_csv_text(df)
    assert text.startswith("n,k,d,ts,amount,score,flag,s\n")
    lines = text.splitlines()
    # Sorted (row 1 first), nulls empty, no scientific notation, ISO dates, UTC timestamps.
    assert lines[1] == "1,-1,,,,,,"
    assert lines[2] == '2,4503599627370495,2026-04-01,2026-04-01 00:02:09,636.37,87.5,true,' \
                       '"a, ""quoted"" value"'  # fmt: skip
    assert text.endswith("\n") and "\r" not in text
    assert to_csv_text(df.orderBy(F.desc("n"))) == text  # input order does not matter


def test_export_list_is_every_gold_table_and_no_raw_pii_column():
    from src.gold.apply_pii_policy import GOLD_TABLES

    assert set(EXPORT_TABLES) == set(GOLD_TABLES)  # every dimension, fact, bridge, nopii
    doc = (REPO_ROOT / "docs" / "DATA_CLASSIFICATION.md").read_text()
    not_in_gold = doc[doc.index("Not in gold at all") : doc.index("| Table |")]
    for col in RAW_PII_COLUMNS:
        assert f"`{col}`" in not_in_gold, col  # the list mirrors the classification


# ------------------------------------------------------------------ real data


@pytest.fixture(scope="module")
def exported(spark, real_gold_all, tmp_path_factory):
    config, _ = real_gold_all
    out = tmp_path_factory.mktemp("export") / "powerbi"
    config = {**config, "export": {"powerbi_dir": str(out)}}
    first = export_powerbi(spark, config)
    hashes = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in out.glob("*.csv")}
    return config, first, hashes


@pytest.mark.slow
def test_one_file_per_gold_table_with_the_same_row_count(spark, exported):
    config, result, _ = exported
    out = export_dir(config)
    assert sorted(p.name for p in out.iterdir()) == sorted(
        f"{t.split('.')[1]}.csv" for t in EXPORT_TABLES
    )
    for table in EXPORT_TABLES:
        rows = _rows(out / f"{table.split('.')[1]}.csv")
        gold = read_table(spark, config, table)
        assert rows[0] == gold.columns, table  # header, in gold column order
        assert len(rows) - 1 == gold.count() == result[table], table
        assert not set(rows[0]) & set(RAW_PII_COLUMNS), table
    assert result["gold.fact_lead_funnel"] == 47_201 and result["gold.fact_call"] == 62_829


@pytest.mark.slow
def test_a_second_export_is_byte_identical(spark, exported):
    config, _, first = exported
    export_powerbi(spark, config)
    out = export_dir(config)
    second = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in out.glob("*.csv")}
    assert second == first


@pytest.mark.slow
def test_surrogate_keys_survive_the_round_trip_exactly(spark, exported):
    config, _, _ = exported
    out = export_dir(config)
    for table in EXPORT_TABLES:
        gold = read_table(spark, config, table)
        keys = [c for c in gold.columns if c.endswith("_key")]
        rows = _rows(out / f"{table.split('.')[1]}.csv")
        header = rows[0]
        for k in keys:
            i = header.index(k)
            text = [r[i] for r in rows[1:] if r[i] != ""]
            assert all(INT_TEXT.match(v) for v in text), (table, k)  # no 1.2E15, no 12.0
            assert all(abs(int(v)) < 2**53 for v in text), (table, k)
            want = sorted(r[0] for r in gold.select(k).where(F.col(k).isNotNull()).collect())
            assert sorted(int(v) for v in text) == want, (table, k)


@pytest.mark.slow
def test_pii_scan_passes_on_the_exported_files(spark, exported):
    """The step 11 scan, run on the CSV files read back with each gold table's types."""
    from src.gold.apply_pii_policy import pii_leaks, raw_pii_values

    config, _, _ = exported
    out = export_dir(config)
    tables = {}
    for table in EXPORT_TABLES:
        schema = read_table(spark, config, table).schema
        tables[table] = (
            spark.read.option("header", True)
            .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
            .option("mode", "FAILFAST")  # a value that does not parse back is an error
            .schema(schema)
            .csv(str(out / f"{table.split('.')[1]}.csv"))
        )
    leaks = pii_leaks(tables, raw_pii_values(spark, config))
    assert leaks.count() == 0, leaks.collect()
    # The read-back is complete, not silently short.
    assert tables["gold.dim_lead"].count() == 47_201
