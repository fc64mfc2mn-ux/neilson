"""Bronze ingestion: all strings, metadata columns, idempotent append, manifest counts."""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from src.bronze.ingest import (
    BRONZE_TABLES,
    add_bronze_metadata,
    ingest_all,
    ingest_source,
    latest_batch,
    read_source,
)
from src.common.config import load_config
from src.common.tables import read_table

REPO_ROOT = Path(__file__).resolve().parents[1]
METADATA = ("_ingest_ts", "_source_file", "_row_hash")


def _config(tmp_path: Path, sources: dict) -> dict:
    return {
        "environment": "local",
        "sources": sources,
        "lakehouse": {"local_root": str(tmp_path / "lakehouse")},
        "run_id": "test-run",
    }


def _csv_source(path: Path, text: str) -> dict:
    path.write_text(text, encoding="utf-8")
    return {"local_path": str(path), "format": "csv", "options": {"header": True}}


# ---------------------------------------------------------------- unit: metadata


def test_row_hash_is_deterministic_and_value_sensitive(spark):
    df = spark.createDataFrame(
        [("a", "1"), ("a", "1"), ("a", "2"), ("a", None), ("a", "")],
        "k string, v string",
    )
    hashes = [r._row_hash for r in add_bronze_metadata(df, "f.csv").collect()]
    assert hashes[0] == hashes[1]  # identical rows hash identically
    assert len(set(hashes[1:])) == 4  # a changed value, a null and "" all differ


def test_row_hash_does_not_collide_across_column_boundaries(spark):
    df = spark.createDataFrame([("ab", "c"), ("a", "bc")], "x string, y string")
    hashes = {r._row_hash for r in add_bronze_metadata(df, "f.csv").collect()}
    assert len(hashes) == 2


def test_csv_values_stay_raw_strings(spark, tmp_path):
    source = _csv_source(tmp_path / "s.csv", "id,amount,flag\n007,1.50,Y\n008,,N\n")
    df = read_source(spark, _config(tmp_path, {"s": source}), "s")
    assert all(isinstance(f.dataType, StringType) for f in df.schema.fields)
    assert [r.id for r in df.collect()] == ["007", "008"]  # leading zeros kept


def test_json_nested_values_become_json_strings(spark, tmp_path):
    path = tmp_path / "e.json"
    path.write_text('[{"id": 1, "meta": {"a": 2}, "ok": true}]', encoding="utf-8")
    source = {"local_path": str(path), "format": "json", "options": {"multiLine": True}}
    df = read_source(spark, _config(tmp_path, {"e": source}), "e")
    assert all(isinstance(f.dataType, StringType) for f in df.schema.fields)
    row = df.collect()[0]
    assert (row.id, row.ok) == ("1", "true")
    # primitivesAsString keeps nested primitives as text as well: no typing in bronze.
    assert row.meta.replace(" ", "") == '{"a":"2"}'


# ---------------------------------------------------------------- idempotent append


def test_rerun_appends_nothing_and_changed_file_appends_new_batch(spark, tmp_path):
    path = tmp_path / "leads.csv"
    config = _config(tmp_path, {"leads": _csv_source(path, "id,v\n1,a\n2,b\n2,b\n")})

    first = ingest_source(spark, config, "leads")
    assert (first.rows_read, first.rows_appended, first.skipped) == (3, 3, False)

    second = ingest_source(spark, config, "leads")
    assert (second.rows_appended, second.skipped) == (0, True)
    table = read_table(spark, config, "bronze.leads_raw")
    assert table.count() == 3  # exact duplicate rows inside the file are kept

    path.write_text("id,v\n1,a\n2,c\n", encoding="utf-8")
    third = ingest_source(spark, config, "leads")
    assert (third.rows_appended, third.skipped) == (2, False)

    table = read_table(spark, config, "bronze.leads_raw")
    assert table.count() == 5  # append-only: both batches are kept
    latest = latest_batch(table)
    assert sorted((r.id, r.v) for r in latest.collect()) == [("1", "a"), ("2", "c")]


def test_two_call_files_share_one_table(spark, tmp_path):
    uk = _csv_source(tmp_path / "calls_uk_au.csv", "call_id,duration_seconds\nC1,30\n")
    ca = _csv_source(tmp_path / "calls_ca_us.csv", "call_id,duration_minutes\nC2,0.5\nC3,1\n")
    config = _config(tmp_path, {"calls_uk_au": uk, "calls_ca_us": ca})
    ingest_source(spark, config, "calls_uk_au")
    ingest_source(spark, config, "calls_ca_us")

    calls = read_table(spark, config, "bronze.calls_raw")
    by_file = {r._source_file: r["count"] for r in calls.groupBy("_source_file").count().collect()}
    assert by_file == {"calls_uk_au.csv": 1, "calls_ca_us.csv": 2}
    assert {"duration_seconds", "duration_minutes"} <= set(calls.columns)

    # Rerunning both files appends nothing.
    assert ingest_source(spark, config, "calls_uk_au").skipped
    assert ingest_source(spark, config, "calls_ca_us").skipped
    assert read_table(spark, config, "bronze.calls_raw").count() == 3


# ---------------------------------------------------------------- real data reconciliation


def _manifest_counts() -> dict[str, tuple[int, int]]:
    text = (REPO_ROOT / "data" / "MANIFEST.md").read_text(encoding="utf-8")
    rows = re.findall(r"\| `([^`]+)` \| [^|]+\| (\d+) \| (\d+) \|", text)
    return {Path(f).name: (int(n), int(c)) for f, n, c in rows}


@pytest.fixture(scope="module")
def real_bronze(spark, tmp_path_factory):
    config = load_config(str(REPO_ROOT / "config" / "config.yaml"))
    config["lakehouse"] = {"local_root": str(tmp_path_factory.mktemp("lh") / "lakehouse")}
    config["run_id"] = "test-real"
    config["sources"] = {
        name: {**src, "local_path": str(REPO_ROOT / src["local_path"])}
        for name, src in config["sources"].items()
    }
    first = ingest_all(spark, config)
    second = ingest_all(spark, config)
    return config, first, second


def test_every_bronze_table_matches_manifest(spark, real_bronze):
    config, first, _ = real_bronze
    manifest = _manifest_counts()
    assert len(first) == 12
    for result in first:
        source_file = Path(config["sources"][result.source_name]["local_path"]).name
        rows, cols = manifest[source_file]
        df = latest_batch(read_table(spark, config, result.table)).where(
            F.col("_source_file") == source_file
        )
        assert result.rows_appended == rows, source_file
        assert df.count() == rows, source_file
        assert len(read_source(spark, config, result.source_name).columns) == cols, source_file


def test_bronze_tables_have_exact_names_and_string_columns(spark, real_bronze):
    config, _, _ = real_bronze
    expected = {
        "bronze.leads_raw", "bronze.consent_events_raw", "bronze.calls_raw",
        "bronze.policies_raw", "bronze.marketing_spend_raw", "bronze.campaigns_raw",
        "bronze.agents_raw", "bronze.fx_rates_raw", "bronze.ref_channel_raw",
        "bronze.ref_region_raw", "bronze.sec_user_region_raw",
    }  # fmt: skip
    assert set(BRONZE_TABLES.values()) == expected
    for name in expected:
        df = read_table(spark, config, name)
        assert set(METADATA) <= set(df.columns), name
        for field in df.schema.fields:
            if field.name != "_ingest_ts":
                assert isinstance(field.dataType, StringType), (name, field.name)


def test_calls_raw_holds_both_files(spark, real_bronze):
    config, _, _ = real_bronze
    calls = read_table(spark, config, "bronze.calls_raw")
    by_file = {r._source_file: r["count"] for r in calls.groupBy("_source_file").count().collect()}
    assert by_file == {"calls_uk_au.csv": 41_000, "calls_ca_us.csv": 23_000}


def test_second_full_run_is_idempotent(spark, real_bronze):
    config, first, second = real_bronze
    assert all(r.skipped and r.rows_appended == 0 for r in second)
    assert read_table(spark, config, "bronze.leads_raw").count() == 48_000
