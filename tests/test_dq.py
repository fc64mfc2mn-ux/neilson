"""silver.dq_exception writer and the shared reconciliation check."""

from __future__ import annotations

from pathlib import Path

import pytest
from pyspark.sql.types import IntegerType, StringType, TimestampType

import src.common.dq as dq
from src.common.dq import (
    CHECK_DEFECTS,
    CONTRACT_COLUMNS,
    DQ_TABLE,
    ReconciliationError,
    assert_reconciled,
    defect_ids,
    quarantined_count,
    read_dq_exceptions,
    write_dq_exception,
)
from src.common.tables import read_table


def _config(tmp_path: Path, run_id: str = "run-1") -> dict:
    return {
        "environment": "local",
        "sources": {},
        "lakehouse": {"local_root": str(tmp_path / "lakehouse")},
        "run_id": run_id,
    }


@pytest.fixture
def rows(spark):
    return spark.createDataFrame(
        [("L1", "a", 1), ("L2", "b", 2), (None, "c", 3)], "lead_id string, src string, n int"
    )


def test_writes_and_reads_a_rejected_row(spark, tmp_path):
    config = _config(tmp_path)
    one = spark.createDataFrame([("L1",)], "lead_id string")
    written = write_dq_exception(
        one, "lead_pk_conflict", "bronze.leads_raw", "lead_id", "error", config=config, defect_id=2
    )
    assert written == 1

    row = read_table(spark, config, DQ_TABLE).collect()[0]
    assert (row.check_name, row.source_table, row.key_value, row.severity) == (
        "lead_pk_conflict",
        "bronze.leads_raw",
        "L1",
        "error",
    )
    assert row.detected_at_utc is not None
    assert (row.defect_id, row.run_id, row.action) == (2, "run-1", "quarantined")


def test_contract_columns_and_types(spark, tmp_path, rows):
    config = _config(tmp_path)
    write_dq_exception(
        rows, "c", "t", "lead_id", "warning", config=config, action="flagged", defect_id=2
    )
    df = read_table(spark, config, DQ_TABLE)
    assert df.columns[:5] == list(CONTRACT_COLUMNS)
    types = {f.name: f.dataType for f in df.schema.fields}
    assert isinstance(types["detected_at_utc"], TimestampType)
    assert isinstance(types["defect_id"], IntegerType)
    for name in ("check_name", "source_table", "key_value", "severity", "run_id", "action"):
        assert isinstance(types[name], StringType), name


def test_null_and_composite_keys(spark, tmp_path, rows):
    config = _config(tmp_path)
    write_dq_exception(rows, "single", "t", "lead_id", "error", config=config, defect_id=2)
    write_dq_exception(
        rows, "composite", "t", ["lead_id", "src"], "error", config=config, defect_id=2
    )
    df = read_dq_exceptions(spark, config)
    single = sorted(r.key_value for r in df.where("check_name = 'single'").collect())
    composite = sorted(r.key_value for r in df.where("check_name = 'composite'").collect())
    assert single == ["<null>", "L1", "L2"]
    assert composite == ["<null>|c", "L1|a", "L2|b"]


def test_detail_column_is_optional_per_row(spark, tmp_path, rows):
    config = _config(tmp_path)
    write_dq_exception(
        rows, "with_detail", "t", "lead_id", "info", config=config, detail_col="src", defect_id=2
    )
    details = sorted(r.detail for r in read_dq_exceptions(spark, config).collect())
    assert details == ["a", "b", "c"]


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"severity": "fatal"}, "severity"),
        ({"action": "deleted"}, "action"),
        ({"key_column": "missing"}, "missing"),
        ({"defect_id": 99}, "DEFECT_REGISTER"),
    ],
)
def test_invalid_arguments_raise(spark, tmp_path, rows, kwargs, message):
    args = {"check_name": "c", "source_table": "t", "key_column": "lead_id", "severity": "error"}
    args.update({k: v for k, v in kwargs.items() if k in args})
    extra = {"defect_id": 2, **{k: v for k, v in kwargs.items() if k not in args}}
    with pytest.raises(ValueError, match=message):
        write_dq_exception(rows, **args, config=_config(tmp_path), **extra)


def test_empty_frame_writes_nothing(spark, tmp_path, rows):
    config = _config(tmp_path)
    assert (
        write_dq_exception(rows.limit(0), "c", "t", "lead_id", "error", config=config, defect_id=2)
        == 0
    )
    assert read_dq_exceptions(spark, config).count() == 0  # table exists, but empty


def test_each_run_is_kept_and_readable_on_its_own(spark, tmp_path, rows):
    write_dq_exception(
        rows, "c", "t", "lead_id", "error", config=_config(tmp_path, "run-1"), defect_id=2
    )
    write_dq_exception(
        rows, "c", "t", "lead_id", "error", config=_config(tmp_path, "run-2"), defect_id=2
    )
    config = _config(tmp_path, "run-2")
    assert read_dq_exceptions(spark, config, all_runs=True).count() == 6
    assert read_dq_exceptions(spark, config).count() == 3  # current run only


def test_quarantined_count_ignores_flagged_and_other_tables(spark, tmp_path, rows):
    config = _config(tmp_path)
    write_dq_exception(
        rows, "q", "bronze.leads_raw", "lead_id", "error", config=config, defect_id=2
    )
    write_dq_exception(
        rows,
        "f",
        "bronze.leads_raw",
        "lead_id",
        "warning",
        config=config,
        action="flagged",
        defect_id=2,
    )
    write_dq_exception(
        rows, "q", "bronze.calls_raw", "lead_id", "error", config=config, defect_id=2
    )
    assert quarantined_count(spark, config, "bronze.leads_raw") == 3


def test_defect_ids_match_the_register():
    ids = defect_ids()
    assert ids == set(range(1, len(ids) + 1))  # numbered 1..N with no gaps
    assert 27 in ids


def test_original_five_positional_arguments_still_work(spark, tmp_path, monkeypatch):
    """The starter stub's call shape: no defect_id, no config."""
    config = _config(tmp_path, run_id="adhoc")
    monkeypatch.setattr(dq, "default_config", lambda: config)
    one = spark.createDataFrame([("LD-1",)], "lead_id string")

    written = write_dq_exception(one, "lead_pk_conflict", "bronze.leads_raw", "lead_id", "error")

    assert written == 1
    row = read_table(spark, config, DQ_TABLE).collect()[0]
    assert (row.check_name, row.source_table, row.key_value, row.severity) == (
        "lead_pk_conflict",
        "bronze.leads_raw",
        "LD-1",
        "error",
    )
    assert row.defect_id == CHECK_DEFECTS["lead_pk_conflict"] == 2


def test_unmapped_check_name_without_defect_id_fails(spark, tmp_path, rows):
    with pytest.raises(ValueError, match="no registered defect"):
        write_dq_exception(rows, "not_a_rule", "t", "lead_id", "error", config=_config(tmp_path))


def test_every_mapped_check_points_at_a_registered_defect():
    assert CHECK_DEFECTS, "mapping must not be empty"
    assert set(CHECK_DEFECTS.values()) <= defect_ids()


def test_default_config_is_the_committed_run_config():
    config = dq.default_config()
    assert config["environment"] == "local"
    assert len(config["sources"]) == 12
    assert config["run_id"] == "adhoc"


def test_assert_reconciled():
    assert_reconciled("t", rows_in=10, rows_out=7, rows_quarantined=3)
    with pytest.raises(ReconciliationError, match="t: 10 in != 7 out \\+ 2 quarantined"):
        assert_reconciled("t", rows_in=10, rows_out=7, rows_quarantined=2)
