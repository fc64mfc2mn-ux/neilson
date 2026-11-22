"""A fixture breaching a stated threshold raises, and one within it
passes."""

from __future__ import annotations

from datetime import datetime

import pytest

from src.common.config import load_config
from src.common.dq import CHECK_DEFECTS, DQ_SCHEMA, defect_ids
from src.common.tables import append_table
from src.dq_gate import GateConfigError, GateLine, evaluate, load_expected, run_gate

LINES = [
    GateLine("lead_pk_conflict", 2, 6, "3 ids twice", "t1"),
    GateLine("call_end_before_start", 16, 380, "380 calls", "t2"),
]
T = datetime(2026, 11, 22, 12, 0)


def _dq(spark, rows):
    """rows: (check_name, severity, run_id, n)."""
    data = [
        (check, "src", f"k{i}", severity, T, CHECK_DEFECTS.get(check, 2), run, "quarantined", None)
        for check, severity, run, n in rows
        for i in range(n)
    ]
    return spark.createDataFrame(data, DQ_SCHEMA)


def test_dq_gate_threshold(spark):
    breach = evaluate({"lead_pk_conflict": 7, "call_end_before_start": 380}, LINES)
    assert not breach.passed
    assert any("lead_pk_conflict: 7 > expected 6" in f for f in breach.failures)
    clean = evaluate({"lead_pk_conflict": 6, "call_end_before_start": 380}, LINES)
    assert clean.passed and clean.failures == [] and clean.total == clean.expected_total == 386


def test_an_error_check_with_no_expected_line_fails():
    result = evaluate({"lead_pk_conflict": 6, "call_end_before_start": 380, "new_check": 1}, LINES)
    assert not result.passed
    assert any("new_check" in f and "no expected line" in f for f in result.failures)
    assert any("total 387 > expected 386" in f for f in result.failures)


def test_below_expected_warns_but_passes():
    result = evaluate({"lead_pk_conflict": 6}, LINES)
    assert result.passed
    assert result.warnings == ["call_end_before_start: 0 < expected 380 (fewer errors than the "
                               "register; check the source before lowering the line)"]  # fmt: skip


def test_gate_reads_the_latest_run_and_error_severity_only(spark, tmp_path):
    config = {"lakehouse": {"local_root": str(tmp_path)}, "environment": "local",
              "dq_gate": {"expected_errors": [
                  {"check_name": x.check_name, "defect_id": x.defect_id, "expected": x.expected,
                   "register": x.register, "test": x.test} for x in LINES]}}  # fmt: skip
    rows = [
        ("lead_pk_conflict", "error", "20261122T100000Z-old", 50),  # an older run: ignored
        ("lead_pk_conflict", "error", "20261122T120000Z-new", 6),
        ("call_end_before_start", "error", "20261122T120000Z-new", 380),
        ("call_start_before_lead", "warning", "20261122T120000Z-new", 999),  # not gated
    ]
    append_table(spark, _dq(spark, rows), config, "silver.dq_exception")
    result = run_gate(spark, config)
    assert result.run_id == "20261122T120000Z-new" and result.passed
    seeded = [("lead_unidentifiable", "error", "20261122T130000Z-seeded", 1)]
    append_table(spark, _dq(spark, seeded), config, "silver.dq_exception")
    assert not run_gate(spark, config).passed  # an unexpected error check in the newest run


def test_committed_expected_lines_match_the_register_and_sum_to_602():
    config = load_config("config/config.yaml")
    lines = load_expected(config)
    assert sum(x.expected for x in lines) == 602  # error rows per full run at step 12
    for x in lines:
        assert CHECK_DEFECTS[x.check_name] == x.defect_id in defect_ids(), x.check_name
        assert x.test.startswith("tests/") and "::test_" in x.test, x.check_name
    assert {x.check_name for x in lines} == {
        "lead_pk_conflict", "lead_unidentifiable", "fx_rate_missing", "spend_fx_rate_missing",
        "call_timestamp_unparseable", "call_end_before_start",
    }  # fmt: skip


def test_a_line_whose_defect_disagrees_with_the_mapping_is_refused():
    config = {"dq_gate": {"expected_errors": [
        {"check_name": "lead_pk_conflict", "defect_id": 3, "expected": 6, "register": "x",
         "test": "tests/x.py::test_x"}]}}  # fmt: skip
    with pytest.raises(GateConfigError, match="lead_pk_conflict"):
        load_expected(config)
