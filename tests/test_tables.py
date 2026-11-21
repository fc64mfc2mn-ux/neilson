"""Path-based Delta table helpers."""

from __future__ import annotations

from src.common.tables import append_table, register_table


def _config(root) -> dict:
    return {"environment": "local", "sources": {}, "lakehouse": {"local_root": str(root)}}


def test_register_table_repoints_when_lakehouse_changes(spark, tmp_path):
    first, second = _config(tmp_path / "a"), _config(tmp_path / "b")
    append_table(spark, spark.createDataFrame([(1,)], "x int"), first, "scratch.t")
    append_table(spark, spark.createDataFrame([(2,), (3,)], "x int"), second, "scratch.t")

    assert spark.table("scratch.t").count() == 2  # name follows the latest registration

    register_table(spark, first, "scratch.t")
    assert spark.table("scratch.t").count() == 1
    # Re-pointing only changes the catalog entry; the other table's data is untouched.
    register_table(spark, second, "scratch.t")
    assert spark.table("scratch.t").count() == 2
