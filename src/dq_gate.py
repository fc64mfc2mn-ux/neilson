"""Data-quality gate: python -m src.dq_gate --config config/config.yaml

Reads `silver.dq_exception` for one pipeline run (the latest by default) and
compares its error-severity rows with the expected counts in config
(`dq_gate.expected_errors`): one line per check, each with its register
defect id, the register's evidence and the test that proves the count. The
lines are built from docs/DEFECT_REGISTER.md, not copied from a run. Rules
(docs/RUNBOOK.md, "Data-quality gates and thresholds"):

(a) an error check above its expected count fails;
(b) an error check with no expected line fails;
(c) the run's error total above the sum of the lines fails (a backstop:
    with (a) and (b) holding it cannot trigger alone).

Below an expected count is a warning, not a failure: fewer errors than the
register means the source improved or a check stopped firing, and someone
should look before the line is lowered. Warning and info rows are not gated.

Exit code 0 when the gate passes, 1 when it fails or cannot run.
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import load_config
from src.common.dq import CHECK_DEFECTS, DQ_TABLE, defect_ids
from src.common.tables import read_table, table_exists

log = logging.getLogger("northgate.dq_gate")


class GateConfigError(ValueError):
    """The expected-count lines in config are missing or disagree with the register."""


@dataclass(frozen=True)
class GateLine:
    check_name: str
    defect_id: int
    expected: int
    register: str
    test: str


@dataclass
class GateResult:
    run_id: str | None
    counts: dict[str, int]
    total: int
    expected_total: int
    failures: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.failures


def load_expected(config: dict) -> list[GateLine]:
    """The expected error lines, checked against CHECK_DEFECTS and the register."""
    raw = config.get("dq_gate", {}).get("expected_errors")
    if not raw:
        raise GateConfigError("config has no dq_gate.expected_errors lines")
    lines = [GateLine(r["check_name"], int(r["defect_id"]), int(r["expected"]),
                      str(r["register"]), str(r["test"])) for r in raw]  # fmt: skip
    names = [x.check_name for x in lines]
    if len(names) != len(set(names)):
        raise GateConfigError("dq_gate.expected_errors names a check twice")
    for x in lines:
        if CHECK_DEFECTS.get(x.check_name) != x.defect_id or x.defect_id not in defect_ids():
            raise GateConfigError(
                f"{x.check_name}: defect {x.defect_id} does not match CHECK_DEFECTS "
                f"({CHECK_DEFECTS.get(x.check_name)}) and the register"
            )
        if x.expected < 0:
            raise GateConfigError(f"{x.check_name}: expected count is negative")
    return lines


def evaluate(counts: dict[str, int], lines: list[GateLine]) -> GateResult:
    """Apply rules (a), (b), (c) and the below-expected warning to error counts by check."""
    expected = {x.check_name: x.expected for x in lines}
    result = GateResult(None, dict(counts), sum(counts.values()), sum(expected.values()))
    for check, n in sorted(counts.items()):
        if check not in expected:
            result.failures.append(f"{check}: {n} error rows and no expected line")
        elif n > expected[check]:
            result.failures.append(f"{check}: {n} > expected {expected[check]}")
    for check, want in sorted(expected.items()):
        got = counts.get(check, 0)
        if got < want:
            result.warnings.append(
                f"{check}: {got} < expected {want} (fewer errors than the register; "
                "check the source before lowering the line)"
            )
    if result.total > result.expected_total:
        result.failures.append(f"total {result.total} > expected {result.expected_total}")
    return result


def latest_run_id(dq: DataFrame) -> str | None:
    """The run that wrote the most recent exception."""
    row = dq.orderBy(F.col("detected_at_utc").desc(), F.col("run_id").desc()).first()
    return None if row is None else row.run_id


def run_gate(spark: SparkSession, config: dict, run_id: str | None = None) -> GateResult:
    lines = load_expected(config)
    if not table_exists(spark, config, DQ_TABLE):
        raise GateConfigError(f"{DQ_TABLE} does not exist: run the pipeline first")
    dq = read_table(spark, config, DQ_TABLE)
    run_id = run_id or latest_run_id(dq)
    errors = dq.where((F.col("run_id") == run_id) & (F.col("severity") == "error"))
    counts = {r.check_name: r["count"] for r in errors.groupBy("check_name").count().collect()}
    result = evaluate(counts, lines)
    result.run_id = run_id
    return result


def report(result: GateResult, lines: list[GateLine]) -> str:
    rows = [f"DQ gate, run {result.run_id}: {'PASS' if result.passed else 'FAIL'}",
            f"{'check':32} {'defect':>6} {'expected':>8} {'actual':>8}"]  # fmt: skip
    for x in lines:
        rows.append(f"{x.check_name:32} {x.defect_id:>6} {x.expected:>8} "
                    f"{result.counts.get(x.check_name, 0):>8}")  # fmt: skip
    for check in sorted(set(result.counts) - {x.check_name for x in lines}):
        rows.append(f"{check:32} {'?':>6} {'-':>8} {result.counts[check]:>8}")
    rows.append(f"{'total':32} {'':>6} {result.expected_total:>8} {result.total:>8}")
    rows += [f"WARN {w}" for w in result.warnings] + [f"FAIL {f}" for f in result.failures]
    return "\n".join(rows)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Gate a pipeline run on silver.dq_exception.")
    parser.add_argument("--config", required=True)
    parser.add_argument("--run-id", help="Gate this run instead of the latest")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    from src.common.config import resolve_lakehouse_root
    from src.common.spark_session import get_spark, spark_settings, stop_spark

    try:
        config = load_config(args.config)
        lines = load_expected(config)
        spark = get_spark(warehouse_dir=resolve_lakehouse_root(config), **spark_settings(config))
    except Exception:
        log.exception("DQ gate could not start")
        return 1
    try:
        spark.sparkContext.setLogLevel("ERROR")
        result = run_gate(spark, config, args.run_id)
        print(report(result, lines))
        return 0 if result.passed else 1
    except Exception:
        log.exception("DQ gate could not run")
        return 1
    finally:
        stop_spark(spark)


if __name__ == "__main__":
    sys.exit(main())
