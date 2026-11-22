"""The bridge from Lead to Policy to Connected Call to Policy: python -m src.kpi_reconciliation
--config config/config.yaml.

Runs `kpi_bridge_steps` (src/sql/findings_powerbi.sql) on gold and writes
powerbi/kpi_reconciliation.csv: for each period, seven steps, each changing one thing, with
the counts, the rate and the change in percentage points. Rates are rounded to four decimals
of a percent, and each step's change is the difference of the rounded rates, so the steps add
up exactly to the end rate with no residual. The write fails if they would not.
"""

from __future__ import annotations

import argparse
import csv
import io
import sys
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

from pyspark.sql import SparkSession

from src.common.config import load_config, resolve_lakehouse_root
from src.common.spark_session import get_spark, spark_settings
from src.common.tables import register_table
from src.powerbi_expected import GOLD_PREFIX, GOLD_TABLES, run_query

QUERY = "kpi_bridge_steps"
DEFAULT_OUT = Path("powerbi/kpi_reconciliation.csv")
STEP_NAMES = (
    "Lead to Policy Conversion Rate",
    "Numerator date: policy start, not lead created",
    "Denominator grain: leads become calls",
    "Connected calls only",
    "Call date: calls made in the period",
    "Business-hours leads only (both sides)",
    "Agent attribution: last call (a slice, no change)",
)
HEADER = (
    "period", "step", "step_name", "numerator", "denominator", "rate_pct", "delta_pp",
)  # fmt: skip
PCT = Decimal("0.0001")


def _pct(numerator: int, denominator: int) -> Decimal:
    return (Decimal(numerator) * 100 / Decimal(denominator)).quantize(PCT, ROUND_HALF_UP)


def bridge_rows(spark: SparkSession, prefix: str = GOLD_PREFIX) -> list[dict]:
    """One dict per period and step. `delta_pp` is the change from the previous step."""
    out: list[dict] = []
    for r in run_query(spark, QUERY, prefix):
        assert r.step_name == STEP_NAMES[r.step], (r.step, r.step_name)
        out.append({
            "period": r.period, "step": r.step, "step_name": r.step_name,
            "numerator": int(r.numerator), "denominator": int(r.denominator),
            "rate_pct": _pct(int(r.numerator), int(r.denominator)),
        })  # fmt: skip
    out.sort(key=lambda x: (x["period"] != "Q2 2026", x["step"]))
    for i, row in enumerate(out):
        first = row["step"] == 0
        row["delta_pp"] = Decimal(0) if first else row["rate_pct"] - out[i - 1]["rate_pct"]
    for period in {r["period"] for r in out}:
        mine = [r for r in out if r["period"] == period]
        residual = mine[0]["rate_pct"] + sum((r["delta_pp"] for r in mine[1:]), Decimal(0))
        if residual != mine[-1]["rate_pct"]:
            raise AssertionError(f"Bridge for {period} leaves a residual")
    for row in out:
        row["rate_pct"] = str(row["rate_pct"])
        row["delta_pp"] = "" if row["step"] == 0 else f"{row['delta_pp']:+}"
    return out


def bridge_csv(rows: list[dict]) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(HEADER)
    writer.writerows([r[h] for h in HEADER] for r in rows)
    return buf.getvalue()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", required=True)
    parser.add_argument("--out", default=str(DEFAULT_OUT))
    args = parser.parse_args(argv)
    config = load_config(args.config)
    spark = get_spark(warehouse_dir=resolve_lakehouse_root(config), **spark_settings(config))
    for table in GOLD_TABLES:
        register_table(spark, config, f"gold.{table}")
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(bridge_csv(bridge_rows(spark)), encoding="utf-8", newline="")
    print(f"Wrote {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
