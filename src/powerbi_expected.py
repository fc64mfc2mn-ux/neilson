"""Expected Power BI measure values: python -m src.powerbi_expected --config config/config.yaml

Runs the expected_* queries in src/sql/findings_powerbi.sql on gold and writes
powerbi/expected_values.csv: one row per measure and slice (the whole dataset, each
region and month; channel, seniority and first-call bucket where a visual needs them),
so the values in the Power BI model can be compared with them by hand. The queries
mirror powerbi/measures.dax; tests/test_powerbi_model.py checks the shared logic and
that the committed file is reproducible.
"""

from __future__ import annotations

import argparse
import csv
import io
import sys
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

from pyspark.sql import Row, SparkSession

from src.common.config import load_config, resolve_lakehouse_root
from src.common.findings import load_findings
from src.common.spark_session import get_spark, spark_settings
from src.common.tables import register_table
from src.gold.build_lead_funnel import FIRST_CALL_BUCKETS

FINDINGS = Path(__file__).resolve().parent / "sql" / "findings_powerbi.sql"
GOLD_PREFIX = "gold."


def run_query(spark: SparkSession, name: str, prefix: str = GOLD_PREFIX) -> list[Row]:
    """Run a named query from findings_powerbi.sql. `prefix` replaces `gold.`, so the
    fixture tests can point the same SQL at global temp views (`global_temp.`)."""
    queries = load_findings(FINDINGS)
    if name not in queries:
        raise KeyError(f"No query named {name!r} in {FINDINGS}; have {sorted(queries)}")
    return spark.sql(queries[name].replace(GOLD_PREFIX, prefix)).collect()


# Core measures (brief: 1, 2, 3, 5, 7, 9), in the brief's order, with their query; then
# Cost Per Policy (GBP) (stretch 10), which the channel visual needs.
CORE_MEASURES = (
    ("Marketing Spend (GBP)", "expected_marketing_spend_gbp"),
    ("Leads (Deduplicated)", "expected_leads_deduplicated"),
    ("Cost Per Lead (GBP)", "expected_cost_per_lead_gbp"),
    ("Lead to Policy Conversion Rate", "expected_lead_to_policy_conversion_rate"),
    ("Average Handle Time (Seconds)", "expected_average_handle_time_seconds"),
    ("Time to First Call (Median Minutes)", "expected_time_to_first_call_median_minutes"),
)
EXTRA_MEASURES = (("Cost Per Policy (GBP)", "expected_cost_per_policy_gbp"),)
MEASURES = CORE_MEASURES + EXTRA_MEASURES
GOLD_TABLES = (
    "dim_date", "dim_region", "dim_channel", "dim_agent", "dim_agent_seniority",
    "fact_marketing_spend_daily", "fact_lead", "fact_lead_funnel", "fact_policy", "fact_call",
)  # fmt: skip
HEADER = ("measure", "slice", "slice_value", "value")
SLICE_ORDER = {"all": 0, "region": 1, "month": 2, "channel": 3, "seniority": 4,
               "first_call_bucket": 5}  # fmt: skip
BUCKET_ORDER = {name: i for i, (name, _) in enumerate(FIRST_CALL_BUCKETS)}
DEFAULT_OUT = Path("powerbi/expected_values.csv")


def _value(v) -> str:
    """Six decimal places, half-up, plain digits; empty for NULL (DAX BLANK)."""
    if v is None:
        return ""
    return str(Decimal(str(v)).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP))


def _order(r: Row) -> tuple:
    """Slice type, then value; buckets in bucket order, '(blank)' after them."""
    within = (
        BUCKET_ORDER.get(r.slice_value, len(BUCKET_ORDER)) if r.slice == "first_call_bucket" else 0
    )
    return (SLICE_ORDER[r.slice], within, r.slice_value)


def expected_rows(spark: SparkSession, prefix: str = GOLD_PREFIX) -> list[tuple[str, ...]]:
    rows = []
    for measure, query in MEASURES:
        found = sorted(run_query(spark, query, prefix), key=_order)
        rows += [(measure, r.slice, r.slice_value, _value(r.value)) for r in found]
    return rows


def expected_csv(spark: SparkSession, prefix: str = GOLD_PREFIX) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(HEADER)
    writer.writerows(expected_rows(spark, prefix))
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
    out.write_text(expected_csv(spark), encoding="utf-8", newline="")
    print(f"Wrote {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
