"""A fixture of 8 leads in 3 identity clusters collapses to 3 surviving
leads, and `lead_dedup_map` has 8 rows."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from src.common.findings import run_finding
from src.silver.dedupe_leads import dedupe_leads, normalise_email, normalise_phone

T0 = datetime(2026, 4, 1, 9, 0, 0)
FINDINGS_LEADS = Path(__file__).resolve().parents[1] / "src" / "sql" / "findings_leads.sql"
SCHEMA = "lead_id string, created_at_utc timestamp, email string, phone string"


def _leads(spark, rows):
    return spark.createDataFrame(rows, SCHEMA)


def test_dedupe_leads_collapses_clusters(spark):
    h = timedelta(hours=1)
    leads = _leads(
        spark,
        [
            # Cluster A: same email, each within 48h of the previous (chained: A3 is 80h
            # after A1 but 40h after A2).
            ("A1", T0, "anna@x.example", None),
            ("A2", T0 + 40 * h, " ANNA@x.example ", "111"),
            ("A3", T0 + 80 * h, "anna@x.example", None),
            # Cluster B: B1-B2 share an email, B2-B3 share a phone (B3 linked transitively).
            ("B2", T0 + 2 * h, "ben@x.example", "(220) 766-4317"),
            ("B1", T0 + 1 * h, "ben@x.example", None),
            ("B3", T0 + 3 * h, None, "220-766-4317"),
            # Cluster C: phone only, different punctuation.
            ("C1", T0, None, "+61 445 761280"),
            ("C2", T0 + 47 * h, "carl@x.example", "61445761280"),
        ],
    )
    survivors, mapping = dedupe_leads(leads)

    assert sorted(r.lead_id for r in survivors.collect()) == ["A1", "B1", "C1"]
    rows = {r.source_lead_id: r for r in mapping.collect()}
    assert len(rows) == 8
    assert {k: v.surviving_lead_id for k, v in rows.items()} == {
        "A1": "A1", "A2": "A1", "A3": "A1",
        "B1": "B1", "B2": "B1", "B3": "B1",
        "C1": "C1", "C2": "C1",
    }  # fmt: skip
    assert {r.cluster_size for r in rows.values()} == {2, 3}
    assert rows["A1"].match_rule == "self"
    assert rows["A2"].match_rule == "email"
    assert rows["B2"].match_rule == "email"
    assert rows["B3"].match_rule == "transitive"
    assert rows["C2"].match_rule == "phone"
    sizes = {r.lead_id: r.dedup_cluster_size for r in survivors.collect()}
    assert sizes == {"A1": 3, "B1": 3, "C1": 2}


def test_gap_over_48_hours_starts_a_new_cluster(spark):
    leads = _leads(
        spark,
        [
            ("X1", T0, "x@x.example", None),
            ("X2", T0 + timedelta(hours=48), "x@x.example", None),  # exactly 48h: same
            ("X3", T0 + timedelta(hours=96, seconds=1), "x@x.example", None),  # >48h after X2
        ],
    )
    survivors, mapping = dedupe_leads(leads)
    assert sorted(r.lead_id for r in survivors.collect()) == ["X1", "X3"]
    assert {r.source_lead_id: r.surviving_lead_id for r in mapping.collect()} == {
        "X1": "X1", "X2": "X1", "X3": "X3"
    }  # fmt: skip


def test_earliest_lead_survives_even_if_flagged_as_the_duplicate(spark):
    leads = _leads(
        spark,
        [
            ("ORIG", T0 + timedelta(hours=6), "m@x.example", "07814750241"),
            ("ORIG-DUP1", T0, "m@x.example", "07814750241"),
        ],
    )
    survivors, _ = dedupe_leads(leads)
    assert [r.lead_id for r in survivors.collect()] == ["ORIG-DUP1"]


def test_leads_sharing_no_key_are_not_merged(spark):
    leads = _leads(
        spark,
        [("K1", T0, "k@x.example", None), ("K1-DUP1", T0 + timedelta(hours=1), None, "492")],
    )
    survivors, mapping = dedupe_leads(leads)
    assert survivors.count() == 2
    assert mapping.where("source_lead_id != surviving_lead_id").count() == 0


def test_normalisers(spark):
    df = spark.createDataFrame(
        [(" A.B@X.Example ", "+44 7815 778995"), ("", "()"), (None, None)], "e string, p string"
    )
    out = df.select(normalise_email("e").alias("e"), normalise_phone("p").alias("p")).collect()
    assert [(r.e, r.p) for r in out] == [
        ("a.b@x.example", "447815778995"),
        (None, None),
        (None, None),
    ]


def test_phone_normaliser_is_a_parameter(spark):
    """Digits-only (the decided rule) does not link a UK number written with and without
    the country code; E.164 does. Used by the real-data check below."""
    from src.gold.apply_pii_policy import phone_e164

    leads = _leads(
        spark,
        [("P1", T0, None, "+44 7815 778995"), ("P2", T0 + timedelta(hours=3), None, "07815778995")],
    )
    _, digits = dedupe_leads(leads)
    _, e164 = dedupe_leads(leads, phone_normaliser=phone_e164)
    assert {r.source_lead_id: r.surviving_lead_id for r in digits.collect()} == {
        "P1": "P1", "P2": "P2",
    }  # fmt: skip
    assert {r.source_lead_id: r.surviving_lead_id for r in e164.collect()} == {
        "P1": "P1", "P2": "P1",
    }  # fmt: skip


@pytest.mark.slow
def test_phone_normalisation_extra_links(spark, real_leads):
    """The 48h email-or-phone rule with E.164 phones finds the same 758 duplicates.

    The named query counts E.164 links joining two leads that silver put in different
    clusters (none); the rerun of the real dedup with E.164 phones gives the same map.
    """
    from src.bronze.ingest import latest_batch
    from src.common.tables import read_table
    from src.gold.apply_pii_policy import phone_e164
    from src.silver.leads import LEADS_SOURCE, conform_leads

    config, _, _ = real_leads
    r = run_finding(spark, FINDINGS_LEADS, "phone_normalisation_extra_links")[0]
    assert r.extra_links == 0
    assert r.unparseable_phones == 0
    assert r.e164_links >= r.digit_links > 0

    silver_map = read_table(spark, config, "silver.lead_dedup_map")
    in_scope = conform_leads(latest_batch(read_table(spark, config, LEADS_SOURCE))).join(
        silver_map.select(F.col("source_lead_id").alias("lead_id")), "lead_id"
    )
    _, e164_map = dedupe_leads(in_scope, phone_normaliser=phone_e164)
    dupes = e164_map.where("source_lead_id != surviving_lead_id").count()
    assert dupes == 758
    pairs = lambda m: sorted(m.select("source_lead_id", "surviving_lead_id").collect())  # noqa: E731
    assert pairs(e164_map) == pairs(silver_map)
