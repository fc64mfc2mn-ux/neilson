"""Step 11: PII policy and gold.dim_lead (plus the no-PII lead table).

Unit tests use small fixtures; the slow tests build dim_lead on the real data
and scan every gold column for raw identifiers taken from silver and bronze.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from datetime import date
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from src.common.findings import run_finding
from src.common.tables import read_table
from src.gold.apply_pii_policy import (
    NOPII_COLUMNS,
    UNKNOWN_AGE_BAND,
    age_band,
    apply_pii_policy,
    build_dim_lead,
    no_pii_view,
    phone_e164,
    pii_hasher,
    pii_leaks,
    postcode_outward,
)

SALT = "unit-test-salt-1f2e3d"
REPO_ROOT = Path(__file__).resolve().parents[1]
CLASSIFICATION = REPO_ROOT / "docs" / "DATA_CLASSIFICATION.md"
FINDINGS_LEADS = REPO_ROOT / "src" / "sql" / "findings_leads.sql"

LEAD_COLS = (
    "lead_id string, campaign_id string, created_at_utc timestamp, region_code string, "
    "channel_code string, source_detail string, product_interest string, first_name string, "
    "last_name string, email string, phone string, postcode string, date_of_birth date, "
    "smoker_flag boolean, health_condition_flag boolean, estimated_cover_amount decimal(12,2), "
    "consent_marketing string, consent_timestamp_utc timestamp, consent_source string, "
    "lead_status string, is_orphan_campaign boolean, dedup_cluster_size int"
)


def _leads(spark, rows):
    """rows: (lead_id, region, email, phone, postcode, dob, consent_marketing)."""
    data = [
        (lid, "CMP-1", None, region, "ORGANIC", "organic/search", "TERM_LIFE", "Ann", "Lee",
         email, phone, postcode, dob, True, False, None, consent, None, "web", "NEW", False, 1)
        for lid, region, email, phone, postcode, dob, consent in rows
    ]  # fmt: skip
    return spark.createDataFrame(data, LEAD_COLS).withColumn(
        "created_at_utc", F.to_timestamp(F.lit("2026-05-10 12:00:00"))
    )


def _consent(spark, rows):
    return spark.createDataFrame(rows, "lead_id string, c boolean, p boolean").select(
        "lead_id",
        F.to_timestamp(F.lit("2026-05-20 09:00:00")).alias("current_event_ts_utc"),
        F.col("c").alias("consent_marketing_current"),
        F.col("p").alias("consent_phone_current_flag"),
    )


def _sha(value: str, salt: str = SALT) -> str:
    return hashlib.sha256((salt + value).encode()).hexdigest()


# ------------------------------------------------------------------ hashing


def test_hash_is_salted_sha256_deterministic_and_salt_sensitive(spark):
    df = spark.createDataFrame([("alice@mail.example",), (None,)], "v string")
    h1 = [r.h for r in df.select(pii_hasher(SALT)("v").alias("h")).collect()]
    h2 = [r.h for r in df.select(pii_hasher(SALT)("v").alias("h")).collect()]
    other = [r.h for r in df.select(pii_hasher("another-salt")("v").alias("h")).collect()]
    assert h1 == h2  # same salt, same hash, every run
    assert h1[0] == _sha("alice@mail.example")  # reproducible outside Spark
    assert re.fullmatch(r"[0-9a-f]{64}", h1[0])
    assert other[0] != h1[0]  # a different salt gives a different hash
    assert h1[1] is None and other[1] is None  # no hash of a missing value


@pytest.mark.parametrize("salt", ["", "   ", None, "REPLACE_WITH_A_LOCAL_ONLY_SALT_VALUE"])
def test_hash_refuses_an_empty_or_placeholder_salt(salt):
    with pytest.raises(ValueError, match="salt"):
        pii_hasher(salt)


@pytest.mark.parametrize(
    "raw, expected",
    [
        # UK: international, national with brackets, national digits, international digits
        ("+44 7815 778995", "+447815778995"),
        ("(07815) 778995", "+447815778995"),
        ("07815778995", "+447815778995"),
        ("447815778995", "+447815778995"),
        # AU
        ("+61 424 402625", "+61424402625"),
        ("(0424) 402625", "+61424402625"),
        ("0424402625", "+61424402625"),
        ("61424402625", "+61424402625"),
        # CA and US (NANP)
        ("+1 891 365 9685", "+18913659685"),
        ("(891) 365-9685", "+18913659685"),
        ("891-365-9685", "+18913659685"),
        ("18913659685", "+18913659685"),
        # Not a phone number in any of the four formats
        ("12345", None),
        ("", None),
        (None, None),
    ],
)
def test_phone_is_normalised_to_e164(spark, raw, expected):
    df = spark.createDataFrame([(raw,)], "phone string")
    assert df.select(phone_e164("phone").alias("p")).first().p == expected


def test_same_person_written_two_ways_hashes_the_same(spark):
    leads = _leads(
        spark,
        [
            ("L1", "UK", "Alice.Smith@Mail.Example ", "+44 7815 778995", "SL4 8IB", None, "Y"),
            ("L2", "UK", "alice.smith@mail.example", "(07815) 778995", "SL4 8IB", None, "Y"),
            ("L3", "UK", "bob@mail.example", "07999000111", "SL4 8IB", None, "Y"),
        ],
    )
    rows = {r.lead_id: r for r in apply_pii_policy(leads, _consent(spark, []), SALT).collect()}
    assert rows["L1"].email_hash == rows["L2"].email_hash == _sha("alice.smith@mail.example")
    assert rows["L1"].phone_hash == rows["L2"].phone_hash == _sha("+447815778995")
    assert rows["L3"].email_hash != rows["L1"].email_hash
    assert rows["L3"].phone_hash != rows["L1"].phone_hash


def test_salt_never_appears_in_the_query_plan_or_the_logs(spark, caplog):
    leads = _leads(spark, [("L1", "UK", "a@mail.example", "07815778995", "SL4 8IB", None, "Y")])
    with caplog.at_level(logging.DEBUG):
        dim = apply_pii_policy(leads, _consent(spark, []), SALT)
        dim.collect()
    plan = dim._jdf.queryExecution().toString()
    assert SALT not in plan
    assert all(SALT not in r.getMessage() for r in caplog.records)


# ------------------------------------------------------------------ generalisation


def test_age_band_is_ten_years_at_lead_creation_with_an_unknown_band(spark):
    created = date(2026, 5, 10)
    dobs = {
        "18": date(2008, 5, 10),  # 18 on the day
        "19": date(2006, 5, 11),  # 19, a day short of 20
        "29": date(1996, 5, 11),
        "30": date(1996, 5, 10),
        "89": date(1936, 5, 11),
        "100": date(1926, 5, 10),
        "null": None,
    }
    df = spark.createDataFrame(
        [(k, created, v) for k, v in dobs.items()], "k string, created date, dob date"
    )
    got = {
        r.k: (r.band, r.sort)
        for r in df.select("k", *[c.alias(n) for n, c in zip(
            ("band", "sort"), age_band("dob", "created"), strict=True)]).collect()
    }  # fmt: skip
    assert got == {
        "18": ("18-19", 1),  # silver nulls any DOB under 18, so the first decade starts at 18
        "19": ("18-19", 1),
        "29": ("20-29", 2),
        "30": ("30-39", 3),
        "89": ("80-89", 8),
        "100": ("90+", 9),  # 90 to 100: silver nulls anything over 100
        "null": (UNKNOWN_AGE_BAND, 99),  # nulled or missing DOB (assumption 18)
    }


@pytest.mark.parametrize(
    "region, raw, outward",
    [
        ("UK", "SL4 8IB", "SL4"),  # AA9 9AA
        ("UK", "m1 1ae", "M1"),  # A9 9AA, case and spacing normalised first
        ("UK", "SW1A 1AA", "SW1A"),  # AA9A 9AA
        ("UK", "EC12 3AB", "EC12"),  # AA99 9AA
        ("CA", "C5H 4R3", "C5H"),  # forward sortation area
        ("US", "74215", "742"),  # ZIP3
        ("AU", "5063", "506"),  # first three digits (no outward part exists)
        ("UK", "5063", None),  # wrong format for the region: not served
        ("UK", "C5H 4R3", None),
        ("US", "7421", None),  # four-digit ZIP (lost leading zero?): not guessed
        ("AU", None, None),
    ],
)
def test_postcode_outward_rule_per_region(spark, region, raw, outward):
    df = spark.createDataFrame([(region, raw)], "region_code string, postcode string")
    value, valid = postcode_outward("postcode", "region_code")
    r = df.select(value.alias("o"), valid.alias("v")).first()
    assert r.o == outward
    assert r.v is (raw is None or outward is not None)  # null postcode is not a format error


# ------------------------------------------------------------------ dim_lead


def test_dim_lead_drops_names_and_raw_identifiers_and_keeps_health_flags(spark):
    leads = _leads(spark, [("L1", "UK", "a@mail.example", "07815778995", "SL4 8IB",
                            date(1980, 1, 1), "Y")])  # fmt: skip
    dim = apply_pii_policy(leads, _consent(spark, []), SALT)
    cols = set(dim.columns)
    raw = {"first_name", "last_name", "email", "phone", "postcode", "date_of_birth",
           "email_norm", "phone_norm", "consent_marketing", "consent_timestamp_utc",
           "consent_source", "call_notes", "recording_url"}  # fmt: skip
    assert not raw & cols
    assert {"lead_key", "lead_id", "email_hash", "phone_hash", "date_of_birth_hash", "age_band",
            "postcode_outward", "smoker_flag", "health_condition_flag"} <= cols  # fmt: skip
    r = dim.first()
    assert r.date_of_birth_hash == _sha("1980-01-01")
    assert (r.age_band, r.postcode_outward) == ("40-49", "SL4")
    assert 0 < r.lead_key < 2**53


def test_no_pii_table_has_no_identifiers_hashes_or_health_flags(spark):
    leads = _leads(spark, [("L1", "UK", "a@mail.example", "07815778995", "SL4 8IB",
                            date(1980, 1, 1), "Y")])  # fmt: skip
    view = no_pii_view(apply_pii_policy(leads, _consent(spark, []), SALT))
    assert tuple(view.columns) == NOPII_COLUMNS
    banned = {"lead_id", "email_hash", "phone_hash", "date_of_birth_hash", "smoker_flag",
              "health_condition_flag", "estimated_cover_amount", "consent_marketing_current",
              "consent_phone_current_flag", "created_at_utc"}  # fmt: skip
    assert not banned & set(view.columns)
    assert view.first().age_band == "40-49"


def test_marketing_consent_comes_from_consent_current_not_the_lead_flag(spark):
    leads = _leads(
        spark,
        [
            ("WITHDRAWN", "UK", None, "07815778995", None, None, "Y"),  # defect 25 shape
            ("REGRANTED", "UK", None, "07815778996", None, None, "N"),
            ("NO_EVENTS", "UK", None, "07815778997", None, None, "Y"),
        ],
    )
    consent = _consent(spark, [("WITHDRAWN", False, False), ("REGRANTED", True, True)])
    rows = {r.lead_id: r for r in apply_pii_policy(leads, consent, SALT).collect()}
    assert (rows["WITHDRAWN"].consent_marketing_current,
            rows["WITHDRAWN"].consent_marketing_source) == (False, "consent_events")  # fmt: skip
    assert rows["WITHDRAWN"].consent_phone_current_flag is False
    assert rows["REGRANTED"].consent_marketing_current is True
    # No events: the capture-time flag is the only evidence, and is labelled as such.
    assert (rows["NO_EVENTS"].consent_marketing_current,
            rows["NO_EVENTS"].consent_marketing_source) == (True, "lead_capture")  # fmt: skip
    assert rows["NO_EVENTS"].consent_phone_current_flag is None


# ------------------------------------------------------------------ leak scan


def test_leak_scan_finds_planted_raw_values_and_patterns(spark):
    raw = spark.createDataFrame(
        [("Ann.Lee@mail.example", "email"), ("SL4 8IB", "postcode"), ("Mentions asthma", "note")],
        "value string, kind string",
    )
    clean = spark.createDataFrame([(1, "SL4", "Core", 2026)], "k long, a string, b string, y int")
    dirty = spark.createDataFrame(
        [
            (1, " ann.lee@mail.example", "x"),  # raw email, case and space changed
            (2, "sl4 8ib", "x"),  # raw postcode
            (3, "someone@else.example", "x"),  # email pattern, not in the raw set
            (4, "+44 7700 900123", "x"),  # phone pattern
            (5, "x", "https://aurora.example/rec/00000001.wav"),  # recording URL pattern
        ],
        "k long, a string, b string",
    )
    assert pii_leaks({"gold.clean": clean}, raw).count() == 0
    hits = {(r.column, r.reason) for r in pii_leaks({"gold.dirty": dirty}, raw).collect()}
    assert hits == {
        ("a", "raw email"),
        ("a", "raw postcode"),
        ("a", "email pattern"),
        ("a", "phone pattern"),
        ("b", "recording URL pattern"),
    }


def test_leak_scan_ignores_postcode_lookalike_integers(spark):
    # AU postcode 2026 is also a year: integers are not postcodes.
    raw = spark.createDataFrame([("2026", "postcode")], "value string, kind string")
    gold = spark.createDataFrame([(2026,)], "year int")
    assert pii_leaks({"gold.dim_date": gold}, raw).count() == 0


def test_leak_scan_exempts_calendar_labels_from_name_checks_only(spark):
    # "May" is a surname in silver.lead and a month in dim_date: not a leak. A raw email
    # in the same column still is.
    raw = spark.createDataFrame(
        [("May", "last_name"), ("may@mail.example", "email")], "value string, kind string"
    )
    month = spark.createDataFrame([("May",)], "month_name string")
    assert pii_leaks({"gold.dim_date": month}, raw).count() == 0
    hits = pii_leaks({"gold.dim_lead": month}, raw).collect()  # elsewhere, a name is a leak
    assert [(r.column, r.reason) for r in hits] == [("month_name", "raw last_name")]
    planted = spark.createDataFrame([("may@mail.example",)], "month_name string")
    reasons = {r.reason for r in pii_leaks({"gold.dim_date": planted}, raw).collect()}
    assert reasons == {"raw email", "email pattern"}


# ------------------------------------------------------------------ real data


@pytest.fixture(scope="module")
def real_dim_lead(spark, real_leads, tmp_path_factory):
    """All silver, the step 10 dimensions and dim_lead, with a test-only salt."""
    from src.gold.build_dimensions import build_dimensions
    from src.silver.calls import build_calls
    from src.silver.policy import build_policy
    from src.silver.spend import build_spend

    secrets = tmp_path_factory.mktemp("secrets") / "secrets.json"
    secrets.write_text(json.dumps({"pii_hash_salt": "real-data-test-salt-9c8b"}))
    config, _, _ = real_leads
    config = {**config, "run_id": "test-dim-lead", "secrets_file": str(secrets)}
    build_calls(spark, config)
    build_policy(spark, config)
    build_spend(spark, config)
    build_dimensions(spark, config)
    return config, build_dim_lead(spark, config)


@pytest.mark.slow
def test_dim_lead_reconciles_to_silver_lead(spark, real_dim_lead):
    config, result = real_dim_lead
    dim = read_table(spark, config, "gold.dim_lead")
    nopii = read_table(spark, config, "gold.dim_lead_nopii")
    silver = read_table(spark, config, "silver.lead")
    assert (result.rows_in, result.rows_out) == (47_201, 47_201)
    assert dim.count() == nopii.count() == silver.count() == 47_201
    assert dim.join(silver, "lead_id", "left_anti").count() == 0
    assert silver.join(dim, "lead_id", "left_anti").count() == 0
    k = dim.agg(F.countDistinct("lead_key").alias("n"), F.max("lead_key").alias("hi")).first()
    assert k.n == 47_201 and k.hi < 2**53  # no surrogate key collision
    assert nopii.join(dim, "lead_key", "left_anti").count() == 0


@pytest.mark.slow
def test_dim_lead_generalisation_and_consent_counts(spark, real_dim_lead):
    config, result = real_dim_lead
    dim = read_table(spark, config, "gold.dim_lead")
    silver = read_table(spark, config, "silver.lead")
    # Every null DOB (60 nulled at step 7, plus any missing at source) is UNKNOWN.
    null_dob = silver.where("date_of_birth IS NULL").count()
    assert dim.where(F.col("age_band") == UNKNOWN_AGE_BAND).count() == null_dob
    assert dim.where("date_of_birth_hash IS NULL").count() == null_dob
    # Postcodes not in their region's format (defect 29): outward code withheld, flagged.
    assert result.postcode_mismatches == 24  # UK: 11 four-digit, 10 Canadian; US: 3 four-digit
    finding = run_finding(spark, FINDINGS_LEADS, "postcode_region_format")
    assert {(r.region_code, r.shape): r.leads for r in finding} == {
        ("UK", "9999"): 11, ("UK", "A9A 9A9"): 10, ("US", "9999"): 3,
    }  # fmt: skip
    assert dim.where("NOT postcode_format_valid").count() == 24
    assert dim.where("postcode_outward IS NULL AND postcode_format_valid").count() == (
        silver.where("postcode IS NULL").count()
    )
    # Phones that fit none of the four national formats: no hash, flagged (defect 29).
    assert result.phone_unparseable == 0
    # Consent (defect 25): 2,899 surviving leads have events; the 284 leads whose record says
    # Y but whose latest event withdrew consent read N in gold.
    by_source = {r.consent_marketing_source: r["count"]
                 for r in dim.groupBy("consent_marketing_source").count().collect()}  # fmt: skip
    assert by_source == {"consent_events": 2_899, "lead_capture": 47_201 - 2_899}
    stale = dim.join(silver.select("lead_id", "consent_marketing"), "lead_id").where(
        "consent_marketing = 'Y' AND NOT consent_marketing_current"
    )
    assert stale.count() == 284


@pytest.mark.slow
def test_hashes_and_keys_are_identical_on_a_rebuild(spark, real_dim_lead):
    config, _ = real_dim_lead
    cols = ["lead_id", "lead_key", "email_hash", "phone_hash", "date_of_birth_hash"]
    before = sorted(read_table(spark, config, "gold.dim_lead").select(*cols).collect())
    build_dim_lead(spark, {**config, "run_id": "test-dim-lead-rerun"})
    after = sorted(read_table(spark, config, "gold.dim_lead").select(*cols).collect())
    assert before == after


@pytest.mark.slow
def test_no_gold_column_holds_a_raw_identifier_note_or_recording(spark, real_dim_lead):
    """The key test: every column of every gold table against real silver and bronze values."""
    from src.gold.apply_pii_policy import GOLD_TABLES, raw_pii_values

    config, _ = real_dim_lead
    raw = raw_pii_values(spark, config)
    kinds = {r.kind: r["count"] for r in raw.groupBy("kind").count().collect()}
    # The reference set is real and large, not a token sample.
    for kind in ("first_name", "last_name", "email", "phone", "phone_e164", "postcode",
                 "date_of_birth", "call_notes", "recording_url", "agent_name", "agent_email",
                 "manager_name"):  # fmt: skip
        assert kinds.get(kind, 0) > 0, kind
    assert kinds["email"] > 40_000 and kinds["recording_url"] > 40_000
    tables = {t: read_table(spark, config, t) for t in GOLD_TABLES}
    assert {"gold.dim_lead", "gold.dim_lead_nopii", "gold.dim_agent"} <= set(tables)
    leaks = pii_leaks(tables, raw)
    assert leaks.count() == 0, leaks.limit(20).collect()


@pytest.mark.slow
def test_classification_covers_every_gold_column(spark, real_dim_lead):
    """docs/DATA_CLASSIFICATION.md names every gold column once, with a tier from the brief."""
    from src.gold.apply_pii_policy import GOLD_TABLES

    config, _ = real_dim_lead
    row = re.compile(r"^\|\s*`(gold\.\w+)`\s*\|\s*`(\w+)`\s*\|\s*([0-3])\s*\|\s*(.+?)\s*\|$")
    doc = {}
    for line in CLASSIFICATION.read_text().splitlines():
        m = row.match(line)
        if m:
            assert (m[1], m[2]) not in doc, f"{m[1]}.{m[2]} listed twice"
            doc[(m[1], m[2])] = int(m[3])
    actual = {(t, c) for t in GOLD_TABLES for c in read_table(spark, config, t).columns}
    assert set(doc) == actual
    # The no-PII table carries tier 0 and 1 only.
    assert all(doc[("gold.dim_lead_nopii", c)] <= 1 for c in NOPII_COLUMNS)
    # The brief's tier 3 columns that reach gold are the two health flags, in dim_lead only.
    assert {k for k, tier in doc.items() if tier == 3} == {
        ("gold.dim_lead", "smoker_flag"), ("gold.dim_lead", "health_condition_flag"),
    }  # fmt: skip
