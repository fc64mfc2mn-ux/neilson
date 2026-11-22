"""Apply the PII handling policy and build gold.dim_lead (step 11).

`gold.dim_lead`, one row per surviving lead (`silver.lead`):

- `email`, `phone` and `date_of_birth` become salted SHA-256 hashes
  (`sha256(salt + value)`, hex). The salt is `pii_hash_salt` from
  `config/secrets.json`; it is applied inside a Python UDF so it never appears
  in a Spark plan, and it is never logged. Values are normalised first so one
  person written two ways hashes the same: email lowercased and trimmed (the
  silver dedup rule), phone in E.164 (`phone_e164`), DOB as `yyyy-MM-dd`.
- `first_name` and `last_name` are dropped.
- `date_of_birth` becomes `age_band`: whole-year age at lead creation (the
  silver rule), in decades; `18-19` first (silver nulls anything under 18),
  `90+` last, `UNKNOWN` for a null DOB (assumption 18).
- `postcode` becomes `postcode_outward`, by region (`POSTCODE_RULES`). A
  postcode not in its region's format is withheld and flagged (defect 29).
- Marketing consent comes from `silver.consent_current` (defect 25). A lead
  with no consent event keeps its capture-time flag, labelled `lead_capture`.
- `smoker_flag` and `health_condition_flag` (tier 3) stay in `dim_lead` for
  roles cleared for them; `gold.dim_lead_nopii` (`NOPII_COLUMNS`) is what the
  `Marketing_Analyst_NoPII` role reads: no identifiers, hashes, consent,
  financial or health columns.

`call_notes` and `recording_url` never reach gold. `pii_leaks` scans every
column of every gold table against real raw values from silver and bronze and
against email, phone and recording-URL patterns; `scan_gold_for_pii`, the
last pipeline stage, stops the run on any hit. Column tiers are in docs/DATA_CLASSIFICATION.md.
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Callable
from dataclasses import dataclass
from functools import reduce

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from src.common.config import load_secrets
from src.common.dq import ReconciliationError, write_dq_exception
from src.common.tables import overwrite_table, read_table, table_exists
from src.gold.build_dimensions import surrogate_key
from src.silver.dedupe_leads import normalise_email

log = logging.getLogger("northgate.gold")

UNKNOWN_AGE_BAND = "UNKNOWN"
UNKNOWN_AGE_SORT = 99
MIN_AGE, MAX_AGE = 18, 100  # as src/silver/leads.py: outside this, the DOB is already null

# Region -> (format regex after upper-casing and single spacing, outward part length or
# None for "up to the space"). UK: outward code. CA: forward sortation area. US: ZIP3.
# AU: postcodes have no outward part, and the whole code would be the raw value, so the
# first three digits (a group of neighbouring postcodes). See assumption 25.
POSTCODE_RULES: dict[str, tuple[str, int | None]] = {
    "UK": (r"^[A-Z]{1,2}[0-9][A-Z0-9]? [0-9][A-Z]{2}$", None),
    "CA": (r"^[A-Z][0-9][A-Z] [0-9][A-Z][0-9]$", 3),
    "US": (r"^[0-9]{5}$", 3),
    "AU": (r"^[0-9]{4}$", 3),
}

NOPII_COLUMNS = (
    "lead_key",
    "created_date",
    "region_code",
    "campaign_id",
    "channel_code",
    "source_detail",
    "product_interest",
    "lead_status",
    "age_band",
    "age_band_sort",
    "postcode_outward",
    "is_orphan_campaign",
    "dedup_cluster_size",
)

# Every gold table the leak scan covers. Step 12 adds the facts and bridges.
GOLD_TABLES = (
    "gold.dim_date",
    "gold.dim_region",
    "gold.dim_channel",
    "gold.dim_campaign",
    "gold.dim_product",
    "gold.dim_agent",
    "gold.dim_lead",
    "gold.dim_lead_nopii",
    "gold.fact_lead",
    "gold.fact_call",
    "gold.fact_policy",
    "gold.fact_marketing_spend_daily",
    "gold.fact_lead_funnel",
    "gold.bridge_lead_call",
    "gold.bridge_user_region",
)
# Staff sign-in names that Power BI RLS matches with USERPRINCIPALNAME(): emails by design
# (tier 2, workforce identity). Exempt from the email pattern only; still compared with
# every raw customer and agent value.
EMAIL_PATTERN_EXEMPT = frozenset({("gold.bridge_user_region", "user_principal_name")})

EMAIL_PATTERN = r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"
PHONE_PATTERN = r"^\+?\(?[0-9][0-9 ().-]{8,}[0-9]$"
URL_PATTERN = r"(?i)(https?://|www\.|/rec/|\.(wav|mp3|m4a)\b)"
PHONE_KINDS = ("phone", "phone_e164")
NAME_KINDS = ("first_name", "last_name", "agent_name", "manager_name")
# Calendar labels generated in code, not read from any input: "May" and "June" are also
# real surnames and first names in silver.lead, so names are not compared here. Every
# other check (email, phone, postcode, notes, URLs, patterns) still applies.
NAME_CHECK_EXEMPT = frozenset(
    {("gold.dim_date", "month_name"), ("gold.dim_date", "month_short"),
     ("gold.dim_date", "day_name")}
)  # fmt: skip


def _col(c: str | Column) -> Column:
    return F.col(c) if isinstance(c, str) else c


# ------------------------------------------------------------------ hashing


def pii_hasher(salt: str | None) -> Callable[[str | Column], Column]:
    """Return `h(col)`: hex SHA-256 of salt + value, null for null.

    The salt lives only in the UDF's closure, so query plans and logs show the
    function name and its input column, never the salt.
    """
    if salt is None or not str(salt).strip() or str(salt).startswith("REPLACE_"):
        raise ValueError("PII hash salt is missing, empty or still a placeholder")
    salt_bytes = str(salt).encode()

    def pii_sha256(value: str | None) -> str | None:
        if value is None:
            return None
        return hashlib.sha256(salt_bytes + value.encode()).hexdigest()

    udf = F.udf(pii_sha256, StringType())
    return lambda c: udf(_col(c))


def phone_e164(c: str | Column) -> Column:
    """E.164 from the four national formats in the pack, null if none fits.

    Decided by the digits, not the lead's region: an explicit `+` keeps its
    country code; 12 digits from 44 and 11 from 61 or 1 already have one; a
    national UK number is 11 digits from 0, AU 10 digits from 0, NANP 10
    digits from 2 to 9. The prefixes do not overlap.
    """
    raw = F.trim(_col(c))
    d = F.regexp_replace(raw, r"\D", "")
    n = F.length(d)
    plus = F.concat(F.lit("+"), d)
    return (
        F.when(raw.startswith("+"), F.when(n.between(11, 13), plus))
        .when((n == 12) & d.startswith("44"), plus)
        .when((n == 11) & d.startswith("0"), F.concat(F.lit("+44"), F.substring(d, 2, 10)))
        .when((n == 11) & d.startswith("61"), plus)
        .when((n == 10) & d.startswith("0"), F.concat(F.lit("+61"), F.substring(d, 2, 9)))
        .when((n == 11) & d.startswith("1"), plus)
        .when((n == 10) & d.rlike("^[2-9]"), F.concat(F.lit("+1"), d))
    )


# ------------------------------------------------------------------ generalisation


def age_band(dob: str | Column, at: str | Column) -> tuple[Column, Column]:
    """(label, sort) for the whole-year age at `at`: 18-19, 20-29 ... 80-89, 90+, UNKNOWN."""
    age = F.floor(F.months_between(_col(at), _col(dob)) / 12)
    decade = F.least(F.floor(age / 10), F.lit(9)).cast("int")
    unknown = _col(dob).isNull() | (age < MIN_AGE) | (age > MAX_AGE)
    label = (
        F.when(unknown, F.lit(UNKNOWN_AGE_BAND))
        .when(age < 20, F.lit("18-19"))
        .when(age >= 90, F.lit("90+"))
        .otherwise(F.format_string("%d-%d", decade * 10, decade * 10 + 9))
    )
    return label, F.when(unknown, F.lit(UNKNOWN_AGE_SORT)).otherwise(decade)


def postcode_outward(postcode: str | Column, region: str | Column) -> tuple[Column, Column]:
    """(outward part, format valid) by region. Null postcodes are valid with a null part."""
    pc = F.upper(F.regexp_replace(F.trim(_col(postcode)), r"\s+", " "))
    value: Column | None = None
    valid: Column | None = None
    for code, (pattern, length) in POSTCODE_RULES.items():
        fits = (_col(region) == code) & pc.rlike(pattern)
        part = F.substring_index(pc, " ", 1) if length is None else F.substring(pc, 1, length)
        value = F.when(fits, part) if value is None else value.when(fits, part)
        valid = fits if valid is None else valid | fits
    return value, pc.isNull() | F.coalesce(valid, F.lit(False))


def postcode_shape(postcode: str | Column) -> Column:
    """Letters to A, digits to 9: the format without the value, safe to log."""
    up = F.upper(F.trim(_col(postcode)))
    return F.regexp_replace(F.regexp_replace(up, "[A-Z]", "A"), "[0-9]", "9")


# ------------------------------------------------------------------ dim_lead


def apply_pii_policy(leads: DataFrame, consent_current: DataFrame, salt: str) -> DataFrame:
    """gold.dim_lead from silver.lead and silver.consent_current."""
    h = pii_hasher(salt)
    band, band_sort = age_band("date_of_birth", F.to_date("created_at_utc"))
    outward, valid = postcode_outward("postcode", "region_code")
    consent = consent_current.select(
        "lead_id",
        F.lit(True).alias("_has_events"),
        F.col("consent_marketing_current").alias("_c_marketing"),
        F.col("consent_phone_current_flag").alias("_c_phone"),
        F.col("current_event_ts_utc").alias("_c_ts"),
    )
    has = F.coalesce(F.col("_has_events"), F.lit(False))
    return leads.join(consent, "lead_id", "left").select(
        surrogate_key("lead_id").alias("lead_key"),
        "lead_id",
        "created_at_utc",
        "region_code",
        "campaign_id",
        "channel_code",
        "source_detail",
        "product_interest",
        "lead_status",
        h(normalise_email("email")).alias("email_hash"),
        h(phone_e164("phone")).alias("phone_hash"),
        h(F.date_format("date_of_birth", "yyyy-MM-dd")).alias("date_of_birth_hash"),
        band.alias("age_band"),
        band_sort.alias("age_band_sort"),
        outward.alias("postcode_outward"),
        valid.alias("postcode_format_valid"),
        "estimated_cover_amount",
        "smoker_flag",
        "health_condition_flag",
        F.when(has, F.col("_c_marketing"))
        .otherwise(F.upper(F.trim("consent_marketing")) == "Y")
        .alias("consent_marketing_current"),
        F.when(has, F.lit("consent_events"))
        .otherwise(F.lit("lead_capture"))
        .alias("consent_marketing_source"),
        F.col("_c_ts").alias("consent_current_event_ts_utc"),
        F.col("_c_phone").alias("consent_phone_current_flag"),
        "is_orphan_campaign",
        "dedup_cluster_size",
    )


def no_pii_view(dim_lead: DataFrame) -> DataFrame:
    """gold.dim_lead_nopii: tier 0 and 1 columns only, for Marketing_Analyst_NoPII."""
    return dim_lead.withColumn("created_date", F.to_date("created_at_utc")).select(*NOPII_COLUMNS)


# ------------------------------------------------------------------ leak scan


def raw_pii_values(spark: SparkSession, config: dict) -> DataFrame:
    """(value, kind): every raw identifier, note and recording URL in silver and bronze."""
    sources = {
        "silver.lead": ("first_name", "last_name", "email", "phone", "postcode",
                        "date_of_birth", "email_norm", "phone_norm"),
        "bronze.leads_raw": ("first_name", "last_name", "email", "phone", "postcode",
                             "date_of_birth"),
        "silver.call": ("call_notes", "recording_url"),
        "bronze.calls_raw": ("call_notes", "recording_url"),
        "silver.agent": ("agent_name", "agent_email"),
        "bronze.agents_raw": ("agent_name", "agent_email", "manager_name"),
    }  # fmt: skip
    kind = {"email_norm": "email", "phone_norm": "phone"}
    parts = []
    for table, cols in sources.items():
        df = read_table(spark, config, table)
        for c in cols:
            parts.append(df.select(F.col(c).cast("string").alias("value"),
                                   F.lit(kind.get(c, c)).alias("kind")))  # fmt: skip
        if "phone" in cols:
            parts.append(df.select(phone_e164("phone").alias("value"),
                                   F.lit("phone_e164").alias("kind")))  # fmt: skip
    return (
        reduce(DataFrame.unionByName, parts)
        .where(F.col("value").isNotNull() & (F.trim("value") != ""))
        .distinct()
    )


def _in_pairs(rules: frozenset[tuple[str, str]]) -> Column:
    """True for cells whose (table, column) is in `rules`."""
    conds = [(F.col("table") == t) & (F.col("column") == c) for t, c in sorted(rules)]
    return reduce(lambda a, b: a | b, conds, F.lit(False))


def pii_leaks(tables: dict[str, DataFrame], raw: DataFrame) -> DataFrame:
    """(table, column, reason, cells) for every gold cell that looks like raw PII.

    String columns are compared (trimmed, lowercased) with every raw value and
    tested against email, phone and recording-URL patterns. Other columns are
    compared with raw phone numbers only: a number could hold a phone, but
    integers such as the year 2026 legitimately equal four-digit postcodes and
    dates can equal a future DOB. Names are not compared in the generated
    calendar labels (`NAME_CHECK_EXEMPT`). The result never contains the value.
    """
    norm = lambda c: F.lower(F.trim(c))  # noqa: E731
    # One long frame of distinct (table, column, value) cells for every table, then one
    # join with the raw values: one plan, not one branch per table.
    longs = []
    for table, df in tables.items():
        cells = [
            F.struct(
                F.lit(f.name).alias("column"),
                F.col(f.name).cast("string").alias("value"),
                F.lit(isinstance(f.dataType, StringType)).alias("is_string"),
            )
            for f in df.schema.fields
        ]
        longs.append(
            df.select(F.explode(F.array(*cells)).alias("c"))
            .select(F.lit(table).alias("table"), "c.*")
            .where("value IS NOT NULL")
            .groupBy("table", "column", "is_string", "value")
            .agg(F.count(F.lit(1)).alias("n"))
        )
    cells = reduce(DataFrame.unionByName, longs).withColumn("_v", norm(F.col("value")))
    known = raw.select(norm(F.col("value")).alias("_v"), "kind").distinct()

    equal = (
        cells.join(known, "_v")
        .where(F.col("is_string") | F.col("kind").isin(*PHONE_KINDS))
        .where(~(_in_pairs(NAME_CHECK_EXEMPT) & F.col("kind").isin(*NAME_KINDS)))
        .select("table", "column", F.concat(F.lit("raw "), "kind").alias("reason"), "n")
    )
    s = cells.where("is_string")
    digits = F.length(F.regexp_replace("value", r"\D", ""))
    patterns = [
        s.where(F.col("value").rlike(EMAIL_PATTERN) & ~_in_pairs(EMAIL_PATTERN_EXEMPT)).select(
            "table", "column", F.lit("email pattern").alias("reason"), "n"
        ),
        s.where(F.trim("value").rlike(PHONE_PATTERN) & (digits >= 10)).select(
            "table", "column", F.lit("phone pattern").alias("reason"), "n"
        ),
        s.where(F.col("value").rlike(URL_PATTERN)).select(
            "table", "column", F.lit("recording URL pattern").alias("reason"), "n"
        ),
    ]
    hits = reduce(DataFrame.unionByName, [equal, *patterns])
    return hits.groupBy("table", "column", "reason").agg(F.sum("n").alias("cells"))


# ------------------------------------------------------------------ stage


@dataclass(frozen=True)
class DimLeadResult:
    rows_in: int
    rows_out: int
    nopii_rows: int
    postcode_mismatches: int
    phone_unparseable: int


def build_dim_lead(spark: SparkSession, config: dict) -> DimLeadResult:
    """Write gold.dim_lead and gold.dim_lead_nopii, reconcile, flag, scan gold for PII."""
    leads = read_table(spark, config, "silver.lead")
    consent = read_table(spark, config, "silver.consent_current")
    salt = load_secrets(config)["pii_hash_salt"]
    overwrite_table(spark, apply_pii_policy(leads, consent, salt), config, "gold.dim_lead")
    dim = read_table(spark, config, "gold.dim_lead")
    rows_in, rows_out = leads.count(), dim.count()
    keys = dim.select("lead_key").distinct().count()
    if not rows_in == rows_out == keys:
        raise ReconciliationError(
            f"gold.dim_lead: {rows_out} rows, {keys} keys != {rows_in} from silver.lead"
        )
    overwrite_table(spark, no_pii_view(dim), config, "gold.dim_lead_nopii")
    nopii_rows = read_table(spark, config, "gold.dim_lead_nopii").count()
    if nopii_rows != rows_out:
        raise ReconciliationError(f"gold.dim_lead_nopii: {nopii_rows} rows != {rows_out}")

    # Defect 29: postcodes not in their region's format, phones in no known format.
    _, valid = postcode_outward("postcode", "region_code")
    bad_postcode = leads.where(~valid).select(
        "lead_id",
        F.format_string("region %s, postcode format %s", F.col("region_code"),
                        postcode_shape("postcode")).alias("detail"),
    )  # fmt: skip
    bad_phone = leads.where(F.col("phone").isNotNull() & phone_e164("phone").isNull()).select(
        "lead_id", F.lit("phone in none of the UK, AU or NANP formats").alias("detail")
    )
    flags = {}
    for check, df in (("lead_postcode_region_mismatch", bad_postcode),
                      ("lead_phone_unparseable", bad_phone)):  # fmt: skip
        flags[check] = write_dq_exception(df, check, "silver.lead", "lead_id", "warning",
                                          config=config, action="flagged",
                                          detail_col="detail")  # fmt: skip

    result = DimLeadResult(rows_in, rows_out, nopii_rows, flags["lead_postcode_region_mismatch"],
                           flags["lead_phone_unparseable"])  # fmt: skip
    log.info(
        "Reconciled gold.dim_lead: %d from silver.lead, %d in gold.dim_lead_nopii; "
        "%d postcodes and %d phones flagged",
        rows_in, nopii_rows, result.postcode_mismatches, result.phone_unparseable,
    )  # fmt: skip
    return result


def scan_gold_for_pii(spark: SparkSession, config: dict) -> int:
    """Last pipeline stage: every gold table must exist and hold no raw PII.

    Returns the number of tables scanned. The error names table, column and
    reason, never the value.
    """
    missing = [t for t in GOLD_TABLES if not table_exists(spark, config, t)]
    if missing:
        raise ReconciliationError(f"PII scan: gold tables not built: {missing}")
    tables = {t: read_table(spark, config, t) for t in GOLD_TABLES}
    leaks = pii_leaks(tables, raw_pii_values(spark, config)).collect()
    if leaks:
        raise ReconciliationError(
            "Raw PII in gold: "
            + "; ".join(f"{r.table}.{r.column} {r.reason} ({r.cells})" for r in leaks)
        )
    log.info("PII scan clean: %d gold tables, every column", len(tables))
    return len(tables)
