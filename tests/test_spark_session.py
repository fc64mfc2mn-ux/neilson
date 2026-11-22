"""Local Spark session settings (speed) and the UTC session timezone."""

from __future__ import annotations

from src.common.spark_session import DEFAULT_SETTINGS, spark_settings


def test_session_has_speed_settings_and_utc(spark):
    conf = spark.sparkContext.getConf()
    assert spark.conf.get("spark.sql.shuffle.partitions") == "8"
    assert conf.get("spark.default.parallelism") == "8"
    assert conf.get("spark.ui.enabled") == "false"
    assert spark.conf.get("spark.sql.adaptive.enabled") == "true"
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"


def test_settings_default_and_config_override():
    assert (
        spark_settings({})
        == DEFAULT_SETTINGS
        == {
            "shuffle_partitions": 8,
            "default_parallelism": 8,
        }
    )
    assert spark_settings({"spark": {"shuffle_partitions": 16}}) == {
        "shuffle_partitions": 16,
        "default_parallelism": 8,
    }
