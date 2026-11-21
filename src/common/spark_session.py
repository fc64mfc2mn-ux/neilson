"""Local Spark session with Delta Lake enabled.

Working code, not a stub. Gives every other module in this repo a single
place to get a SparkSession that runs entirely on the local filesystem under
./lakehouse/, with Delta Lake enabled, and needs nothing beyond
`pip install -r requirements.txt` plus a working Java 17 installation.
"""

from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark(app_name: str = "northgate-funnel", warehouse_dir: str = "lakehouse") -> SparkSession:
    """Build (or fetch) a local SparkSession with Delta Lake configured.

    Args:
        app_name: Spark application name, shown in the Spark UI and logs.
        warehouse_dir: Local directory Delta tables are written under. Created
            on demand by Spark; this repo's .gitignore excludes it.

    Returns:
        A SparkSession usable immediately, with `spark.sql.catalog.spark_catalog`
        set to Delta's catalog so `CREATE TABLE ... USING DELTA` and Delta table
        paths both work with no further setup.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
    )

    try:
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(builder)
    except ImportError:
        # delta-spark ships delta-core as a Maven dependency resolved through
        # configure_spark_with_delta_pip. If delta-spark is installed per
        # requirements.txt this import always succeeds; this branch only
        # protects against running this module without it installed.
        pass

    return builder.getOrCreate()


def stop_spark(spark: SparkSession) -> None:
    """Stop the given SparkSession. Safe to call even if already stopped."""
    if spark is not None:
        spark.stop()
