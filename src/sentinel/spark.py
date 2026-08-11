"""SparkSession construction with Delta Lake wired in.

On Databricks the session already exists and already has Delta — attach to it and
apply only our own settings. Locally, build one from scratch and pull the Delta jars
through ``delta-spark``'s ``configure_spark_with_delta_pip`` helper.

The version pins matter: delta-spark 3.2.0 pairs with Spark 3.5.x, which is what
Databricks Runtime 15.4 LTS ships. Mixing them produces protocol errors that appear
on write, not at startup.
"""

from __future__ import annotations

import contextlib
import os
from pathlib import Path
from typing import TYPE_CHECKING

from sentinel.config import Config, load_config

if TYPE_CHECKING:  # pragma: no cover - typing only
    from pyspark.sql import SparkSession

# JDK 17 is the newest Spark 3.5 supports. Fedora ships 25/26 as system Java, which
# fails with an opaque reflective-access crash rather than a version message.
_JDK_CANDIDATES = (
    Path.home() / ".local" / "jdks" / "jdk-17",
    Path("/usr/lib/jvm/java-17-openjdk"),
    Path("/usr/lib/jvm/temurin-17-jdk"),
)

# Spark 3.5 on JDK 17 needs these opens for its Unsafe and Arrow usage.
_ADD_OPENS = (
    "--add-opens=java.base/java.nio=ALL-UNNAMED "
    "--add-opens=java.base/java.lang=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
)


def is_databricks() -> bool:
    """True when running inside a Databricks cluster."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def _ensure_java_home() -> None:
    """Point ``JAVA_HOME`` at a Spark-compatible JDK if the ambient one is too new."""
    current = os.environ.get("JAVA_HOME")
    if current and Path(current, "bin", "java").exists():
        return

    for candidate in _JDK_CANDIDATES:
        if (candidate / "bin" / "java").exists():
            os.environ["JAVA_HOME"] = str(candidate)
            return

    # Not fatal: the default JDK may well be fine. `python -m sentinel.doctor`
    # reports the failure clearly if it is not.


def get_spark(cfg: Config | None = None) -> SparkSession:
    """Return a configured SparkSession, reusing the active one where possible."""
    cfg = cfg or load_config()
    _ensure_java_home()

    from pyspark.sql import SparkSession

    if is_databricks():
        spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
        for key, value in cfg.spark_conf.items():
            # Some cluster-level settings are immutable at runtime. Skipping them is
            # correct — the cluster policy already set them.
            with contextlib.suppress(Exception):
                spark.conf.set(key, value)
        return spark

    from delta import configure_spark_with_delta_pip

    builder = SparkSession.builder.appName(cfg.app_name)
    if cfg.spark_master:
        builder = builder.master(cfg.spark_master)

    builder = (
        builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.driver.extraJavaOptions", _ADD_OPENS)
        .config("spark.executor.extraJavaOptions", _ADD_OPENS)
    )

    for key, value in cfg.spark_conf.items():
        builder = builder.config(key, value)

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def stop_spark() -> None:
    """Stop the active session, if any. Used by test teardown."""
    from pyspark.sql import SparkSession

    session = SparkSession.getActiveSession()
    if session is not None:
        session.stop()
