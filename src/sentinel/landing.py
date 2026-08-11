"""Step 1 — Raw to Landing.

"Read the generated raw telemetry data without applying any transformations. Land the
raw data safely into a Landing zone as the immutable source of truth."

Taken literally: the reader is ``text``, so each JSON line arrives as one opaque
string. Parsing a line into columns *is* a transformation, and doing it here would
mean a malformed line could fail or be silently dropped before it ever reached
durable storage — which is precisely what an immutable source of truth is for. Landing
therefore stores the bytes plus provenance, and nothing else.

The read is a stream with ``availableNow``: files already dropped are processed and
the query stops. Re-running picks up only what is new, because the checkpoint
remembers which files were consumed.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pyspark.sql import functions as F

from sentinel import tables
from sentinel.config import Config

if TYPE_CHECKING:  # pragma: no cover - typing only
    from pyspark.sql import DataFrame, SparkSession

TABLE = "upi_raw"
STAGE = "landing"


def _reader(spark: SparkSession, cfg: Config) -> DataFrame:
    """Open the raw drop zone.

    Locally this is a plain file stream over a directory. On Databricks the same call
    becomes Auto Loader against a Unity Catalog Volume — one config key apart, because
    incremental file discovery is a platform capability, not pipeline logic.
    """
    landing_cfg = cfg.landing
    fmt = landing_cfg.get("reader_format", "text")
    reader = spark.readStream.format(fmt)

    if fmt == "cloudFiles":
        options: dict[str, Any] = landing_cfg.get("cloud_files", {})
        for key, value in options.items():
            reader = reader.option(key, str(value))

    return reader.load(cfg.path("raw"))


def run(spark: SparkSession, cfg: Config, run_id: str) -> dict[str, int]:
    """Land every raw file not yet consumed. Returns row counts for the run report."""
    tables.ensure_namespaces(spark, cfg)

    before = tables.count(spark, cfg, "landing", TABLE)

    df = (
        _reader(spark, cfg)
        .withColumnRenamed("value", "raw_payload")
        # _metadata is populated by the file source itself; it is the only reliable
        # way to know which file a row came from once several are batched together.
        .withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("landed_at", F.current_timestamp())
        .withColumn("landing_run_id", F.lit(run_id))
        .select("raw_payload", "source_file", "landed_at", "landing_run_id")
    )

    tables.write_stream(df, cfg, "landing", TABLE, stage=STAGE)

    after = tables.count(spark, cfg, "landing", TABLE)
    return {"landed": after - before, "total": after}
