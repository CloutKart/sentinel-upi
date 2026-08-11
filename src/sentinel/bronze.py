"""Step 2 — Landing to Bronze.

"Store the raw data in a structured Delta format. Append simple metadata (e.g.,
ingestion timestamps) while preserving the original messy data."

Structure without cleaning. Every payload field is parsed into its own column and
every one of them stays a string, so ``"842.50"``, ``842.50`` and ``-91.2`` all
survive exactly as sent. Nothing is cast, trimmed, filtered or deduplicated here —
that is Silver's job, and doing any of it in Bronze would destroy the evidence a
quarantine reason is supposed to point at.

Lines that are not JSON at all are kept too, with their fields null and the original
text still in ``raw_payload``. A layer that drops what it cannot parse cannot later
tell you how much it dropped.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from sentinel import landing, tables
from sentinel.config import Config
from sentinel.schemas import CORRUPT_RECORD_COLUMN, PAYLOAD_FIELDS, PAYLOAD_SCHEMA

if TYPE_CHECKING:  # pragma: no cover - typing only
    from pyspark.sql import SparkSession

TABLE = "upi_transactions"
STAGE = "bronze"


def run(spark: SparkSession, cfg: Config, run_id: str) -> dict[str, int]:
    """Parse landed lines into typed-as-string columns. Returns counts for the report."""
    tables.ensure_namespaces(spark, cfg)

    before = tables.count(spark, cfg, "bronze", TABLE)

    source = tables.read_stream(spark, cfg, "landing", landing.TABLE)

    parsed = source.withColumn(
        "payload",
        # PERMISSIVE (the default) records a line that is not JSON instead of failing
        # the micro-batch: the fields come back null and the original text lands in
        # _corrupt_record. The row is still written, and Silver quarantines it.
        F.from_json(
            F.col("raw_payload"),
            PAYLOAD_SCHEMA,
            {"columnNameOfCorruptRecord": CORRUPT_RECORD_COLUMN},
        ),
    )

    df = (
        parsed.select(
            *[F.col(f"payload.{name}").alias(name) for name in PAYLOAD_FIELDS],
            # Distinguishes "the line was never JSON" from "the line was valid JSON
            # but transaction_id was missing". Silver quarantines both, under
            # different reasons, and the run report reports them separately.
            F.col(f"payload.{CORRUPT_RECORD_COLUMN}").isNull().alias("is_parseable"),
            F.col("raw_payload"),
            F.col("source_file"),
            F.col("landed_at"),
            F.col("landing_run_id"),
        )
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("ingest_run_id", F.lit(run_id))
        # Partitioning by ingest date keeps a day's arrivals together, which is how
        # both reprocessing and retention are expressed later.
        .withColumn("ingest_date", F.to_date(F.col("ingested_at")))
    )

    tables.write_stream(df, cfg, "bronze", TABLE, stage=STAGE, partition_by=["ingest_date"])

    after = tables.count(spark, cfg, "bronze", TABLE)
    return {"ingested": after - before, "total": after}
