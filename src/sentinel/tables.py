"""Reading and writing Delta tables, in either environment.

This is the only module that knows whether a table is a path or a Unity Catalog name.
Every pipeline stage calls these helpers with a zone and a table name, and stays
identical between a laptop and a cluster.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sentinel.config import Config

if TYPE_CHECKING:  # pragma: no cover - typing only
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.streaming import StreamingQuery


def ensure_namespaces(spark: SparkSession, cfg: Config) -> None:
    """Create the catalog and schemas a run needs. A no-op off Unity Catalog."""
    if not cfg.uses_catalog:
        return
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {cfg.catalog}")
    for schema in cfg.schemas.values():
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{schema}")


def table_exists(spark: SparkSession, cfg: Config, zone: str, name: str) -> bool:
    """True when the table has been created and can be read."""
    address = cfg.table(zone, name)
    if cfg.uses_catalog:
        return spark.catalog.tableExists(address)
    from delta.tables import DeltaTable

    return DeltaTable.isDeltaTable(spark, address)


def read(spark: SparkSession, cfg: Config, zone: str, name: str) -> DataFrame:
    """Batch-read a Delta table."""
    address = cfg.table(zone, name)
    if cfg.uses_catalog:
        return spark.read.table(address)
    return spark.read.format("delta").load(address)


def read_stream(spark: SparkSession, cfg: Config, zone: str, name: str) -> DataFrame:
    """Stream-read a Delta table, picking up only rows added since the last run."""
    address = cfg.table(zone, name)
    reader = spark.readStream.format("delta")
    if cfg.uses_catalog:
        return reader.table(address)
    return reader.load(address)


def write(
    df: DataFrame,
    cfg: Config,
    zone: str,
    name: str,
    *,
    mode: str = "overwrite",
    partition_by: list[str] | None = None,
) -> None:
    """Batch-write a Delta table.

    ``overwriteSchema`` is set on overwrite because Gold is recomputed from scratch on
    every run and its columns change whenever a fraud rule is added — without it the
    second run after an edit fails on a schema mismatch.
    """
    writer = df.write.format("delta").mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")

    if cfg.uses_catalog:
        writer.saveAsTable(cfg.table(zone, name))
    else:
        writer.save(cfg.table(zone, name))


def write_stream(
    df: DataFrame,
    cfg: Config,
    zone: str,
    name: str,
    *,
    stage: str,
    partition_by: list[str] | None = None,
    options: dict[str, Any] | None = None,
) -> StreamingQuery:
    """Append a stream to a Delta table and wait for it to drain.

    Started with ``availableNow``: the stream processes everything currently waiting
    and then stops. That keeps the pipeline genuinely incremental — checkpointed,
    exactly-once, only new files each run — while every stage still terminates, which
    a continuous trigger would not, and which both the CLI and the tests depend on.
    """
    writer = (
        df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", cfg.checkpoint(stage))
        .trigger(availableNow=True)
    )
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    for key, value in (options or {}).items():
        writer = writer.option(key, str(value))

    address = cfg.table(zone, name)
    query = writer.toTable(address) if cfg.uses_catalog else writer.start(address)
    query.awaitTermination()
    return query


def count(spark: SparkSession, cfg: Config, zone: str, name: str) -> int:
    """Row count of a table, or 0 if it does not exist yet."""
    if not table_exists(spark, cfg, zone, name):
        return 0
    return read(spark, cfg, zone, name).count()
