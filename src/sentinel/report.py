"""The run report — what the pipeline produced, and whether it worked.

Two different questions get answered here, and conflating them is how a pipeline comes
to look healthier than it is:

*What did we serve?* The Gold KPIs and the top alerts.

*What did we get wrong?* The quarantine breakdown, and detection measured against the
labels the generator wrote when it injected the anomalies. A fraud the pipeline never
detected and a fraud whose row was quarantined before scoring are different failures —
the first is a scoring problem, the second an ingestion problem — so they are counted
separately rather than being averaged into one comfortable number.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from sentinel import gold, silver, tables
from sentinel.config import Config
from sentinel.generate.telemetry import load_labels

if TYPE_CHECKING:  # pragma: no cover - typing only
    from pyspark.sql import DataFrame, SparkSession


def _rule(title: str) -> None:
    print(f"\n{title}\n{'─' * max(len(title), 60)}")


def _show(df: DataFrame, n: int = 20) -> None:
    df.show(n, truncate=False)


def detection_summary(spark: SparkSession, cfg: Config, fact: DataFrame) -> DataFrame | None:
    """Join the injected labels onto the scored fact and measure detection by type.

    Returns None when no truth file exists — the pipeline runs perfectly well against
    data it did not generate, it just cannot grade itself.
    """
    labels = load_labels(cfg)
    if not labels:
        return None

    label_df = spark.createDataFrame(
        [(txn_id, kind) for txn_id, kind in labels.items()],
        "transaction_id string, injected_type string",
    )

    joined = label_df.join(
        fact.select("transaction_id", "risk_band", "fraud_score"), "transaction_id", "left"
    )

    return (
        joined.groupBy("injected_type")
        .agg(
            F.count("*").alias("injected"),
            # A labelled transaction with no scored row never reached Gold — it was
            # quarantined in Silver, or deduplicated away as a retry.
            F.sum(F.when(F.col("risk_band").isNull(), 1).otherwise(0)).alias("not_scored"),
            F.sum(F.when(F.col("risk_band") == gold.BAND_HIGH, 1).otherwise(0)).alias("alerted"),
            F.sum(
                F.when(F.col("risk_band").isin(gold.BAND_HIGH, gold.BAND_MEDIUM), 1).otherwise(0)
            ).alias("flagged"),
        )
        .withColumn(
            "recall_flagged",
            F.round(
                F.col("flagged") / F.greatest(F.col("injected") - F.col("not_scored"), F.lit(1)), 3
            ),
        )
        .orderBy("injected_type")
    )


def alert_precision(spark: SparkSession, cfg: Config, fact: DataFrame) -> tuple[int, int] | None:
    """How many HIGH-band alerts were transactions the generator actually injected."""
    labels = load_labels(cfg)
    if not labels:
        return None

    label_df = spark.createDataFrame(
        [(txn_id,) for txn_id in labels], "transaction_id string"
    ).withColumn("is_injected", F.lit(True))

    alerts = fact.filter(F.col("risk_band") == gold.BAND_HIGH).select("transaction_id")
    total = alerts.count()
    true_positives = alerts.join(label_df, "transaction_id", "inner").count()
    return true_positives, total


def print_report(spark: SparkSession, cfg: Config) -> None:
    """Print everything a reviewer needs to judge the run, in one pass."""
    if not tables.table_exists(spark, cfg, "gold", gold.FACT_TABLE):
        print("No Gold tables yet. Run `make gen && make run-local` first.")
        return

    fact = tables.read(spark, cfg, "gold", gold.FACT_TABLE)

    _rule("Daily KPIs")
    _show(tables.read(spark, cfg, "gold", gold.KPI_DAILY_TABLE).orderBy("event_date"), 40)

    _rule("Volume by bank")
    _show(
        tables.read(spark, cfg, "gold", gold.KPI_DIMENSION_TABLE)
        .filter(F.col("dimension") == "payer_bank")
        .orderBy(F.desc("total_amount")),
        10,
    )

    _rule("Top fraud alerts")
    _show(
        tables.read(spark, cfg, "gold", gold.ALERTS_TABLE)
        .select(
            "transaction_id", "event_time", "amount", "payer_vpa_masked", "fraud_score", "reasons"
        )
        .orderBy(F.desc("fraud_score"), F.desc("amount")),
        15,
    )

    _rule("Risk bands")
    _show(fact.groupBy("risk_band").agg(F.count("*").alias("transactions")).orderBy("risk_band"))

    _rule("Quarantine — records rejected in Silver, with the reason")
    if tables.table_exists(spark, cfg, "quarantine", silver.QUARANTINE_TABLE):
        _show(
            tables.read(spark, cfg, "quarantine", silver.QUARANTINE_TABLE)
            .groupBy("reject_reason")
            .agg(F.count("*").alias("records"))
            .orderBy(F.desc("records"))
        )
    else:
        print("(nothing quarantined)")

    summary = detection_summary(spark, cfg, fact)
    if summary is None:
        print("\n(no truth labels found — detection cannot be measured)")
        return

    _rule("Detection vs. injected anomalies")
    print(
        "injected    = anomalies the generator planted\n"
        "not_scored  = quarantined or deduplicated before Gold, so never scored\n"
        "alerted     = scored HIGH   flagged = scored HIGH or MEDIUM\n"
        "recall_flagged = flagged / (injected - not_scored)\n"
    )
    _show(summary)

    precision = alert_precision(spark, cfg, fact)
    if precision:
        true_positives, total = precision
        rate = true_positives / total if total else 0.0
        print(
            f"Alert precision: {true_positives:,} of {total:,} HIGH alerts "
            f"were injected anomalies ({rate:.1%})"
        )
