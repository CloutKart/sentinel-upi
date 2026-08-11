"""Step 4 — Silver to Gold.

"Calculate Key Performance Indicators like total transaction volume. Apply simple
fraud scoring to flag suspicious transactions."

Batch, not streaming, and deliberately so. Everything here is either a full aggregation
or a windowed comparison of a transaction against its neighbours, and under Structured
Streaming both need either a complete-mode sink or a stateful ``foreachBatch`` merge.
Silver is Delta, so recomputing Gold from it is cheap, exact, and an order of magnitude
less machinery than a specification asking for "simple fraud scoring" deserves.

The scoring is additive. Each rule that fires contributes its weight and appends its
name to the row's ``reasons``, so an alert always explains itself — an unexplained
score is not actionable, and an analyst who cannot see why will not trust the next one
either. Every threshold lives in ``conf/base.yaml``, because tuning them is analysis,
not engineering.

No single rule is strong enough on its own to reach the HIGH band. That is the design:
a large payment is not fraud, a payment at 3am is not fraud, and a shared IP is a
coffee shop. A large payment at 3am from an IP serving forty strangers is worth a call.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import Column, Window
from pyspark.sql import functions as F

from sentinel import silver, tables
from sentinel.config import Config

if TYPE_CHECKING:  # pragma: no cover - typing only
    from pyspark.sql import DataFrame, SparkSession

FACT_TABLE = "fact_txn_scored"
ALERTS_TABLE = "fraud_alerts"
KPI_DAILY_TABLE = "kpi_daily"
KPI_DIMENSION_TABLE = "kpi_by_dimension"

# Rule names, used as the `reasons` entries and as the keys in conf/base.yaml.
RULE_HIGH_AMOUNT = "high_amount"
RULE_SHARED_IP = "shared_ip"
RULE_VELOCITY = "velocity"
RULE_FANOUT = "fanout"
RULE_ODD_HOUR = "odd_hour"

BAND_HIGH = "HIGH"
BAND_MEDIUM = "MEDIUM"
BAND_LOW = "LOW"


def _seconds(minutes: int) -> int:
    return int(minutes) * 60


def high_amount_threshold(df: DataFrame, cfg: Config) -> float:
    """The amount above which a transaction is unusual for this dataset.

    Derived from the data rather than hard-coded, so the rule survives a change of
    scale or a shift in what ordinary traffic looks like — but derived *robustly*.

    The obvious implementation, a high percentile, does not work: the anomalies are
    around 0.7% of traffic, so the 99.5th percentile sits inside the fraud population
    and the threshold climbs above most of the fraud it is supposed to catch. On this
    dataset that produced a threshold of ₹103,899 against injected amounts starting at
    ₹60,000. The Tukey fence is built from the quartiles, which live in the dense
    middle of the distribution where a fraction of a percent of outliers cannot move
    them — the same property that makes it robust also makes it uncontaminated by the
    thing being detected.

    Floored, because "large" also has a domain meaning independent of the data.
    """
    rule = cfg.gold["fraud_rules"][RULE_HIGH_AMOUNT]
    multiplier = float(rule["iqr_multiplier"])
    floor = float(rule["floor"])

    row = (
        df.filter(F.col("status") == "SUCCESS")
        .agg(F.percentile_approx("amount", [0.25, 0.75]).alias("quartiles"))
        .first()
    )
    if not row or not row["quartiles"]:
        return floor

    q1, q3 = (float(v) for v in row["quartiles"])
    fence = q3 + multiplier * (q3 - q1)
    return max(fence, floor)


def add_features(df: DataFrame, cfg: Config) -> DataFrame:
    """Attach the behavioural features the rules are expressed over.

    All three windowed features range over *time*, not row counts: ten transactions
    are only a burst if they happened close together, and ``rowsBetween`` would call
    ten transactions spread over a month a burst too.
    """
    rules = cfg.gold["fraud_rules"]

    with_epoch = df.withColumn("event_epoch", F.col("event_time").cast("long"))

    velocity_window = (
        Window.partitionBy("payer_key")
        .orderBy("event_epoch")
        .rangeBetween(-_seconds(rules[RULE_VELOCITY]["window_minutes"]), 0)
    )
    fanout_window = (
        Window.partitionBy("payer_key")
        .orderBy("event_epoch")
        .rangeBetween(-_seconds(rules[RULE_FANOUT]["window_minutes"]), 0)
    )
    # Distinct payers behind one IP, over the whole dataset rather than a window: a
    # money-mule IP does not stop being one because the traffic was spread out.
    ip_window = Window.partitionBy("ip_hash")

    return (
        with_epoch.withColumn("txns_in_window", F.count("*").over(velocity_window))
        .withColumn(
            "distinct_payees_in_window", F.size(F.collect_set("payee_key").over(fanout_window))
        )
        .withColumn("payers_on_ip", F.size(F.collect_set("payer_key").over(ip_window)))
        .withColumn("event_hour", F.hour("event_time"))
    )


def _rule_columns(cfg: Config, threshold: float) -> dict[str, Column]:
    """One boolean column per rule. Separated from scoring so both are readable."""
    rules = cfg.gold["fraud_rules"]
    odd = rules[RULE_ODD_HOUR]

    return {
        RULE_HIGH_AMOUNT: F.col("amount") > F.lit(threshold),
        RULE_SHARED_IP: F.col("payers_on_ip")
        >= F.lit(int(rules[RULE_SHARED_IP]["min_distinct_payers"])),
        RULE_VELOCITY: F.col("txns_in_window")
        > F.lit(int(rules[RULE_VELOCITY]["max_transactions"])),
        RULE_FANOUT: F.col("distinct_payees_in_window")
        > F.lit(int(rules[RULE_FANOUT]["max_distinct_payees"])),
        RULE_ODD_HOUR: (F.col("event_hour") >= F.lit(int(odd["start_hour"])))
        & (F.col("event_hour") < F.lit(int(odd["end_hour"]))),
    }


def score(df: DataFrame, cfg: Config, threshold: float) -> DataFrame:
    """Add ``fraud_score``, ``reasons`` and ``risk_band``."""
    rules = cfg.gold["fraud_rules"]
    bands = cfg.gold["risk_bands"]
    fired = _rule_columns(cfg, threshold)

    score_column = F.lit(0)
    for name, condition in fired.items():
        score_column = score_column + F.when(
            condition, F.lit(int(rules[name]["weight"]))
        ).otherwise(0)

    # array_compact drops the nulls left by rules that did not fire, so `reasons` holds
    # only the rules that actually contributed to the score.
    reasons_column = F.array_compact(
        F.array(*[F.when(condition, F.lit(name)) for name, condition in fired.items()])
    )

    scored = df.withColumn("fraud_score", score_column.cast("int")).withColumn(
        "reasons", reasons_column
    )

    return scored.withColumn(
        "risk_band",
        F.when(F.col("fraud_score") >= int(bands["high"]), BAND_HIGH)
        .when(F.col("fraud_score") >= int(bands["medium"]), BAND_MEDIUM)
        .otherwise(BAND_LOW),
    )


def kpi_daily(scored: DataFrame) -> DataFrame:
    """Per-day business metrics — the "total transaction volume" the spec asks for.

    Volume is measured over successful transactions only. A failed payment moves no
    money, and counting it inflates every revenue figure downstream.
    """
    success = F.col("status") == "SUCCESS"
    return (
        scored.groupBy("event_date")
        .agg(
            F.count("*").alias("txn_count"),
            F.round(F.sum(F.when(success, F.col("amount")).otherwise(0.0)), 2).alias(
                "total_amount"
            ),
            F.sum(F.when(success, 1).otherwise(0)).alias("success_count"),
            F.round(F.avg(F.when(success, F.col("amount"))), 2).alias("avg_ticket"),
            F.countDistinct("payer_key").alias("distinct_payers"),
            F.sum(F.when(F.col("risk_band") != BAND_LOW, 1).otherwise(0)).alias("flagged_count"),
            F.round(
                F.sum(F.when(F.col("risk_band") != BAND_LOW, F.col("amount")).otherwise(0.0)), 2
            ).alias("flagged_amount"),
        )
        .withColumn("success_rate", F.round(F.col("success_count") / F.col("txn_count"), 4))
        .select(
            "event_date",
            "txn_count",
            "total_amount",
            "success_count",
            "success_rate",
            "avg_ticket",
            "distinct_payers",
            "flagged_count",
            "flagged_amount",
        )
        .orderBy("event_date")
    )


def kpi_by_dimension(scored: DataFrame) -> DataFrame:
    """The same KPIs cut by bank, app and city.

    One long table rather than three wide ones: the measures are identical, and a
    dashboard filtering on ``dimension`` beats three near-duplicate tables that drift.
    """
    success = F.col("status") == "SUCCESS"
    measures = [
        F.count("*").alias("txn_count"),
        F.round(F.sum(F.when(success, F.col("amount")).otherwise(0.0)), 2).alias("total_amount"),
        F.round(F.avg(F.when(success, F.col("amount"))), 2).alias("avg_ticket"),
        F.round(F.avg(F.when(success, 1.0).otherwise(0.0)), 4).alias("success_rate"),
        F.sum(F.when(F.col("risk_band") != BAND_LOW, 1).otherwise(0)).alias("flagged_count"),
    ]

    parts = [
        scored.groupBy(F.col(column).alias("dimension_value"))
        .agg(*measures)
        .withColumn("dimension", F.lit(column))
        for column in ("payer_bank", "app", "city")
    ]

    unioned = parts[0]
    for part in parts[1:]:
        unioned = unioned.unionByName(part)

    return unioned.select(
        "dimension",
        "dimension_value",
        "txn_count",
        "total_amount",
        "avg_ticket",
        "success_rate",
        "flagged_count",
    ).orderBy("dimension", F.desc("total_amount"))


def run(spark: SparkSession, cfg: Config, run_id: str) -> dict[str, int]:
    """Recompute every Gold table from Silver. Returns counts for the run report."""
    tables.ensure_namespaces(spark, cfg)

    source = tables.read(spark, cfg, "silver", silver.TABLE)
    threshold = high_amount_threshold(source, cfg)

    scored = (
        score(add_features(source, cfg), cfg, threshold)
        .withColumn("scored_at", F.current_timestamp())
        .withColumn("scoring_run_id", F.lit(run_id))
    )

    fact = scored.drop("event_epoch")
    # Recomputed wholesale every run, so overwrite. The Silver table is the record of
    # what happened; Gold is a derived view of it and holds no state of its own.
    tables.write(fact, cfg, "gold", FACT_TABLE, mode="overwrite", partition_by=["event_date"])

    alerts = (
        fact.filter(F.col("risk_band") == BAND_HIGH)
        .select(
            "transaction_id",
            "event_time",
            "event_date",
            "amount",
            "status",
            "payer_vpa_masked",
            "payee_vpa_masked",
            "payer_bank",
            "app",
            "city",
            "ip_hash",
            "fraud_score",
            "reasons",
        )
        .orderBy(F.desc("fraud_score"), F.desc("amount"))
    )
    tables.write(alerts, cfg, "gold", ALERTS_TABLE, mode="overwrite")
    tables.write(kpi_daily(fact), cfg, "gold", KPI_DAILY_TABLE, mode="overwrite")
    tables.write(kpi_by_dimension(fact), cfg, "gold", KPI_DIMENSION_TABLE, mode="overwrite")

    return {
        "scored": fact.count(),
        "alerts": tables.count(spark, cfg, "gold", ALERTS_TABLE),
        "high_amount_threshold": int(threshold),
    }
