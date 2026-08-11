"""Export the Gold layer to pre-aggregated JSON for the web dashboard.

A browser cannot read Delta. Rather than standing up an API, the dashboard is fed
compact JSON computed here: the page then loads instantly, hosts anywhere static, and
needs no server, no cluster and no credentials in a browser.

Three properties this module has to preserve.

**Almost nothing row-level leaves the warehouse.** Every file but ``alerts.json`` is
an aggregate. ``alerts.json`` does ship transaction rows, but only the columns
``gold.fraud_alerts`` already holds — masked VPAs, a hashed IP — and a test asserts
that nothing matching an unmasked VPA, phone number or dotted IP ever reaches
``public/data``.

**The numbers are the warehouse's numbers.** Detection metrics are not restated here;
this module calls ``report.detection_summary`` and ``report.alert_precision`` and
serialises what they return. Restating them would let the page and the terminal report
drift apart, and the first anyone would know is a reviewer noticing two different
recall figures for the same run.

**Every file says where it came from.** A dashboard with no provenance looks equally
current whether it was exported a minute or a month ago.

Reads through ``sentinel.tables``, so ``SENTINEL_ENV=databricks`` exports from Unity
Catalog with no code change.
"""

from __future__ import annotations

import argparse
import json
import math
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pyspark.sql import Row
from pyspark.sql import functions as F

from sentinel import bronze, gold, landing, report, silver, tables
from sentinel.config import Config, load_config
from sentinel.generate.telemetry import load_labels
from sentinel.spark import get_spark, is_databricks, stop_spark

if TYPE_CHECKING:  # pragma: no cover - typing only
    from pyspark.sql import DataFrame, SparkSession

DEFAULT_OUT = Path(__file__).resolve().parents[3] / "dashboards" / "web" / "public" / "data"

# Score histogram resolution. Weights are multiples of 5, so a 5-point bucket lines the
# bars up with the values that can actually occur instead of smearing them.
SCORE_BUCKET = 5

# Reason combinations are long-tailed; beyond the top handful the bars are unreadable
# and the remainder is more honestly shown as one "other" row.
TOP_COMBINATIONS = 10


def _clean(value: Any) -> Any:
    """Make a Spark value JSON-safe.

    NaN and Infinity are the important cases: ``json.dumps`` emits them as bare
    ``NaN``/``Infinity`` tokens, which are not valid JSON and which ``JSON.parse``
    rejects — the page then fails to load with a syntax error pointing at a byte
    offset rather than at the average that had no rows behind it.
    """
    if isinstance(value, float):
        return None if math.isnan(value) or math.isinf(value) else round(value, 4)
    if isinstance(value, datetime):
        return value.isoformat(sep=" ", timespec="seconds")
    return value


def _rows(df: DataFrame) -> list[dict[str, Any]]:
    """Collect a DataFrame as a list of JSON-safe dicts."""
    return [{k: _clean(v) for k, v in row.asDict().items()} for row in df.collect()]


def _agg(df: DataFrame) -> Row:
    """The single row of an aggregate query.

    A bare ``.first()`` is ``Row | None``, and every caller here immediately indexes
    into it. An aggregate over an empty frame still returns a row, so None means the
    query was not the aggregate the caller thought it was — worth an explicit failure
    rather than a TypeError three lines later.
    """
    row = df.first()
    if row is None:
        raise RuntimeError("aggregate query returned no row")
    return row


def provenance(cfg: Config) -> dict[str, str]:
    """Where these numbers came from. Rendered in the page footer."""
    return {
        "environment": cfg.env,
        "engine": "Databricks (Spark)" if is_databricks() else "Spark (local)",
        "store": "Unity Catalog" if cfg.uses_catalog else "Delta on local filesystem",
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }


# ----------------------------------------------------------------- panels


def headline(spark: SparkSession, cfg: Config, fact: DataFrame) -> dict[str, Any]:
    """The hero numbers: volume, throughput, detection quality."""
    success = fact.filter(F.col("status") == "SUCCESS")
    totals = _agg(
        success.agg(
            F.sum("amount").alias("volume"),
            F.count("*").alias("success_count"),
            F.avg("amount").alias("avg_ticket"),
        )
    )

    scored = fact.count()
    alerts = tables.count(spark, cfg, "gold", gold.ALERTS_TABLE)
    precision = report.alert_precision(spark, cfg, fact)

    return {
        "scored": scored,
        "volume": _clean(float(totals["volume"] or 0.0)),
        "success_count": totals["success_count"],
        "success_rate": _clean(totals["success_count"] / scored if scored else 0.0),
        "avg_ticket": _clean(float(totals["avg_ticket"] or 0.0)),
        "distinct_payers": fact.select("payer_key").distinct().count(),
        "alerts": alerts,
        "flagged": fact.filter(F.col("risk_band") != gold.BAND_LOW).count(),
        "high_amount_threshold": gold.high_amount_threshold(fact, cfg),
        "alert_precision": (
            {"true_positives": precision[0], "alerts": precision[1]} if precision else None
        ),
        "source": provenance(cfg),
    }


def funnel(spark: SparkSession, cfg: Config, fact: DataFrame) -> dict[str, Any]:
    """The medallion stage counts, and what fell out at each step.

    ``deduplicated`` is derived rather than measured: Silver's dedupe drops rows
    without recording them anywhere, so the count is what Bronze holds minus what
    left through the two doors we *can* count. It is reported as its own stage rather
    than folded into the quarantine, because a collapsed retry is not a rejection.
    """
    landed = tables.count(spark, cfg, "landing", landing.TABLE)
    ingested = tables.count(spark, cfg, "bronze", bronze.TABLE)
    cleansed = tables.count(spark, cfg, "silver", silver.TABLE)
    quarantined = tables.count(spark, cfg, "quarantine", silver.QUARANTINE_TABLE)

    reasons: list[dict[str, Any]] = []
    if tables.table_exists(spark, cfg, "quarantine", silver.QUARANTINE_TABLE):
        reasons = _rows(
            tables.read(spark, cfg, "quarantine", silver.QUARANTINE_TABLE)
            .groupBy("reject_reason")
            .agg(F.count("*").alias("records"))
            .orderBy(F.desc("records"))
        )

    unparseable = 0
    if tables.table_exists(spark, cfg, "bronze", bronze.TABLE):
        unparseable = (
            tables.read(spark, cfg, "bronze", bronze.TABLE).filter(~F.col("is_parseable")).count()
        )

    return {
        "stages": [
            {"stage": "landing", "label": "Landed", "rows": landed},
            {"stage": "bronze", "label": "Structured", "rows": ingested},
            {"stage": "silver", "label": "Cleansed", "rows": cleansed},
            {"stage": "gold", "label": "Scored", "rows": fact.count()},
        ],
        "quarantined": quarantined,
        "deduplicated": max(ingested - cleansed - quarantined, 0),
        "unparseable": unparseable,
        "reasons": reasons,
        "source": provenance(cfg),
    }


def scoring(cfg: Config, fact: DataFrame) -> dict[str, Any]:
    """Risk bands, the score distribution, and which rules produced the scores."""
    bands = _rows(
        fact.groupBy("risk_band").agg(F.count("*").alias("transactions")).orderBy("risk_band")
    )

    histogram = _rows(
        fact.withColumn("bucket", (F.floor(F.col("fraud_score") / SCORE_BUCKET) * SCORE_BUCKET))
        .groupBy("bucket")
        .agg(F.count("*").alias("transactions"))
        .orderBy("bucket")
    )

    # One row per (transaction, rule) so a rule that fired can be counted once,
    # independently of whatever else fired alongside it.
    exploded = fact.select("transaction_id", "risk_band", F.explode("reasons").alias("rule"))
    rules = _rows(
        exploded.groupBy("rule")
        .agg(
            F.count("*").alias("fired"),
            F.sum(F.when(F.col("risk_band") == gold.BAND_HIGH, 1).otherwise(0)).alias("in_alerts"),
        )
        .orderBy(F.desc("fired"))
    )

    # Which *combinations* raise an alert. This is the evidence for the design claim
    # that no single rule can reach HIGH on its own.
    combos_df = (
        fact.filter(F.size("reasons") > 0)
        .withColumn("combination", F.array_join(F.array_sort("reasons"), " + "))
        .groupBy("combination", "risk_band")
        .agg(F.count("*").alias("transactions"))
        .orderBy(F.desc("transactions"))
    )
    combinations = _rows(combos_df.limit(TOP_COMBINATIONS))
    shown = sum(row["transactions"] for row in combinations)
    remainder = _agg(combos_df.agg(F.sum("transactions")))[0] or 0

    return {
        "bands": bands,
        "band_thresholds": {
            "medium": int(cfg.gold["risk_bands"]["medium"]),
            "high": int(cfg.gold["risk_bands"]["high"]),
        },
        "bucket_size": SCORE_BUCKET,
        "histogram": histogram,
        "rules": rules,
        "rule_weights": {name: int(r["weight"]) for name, r in cfg.gold["fraud_rules"].items()},
        "combinations": combinations,
        "combinations_other": max(int(remainder) - shown, 0),
        "source": provenance(cfg),
    }


def amount_distribution(cfg: Config, fact: DataFrame) -> dict[str, Any]:
    """The amount histogram behind the high-amount rule, with both thresholds marked.

    Exists to make one specific finding visible: the 99.5th percentile of successful
    amounts sits *inside* the injected anomalies and so climbs above most of them,
    while the Tukey fence built from the quartiles does not move. Both are computed
    here on the same data so the page compares them rather than asserting the result.
    """
    rule = cfg.gold["fraud_rules"][gold.RULE_HIGH_AMOUNT]
    success = fact.filter(F.col("status") == "SUCCESS")

    stats = _agg(
        success.agg(
            F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("quartiles"),
            F.percentile_approx("amount", 0.995).alias("p995"),
            F.max("amount").alias("max"),
        )
    )

    q1, median, q3 = (float(v) for v in stats["quartiles"])
    fence = q3 + float(rule["iqr_multiplier"]) * (q3 - q1)

    # Log-spaced buckets: amounts span five orders of magnitude, and linear buckets put
    # 99% of the mass in the first bar and show nothing of the tail that matters. Half
    # decades rather than whole ones — whole decades give only six bars, too coarse to
    # see where the fence and the percentile actually fall.
    histogram = _rows(
        success.withColumn("step", F.floor(F.log10(F.col("amount")) * 2) / 2)
        .withColumn("bucket", F.pow(F.lit(10.0), F.col("step")))
        .groupBy("bucket")
        .agg(F.count("*").alias("transactions"))
        .orderBy("bucket")
    )

    return {
        "histogram": histogram,
        "q1": _clean(q1),
        "median": _clean(median),
        "q3": _clean(q3),
        "max": _clean(float(stats["max"])),
        "percentile_995": _clean(float(stats["p995"])),
        "tukey_fence": _clean(fence),
        "floor": float(rule["floor"]),
        "applied_threshold": _clean(gold.high_amount_threshold(fact, cfg)),
        "source": provenance(cfg),
    }


def detection(spark: SparkSession, cfg: Config, fact: DataFrame) -> dict[str, Any]:
    """Detection measured against the generator's labels, or an empty result.

    Delegates to the report module so the page cannot disagree with `make report`.
    """
    summary = report.detection_summary(spark, cfg, fact)
    return {
        "available": summary is not None,
        "labels": len(load_labels(cfg)),
        "types": _rows(summary) if summary is not None else [],
        "source": provenance(cfg),
    }


def alerts(spark: SparkSession, cfg: Config, limit: int | None) -> dict[str, Any]:
    """Every HIGH-band alert, for the console to filter client-side.

    Shipped whole rather than paginated: a few thousand rows is a fraction of a
    megabyte gzipped, and filtering in the browser over the complete set beats
    round-tripping to a server this dashboard deliberately does not have.
    """
    if not tables.table_exists(spark, cfg, "gold", gold.ALERTS_TABLE):
        return {"total": 0, "rows": [], "source": provenance(cfg)}

    df = tables.read(spark, cfg, "gold", gold.ALERTS_TABLE).orderBy(
        F.desc("fraud_score"), F.desc("amount")
    )
    total = df.count()
    return {
        "total": total,
        "truncated": bool(limit and total > limit),
        "rows": _rows(df.limit(limit) if limit else df),
        "source": provenance(cfg),
    }


# ----------------------------------------------------------------- run


def export(spark: SparkSession, cfg: Config, out: Path, limit: int | None = None) -> dict[str, int]:
    """Write every panel file. Returns each file's size in bytes."""
    if not tables.table_exists(spark, cfg, "gold", gold.FACT_TABLE):
        raise RuntimeError(
            f"No Gold tables at {cfg.table('gold', gold.FACT_TABLE)}. "
            "Run `make gen && make run-local` first."
        )

    fact = tables.read(spark, cfg, "gold", gold.FACT_TABLE)
    # Read repeatedly by nearly every panel below; without this each one re-reads the
    # Delta table and the export takes minutes instead of seconds.
    fact.cache()

    try:
        payloads: dict[str, Any] = {
            "headline": headline(spark, cfg, fact),
            "funnel": funnel(spark, cfg, fact),
            "daily": {
                "rows": _rows(
                    tables.read(spark, cfg, "gold", gold.KPI_DAILY_TABLE).orderBy("event_date")
                ),
                "source": provenance(cfg),
            },
            "dimensions": {
                "rows": _rows(
                    tables.read(spark, cfg, "gold", gold.KPI_DIMENSION_TABLE).orderBy(
                        "dimension", F.desc("total_amount")
                    )
                ),
                "source": provenance(cfg),
            },
            "scoring": scoring(cfg, fact),
            "amounts": amount_distribution(cfg, fact),
            "detection": detection(spark, cfg, fact),
            "alerts": alerts(spark, cfg, limit),
        }
    finally:
        fact.unpersist()

    out.mkdir(parents=True, exist_ok=True)
    sizes: dict[str, int] = {}
    for name, payload in payloads.items():
        path = out / f"{name}.json"
        # allow_nan=False turns a stray NaN into a loud failure here rather than a
        # silent one in the browser. _clean should have caught it; this is the net.
        path.write_text(json.dumps(payload, indent=2, allow_nan=False))
        sizes[name] = path.stat().st_size

    return sizes


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Export the Gold layer to dashboard JSON.")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT, help="output directory")
    parser.add_argument("--env", default=None, help="override SENTINEL_ENV")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="cap the number of alert rows exported (default: all of them)",
    )
    args = parser.parse_args(argv)

    cfg = load_config(args.env)
    spark = get_spark(cfg)
    try:
        sizes = export(spark, cfg, args.out, args.limit)
    finally:
        stop_spark()

    print(f"wrote {len(sizes)} files to {args.out}")
    for name, size in sorted(sizes.items(), key=lambda kv: -kv[1]):
        print(f"  {name + '.json':<20} {size / 1024:>8.1f} KB")
    print(f"  {'total':<20} {sum(sizes.values()) / 1024:>8.1f} KB")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
