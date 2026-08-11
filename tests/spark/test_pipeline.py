"""End to end: generator -> landing -> bronze -> silver -> gold, in a temp directory.

The unit tests pin each rule in isolation. This one asserts the properties that only
exist once the layers are wired together — that nothing is lost between them, that a
known-bad row ends up quarantined rather than served, and that a known-fraudulent row
ends up in the alerts.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pyspark.sql import functions as F

from sentinel import bronze, gold, landing, silver, tables
from sentinel.generate.telemetry import TelemetryGenerator

pytestmark = pytest.mark.spark

SCALE = 0.02
# Pinned so the fixture is byte-identical between runs; see the generator's docstring.
WINDOW_END = datetime(2026, 8, 11, 12, 0, tzinfo=UTC)


@pytest.fixture(scope="module")
def pipeline(spark, cfg, tmp_path_factory):
    """Generate a small dataset and run all four stages over it once."""
    root = tmp_path_factory.mktemp("pipeline")
    paths = dict(cfg.paths)
    for zone in (
        "raw",
        "landing",
        "bronze",
        "silver",
        "gold",
        "quarantine",
        "checkpoints",
        "truth",
    ):
        cfg.paths[zone] = str(root / zone)

    generated = TelemetryGenerator(cfg, scale=SCALE, seed=11, end=WINDOW_END).write()

    counts = {
        "landing": landing.run(spark, cfg, "test"),
        "bronze": bronze.run(spark, cfg, "test"),
        "silver": silver.run(spark, cfg, "test"),
        "gold": gold.run(spark, cfg, "test"),
    }

    yield generated, counts, cfg

    cfg.paths.update(paths)


def test_landing_holds_one_row_per_generated_line(spark, pipeline):
    """Nothing is filtered before durable storage — that is what Landing is for."""
    generated, counts, _ = pipeline
    assert counts["landing"]["landed"] == generated.records


def test_bronze_preserves_every_landed_row(spark, pipeline):
    """Including the lines that are not JSON at all."""
    _, counts, _ = pipeline
    assert counts["bronze"]["ingested"] == counts["landing"]["landed"]


def test_bronze_keeps_the_original_messy_values_untouched(spark, pipeline):
    """The layer's whole purpose: structure without cleaning."""
    _, _, cfg = pipeline
    df = tables.read(spark, cfg, "bronze", bronze.TABLE)

    # Amounts still arrive in both encodings, and negatives are still present.
    assert df.filter(F.col("amount").rlike(r"^-")).count() > 0
    # Casing has not been folded and whitespace has not been trimmed.
    assert (
        df.filter(F.col("status").isNotNull() & (F.col("status") != F.upper("status"))).count() > 0
    )
    assert df.filter(F.col("payer_vpa") != F.trim("payer_vpa")).count() > 0


def test_bronze_flags_unparseable_lines_without_dropping_them(spark, pipeline):
    _, _, cfg = pipeline
    df = tables.read(spark, cfg, "bronze", bronze.TABLE)
    corrupt = df.filter(~F.col("is_parseable"))
    assert corrupt.count() > 0
    # The original text survives even though no field could be parsed from it.
    assert corrupt.filter(F.col("raw_payload").isNull()).count() == 0


def test_every_bronze_row_is_either_cleansed_or_quarantined(spark, pipeline):
    """The accounting identity of the Silver layer.

    Rows may only leave via the quarantine or the dedupe — never by silently vanishing,
    which is exactly what a bad filter predicate looks like from the outside.
    """
    _, counts, cfg = pipeline
    ingested = counts["bronze"]["ingested"]
    cleansed = counts["silver"]["cleansed"]
    quarantined = counts["silver"]["quarantined"]

    bronze_ids = tables.read(spark, cfg, "bronze", bronze.TABLE).filter(
        F.col("transaction_id").isNotNull()
    )
    duplicates = bronze_ids.count() - bronze_ids.select(F.trim("transaction_id")).distinct().count()

    assert cleansed + quarantined + duplicates == ingested


def test_silver_contains_no_invalid_records(spark, pipeline):
    """Everything Step 3 promises to remove is actually gone."""
    _, _, cfg = pipeline
    df = tables.read(spark, cfg, "silver", silver.TABLE)

    assert df.filter(F.col("amount") <= 0).count() == 0
    assert df.filter(F.col("transaction_id").isNull()).count() == 0
    assert df.filter(F.col("event_time").isNull()).count() == 0
    assert df.filter(F.col("status") != F.upper("status")).count() == 0
    assert df.filter(F.col("currency") != "INR").count() == 0


def test_silver_deduplicates_the_retry_storms(spark, pipeline):
    _, _, cfg = pipeline
    df = tables.read(spark, cfg, "silver", silver.TABLE)
    assert df.count() == df.select("transaction_id").distinct().count()


def test_quarantined_rows_carry_a_reason_and_the_original_payload(spark, pipeline):
    """A rejection nobody can investigate is only marginally better than a silent drop."""
    _, _, cfg = pipeline
    rejects = tables.read(spark, cfg, "quarantine", silver.QUARANTINE_TABLE)

    assert rejects.count() > 0
    assert rejects.filter(F.col("reject_reason").isNull()).count() == 0
    assert rejects.filter(F.col("raw_payload").isNull()).count() == 0
    # More than one kind of defect made it through to be rejected.
    assert rejects.select("reject_reason").distinct().count() > 1


def test_no_personally_identifying_value_reaches_gold(spark, pipeline):
    """Checks the values, not just the column names.

    A masking function that silently stopped masking would still produce a column
    called payer_vpa_masked, and the schema test in test_silver would still pass.
    """
    generated, _, cfg = pipeline
    fact = tables.read(spark, cfg, "gold", gold.FACT_TABLE)

    assert fact.filter(F.col("payer_vpa_masked").contains("@")).count() > 0
    assert fact.filter(F.col("payer_vpa_masked").rlike(r"^user\d")).count() == 0
    assert fact.filter(F.col("payer_phone_masked").rlike(r"^\d{6}")).count() == 0
    # The hashes are hex digests, not the addresses they stand in for.
    assert fact.filter(F.col("ip_hash").contains(".")).count() == 0
    assert fact.filter(~F.col("ip_hash").rlike(r"^[0-9a-f]{16}$")).count() == 0


def test_gold_scores_every_silver_row(spark, pipeline):
    _, counts, cfg = pipeline
    assert counts["gold"]["scored"] == counts["silver"]["total"]


def test_injected_fraud_reaches_the_alerts_table(spark, pipeline):
    """The end-to-end claim: a planted anomaly survives four layers and is flagged."""
    generated, _, cfg = pipeline
    alerts = tables.read(spark, cfg, "gold", gold.ALERTS_TABLE)

    assert alerts.count() > 0
    alerted_ids = {row["transaction_id"] for row in alerts.select("transaction_id").collect()}
    assert alerted_ids & set(generated.labels), "no injected anomaly raised an alert"


def test_alerts_explain_themselves(spark, pipeline):
    """An alert with no reasons is not actionable, and will not be trusted."""
    _, _, cfg = pipeline
    alerts = tables.read(spark, cfg, "gold", gold.ALERTS_TABLE)
    assert alerts.filter(F.size("reasons") == 0).count() == 0
    assert alerts.filter(F.size("reasons") < 2).count() == 0


def test_daily_kpis_reconcile_with_the_fact_table(spark, pipeline):
    """The aggregate and the detail must agree, or the dashboard is lying."""
    _, _, cfg = pipeline
    fact = tables.read(spark, cfg, "gold", gold.FACT_TABLE)
    daily = tables.read(spark, cfg, "gold", gold.KPI_DAILY_TABLE)

    assert daily.agg(F.sum("txn_count")).first()[0] == fact.count()

    expected = fact.filter(F.col("status") == "SUCCESS").agg(F.sum("amount")).first()[0]
    actual = daily.agg(F.sum("total_amount")).first()[0]
    assert abs(actual - expected) < 1.0  # rounding, applied per day


def test_rerunning_without_new_data_is_a_no_op(spark, pipeline):
    """The incrementality claim.

    Checkpoints mean a second run consumes nothing. Without this, re-running the
    pipeline would double every row and no test would notice.
    """
    _, _, cfg = pipeline
    before = tables.count(spark, cfg, "silver", silver.TABLE)

    assert landing.run(spark, cfg, "rerun")["landed"] == 0
    assert bronze.run(spark, cfg, "rerun")["ingested"] == 0
    assert silver.run(spark, cfg, "rerun")["cleansed"] == 0
    assert tables.count(spark, cfg, "silver", silver.TABLE) == before


def test_a_second_batch_flows_through_incrementally(spark, pipeline):
    """New files are picked up; old ones are not reprocessed."""
    _, _, cfg = pipeline
    before = tables.count(spark, cfg, "silver", silver.TABLE)

    added = TelemetryGenerator(cfg, scale=0.01, seed=77, end=WINDOW_END).write()

    landed = landing.run(spark, cfg, "batch2")["landed"]
    assert landed == added.records

    bronze.run(spark, cfg, "batch2")
    silver.run(spark, cfg, "batch2")
    assert tables.count(spark, cfg, "silver", silver.TABLE) > before
