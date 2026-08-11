"""The exported JSON, asserted against the Gold tables it claims to describe.

A dashboard is a second implementation of every number it shows. Without these tests
the page and the warehouse drift, and nobody finds out until a reviewer notices two
different recall figures for the same run — by which point the dashboard has been
believed for weeks.

So each panel is checked against the tables directly, and the disclosure rule is
checked against the emitted bytes rather than against intent.
"""

from __future__ import annotations

import json
import re

import pytest
from pyspark.sql import functions as F

from sentinel import bronze, gold, landing, silver, tables
from sentinel.generate.telemetry import TelemetryGenerator
from sentinel.web import export as web

pytestmark = pytest.mark.spark

# Large enough that the shared_ip rule actually fires and alerts exist. At 0.02 the
# suspicious-IP pool is spread across too few payers to reach the rule's minimum, so
# nothing reaches HIGH and every assertion about the alert console passes vacuously.
SCALE = 0.05


@pytest.fixture(scope="module")
def exported(spark, cfg, tmp_path_factory):
    """Run the whole pipeline over a small dataset, then export it."""
    root = tmp_path_factory.mktemp("web")
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

    TelemetryGenerator(cfg, scale=SCALE, seed=23).write()
    landing.run(spark, cfg, "web")
    bronze.run(spark, cfg, "web")
    silver.run(spark, cfg, "web")
    gold.run(spark, cfg, "web")

    out = root / "data"
    web.export(spark, cfg, out)
    payloads = {path.stem: json.loads(path.read_text()) for path in out.glob("*.json")}

    yield payloads, out, cfg

    cfg.paths.update(paths)


def test_every_panel_file_is_written(exported):
    payloads, _, _ = exported
    assert set(payloads) == {
        "headline",
        "funnel",
        "daily",
        "dimensions",
        "scoring",
        "amounts",
        "detection",
        "alerts",
    }


def test_every_file_carries_provenance(exported):
    """A dashboard with no provenance looks equally current a minute or a month later."""
    payloads, _, _ = exported
    for name, payload in payloads.items():
        source = payload["source"]
        assert source["environment"] == "local", name
        assert source["engine"], name
        assert source["generated_at"], name


# ----------------------------------------------------------------- fidelity


def test_headline_matches_the_fact_table(spark, exported):
    payloads, _, cfg = exported
    fact = tables.read(spark, cfg, "gold", gold.FACT_TABLE)
    headline = payloads["headline"]

    assert headline["scored"] == fact.count()
    assert headline["alerts"] == tables.count(spark, cfg, "gold", gold.ALERTS_TABLE)
    assert headline["flagged"] == fact.filter(F.col("risk_band") != gold.BAND_LOW).count()

    volume = fact.filter(F.col("status") == "SUCCESS").agg(F.sum("amount")).first()[0]
    assert headline["volume"] == pytest.approx(volume, rel=1e-6)


def test_funnel_stage_counts_match_the_tables(spark, exported):
    payloads, _, cfg = exported
    stages = {s["stage"]: s["rows"] for s in payloads["funnel"]["stages"]}

    assert stages["landing"] == tables.count(spark, cfg, "landing", landing.TABLE)
    assert stages["bronze"] == tables.count(spark, cfg, "bronze", bronze.TABLE)
    assert stages["silver"] == tables.count(spark, cfg, "silver", silver.TABLE)
    assert stages["gold"] == tables.count(spark, cfg, "gold", gold.FACT_TABLE)


def test_the_funnel_accounts_for_every_row_that_left(exported):
    """Rows may only leave Bronze through the quarantine or the dedupe.

    If this drifts, the funnel chart shows rows vanishing into nothing — which is
    exactly what a real filter bug would look like, so it must not be possible to
    draw it when nothing is wrong.
    """
    funnel = exported[0]["funnel"]
    stages = {s["stage"]: s["rows"] for s in funnel["stages"]}
    assert stages["bronze"] == stages["silver"] + funnel["quarantined"] + funnel["deduplicated"]


def test_quarantine_reasons_sum_to_the_quarantine_total(exported):
    funnel = exported[0]["funnel"]
    assert sum(r["records"] for r in funnel["reasons"]) == funnel["quarantined"]


def test_risk_bands_sum_to_the_scored_total(exported):
    payloads, _, _ = exported
    bands = {b["risk_band"]: b["transactions"] for b in payloads["scoring"]["bands"]}
    assert sum(bands.values()) == payloads["headline"]["scored"]
    # A band with no rows is absent from the group-by rather than present as zero.
    assert bands.get(gold.BAND_HIGH, 0) == payloads["headline"]["alerts"]


def test_the_fixture_actually_produces_alerts(exported):
    """Guards the tests below from passing vacuously on an empty alert table."""
    assert exported[0]["headline"]["alerts"] > 0


def test_score_histogram_accounts_for_every_transaction(exported):
    payloads, _, _ = exported
    total = sum(b["transactions"] for b in payloads["scoring"]["histogram"])
    assert total == payloads["headline"]["scored"]


def test_daily_kpis_reconcile_with_the_headline(exported):
    payloads, _, _ = exported
    rows = payloads["daily"]["rows"]
    assert sum(r["txn_count"] for r in rows) == payloads["headline"]["scored"]
    assert sum(r["total_amount"] for r in rows) == pytest.approx(
        payloads["headline"]["volume"], rel=1e-4
    )


def test_each_dimension_covers_every_transaction(exported):
    payloads, _, _ = exported
    rows = payloads["dimensions"]["rows"]
    assert {r["dimension"] for r in rows} == {"payer_bank", "app", "city"}
    for dimension in ("payer_bank", "app", "city"):
        covered = sum(r["txn_count"] for r in rows if r["dimension"] == dimension)
        assert covered == payloads["headline"]["scored"], dimension


def test_alerts_payload_holds_every_high_band_row(spark, exported):
    payloads, _, cfg = exported
    alerts = payloads["alerts"]
    assert alerts["total"] == tables.count(spark, cfg, "gold", gold.ALERTS_TABLE)
    assert len(alerts["rows"]) == alerts["total"]
    assert all(
        row["fraud_score"] >= payloads["scoring"]["band_thresholds"]["high"]
        for row in alerts["rows"]
    )


def test_detection_matches_the_terminal_report(spark, exported):
    """The page and `make report` must not be able to disagree."""
    payloads, _, cfg = exported
    fact = tables.read(spark, cfg, "gold", gold.FACT_TABLE)
    from sentinel import report

    expected = {
        row["injected_type"]: row.asDict()
        for row in report.detection_summary(spark, cfg, fact).collect()
    }
    exported_types = {row["injected_type"]: row for row in payloads["detection"]["types"]}

    assert set(exported_types) == set(expected)
    for name, row in expected.items():
        for field in ("injected", "not_scored", "alerted", "flagged"):
            assert exported_types[name][field] == row[field], f"{name}.{field}"


def test_the_threshold_comparison_is_computed_not_asserted(exported):
    """Both statistics come from the same data, so the page compares rather than claims.

    The finding it illustrates: a high percentile is dragged up by the anomalies it
    should catch, while the quartile-based fence is not.
    """
    amounts = exported[0]["amounts"]
    assert amounts["percentile_995"] > amounts["tukey_fence"]
    assert amounts["q1"] < amounts["median"] < amounts["q3"]
    assert amounts["applied_threshold"] == max(amounts["tukey_fence"], amounts["floor"])


# ----------------------------------------------------------------- disclosure


def test_no_unmasked_identifier_reaches_the_export(exported):
    """Checked against the emitted bytes, not against intent.

    Everything but alerts.json is an aggregate; alerts.json ships rows, but only the
    masked columns fraud_alerts already holds. A masking regression upstream would
    otherwise publish real VPAs to a static site with no further review.
    """
    _, out, _ = exported

    patterns = {
        "raw payer VPA": re.compile(r"user\d{6}@"),
        "raw merchant VPA": re.compile(r"merchant\d{5}@"),
        "phone number": re.compile(r"\b[6-9]\d{9}\b"),
        "IPv4 address": re.compile(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b"),
        "device id": re.compile(r"DEV-[0-9a-f]{12}"),
    }

    for path in out.glob("*.json"):
        text = path.read_text()
        for label, pattern in patterns.items():
            assert not pattern.search(text), f"{label} leaked into {path.name}"


def test_only_the_alerts_file_carries_row_level_data(exported):
    """Everything else must be an aggregate, so the payload cannot become a data dump.

    Checked on the JSON *keys*, not on the serialised text: a substring search matches
    the reject reason "missing_transaction_id", which is an aggregate label and not a
    transaction at all.
    """
    payloads, _, _ = exported

    def keys(node: object) -> set[str]:
        if isinstance(node, dict):
            return set(node) | {k for v in node.values() for k in keys(v)}
        if isinstance(node, list):
            return {k for item in node for k in keys(item)}
        return set()

    for name, payload in payloads.items():
        if name == "alerts":
            continue
        assert "transaction_id" not in keys(payload), name
