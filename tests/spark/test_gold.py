"""Gold's fraud scoring and KPIs.

The scoring rules are the part of this pipeline most likely to be tuned later, so each
one is pinned individually: it must fire when it should, stay silent when it should
not, and contribute its configured weight and no more.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest
from pyspark.sql import functions as F

from sentinel.gold import (
    BAND_HIGH,
    BAND_LOW,
    RULE_FANOUT,
    RULE_HIGH_AMOUNT,
    RULE_ODD_HOUR,
    RULE_SHARED_IP,
    RULE_VELOCITY,
    add_features,
    high_amount_threshold,
    kpi_by_dimension,
    kpi_daily,
    score,
)

pytestmark = pytest.mark.spark

BASE = datetime(2026, 8, 9, 14, 0, 0)

SILVER_SCHEMA = (
    "transaction_id string, event_time timestamp, event_date string, amount double, "
    "currency string, status string, txn_type string, payer_bank string, "
    "payee_bank string, app string, city string, state string, "
    "merchant_category string, latency_ms int, payer_key string, payee_key string, "
    "ip_hash string"
)


def _txn(txn_id, *, amount=500.0, when=BASE, payer="P1", payee="M1", ip="IP1", status="SUCCESS"):
    return (
        txn_id,
        when,
        when.strftime("%Y-%m-%d"),
        amount,
        "INR",
        status,
        "P2M",
        "HDFC",
        "SBI",
        "GPay",
        "Pune",
        "MH",
        "5411",
        300,
        payer,
        payee,
        ip,
    )


def _scored(spark, cfg, rows, threshold=50000.0):
    df = spark.createDataFrame(rows, SILVER_SCHEMA)
    return score(add_features(df, cfg), cfg, threshold)


def _reasons(scored, txn_id):
    row = scored.filter(F.col("transaction_id") == txn_id).first()
    return set(row["reasons"]), row["fraud_score"], row["risk_band"]


# ----------------------------------------------------------------- threshold


def test_threshold_is_not_dragged_up_by_the_anomalies_it_should_catch(spark, cfg):
    """The regression this rule was rewritten for.

    A high percentile of a distribution containing 1% enormous outliers sits *inside*
    those outliers. The quartile-based fence does not move.
    """
    ordinary = [_txn(f"N{i}", amount=400.0 + i) for i in range(1000)]
    fraud = [_txn(f"F{i}", amount=150000.0) for i in range(10)]

    clean_threshold = high_amount_threshold(spark.createDataFrame(ordinary, SILVER_SCHEMA), cfg)
    contaminated = high_amount_threshold(
        spark.createDataFrame(ordinary + fraud, SILVER_SCHEMA), cfg
    )
    assert contaminated == clean_threshold
    assert contaminated < 150000.0


def test_threshold_never_falls_below_the_configured_floor(spark, cfg):
    """Uniformly tiny traffic must not make ₹900 'unusually large'."""
    rows = [_txn(f"N{i}", amount=100.0) for i in range(50)]
    floor = float(cfg.gold["fraud_rules"][RULE_HIGH_AMOUNT]["floor"])
    assert high_amount_threshold(spark.createDataFrame(rows, SILVER_SCHEMA), cfg) == floor


def test_threshold_ignores_failed_transactions(spark, cfg):
    """A failed payment moved no money and should not shape the notion of 'large'."""
    rows = [_txn(f"N{i}", amount=100.0) for i in range(50)]
    rows += [_txn(f"X{i}", amount=400000.0, status="FAILED") for i in range(50)]
    floor = float(cfg.gold["fraud_rules"][RULE_HIGH_AMOUNT]["floor"])
    assert high_amount_threshold(spark.createDataFrame(rows, SILVER_SCHEMA), cfg) == floor


# ----------------------------------------------------------------- rules


def test_ordinary_transaction_scores_zero(spark, cfg):
    scored = _scored(spark, cfg, [_txn("T1")])
    reasons, points, band = _reasons(scored, "T1")
    assert reasons == set()
    assert points == 0
    assert band == BAND_LOW


def test_high_amount_fires_above_the_threshold(spark, cfg):
    scored = _scored(spark, cfg, [_txn("T1", amount=90000.0)])
    reasons, points, _ = _reasons(scored, "T1")
    assert reasons == {RULE_HIGH_AMOUNT}
    assert points == cfg.gold["fraud_rules"][RULE_HIGH_AMOUNT]["weight"]


def test_shared_ip_fires_only_once_enough_distinct_payers_use_it(spark, cfg):
    """Four people behind one router is a household. Forty is not."""
    minimum = int(cfg.gold["fraud_rules"][RULE_SHARED_IP]["min_distinct_payers"])

    below = _scored(spark, cfg, [_txn(f"T{i}", payer=f"P{i}") for i in range(minimum - 1)])
    assert RULE_SHARED_IP not in _reasons(below, "T0")[0]

    at_limit = _scored(spark, cfg, [_txn(f"T{i}", payer=f"P{i}") for i in range(minimum)])
    assert RULE_SHARED_IP in _reasons(at_limit, "T0")[0]


def test_velocity_measures_time_not_row_count(spark, cfg):
    """Twenty transactions spread over a month are not a burst."""
    slow = [_txn(f"T{i}", when=BASE + timedelta(days=i), payer="P1") for i in range(20)]
    assert RULE_VELOCITY not in _reasons(_scored(spark, cfg, slow), "T19")[0]

    fast = [_txn(f"T{i}", when=BASE + timedelta(seconds=10 * i), payer="P1") for i in range(20)]
    assert RULE_VELOCITY in _reasons(_scored(spark, cfg, fast), "T19")[0]


def test_velocity_does_not_fire_at_the_start_of_a_burst(spark, cfg):
    """The window is trailing, so the first transactions genuinely are not yet a burst.

    Documenting this as intended rather than as a miss: nothing can flag the first
    payment of a spree, and a rule that claimed to would be looking at the future.
    """
    burst = [_txn(f"T{i}", when=BASE + timedelta(seconds=10 * i), payer="P1") for i in range(20)]
    scored = _scored(spark, cfg, burst)
    assert RULE_VELOCITY not in _reasons(scored, "T0")[0]
    assert RULE_VELOCITY in _reasons(scored, "T19")[0]


def test_fanout_counts_distinct_payees_not_transactions(spark, cfg):
    """Fifty payments to one merchant is a regular; fifty payees is a mule."""
    limit = int(cfg.gold["fraud_rules"][RULE_FANOUT]["max_distinct_payees"])

    repeat = [
        _txn(f"T{i}", when=BASE + timedelta(seconds=30 * i), payer="P1", payee="M1")
        for i in range(limit + 10)
    ]
    assert RULE_FANOUT not in _reasons(_scored(spark, cfg, repeat), f"T{limit + 9}")[0]

    spread = [
        _txn(f"T{i}", when=BASE + timedelta(seconds=30 * i), payer="P1", payee=f"M{i}")
        for i in range(limit + 10)
    ]
    assert RULE_FANOUT in _reasons(_scored(spark, cfg, spread), f"T{limit + 9}")[0]


def test_odd_hour_reads_the_session_timezone(spark, cfg):
    """The pipeline runs in IST; 03:00 IST is the small hours, 03:00 UTC is 08:30."""
    scored = _scored(spark, cfg, [_txn("T1", when=datetime(2026, 8, 9, 3, 0))])
    assert RULE_ODD_HOUR in _reasons(scored, "T1")[0]

    daytime = _scored(spark, cfg, [_txn("T1", when=datetime(2026, 8, 9, 13, 0))])
    assert RULE_ODD_HOUR not in _reasons(daytime, "T1")[0]


# ----------------------------------------------------------------- scoring


def test_no_single_rule_reaches_the_high_band(spark, cfg):
    """The core design claim: one signal is a coincidence, several are a case."""
    weights = cfg.gold["fraud_rules"]
    high = int(cfg.gold["risk_bands"]["high"])
    for name, rule in weights.items():
        assert int(rule["weight"]) < high, f"{name} alone would raise an alert"


def test_weights_accumulate_and_reasons_list_every_rule_that_fired(spark, cfg):
    """A large payment at 3am from an IP serving many strangers: three rules, one alert."""
    rows = [
        _txn(f"P{i}", when=datetime(2026, 8, 9, 3, 0), payer=f"OTHER{i}", ip="IP1")
        for i in range(6)
    ]
    rows.append(_txn("T1", amount=120000.0, when=datetime(2026, 8, 9, 3, 0), ip="IP1"))

    reasons, points, band = _reasons(_scored(spark, cfg, rows), "T1")
    rules = cfg.gold["fraud_rules"]
    assert reasons == {RULE_HIGH_AMOUNT, RULE_SHARED_IP, RULE_ODD_HOUR}
    assert points == sum(
        int(rules[r]["weight"]) for r in (RULE_HIGH_AMOUNT, RULE_SHARED_IP, RULE_ODD_HOUR)
    )
    assert band == BAND_HIGH


def test_reasons_is_empty_rather_than_full_of_nulls(spark, cfg):
    """array_compact, not array — nulls in `reasons` would break every consumer."""
    row = _scored(spark, cfg, [_txn("T1")]).first()
    assert row["reasons"] == []


# ----------------------------------------------------------------- KPIs


def test_daily_volume_counts_only_successful_transactions(spark, cfg):
    """A failed payment moves no money; counting it inflates every revenue figure."""
    rows = [_txn("T1", amount=1000.0), _txn("T2", amount=9999.0, status="FAILED")]
    daily = kpi_daily(_scored(spark, cfg, rows)).first()

    assert daily["txn_count"] == 2
    assert daily["total_amount"] == 1000.0
    assert daily["success_count"] == 1
    assert daily["success_rate"] == 0.5
    assert daily["avg_ticket"] == 1000.0


def test_distinct_payers_is_a_count_of_people_not_transactions(spark, cfg):
    rows = [_txn("T1", payer="P1"), _txn("T2", payer="P1"), _txn("T3", payer="P2")]
    assert kpi_daily(_scored(spark, cfg, rows)).first()["distinct_payers"] == 2


def test_dimensional_kpis_cover_every_cut_without_losing_transactions(spark, cfg):
    rows = [_txn("T1"), _txn("T2")]
    dimensional = kpi_by_dimension(_scored(spark, cfg, rows))

    assert {r["dimension"] for r in dimensional.collect()} == {"payer_bank", "app", "city"}
    for dimension in ("payer_bank", "app", "city"):
        total = (
            dimensional.filter(F.col("dimension") == dimension).agg(F.sum("txn_count")).first()[0]
        )
        assert total == 2, f"{dimension} lost or duplicated rows"
