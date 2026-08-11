"""Silver's cleansing rules, tested on hand-written rows.

These use fixed inputs rather than generated data: the point is to pin down exactly
what each rule does to each defect, including the awkward cases the generator only
produces occasionally.
"""

from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from sentinel.schemas import PAYLOAD_FIELDS
from sentinel.silver import mask_phone, mask_vpa, parse_amount, parse_event_time, standardise

pytestmark = pytest.mark.spark

# The Bronze contract: every payload field as a string, plus the columns Bronze adds.
BRONZE_COLUMNS = [*PAYLOAD_FIELDS, "raw_payload", "source_file", "ingested_at", "is_parseable"]


def _bronze_row(**overrides):
    row = {name: None for name in PAYLOAD_FIELDS}
    row.update(
        {
            "transaction_id": "TXN1",
            "event_time": "2026-08-09T14:17:09Z",
            "payer_vpa": "user000012@oksbi",
            "payee_vpa": "merchant00005@ybl",
            "amount": "842.50",
            "currency": "INR",
            "status": "SUCCESS",
            "txn_type": "P2M",
            "payer_bank": "SBI",
            "payer_phone": "9876543210",
            "device_id": "DEV-abc",
            "ip_address": "10.0.0.1",
            "city": "pune",
            "latency_ms": "310",
        }
    )
    row.update(overrides)
    row.update(
        {
            "raw_payload": "{}",
            "source_file": "file:///raw/x.json",
            "ingested_at": None,
            "is_parseable": overrides.get("is_parseable", True),
        }
    )
    return row


def _frame(spark, rows):
    schema = ", ".join(
        f"{name} timestamp"
        if name == "ingested_at"
        else f"{name} boolean"
        if name == "is_parseable"
        else f"{name} string"
        for name in BRONZE_COLUMNS
    )
    return spark.createDataFrame([tuple(row[c] for c in BRONZE_COLUMNS) for row in rows], schema)


# ----------------------------------------------------------------- type handling


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("2026-08-09T14:17:09Z", "2026-08-09 19:47:09"),  # UTC, rendered in IST
        ("1786285029000", "2026-08-09 19:47:09"),  # the same instant, epoch millis
        ("09-08-2026 19:47:09", "2026-08-09 19:47:09"),  # IST wall clock, no timezone
    ],
)
def test_the_three_timestamp_formats_resolve_to_one_instant(spark, raw, expected):
    """All three encode the same moment. If they did not agree, every daily KPI would
    be wrong for 12% of rows and nothing would report an error."""
    df = spark.createDataFrame([(raw,)], "t string")
    parsed = df.select(parse_event_time(F.col("t")).alias("ts")).first()["ts"]
    assert parsed.strftime("%Y-%m-%d %H:%M:%S") == expected


def test_unrecognised_timestamp_becomes_null_rather_than_a_wrong_date(spark):
    """Silence beats a plausible lie: a null is quarantined, a wrong date is served."""
    df = spark.createDataFrame([("not a date",), ("2026/08/09",)], "t string")
    assert df.select(parse_event_time(F.col("t")).alias("ts")).filter("ts is not null").count() == 0


def test_amount_parses_from_both_json_number_and_json_string(spark):
    df = spark.createDataFrame([("842.5",), ("842.50",), ("-91.2",)], "a string")
    values = [r["v"] for r in df.select(parse_amount(F.col("a")).alias("v")).collect()]
    assert values == [842.5, 842.5, -91.2]


# ----------------------------------------------------------------- masking


def test_vpa_masking_keeps_the_bank_handle_and_hides_the_person(spark):
    df = spark.createDataFrame([("user000012@oksbi",)], "v string")
    assert df.select(mask_vpa(F.col("v")).alias("m")).first()["m"] == "us***@oksbi"


def test_phone_masking_keeps_only_the_last_four_digits(spark):
    df = spark.createDataFrame([("9876543210",)], "p string")
    masked = df.select(mask_phone(F.col("p")).alias("m")).first()["m"]
    assert masked == "XXXXXX3210"
    assert "987654" not in masked


def test_silver_carries_no_unmasked_identifier_columns(spark, cfg):
    """The strongest available check: the raw columns are simply not in the output."""
    from sentinel.silver import _valid

    result = _valid(standardise(_frame(spark, [_bronze_row()]), cfg), cfg)
    for leaked in ("payer_vpa", "payee_vpa", "payer_phone", "device_id", "ip_address"):
        assert leaked not in result.columns


def test_hashes_are_stable_across_rows(spark, cfg):
    """Gold groups by ip_hash, which only works if the same IP hashes identically."""
    rows = [_bronze_row(transaction_id="A"), _bronze_row(transaction_id="B")]
    result = standardise(_frame(spark, rows), cfg).select("ip_hash").distinct()
    assert result.count() == 1


# ----------------------------------------------------------------- standardising


def test_whitespace_and_case_are_normalised(spark, cfg):
    row = _bronze_row(status="  success  ", currency=" inr ", city="  pune ")
    result = standardise(_frame(spark, [row]), cfg).first()
    assert result["status"] == "SUCCESS"
    assert result["currency"] == "INR"
    assert result["city"] == "Pune"


def test_whitespace_only_values_become_null(spark, cfg):
    """An empty string passes every null check and then fails a cast much later."""
    result = standardise(_frame(spark, [_bronze_row(city="   ")]), cfg).first()
    assert result["city"] is None


def test_app_brand_capitalisation_is_preserved(spark, cfg):
    """GPay and PhonePe are brands; folding their case would corrupt a dimension."""
    result = standardise(_frame(spark, [_bronze_row(app=" GPay ")]), cfg).first()
    assert result["app"] == "GPay"


# ----------------------------------------------------------------- quarantine


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"is_parseable": False}, "malformed_json"),
        ({"transaction_id": None}, "missing_transaction_id"),
        ({"event_time": None}, "missing_event_time"),
        ({"event_time": "2026/08/09 10:00"}, "unparseable_event_time"),
        ({"amount": None}, "missing_amount"),
        ({"amount": "abc"}, "unparseable_amount"),
        ({"amount": "-500"}, "non_positive_amount"),
        ({"amount": "0"}, "non_positive_amount"),
        ({"amount": "99999999"}, "amount_exceeds_limit"),
        ({"status": "TELEPORTED"}, "invalid_status"),
        ({"currency": "USD"}, "invalid_currency"),
    ],
)
def test_each_defect_gets_its_own_reject_reason(spark, cfg, overrides, reason):
    result = standardise(_frame(spark, [_bronze_row(**overrides)]), cfg).first()
    assert result["reject_reason"] == reason


def test_a_clean_row_has_no_reject_reason(spark, cfg):
    assert standardise(_frame(spark, [_bronze_row()]), cfg).first()["reject_reason"] is None


def test_a_malformed_line_is_reported_as_malformed_not_as_a_missing_field(spark, cfg):
    """Both are true of a garbage line; only the first is useful."""
    row = _bronze_row(is_parseable=False, transaction_id=None, amount=None)
    assert standardise(_frame(spark, [row]), cfg).first()["reject_reason"] == "malformed_json"
