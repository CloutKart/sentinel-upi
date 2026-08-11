"""Explicit schemas for every layer.

Bronze reads the payload as **all strings**. That is deliberate: the generator emits
``amount`` as both a number and a string, and timestamps in three formats. Inferring
a schema over that either fails the batch or silently nulls whichever variant loses,
and neither is acceptable in a layer whose job is to preserve the raw data. Typing
happens once, in Silver, where a failed cast is quarantined with a reason.

Schema inference is also avoided for a second reason: it costs an extra pass over the
data and, in a stream, can change between micro-batches.
"""

from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Fields as the payer's device emits them. Order matches the generator's output.
PAYLOAD_FIELDS: tuple[str, ...] = (
    "transaction_id",
    "event_time",
    "payer_vpa",
    "payee_vpa",
    "amount",
    "currency",
    "status",
    "txn_type",
    "payer_bank",
    "payee_bank",
    "app",
    "device_id",
    "ip_address",
    "city",
    "state",
    "payer_phone",
    "merchant_category",
    "latency_ms",
)

# The column `from_json` parks an unparseable line in. It must be declared in the
# schema for PERMISSIVE mode to populate it — and it is the only way to tell a line
# that was never JSON apart from one that was valid but missing fields, because
# from_json returns an all-null *struct* for both, not a null.
CORRUPT_RECORD_COLUMN = "_corrupt_record"

# Everything a string. See the module docstring.
PAYLOAD_SCHEMA = StructType(
    [StructField(name, StringType(), True) for name in PAYLOAD_FIELDS]
    + [StructField(CORRUPT_RECORD_COLUMN, StringType(), True)]
)

# Landing: the untransformed line plus provenance. No payload columns at all — Step 1
# of the specification lands raw data "without applying any transformations", and
# parsing a line into columns is a transformation.
LANDING_SCHEMA = StructType(
    [
        StructField("raw_payload", StringType(), False),
        StructField("source_file", StringType(), True),
        StructField("landed_at", TimestampType(), False),
        StructField("landing_run_id", StringType(), False),
    ]
)

# Silver: typed and cleansed. PII columns are the masked or hashed derivatives, never
# the originals.
SILVER_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("event_time", TimestampType(), False),
        StructField("event_date", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("currency", StringType(), True),
        StructField("status", StringType(), True),
        StructField("txn_type", StringType(), True),
        StructField("payer_bank", StringType(), True),
        StructField("payee_bank", StringType(), True),
        StructField("app", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("latency_ms", IntegerType(), True),
        # Masked / hashed identifiers.
        StructField("payer_vpa_masked", StringType(), True),
        StructField("payee_vpa_masked", StringType(), True),
        StructField("payer_phone_masked", StringType(), True),
        StructField("payer_key", StringType(), True),
        StructField("payee_key", StringType(), True),
        StructField("device_hash", StringType(), True),
        StructField("ip_hash", StringType(), True),
        # Lineage.
        StructField("ingested_at", TimestampType(), True),
        StructField("cleansed_at", TimestampType(), True),
        StructField("source_file", StringType(), True),
    ]
)

# Quarantine: the Bronze row exactly as it was, plus why it was rejected. Rejected
# rows are kept rather than dropped so the run report can say what fell out and why.
QUARANTINE_EXTRA_FIELDS = StructType(
    [
        StructField("reject_reason", StringType(), False),
        StructField("rejected_at", TimestampType(), False),
    ]
)

# Gold fact: every Silver column plus the scoring output.
SCORING_FIELDS = StructType(
    [
        StructField("fraud_score", IntegerType(), False),
        StructField("risk_band", StringType(), False),
        StructField("reasons", ArrayType(StringType()), False),
    ]
)

KPI_DAILY_SCHEMA = StructType(
    [
        StructField("event_date", StringType(), False),
        StructField("txn_count", LongType(), False),
        StructField("total_amount", DoubleType(), False),
        StructField("success_count", LongType(), False),
        StructField("success_rate", DoubleType(), False),
        StructField("avg_ticket", DoubleType(), False),
        StructField("distinct_payers", LongType(), False),
        StructField("flagged_count", LongType(), False),
        StructField("flagged_amount", DoubleType(), False),
    ]
)
