"""Step 3 — Bronze to Silver.

"Fix structural corruption and standardize the dataset: handle null values and type
mismatches, mask PII, filter out invalid records."

Three things happen here, in this order:

*Standardise.* Trim the whitespace, fold the casing, and turn the string columns
Bronze preserved into real types. ``amount`` arrives as both ``842.50`` and
``"842.50"``; ``event_time`` arrives in three formats including one with no timezone
at all. Both are resolved by shape, not by hope.

*Mask.* Identifiers never reach Silver in the clear. VPAs and the phone number are
partially masked so a human can still recognise a record; device id and IP address
are salted one-way hashes. The hashes are stable, which is what lets Gold group by IP
without Gold ever seeing one.

*Filter.* Invalid records are written to a quarantine table with the reason attached,
not deleted. "Filter out" and "throw away" are different instructions, and a pipeline
that cannot say what it discarded cannot be trusted about what it kept.

Finally the retry storms are deduplicated: the same ``transaction_id`` re-sent within
the watermark collapses to one row.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import Column
from pyspark.sql import functions as F

from sentinel import bronze, tables
from sentinel.config import Config

if TYPE_CHECKING:  # pragma: no cover - typing only
    from pyspark.sql import DataFrame, SparkSession

TABLE = "upi_transactions"
QUARANTINE_TABLE = "upi_rejects"
STAGE_SILVER = "silver"
STAGE_QUARANTINE = "silver_quarantine"

# Shapes the generator emits, matched before parsing so a value is only ever handed to
# a parser that can read it. Letting one format fall through to another parser is how
# 11-08-2026 quietly becomes the year 11.
_ISO_8601 = r"^\d{4}-\d{2}-\d{2}T"
_EPOCH_MILLIS = r"^\d{13}$"
_EPOCH_SECONDS = r"^\d{10}$"
_LOCAL_DMY = r"^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$"


def _clean(name: str) -> Column:
    """Trim a string column, mapping the empty string to null.

    Whitespace-only is absence, not a value. Without this, `` "" `` survives every
    null check downstream and then fails a cast three layers later.
    """
    trimmed = F.trim(F.col(name))
    return F.when(trimmed == "", None).otherwise(trimmed)


def parse_event_time(column: Column) -> Column:
    """Resolve the three emitted timestamp formats into one instant.

    ISO-8601 carries its own ``Z``. Epoch values are unambiguous. The bare
    ``dd-MM-yyyy HH:mm:ss`` has no timezone at all, so it is interpreted in the
    session timezone — Asia/Kolkata, which is where the device that sent it was.
    """
    return (
        F.when(column.rlike(_ISO_8601), F.to_timestamp(column))
        .when(column.rlike(_EPOCH_MILLIS), F.timestamp_millis(column.cast("long")))
        .when(column.rlike(_EPOCH_SECONDS), F.timestamp_seconds(column.cast("long")))
        .when(column.rlike(_LOCAL_DMY), F.to_timestamp(column, "dd-MM-yyyy HH:mm:ss"))
    )


def parse_amount(column: Column) -> Column:
    """Cast an amount that may have arrived as a JSON number or a JSON string.

    The cast handles both — this function exists to be explicit that the type mismatch
    is expected and handled, rather than leaving a bare ``.cast()`` that reads like an
    accident.
    """
    return column.cast("double")


def mask_vpa(column: Column) -> Column:
    """``user000012@oksbi`` -> ``us***@oksbi``.

    The handle is kept because it identifies the bank, which is analytically useful
    and not personal. The local part is not.
    """
    local = F.substring_index(column, "@", 1)
    handle = F.substring_index(column, "@", -1)
    return F.when(column.isNotNull(), F.concat(F.substring(local, 1, 2), F.lit("***@"), handle))


def mask_phone(column: Column) -> Column:
    """Keep the last four digits — enough for a support agent, useless to anyone else."""
    return F.when(column.isNotNull(), F.concat(F.lit("XXXXXX"), F.substring(column, -4, 4)))


def hash_value(column: Column, salt: str) -> Column:
    """Salted SHA-256, truncated. Stable across runs, so it still works as a join key.

    Truncated to 16 hex characters purely for readability; collisions at this width are
    irrelevant for grouping a few million rows, and the full digest makes every sample
    output unreadable.
    """
    return F.when(
        column.isNotNull(), F.substring(F.sha2(F.concat(F.lit(salt), column), 256), 1, 16)
    )


def reject_reason(cfg: Config) -> Column:
    """First failing rule wins, so every quarantined row has exactly one reason.

    Ordered from structural to semantic: a line that was never JSON should be reported
    as such, not as "missing transaction_id", which would be true but useless.
    """
    max_amount = float(cfg.silver["max_amount"])
    statuses = [s.upper() for s in cfg.silver["valid_statuses"]]
    currencies = [c.upper() for c in cfg.silver["valid_currencies"]]

    return (
        F.when(~F.col("is_parseable"), "malformed_json")
        .when(F.col("transaction_id").isNull(), "missing_transaction_id")
        .when(F.col("event_time_raw").isNull(), "missing_event_time")
        .when(F.col("event_time").isNull(), "unparseable_event_time")
        .when(F.col("amount_raw").isNull(), "missing_amount")
        .when(F.col("amount").isNull(), "unparseable_amount")
        .when(F.col("amount") <= 0, "non_positive_amount")
        .when(F.col("amount") > max_amount, "amount_exceeds_limit")
        .when(~F.col("status").isin(statuses), "invalid_status")
        .when(~F.col("currency").isin(currencies), "invalid_currency")
    )


def standardise(df: DataFrame, cfg: Config) -> DataFrame:
    """Trim, fold case, type and mask. Adds ``reject_reason``; drops nothing."""
    salt = str(cfg.silver["pii_salt"])

    typed = (
        df.withColumn("transaction_id", _clean("transaction_id"))
        .withColumn("event_time_raw", _clean("event_time"))
        .withColumn("amount_raw", _clean("amount"))
        .withColumn("event_time", parse_event_time(F.col("event_time_raw")))
        .withColumn("amount", parse_amount(F.col("amount_raw")))
        # Casing is noise from the client, not information: "success", " Success "
        # and "SUCCESS" are one status.
        .withColumn("status", F.upper(_clean("status")))
        .withColumn("currency", F.upper(_clean("currency")))
        .withColumn("txn_type", F.upper(_clean("txn_type")))
        .withColumn("payer_bank", F.upper(_clean("payer_bank")))
        .withColumn("payee_bank", F.upper(_clean("payee_bank")))
        # App names are brands with deliberate internal capitals — GPay, PhonePe — so
        # they are trimmed and otherwise left exactly as sent.
        .withColumn("app", _clean("app"))
        .withColumn("city", F.initcap(_clean("city")))
        .withColumn("state", F.upper(_clean("state")))
        .withColumn("merchant_category", _clean("merchant_category"))
        .withColumn("latency_ms", _clean("latency_ms").cast("int"))
    )

    masked = (
        typed.withColumn("payer_vpa_masked", mask_vpa(_clean("payer_vpa")))
        .withColumn("payee_vpa_masked", mask_vpa(_clean("payee_vpa")))
        .withColumn("payer_phone_masked", mask_phone(_clean("payer_phone")))
        .withColumn("payer_key", hash_value(_clean("payer_vpa"), salt))
        .withColumn("payee_key", hash_value(_clean("payee_vpa"), salt))
        .withColumn("device_hash", hash_value(_clean("device_id"), salt))
        .withColumn("ip_hash", hash_value(_clean("ip_address"), salt))
    )

    return masked.withColumn("reject_reason", reject_reason(cfg))


def _valid(df: DataFrame, cfg: Config) -> DataFrame:
    """The rows that passed, deduplicated, with the raw and PII columns removed."""
    watermark = str(cfg.silver["watermark"])

    return (
        df.filter(F.col("reject_reason").isNull())
        .withColumn("event_date", F.date_format("event_time", "yyyy-MM-dd"))
        .withColumn("cleansed_at", F.current_timestamp())
        # Retry storms re-send a transaction under the same id. Bounding the dedupe by
        # the watermark keeps the streaming state finite — an unbounded distinct over
        # every id ever seen grows without limit and eventually kills the job.
        .withWatermark("event_time", watermark)
        .dropDuplicatesWithinWatermark(["transaction_id"])
        .select(
            "transaction_id",
            "event_time",
            "event_date",
            "amount",
            "currency",
            "status",
            "txn_type",
            "payer_bank",
            "payee_bank",
            "app",
            "city",
            "state",
            "merchant_category",
            "latency_ms",
            "payer_vpa_masked",
            "payee_vpa_masked",
            "payer_phone_masked",
            "payer_key",
            "payee_key",
            "device_hash",
            "ip_hash",
            "ingested_at",
            "cleansed_at",
            "source_file",
        )
    )


def _rejected(df: DataFrame) -> DataFrame:
    """The rows that failed, kept verbatim so the reason can be checked by hand."""
    return df.filter(F.col("reject_reason").isNotNull()).select(
        "transaction_id",
        F.col("event_time_raw").alias("event_time"),
        F.col("amount_raw").alias("amount"),
        "status",
        "currency",
        "reject_reason",
        F.current_timestamp().alias("rejected_at"),
        "raw_payload",
        "source_file",
        "ingested_at",
    )


def run(spark: SparkSession, cfg: Config, run_id: str) -> dict[str, int]:
    """Cleanse Bronze into Silver, quarantining what fails. Returns counts."""
    tables.ensure_namespaces(spark, cfg)

    before_clean = tables.count(spark, cfg, "silver", TABLE)
    before_reject = tables.count(spark, cfg, "quarantine", QUARANTINE_TABLE)

    # Two queries over the same source rather than one foreachBatch writing both
    # sinks. It reads Bronze twice, which at this scale costs less than the stateful
    # complexity of the alternative, and each stream keeps an independent checkpoint —
    # so a failure in one cannot leave the other silently ahead of it.
    source = tables.read_stream(spark, cfg, "bronze", bronze.TABLE)
    prepared = standardise(source, cfg)

    tables.write_stream(
        _valid(prepared, cfg), cfg, "silver", TABLE, stage=STAGE_SILVER, partition_by=["event_date"]
    )
    tables.write_stream(
        _rejected(prepared), cfg, "quarantine", QUARANTINE_TABLE, stage=STAGE_QUARANTINE
    )

    after_clean = tables.count(spark, cfg, "silver", TABLE)
    after_reject = tables.count(spark, cfg, "quarantine", QUARANTINE_TABLE)

    return {
        "cleansed": after_clean - before_clean,
        "quarantined": after_reject - before_reject,
        "total": after_clean,
    }
