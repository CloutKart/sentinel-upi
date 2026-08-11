"""The generator's contract with the rest of the pipeline.

Every layer downstream exists to handle defects this module injects, so these tests
assert the defects are actually there. Without them a refactor could quietly stop
injecting negative amounts, Silver's quarantine would empty out, and the pipeline
would look like it had improved.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from sentinel.config import load_config
from sentinel.generate.telemetry import (
    FRAUD_FANOUT,
    FRAUD_HIGH_AMOUNT,
    FRAUD_ODD_HOUR,
    FRAUD_VELOCITY,
    IST,
    TelemetryGenerator,
    load_labels,
)

SCALE = 0.05


@pytest.fixture(scope="module")
def cfg():
    return load_config("local")


@pytest.fixture(scope="module")
def records(cfg):
    return TelemetryGenerator(cfg, scale=SCALE, seed=7).build()


def test_run_is_reproducible(cfg):
    """Same seed and same window, same data.

    The window anchor has to be pinned too: by default it is ``now``, so seed alone
    does not determine the timestamps. Debugging a pipeline over shifting input is
    hopeless, which is why `end` is a parameter at all.
    """
    end = datetime(2026, 8, 11, 12, 0, tzinfo=UTC)
    first = TelemetryGenerator(cfg, scale=0.01, seed=99, end=end).build()
    second = TelemetryGenerator(cfg, scale=0.01, seed=99, end=end).build()
    assert first == second


def test_volume_scales(cfg):
    generator = TelemetryGenerator(cfg, scale=SCALE, seed=7)
    expected = int(cfg.generator["transactions"] * SCALE)
    # Injected anomalies and duplicates are additional, so the total exceeds the base.
    assert len(generator.build()) > expected


def test_every_corruption_type_is_injected(cfg, records):
    """Each configured defect appears at least once, so no layer is tested against zero."""
    generator = TelemetryGenerator(cfg, scale=SCALE, seed=7)
    generator.build()
    for kind in (
        "null_optional",
        "null_required",
        "whitespace",
        "amount_as_string",
        "negative_amount",
        "case_noise",
        "duplicate",
    ):
        assert generator.corruption_counts.get(kind, 0) > 0, f"{kind} was never injected"


def test_amount_arrives_as_both_number_and_string(records):
    """The type mismatch Silver's tolerant cast exists for."""
    amounts = [r["amount"] for r in records if r["amount"] is not None]
    assert any(isinstance(a, str) for a in amounts)
    assert any(isinstance(a, (int, float)) for a in amounts)


def test_negative_and_zero_amounts_are_present(records):
    """Silver must quarantine these; they have to exist for that to mean anything."""
    numeric = [float(r["amount"]) for r in records if r["amount"] is not None]
    assert any(a < 0 for a in numeric)
    assert any(a == 0 for a in numeric)


def test_three_timestamp_formats_are_emitted(records):
    """ISO-8601, epoch millis and a bare local dd-MM-yyyy, all in the same feed."""
    times = [r["event_time"] for r in records if isinstance(r["event_time"], str)]
    assert any("T" in t and t.endswith("Z") for t in times)
    assert any(t.isdigit() and len(t) == 13 for t in times)
    assert any(len(t) == 19 and t[2] == "-" and t[5] == "-" for t in times)


def test_whitespace_and_case_noise_survive_into_the_payload(records):
    strings = [r["status"] for r in records if isinstance(r["status"], str)]
    assert any(s != s.strip() for s in strings), "no whitespace padding injected"
    assert any(s.strip() != s.strip().upper() for s in strings), "no case noise injected"


def test_required_fields_are_sometimes_null(records):
    """The rows Silver quarantines rather than repairs."""
    assert any(r["transaction_id"] is None for r in records)
    assert any(r["amount"] is None for r in records)
    assert any(r["event_time"] is None for r in records)


def test_duplicate_transaction_ids_exist(records):
    """Retry storms. Silver's watermarked dedupe collapses these."""
    ids = [r["transaction_id"] for r in records if r["transaction_id"]]
    assert len(ids) > len(set(ids))


def test_all_four_anomaly_types_are_injected(cfg):
    generator = TelemetryGenerator(cfg, scale=SCALE, seed=7)
    generator.build()
    kinds = set(generator.labels.values())
    assert kinds == {FRAUD_HIGH_AMOUNT, FRAUD_VELOCITY, FRAUD_FANOUT, FRAUD_ODD_HOUR}


def test_labels_never_leak_into_the_payload(cfg):
    """The whole point of the truth file.

    A label in the payload would make Gold's detection numbers meaningless, and the
    mistake is invisible until someone asks why recall is 1.0.
    """
    generator = TelemetryGenerator(cfg, scale=SCALE, seed=7)
    records = generator.build()
    assert generator.labels
    forbidden = {"is_fraud", "fraud", "label", "injected_type", "is_anomaly"}
    for record in records:
        assert not forbidden & set(record)


def test_high_amount_anomalies_exceed_the_scoring_floor(cfg):
    """Injected amounts must clear the rule's floor, or detection is untestable."""
    generator = TelemetryGenerator(cfg, scale=SCALE, seed=7)
    records = generator.build()
    floor = float(cfg.gold["fraud_rules"]["high_amount"]["floor"])
    labelled = {
        r["transaction_id"]
        for r in records
        if generator.labels.get(str(r["transaction_id"])) == FRAUD_HIGH_AMOUNT
    }
    amounts = [
        float(r["amount"])
        for r in records
        if r["transaction_id"] in labelled and r["amount"] is not None
    ]
    assert amounts
    # Corruption can flip an injected amount negative after the fact, so this asserts
    # the rule rather than every single row.
    assert min(abs(a) for a in amounts) > floor


def test_odd_hour_anomalies_land_in_the_small_hours(cfg):
    """The rule reads IST; the payload is UTC. This catches an offset error."""
    generator = TelemetryGenerator(cfg, scale=SCALE, seed=7)
    records = generator.build()
    end = int(cfg.gold["fraud_rules"]["odd_hour"]["end_hour"])

    hours = []
    for record in records:
        if generator.labels.get(str(record["transaction_id"])) != FRAUD_ODD_HOUR:
            continue
        raw = record["event_time"]
        if not isinstance(raw, str) or not raw.endswith("Z"):
            continue  # corrupted into another format; covered by the Spark tests
        hours.append((datetime.strptime(raw, "%Y-%m-%dT%H:%M:%SZ") + IST).hour)

    assert hours
    assert all(hour < end for hour in hours)


def test_write_produces_files_and_a_truth_file(cfg, tmp_path, monkeypatch):
    """Several files, not one — the streaming layers discover work file by file."""
    monkeypatch.setitem(cfg.paths, "raw", str(tmp_path / "raw"))
    monkeypatch.setitem(cfg.paths, "truth", str(tmp_path / "truth"))

    result = TelemetryGenerator(cfg, scale=0.02, seed=3).write()

    assert len(result.files) >= 1
    lines = [line for path in result.files for line in path.read_text().splitlines() if line]
    assert len(lines) == result.records

    labels = load_labels(cfg)
    assert labels == result.labels

    parsed = sum(1 for line in lines if _is_json(line))
    assert parsed < len(lines), "no malformed lines — Bronze's corrupt-record path is untested"


def test_back_to_back_runs_do_not_overwrite_each_other(cfg, tmp_path, monkeypatch):
    """Two batches generated inside the same second must both survive.

    Filenames were stamped to the second, so a second run overwrote the first run's
    raw files and its labels — and back-to-back batches are the normal case here, not
    an edge one.
    """
    monkeypatch.setitem(cfg.paths, "raw", str(tmp_path / "raw"))
    monkeypatch.setitem(cfg.paths, "truth", str(tmp_path / "truth"))

    first = TelemetryGenerator(cfg, scale=0.02, seed=1).write()
    second = TelemetryGenerator(cfg, scale=0.02, seed=2).write()

    assert not set(first.files) & set(second.files)

    combined = load_labels(cfg)
    assert set(first.labels) <= set(combined)
    assert set(second.labels) <= set(combined)

    # Both batches' data is still on disk, not just their labels.
    landed = sum(
        1
        for path in (tmp_path / "raw").glob("*.json")
        for line in path.read_text().splitlines()
        if line
    )
    assert landed == first.records + second.records


def _is_json(line: str) -> bool:
    try:
        json.loads(line)
    except ValueError:
        return False
    return True
