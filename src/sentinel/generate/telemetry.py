"""Step 0 — the UPI telemetry generator.

The specification supplies no dataset: "DATA WILL BE GENERATED, NO DATASET WOULD BE
GIVEN." Everything downstream therefore exists to solve problems this module
deliberately creates, in two flavours the spec names explicitly:

*Structural corruption* — nulls, whitespace, type mismatches, negative values. These
are defects of *form*. Silver fixes or quarantines them.

*Logical anomalies* — high amounts from suspicious IPs, velocity bursts, payee
fan-out, odd-hour activity. These are defects of *behaviour*: every field is
well-formed, and the transaction is still wrong. Gold scores them.

The two are injected independently, so a fraudulent transaction can also arrive
corrupted — which is the realistic case, and the reason the run report distinguishes
"fraud we failed to detect" from "fraud we never saw because the row was quarantined".

The transaction ids of injected anomalies are written to a truth file beside the raw
data. That label **never enters the payload**: it exists so detection can be measured
rather than asserted.

Pure Python, no Spark. The generator runs before a session exists.
"""

from __future__ import annotations

import json
import random
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

from sentinel.config import Config

# The pipeline reads and writes in IST; so does the simulated traffic.
IST = timedelta(hours=5, minutes=30)

# Anomaly types recorded in the truth file. Kept as constants because the run report
# groups detection rates by them.
FRAUD_HIGH_AMOUNT = "high_amount"
FRAUD_VELOCITY = "velocity"
FRAUD_FANOUT = "fanout"
FRAUD_ODD_HOUR = "odd_hour"

_STATUS_WEIGHTS = {"SUCCESS": 0.88, "FAILED": 0.09, "PENDING": 0.02, "REVERSED": 0.01}

# Fields safe to null out: absent, the transaction is still a transaction.
_OPTIONAL_FIELDS = (
    "city",
    "state",
    "merchant_category",
    "latency_ms",
    "app",
    "payee_bank",
    "device_id",
)
# Fields whose absence makes the row unusable. Silver must quarantine these.
_REQUIRED_FIELDS = ("transaction_id", "amount", "event_time")
# Fields that pick up stray whitespace from upstream systems.
_WHITESPACE_FIELDS = ("payer_vpa", "payee_vpa", "status", "currency", "city", "payer_bank")


@dataclass
class GenerationResult:
    """What a generator run produced, for the CLI to print and tests to assert on."""

    records: int
    files: list[Path]
    labels: dict[str, str]
    corruption_counts: dict[str, int] = field(default_factory=dict)

    @property
    def fraud_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for kind in self.labels.values():
            counts[kind] = counts.get(kind, 0) + 1
        return counts


@dataclass
class _Payer:
    vpa: str
    phone: str
    device_id: str
    bank: str
    ip: str
    city: str
    state: str


@dataclass
class _Merchant:
    vpa: str
    bank: str
    category: str


class TelemetryGenerator:
    """Synthesises UPI payloads with a known quantity of every defect."""

    def __init__(
        self,
        cfg: Config,
        scale: float = 1.0,
        seed: int = 42,
        end: datetime | None = None,
    ) -> None:
        self.cfg = cfg
        self.gen = cfg.generator
        self.scale = scale
        self.rng = random.Random(seed)

        self.n_transactions = max(1, int(self.gen["transactions"] * scale))
        self.n_payers = max(1, int(self.gen["payers"] * scale))
        self.n_merchants = max(1, int(self.gen["merchants"] * scale))
        self.days = int(self.gen["days"])

        # The window ends now and runs backwards, so generated data always looks recent
        # regardless of when the project is run. `end` is injectable because otherwise
        # the seed alone does not determine the output — two runs of the same seed
        # produce different timestamps — and a generator you cannot reproduce exactly
        # is a generator you cannot debug a pipeline against.
        self.end = end or datetime.now(UTC)
        self.start = self.end - timedelta(days=self.days)

        self.corruption = self.gen["corruption"]
        self.fraud = self.gen["fraud"]

        self._seq = 0
        self.labels: dict[str, str] = {}
        self.corruption_counts: dict[str, int] = {}

        self.payers = self._build_payers()
        self.merchants = self._build_merchants()
        # A small pool of IPs the anomalies operate from. Detection leans on the fact
        # that one IP fronting many distinct payers is not a household router.
        self.suspicious_ips = [
            self._random_ip() for _ in range(int(self.fraud["suspicious_ip_count"]))
        ]
        # ...and a smaller pool of legitimately shared IPs — office and cafe wifi.
        # Without these the shared-IP rule would have a perfect precision it has not
        # earned, and the run report's numbers would be theatre.
        self.public_ips = [self._random_ip() for _ in range(8)]

    # ------------------------------------------------------------------ actors

    def _random_ip(self) -> str:
        r = self.rng
        return f"{r.randint(14, 223)}.{r.randint(0, 255)}.{r.randint(0, 255)}.{r.randint(1, 254)}"

    def _build_payers(self) -> list[_Payer]:
        r = self.rng
        banks = self.gen["banks"]
        places = self.gen["cities"]
        payers = []
        for i in range(self.n_payers):
            bank = r.choice(banks)
            place = r.choice(places)
            payers.append(
                _Payer(
                    vpa=f"user{i:06d}@ok{bank.lower()}",
                    phone=f"{r.choice('6789')}{r.randint(0, 999999999):09d}",
                    device_id=f"DEV-{r.getrandbits(48):012x}",
                    bank=bank,
                    ip=self._random_ip(),
                    city=place["city"],
                    state=place["state"],
                )
            )
        return payers

    def _build_merchants(self) -> list[_Merchant]:
        r = self.rng
        banks = self.gen["banks"]
        categories = self.gen["merchant_categories"]
        return [
            _Merchant(
                vpa=f"merchant{i:05d}@{r.choice(['ybl', 'paytm', 'axl', 'ibl'])}",
                bank=r.choice(banks),
                category=r.choice(categories),
            )
            for i in range(self.n_merchants)
        ]

    # ------------------------------------------------------------------ payloads

    def _next_id(self) -> str:
        self._seq += 1
        return f"TXN{self._seq:012d}"

    def _random_instant(self) -> datetime:
        """A moment in the window, weighted towards Indian waking hours.

        Uniform timestamps would make the odd-hour rule meaningless: if 5 of every 24
        hours are 'unusual' by construction, flagging them tells you nothing.
        """
        span = (self.end - self.start).total_seconds()
        for _ in range(8):
            moment = self.start + timedelta(seconds=self.rng.uniform(0, span))
            ist_hour = (moment + IST).hour
            # Accept daytime immediately; accept the small hours only occasionally.
            if ist_hour >= 6 or self.rng.random() < 0.06:
                return moment
        return moment

    def _amount(self) -> float:
        """A plausible UPI ticket: mostly small, with a long right tail."""
        return round(self.rng.lognormvariate(6.0, 1.05), 2)

    def _base_record(
        self,
        payer: _Payer,
        merchant: _Merchant,
        when: datetime,
        *,
        amount: float | None = None,
        ip: str | None = None,
        status: str | None = None,
    ) -> dict[str, Any]:
        r = self.rng
        p2p = r.random() < 0.35
        if p2p:
            counterparty = r.choice(self.payers)
            payee_vpa, payee_bank, category = counterparty.vpa, counterparty.bank, None
        else:
            payee_vpa, payee_bank, category = merchant.vpa, merchant.bank, merchant.category

        if status is None:
            status = r.choices(list(_STATUS_WEIGHTS), weights=list(_STATUS_WEIGHTS.values()))[0]

        if ip is None:
            # Most traffic comes from the payer's own connection; a little arrives
            # through shared public wifi.
            ip = self.rng.choice(self.public_ips) if r.random() < 0.02 else payer.ip

        return {
            "transaction_id": self._next_id(),
            "event_time": when.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "payer_vpa": payer.vpa,
            "payee_vpa": payee_vpa,
            "amount": amount if amount is not None else self._amount(),
            "currency": "INR",
            "status": status,
            "txn_type": "P2P" if p2p else "P2M",
            "payer_bank": payer.bank,
            "payee_bank": payee_bank,
            "app": r.choice(self.gen["apps"]),
            "device_id": payer.device_id,
            "ip_address": ip,
            "city": payer.city,
            "state": payer.state,
            "payer_phone": payer.phone,
            "merchant_category": category,
            "latency_ms": int(abs(r.gauss(320, 140))) + 40,
        }

    # ------------------------------------------------------------------ anomalies

    def _label(self, record: dict[str, Any], kind: str) -> dict[str, Any]:
        """Record an injected anomaly in the truth file, never in the payload."""
        self.labels[str(record["transaction_id"])] = kind
        return record

    def _inject_high_amount(self) -> list[dict[str, Any]]:
        """The case the specification names: unusually high amounts from suspicious IPs."""
        lo, hi = self.fraud["high_amount_range"]
        n = int(self.n_transactions * float(self.fraud["high_amount_rate"]))
        out = []
        for _ in range(n):
            payer = self.rng.choice(self.payers)
            record = self._base_record(
                payer,
                self.rng.choice(self.merchants),
                self._random_instant(),
                amount=round(self.rng.uniform(lo, hi), 2),
                ip=self.rng.choice(self.suspicious_ips),
                status="SUCCESS",
            )
            out.append(self._label(record, FRAUD_HIGH_AMOUNT))
        return out

    def _inject_velocity(self) -> list[dict[str, Any]]:
        """A compromised credential draining an account in a few minutes."""
        lo, hi = self.fraud["velocity_burst_size"]
        window = int(self.fraud["velocity_burst_minutes"])
        out = []
        for _ in range(int(self.fraud["velocity_bursts"] * self.scale) or 1):
            payer = self.rng.choice(self.payers)
            ip = self.rng.choice(self.suspicious_ips)
            origin = self._random_instant()
            for _ in range(self.rng.randint(lo, hi)):
                when = origin + timedelta(seconds=self.rng.uniform(0, window * 60))
                record = self._base_record(
                    payer,
                    self.rng.choice(self.merchants),
                    when,
                    amount=round(self.rng.uniform(1500, 9000), 2),
                    ip=ip,
                    status="SUCCESS",
                )
                out.append(self._label(record, FRAUD_VELOCITY))
        return out

    def _inject_fanout(self) -> list[dict[str, Any]]:
        """Mule behaviour: one account pushing money to many unrelated payees."""
        lo, hi = self.fraud["fanout_payees"]
        out = []
        for _ in range(int(self.fraud["fanout_payers"] * self.scale) or 1):
            payer = self.rng.choice(self.payers)
            ip = self.rng.choice(self.suspicious_ips)
            origin = self._random_instant()
            for _ in range(self.rng.randint(lo, hi)):
                when = origin + timedelta(minutes=self.rng.uniform(0, 55))
                record = self._base_record(
                    payer,
                    self.rng.choice(self.merchants),
                    when,
                    amount=round(self.rng.uniform(2000, 15000), 2),
                    ip=ip,
                    status="SUCCESS",
                )
                # Fan-out means distinct payees, so override whatever _base_record chose.
                record["payee_vpa"] = self.rng.choice(self.payers).vpa
                record["txn_type"] = "P2P"
                out.append(self._label(record, FRAUD_FANOUT))
        return out

    def _inject_odd_hour(self) -> list[dict[str, Any]]:
        """Account takeover at 3am: large amounts, small hours, suspicious IP."""
        n = int(self.n_transactions * float(self.fraud["odd_hour_rate"]))
        out = []
        for _ in range(n):
            payer = self.rng.choice(self.payers)
            day = self.start + timedelta(days=self.rng.uniform(0, self.days))
            # Choose an IST hour in [0,5) and express it as the UTC instant.
            ist_moment = (day + IST).replace(
                hour=self.rng.randrange(0, 5),
                minute=self.rng.randrange(60),
                second=self.rng.randrange(60),
            )
            record = self._base_record(
                payer,
                self.rng.choice(self.merchants),
                ist_moment - IST,
                amount=round(self.rng.uniform(60000, 140000), 2),
                ip=self.rng.choice(self.suspicious_ips),
                status="SUCCESS",
            )
            out.append(self._label(record, FRAUD_ODD_HOUR))
        return out

    # ------------------------------------------------------------------ corruption

    def _count(self, kind: str) -> None:
        self.corruption_counts[kind] = self.corruption_counts.get(kind, 0) + 1

    def _corrupt(self, record: dict[str, Any]) -> dict[str, Any]:
        """Apply structural defects. Independent of whether the row is fraudulent."""
        r = self.rng
        c = self.corruption

        if r.random() < c["null_optional_rate"]:
            record[r.choice(_OPTIONAL_FIELDS)] = None
            self._count("null_optional")

        if r.random() < c["null_required_rate"]:
            record[r.choice(_REQUIRED_FIELDS)] = None
            self._count("null_required")

        if r.random() < c["whitespace_rate"]:
            field_name = r.choice(_WHITESPACE_FIELDS)
            if isinstance(record.get(field_name), str):
                pad = " " * r.randint(1, 3)
                record[field_name] = f"{pad}{record[field_name]}{pad}"
                self._count("whitespace")

        # A genuine type mismatch: the same field arrives as a JSON string from some
        # client versions and as a number from others.
        if record.get("amount") is not None and r.random() < c["amount_as_string_rate"]:
            record["amount"] = f"{record['amount']}"
            self._count("amount_as_string")

        if record.get("amount") is not None and r.random() < c["negative_amount_rate"]:
            record["amount"] = -abs(float(record["amount"]))
            self._count("negative_amount")
        elif record.get("amount") is not None and r.random() < c["zero_amount_rate"]:
            record["amount"] = 0
            self._count("zero_amount")

        if r.random() < c["case_noise_rate"]:
            if isinstance(record.get("status"), str):
                record["status"] = r.choice(
                    [record["status"].lower(), record["status"].title(), record["status"]]
                )
            if isinstance(record.get("currency"), str):
                record["currency"] = r.choice(["inr", "Inr", "INR"])
            self._count("case_noise")

        if isinstance(record.get("event_time"), str) and r.random() < c["alt_timestamp_rate"]:
            moment = datetime.strptime(record["event_time"], "%Y-%m-%dT%H:%M:%SZ").replace(
                tzinfo=UTC
            )
            if r.random() < 0.5:
                # Epoch milliseconds, as an SDK that skipped formatting entirely.
                record["event_time"] = str(int(moment.timestamp() * 1000))
                self._count("timestamp_epoch_millis")
            else:
                # Local wall-clock with no timezone at all — the worst common case.
                record["event_time"] = (moment + IST).strftime("%d-%m-%Y %H:%M:%S")
                self._count("timestamp_local_dmy")

        return record

    def _serialise(self, record: dict[str, Any]) -> str:
        """Render one record as a JSON line, occasionally truncating it."""
        line = json.dumps(record)
        if self.rng.random() < self.corruption["malformed_json_rate"]:
            self._count("malformed_json")
            return line[: max(8, int(len(line) * self.rng.uniform(0.3, 0.8)))]
        return line

    # ------------------------------------------------------------------ run

    def build(self) -> list[dict[str, Any]]:
        """Produce the full record set, corrupted and shuffled but not yet written."""
        records = [
            self._base_record(
                self.rng.choice(self.payers),
                self.rng.choice(self.merchants),
                self._random_instant(),
            )
            for _ in range(self.n_transactions)
        ]

        records += self._inject_high_amount()
        records += self._inject_velocity()
        records += self._inject_fanout()
        records += self._inject_odd_hour()

        # Retry storms: the same transaction re-emitted. Duplicated before corruption
        # so the copies diverge in form while sharing a transaction_id — which is
        # exactly what makes naive deduplication fail.
        duplicates = [
            dict(self.rng.choice(records))
            for _ in range(int(len(records) * self.corruption["duplicate_rate"]))
        ]
        self.corruption_counts["duplicate"] = len(duplicates)
        records += duplicates

        records = [self._corrupt(record) for record in records]
        self.rng.shuffle(records)
        return records

    def write(self, destination: Path | None = None) -> GenerationResult:
        """Generate and drop newline-delimited JSON into the raw zone.

        Written as several files, not one, because the layers downstream discover work
        file by file: a single file would make the streaming story untestable.
        """
        raw_dir = Path(destination or self.cfg.path("raw"))
        raw_dir.mkdir(parents=True, exist_ok=True)

        records = self.build()
        per_file = max(1, int(self.gen["records_per_file"]))
        # Timestamp for sortability, random suffix for uniqueness. A second-resolution
        # stamp alone is not unique: two runs inside the same second produced identical
        # filenames, and the second silently overwrote the first run's raw data *and*
        # its labels. Batches arriving back to back is the normal case for this
        # pipeline, not an edge one.
        stamp = f"{datetime.now(UTC).strftime('%Y%m%dT%H%M%S')}-{uuid4().hex[:6]}"

        files: list[Path] = []
        for index in range(0, len(records), per_file):
            chunk = records[index : index + per_file]
            path = raw_dir / f"upi_{stamp}_{index // per_file:04d}.json"
            path.write_text("\n".join(self._serialise(r) for r in chunk) + "\n")
            files.append(path)

        self._write_labels(stamp)

        return GenerationResult(
            records=len(records),
            files=files,
            labels=dict(self.labels),
            corruption_counts=dict(self.corruption_counts),
        )

    def _write_labels(self, stamp: str) -> None:
        """Persist injected-anomaly labels beside the data, never inside it.

        Appended across runs: generating a second batch must not erase the first
        batch's ground truth, or the report silently starts scoring against half the
        answers.
        """
        truth_dir = Path(self.cfg.path("truth"))
        truth_dir.mkdir(parents=True, exist_ok=True)
        (truth_dir / f"fraud_labels_{stamp}.json").write_text(
            json.dumps({"generated_at": stamp, "labels": self.labels}, indent=2)
        )


def load_labels(cfg: Config) -> dict[str, str]:
    """Read every truth file written so far. Empty when none exist."""
    truth_dir = Path(cfg.path("truth"))
    if not truth_dir.exists():
        return {}
    labels: dict[str, str] = {}
    for path in sorted(truth_dir.glob("fraud_labels_*.json")):
        labels.update(json.loads(path.read_text()).get("labels", {}))
    return labels
