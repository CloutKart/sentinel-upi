"""``sentinel-gen`` — drop simulated UPI telemetry into the raw zone."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime

from sentinel.config import load_config
from sentinel.generate.telemetry import TelemetryGenerator


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate raw UPI telemetry (Step 0).")
    parser.add_argument("--scale", type=float, default=1.0, help="multiplier on configured volume")
    parser.add_argument("--seed", type=int, default=42, help="RNG seed")
    parser.add_argument(
        "--end",
        default=None,
        help=(
            "ISO-8601 UTC instant the simulated window ends at; defaults to now. "
            "Pin it alongside --seed for a byte-identical run."
        ),
    )
    parser.add_argument("--env", default=None, help="override SENTINEL_ENV")
    args = parser.parse_args(argv)

    end = datetime.fromisoformat(args.end).replace(tzinfo=UTC) if args.end else None

    cfg = load_config(args.env)
    result = TelemetryGenerator(cfg, scale=args.scale, seed=args.seed, end=end).write()

    print(f"wrote {result.records:,} records to {cfg.path('raw')} in {len(result.files)} files")

    print("\ninjected structural corruption")
    for kind, n in sorted(result.corruption_counts.items()):
        print(f"  {kind:<24} {n:>8,}")

    print("\ninjected fraud anomalies (labels in the truth file, not the payload)")
    for kind, n in sorted(result.fraud_counts.items()):
        print(f"  {kind:<24} {n:>8,}")
    print(f"  {'total':<24} {len(result.labels):>8,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
