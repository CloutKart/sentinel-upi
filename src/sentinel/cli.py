"""``sentinel-run`` — drive one pipeline stage, or all of them.

Each stage is a plain function taking ``(spark, cfg, run_id)``. The Databricks
notebooks call exactly these functions, so the code that runs on a cluster is the code
the tests exercise here.
"""

from __future__ import annotations

import argparse
import time
import uuid

from sentinel import bronze, gold, landing, report, silver
from sentinel.config import load_config
from sentinel.spark import get_spark, stop_spark

STAGES = {
    "landing": landing.run,
    "bronze": bronze.run,
    "silver": silver.run,
    "gold": gold.run,
}

ORDER = ("landing", "bronze", "silver", "gold")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a Project Sentinel pipeline stage.")
    parser.add_argument("stage", choices=[*ORDER, "all", "report"])
    parser.add_argument("--env", default=None, help="override SENTINEL_ENV")
    parser.add_argument(
        "--run-id",
        default=None,
        help="identifier stamped onto every row this run writes; defaults to a new uuid",
    )
    args = parser.parse_args(argv)

    cfg = load_config(args.env)
    run_id = args.run_id or uuid.uuid4().hex[:12]
    spark = get_spark(cfg)

    try:
        if args.stage == "report":
            report.print_report(spark, cfg)
            return 0

        stages = ORDER if args.stage == "all" else (args.stage,)
        print(f"env={cfg.env} run_id={run_id}")

        for name in stages:
            started = time.monotonic()
            counts = STAGES[name](spark, cfg, run_id)
            elapsed = time.monotonic() - started
            summary = "  ".join(f"{k}={v:,}" for k, v in counts.items())
            print(f"  {name:<8} {elapsed:6.1f}s  {summary}")
    finally:
        stop_spark()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
