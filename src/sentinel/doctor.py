"""Toolchain check: can this machine actually start Spark and write Delta?

Run before the pipeline, not during it. A JDK/Spark/Delta mismatch otherwise surfaces
as an opaque JVM crash somewhere inside a streaming query, and the real cause — a
system JDK too new for Spark 3.5 — is nowhere in the traceback.

    make doctor
"""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
from pathlib import Path

from sentinel.config import load_config
from sentinel.spark import get_spark, is_databricks, stop_spark


def main() -> int:
    cfg = load_config()
    print(f"env             {cfg.env}")
    print(f"databricks      {is_databricks()}")

    try:
        spark = get_spark(cfg)
    except Exception as exc:  # pragma: no cover - environment failure path
        print(f"\nFAILED to start Spark: {exc}", file=sys.stderr)
        print(
            "\nMost likely cause: JAVA_HOME points at a JDK newer than 17, which "
            "Spark 3.5 does not support.\nRun `make jdk`, then re-run with "
            "JAVA_HOME=$HOME/.local/jdks/jdk-17.",
            file=sys.stderr,
        )
        return 1

    print(f"spark           {spark.version}")
    print(f"java_home       {os.environ.get('JAVA_HOME', '(inherited)')}")

    tmp = Path(tempfile.mkdtemp(prefix="sentinel-doctor-"))
    try:
        spark.range(5).write.format("delta").mode("overwrite").save(str(tmp / "probe"))
        rows = spark.read.format("delta").load(str(tmp / "probe")).count()
        print(f"delta           OK (round-tripped {rows} rows)")
    except Exception as exc:  # pragma: no cover - environment failure path
        print(f"\nFAILED to write Delta: {exc}", file=sys.stderr)
        print(
            "\nCheck that delta-spark matches Spark: 3.2.0 pairs with 3.5.x.",
            file=sys.stderr,
        )
        return 1
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        stop_spark()

    print("\nToolchain OK.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
