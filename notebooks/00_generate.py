# Databricks notebook source
# MAGIC %md
# MAGIC # Project Sentinel — Step 0: telemetry generation
# MAGIC
# MAGIC Drops simulated UPI JSON payloads into the Unity Catalog Volume the rest of the
# MAGIC pipeline reads from.
# MAGIC
# MAGIC Every notebook here is deliberately thin. All logic lives in the `sentinel` wheel
# MAGIC installed on the cluster, so the code running on this cluster is the code the pytest
# MAGIC suite exercises locally. A notebook that reimplements pipeline logic cannot be unit
# MAGIC tested, and drifts from the local version the moment either one is edited.

# COMMAND ----------

dbutils.widgets.text("env", "databricks")
dbutils.widgets.text("scale", "1.0")
dbutils.widgets.text("seed", "42")

env = dbutils.widgets.get("env") or "databricks"
scale = float(dbutils.widgets.get("scale") or 1.0)
seed = int(dbutils.widgets.get("seed") or 42)

# COMMAND ----------

import os

os.environ["SENTINEL_ENV"] = env

from sentinel.config import load_config
from sentinel.generate.telemetry import TelemetryGenerator

cfg = load_config(env)
print(f"env={cfg.env} catalog={cfg.catalog} volume={cfg.path('raw')}")

# COMMAND ----------

# The Volume must exist before anything can be written to it. Creating it here rather
# than by hand keeps the job runnable against an empty workspace.
spark.sql(f"CREATE CATALOG IF NOT EXISTS {cfg.catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.raw")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.catalog}.raw.upi_drop")

# COMMAND ----------

result = TelemetryGenerator(cfg, scale=scale, seed=seed).write()

print(f"wrote {result.records:,} records in {len(result.files)} files")
print(f"corruption: {result.corruption_counts}")
print(f"anomalies:  {result.fraud_counts}")

dbutils.notebook.exit(str(result.records))
