# Databricks notebook source
# MAGIC %md
# MAGIC # Project Sentinel — Step 4: Silver to Gold
# MAGIC
# MAGIC Recomputes the KPI tables and the fraud scores. Batch rather than streaming: these
# MAGIC are full aggregations over Delta, and a stateful streaming equivalent would be far
# MAGIC more machinery than the result justifies.
# MAGIC
# MAGIC The stage function called below is the same one `sentinel-run gold` calls locally
# MAGIC and the same one the pytest suite covers.

# COMMAND ----------

dbutils.widgets.text("env", "databricks")
dbutils.widgets.text("run_id", "")

env = dbutils.widgets.get("env") or "databricks"
run_id = dbutils.widgets.get("run_id")

# COMMAND ----------

import os
import uuid

os.environ["SENTINEL_ENV"] = env

from sentinel import gold
from sentinel.config import load_config

cfg = load_config(env)
run_id = run_id or uuid.uuid4().hex[:12]
print(f"env={cfg.env} run_id={run_id}")

# COMMAND ----------

counts = gold.run(spark, cfg, run_id)
print(counts)

dbutils.notebook.exit(str(counts))
