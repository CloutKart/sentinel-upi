# Databricks notebook source
# MAGIC %md
# MAGIC # Project Sentinel — Step 3: Bronze to Silver
# MAGIC
# MAGIC Standardises casing and whitespace, resolves the type mismatches, masks PII, and
# MAGIC routes invalid records to `quarantine.upi_rejects` with a reason attached rather
# MAGIC than deleting them.
# MAGIC
# MAGIC The stage function called below is the same one `sentinel-run silver` calls locally
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

from sentinel import silver
from sentinel.config import load_config

cfg = load_config(env)
run_id = run_id or uuid.uuid4().hex[:12]
print(f"env={cfg.env} run_id={run_id}")

# COMMAND ----------

counts = silver.run(spark, cfg, run_id)
print(counts)

dbutils.notebook.exit(str(counts))
