# Databricks notebook source
# MAGIC %md
# MAGIC # Project Sentinel — Step 1: Raw to Landing
# MAGIC
# MAGIC Reads the Volume with Auto Loader and lands every line untransformed into `landing.upi_raw`. Nothing is parsed here: a malformed line must reach durable
# MAGIC storage before anyone tries to interpret it.
# MAGIC
# MAGIC The stage function called below is the same one `sentinel-run landing` calls locally
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

from sentinel import landing
from sentinel.config import load_config

cfg = load_config(env)
run_id = run_id or uuid.uuid4().hex[:12]
print(f"env={cfg.env} run_id={run_id}")

# COMMAND ----------

counts = landing.run(spark, cfg, run_id)
print(counts)

dbutils.notebook.exit(str(counts))
