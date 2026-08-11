# Databricks notebook source
# MAGIC %md
# MAGIC # Project Sentinel — Step 2: Landing to Bronze
# MAGIC
# MAGIC Parses each landed line into columns — all of them strings — and stamps ingestion
# MAGIC metadata on it. Structure without cleaning: the messy values are preserved exactly
# MAGIC as sent, because Silver's quarantine reasons have to point at something.
# MAGIC
# MAGIC The stage function called below is the same one `sentinel-run bronze` calls locally
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

from sentinel import bronze
from sentinel.config import load_config

cfg = load_config(env)
run_id = run_id or uuid.uuid4().hex[:12]
print(f"env={cfg.env} run_id={run_id}")

# COMMAND ----------

counts = bronze.run(spark, cfg, run_id)
print(counts)

dbutils.notebook.exit(str(counts))
