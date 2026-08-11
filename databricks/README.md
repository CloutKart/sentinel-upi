# Deploying to Databricks

The pipeline is the same code in both places. What changes is `conf/databricks.yaml`,
which points the zones at Unity Catalog instead of `./data` and switches the landing
reader from a plain file stream to Auto Loader.

**This has not been run against a live workspace** — there are no workspace credentials
on the machine this was built on. The steps below are what the deployment consists of;
treat the first run as unverified.

## 1. Authenticate

```bash
databricks configure          # host + personal access token
databricks current-user me    # confirm
```

## 2. Build and upload the wheel

Everything the pipeline does lives in this wheel; the notebooks only call into it.

```bash
uv build
databricks fs mkdir dbfs:/Volumes/sentinel/raw/libs
databricks fs cp dist/sentinel-0.1.0-py3-none-any.whl \
  dbfs:/Volumes/sentinel/raw/libs/ --overwrite
```

The wheel declares **no** runtime dependencies on purpose — see the comment in
`pyproject.toml`. pyspark, delta-spark and pyyaml all come from Databricks Runtime 15.4
LTS, and installing our own copies over the runtime's breaks the cluster's compiled
modules before any pipeline code runs.

`conf/` is force-included into the wheel, so the cluster reads the same thresholds the
local runs use with nothing extra to deploy.

## 3. Create the catalog, schemas and Volume

`notebooks/00_generate` creates the catalog, the `raw` schema and the `upi_drop` Volume
itself, and every stage calls `tables.ensure_namespaces`. So this is optional — but if
you would rather the job not hold `CREATE CATALOG`:

```sql
CREATE CATALOG IF NOT EXISTS sentinel;
CREATE SCHEMA  IF NOT EXISTS sentinel.raw;
CREATE SCHEMA  IF NOT EXISTS sentinel.landing;
CREATE SCHEMA  IF NOT EXISTS sentinel.bronze;
CREATE SCHEMA  IF NOT EXISTS sentinel.silver;
CREATE SCHEMA  IF NOT EXISTS sentinel.gold;
CREATE SCHEMA  IF NOT EXISTS sentinel.quarantine;
CREATE VOLUME  IF NOT EXISTS sentinel.raw.upi_drop;
```

## 4. Upload the notebooks and create the job

```bash
databricks workspace import-dir notebooks /Workspace/Shared/sentinel/notebooks --overwrite
databricks jobs create --json @databricks/job_sentinel_pipeline.json
databricks jobs run-now <job-id>
```

## What is different on the cluster

| | Local | Databricks |
|---|---|---|
| Table address | `./data/silver/upi_transactions` | `sentinel.silver.upi_transactions` |
| Raw drop zone | `./data/raw` | `/Volumes/sentinel/raw/upi_drop` |
| Landing reader | `text` file stream | `cloudFiles` (Auto Loader) |
| Checkpoints | `./data/checkpoints/<stage>` | `/Volumes/sentinel/raw/_checkpoints/<stage>` |
| Spark session | built by `spark.py` with Delta jars | already exists; settings applied to it |

No module under `src/sentinel` reads any of this directly. `config.py` resolves it and
`tables.py` is the only place that knows a table can be either a path or a name.

## Before running this against anything real

`conf/base.yaml` sets `silver.pii_salt` to a literal string. It is the salt for the
device and IP hashes, so anyone holding it can confirm a guessed IP by recomputing the
digest. On a real deployment read it from a secret scope:

```python
pii_salt = dbutils.secrets.get(scope="sentinel", key="pii_salt")
```

and pass it through rather than committing it.
