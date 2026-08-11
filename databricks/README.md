# Deploying to Databricks

The pipeline is the same code in both places. What changes is `conf/databricks.yaml`,
which points the zones at Unity Catalog instead of `./data` and switches the landing
reader from a plain file stream to Auto Loader.

```bash
databricks auth login --host https://<your-workspace-host>   # once
make deploy-dry    # render and validate everything, touch nothing
make deploy        # wheel + notebooks + job
make deploy-run    # ...and trigger it
```

> **Not yet run against a live workspace.** There were no workspace credentials on the
> machine this was built on. Every CLI invocation in `deploy.sh` was checked against
> `databricks --help` for v1.11.0, `--dry-run` exercises the whole script except the
> remote calls, and `tests/unit/test_deploy.py` covers the template rendering — but no
> cluster has ever executed it. **Start with `make deploy-dry`.**

## What `deploy.sh` does

| Step | |
|---|---|
| **Preflight** | CLI present, authenticated, `uv` available. Fails here rather than with a 401 six steps later |
| **Compute** | Reads the workspace host, infers the cloud, and picks a matching `node_type_id` |
| **Wheel** | `uv build`, then asserts `conf/` is inside it before anything is uploaded |
| **Catalog** | Creates the catalog, the six schemas and the five volumes, skipping whatever already exists |
| **Publish** | Uploads the wheel to a volume and the notebooks to the workspace |
| **Job** | Looks the job up by name — creates it, or updates that job in place |
| **Run** | Optional, with `--run` |

Every step is idempotent. Running it twice does not produce two jobs, two volumes, or
two copies of anything — the job lookup by name is what prevents a workspace slowly
filling with duplicates that all run on the same schedule.

## Options

```
--profile NAME     CLI profile (default: $DATABRICKS_PROFILE, else the CLI default)
--catalog NAME     target catalog (default: sentinel)
--serverless       omit job_clusters entirely — required on Free Edition
--node-type ID     override the cloud-derived node type
--num-workers N    workers per job cluster (default: 2)
--spark-version V  runtime (default: 15.4.x-scala2.12)
--scale N          generator scale baked into the job's defaults
--out PATH         write the rendered job definition for review or diffing
--run              trigger the job after deploying, and wait
--dry-run          render and validate without calling the workspace
```

Anything you would otherwise pass repeatedly has an env var: `SENTINEL_CATALOG`,
`SENTINEL_NODE_TYPE`, `SENTINEL_NOTEBOOK_DIR`, `SENTINEL_SPARK_VERSION`,
`SENTINEL_NUM_WORKERS`, `SENTINEL_SCALE`, `DATABRICKS_PROFILE`.

## Which compute

**Free Edition is serverless-only** and has no all-purpose clusters, so a job
declaring `job_clusters` fails there. Use `--serverless`, which removes the block and
every `job_cluster_key` rather than trying to paper over it with a cluster definition
the workspace will reject.

Otherwise the node type comes from the workspace host, because node type ids are
entirely cloud-specific and an Azure id on AWS fails at *cluster start* — minutes into
the first run, with an error that never mentions the cloud:

| Host | Cloud | Default |
|---|---|---|
| `*.azuredatabricks.net` | Azure | `Standard_DS3_v2` |
| `*.gcp.databricks.com` | GCP | `n2-highmem-4` |
| `*.cloud.databricks.com` | AWS | `m5d.large` |

If the host cannot be read the script says so, warns that it is falling back to an
Azure id, and continues — pass `--node-type` or `--serverless`.

## What gets created

```
sentinel                                 catalog
├── raw                                  schema
│   ├── upi_drop                         volume — the generator's JSON drop zone
│   ├── checkpoints                      volume — streaming checkpoints
│   ├── truth                            volume — injected-anomaly labels
│   ├── autoloader_schema                volume — Auto Loader schema inference
│   └── libs                             volume — the wheel
├── landing · bronze · silver · gold · quarantine    schemas
└── job "sentinel_pipeline"              5 tasks, generate → landing → bronze → silver → gold
```

`tests/unit/test_deploy.py` asserts this list against `conf/databricks.yaml`, so a
volume added to the config but not to the script fails a test rather than failing
partway through a run as a path error.

If your workspace admin pre-provisions the catalog, the create steps warn and continue
rather than aborting — the deployer legitimately may not hold `CREATE CATALOG`.

## The wheel

Everything the pipeline does lives in it; the notebooks only call into it. It declares
**no** runtime dependencies on purpose — see the comment in `pyproject.toml`. pyspark,
delta-spark and pyyaml all come from Databricks Runtime 15.4 LTS, and installing our
own copies over the runtime's breaks the cluster's compiled modules before any pipeline
code runs.

`conf/` is force-included into the wheel, so the cluster reads the same thresholds the
local runs use with nothing extra to deploy. The script verifies that before upload,
because a wheel missing `conf/` installs cleanly and then fails on the first config
read, minutes into a run.

It goes to a Unity Catalog volume rather than DBFS root: DBFS root is deprecated, and
on a UC-enabled workspace installing libraries from it is restricted by cluster access
mode — which surfaces as a library-install failure at cluster start, not at deploy.

## Doing it by hand

Every step has a UI equivalent if you would rather not use the CLI: upload the wheel
through Catalog → Volumes, import the notebooks through Workspace → Import, and create
the job with Workflows → Create job → *Import from JSON*, pasting the output of
`./databricks/deploy.sh --dry-run --out job.json`.

## What is different on the cluster

| | Local | Databricks |
|---|---|---|
| Table address | `./data/silver/upi_transactions` | `sentinel.silver.upi_transactions` |
| Raw drop zone | `./data/raw` | `/Volumes/sentinel/raw/upi_drop` |
| Landing reader | `text` file stream | `cloudFiles` (Auto Loader) |
| Checkpoints | `./data/checkpoints/<stage>` | `/Volumes/sentinel/raw/checkpoints/<stage>` |
| Spark session | built by `spark.py` with Delta jars | already exists; settings applied to it |

No module under `src/sentinel` reads any of this directly. `config.py` resolves it and
`tables.py` is the only place that knows a table can be either a path or a name.

## Before running this against anything real

`conf/base.yaml` sets `silver.pii_salt` to a literal string, and that value is now
public in this repository — treat it as burned rather than as a placeholder to promote.
It is the salt for the device and IP hashes, so anyone holding it can confirm a guessed
IP by recomputing the digest. On a real deployment read it from a secret scope:

```python
pii_salt = dbutils.secrets.get(scope="sentinel", key="pii_salt")
```

and pass it through rather than committing it.

## Why a script and not a bundle

Databricks Asset Bundles (`databricks bundle deploy`) are the official declarative
route and would replace most of this script. It is a shell script because that matches
how the rest of this repository is operated, because the failure modes above are
easier to explain in sequence than as bundle configuration, and because it keeps the
job definition a plain JSON file you can read. If this grows past one job and one
environment, a bundle is the better shape — `databricks bundle generate job
--existing-job-id <id>` will produce it from whatever this script has already created.
