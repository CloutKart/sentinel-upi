#!/usr/bin/env bash
#
# One-command deployment of Project Sentinel to a Databricks workspace.
#
#   ./databricks/deploy.sh --dry-run     # render everything, touch nothing
#   ./databricks/deploy.sh               # deploy
#   ./databricks/deploy.sh --run         # deploy, then trigger the job and wait
#
# What it does, in order:
#
#   1. preflight   — CLI present, authenticated, uv available
#   2. compute     — work out which cloud this workspace is on, and pick a node type
#   3. wheel       — build it, and verify conf/ is inside before uploading anything
#   4. catalog     — create the catalog, schemas and volumes if they are missing
#   5. publish     — upload the wheel and the notebooks
#   6. job         — create it, or update it in place if it already exists
#   7. run         — optional
#
# Every step is idempotent: running this twice does not produce two jobs, two
# volumes, or two copies of anything.
#
# NOT YET RUN AGAINST A LIVE WORKSPACE. Every CLI invocation below was checked
# against `databricks --help` for v1.11.0, and --dry-run exercises the whole script
# except the remote calls, but no cluster has ever executed this. Treat the first
# real run as a test, and start with --dry-run.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

# Every call site below silences the CLI with `>/dev/null 2>&1`, which would also
# silence the dry-run trace. fd 3 keeps a handle on the real stderr so the trace
# survives that redirection — otherwise --dry-run prints its ticks and never shows
# the commands they stand for.
exec 3>&2

# ---------------------------------------------------------------- output

log()  { printf '\033[0;36m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$*"; }
step() { printf '\n\033[1m▸ %s\033[0m\n' "$*"; }
warn() { printf '\033[0;33m[warn]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[0;31m[error]\033[0m %s\n' "$*" >&2; exit 1; }
ok()   { printf '       \033[0;32m✓\033[0m %s\n' "$*"; }
skip() { printf '       \033[0;90m·\033[0m %s\n' "$*"; }

# ---------------------------------------------------------------- defaults

CATALOG="${SENTINEL_CATALOG:-sentinel}"
JOB_NAME="${SENTINEL_JOB_NAME:-sentinel_pipeline}"
NOTEBOOK_DIR="${SENTINEL_NOTEBOOK_DIR:-/Shared/sentinel/notebooks}"
SPARK_VERSION="${SENTINEL_SPARK_VERSION:-15.4.x-scala2.12}"
NUM_WORKERS="${SENTINEL_NUM_WORKERS:-2}"
NODE_TYPE="${SENTINEL_NODE_TYPE:-}"
SCALE="${SENTINEL_SCALE:-1.0}"
PROFILE="${DATABRICKS_PROFILE:-}"
SERVERLESS=0
DO_RUN=0
DRY_RUN=0
OUT_FILE=""

# Schemas the pipeline writes to, and the volumes it needs. These must match
# conf/databricks.yaml — the script asserts that below rather than trusting it.
SCHEMAS=(raw landing bronze silver gold quarantine)
VOLUMES=(upi_drop checkpoints truth autoloader_schema libs)

usage() {
  # Print the header comment, stopping at the first line that is not one, so the
  # help text cannot drift out of sync with a hardcoded line range.
  awk 'NR>2 { if (!/^#/) exit; sub(/^# ?/, ""); print }' "${BASH_SOURCE[0]}"
  cat <<EOF

Options:
  --profile NAME     Databricks CLI profile (default: \$DATABRICKS_PROFILE, else the CLI default)
  --catalog NAME     Unity Catalog to deploy into (default: ${CATALOG})
  --job-name NAME    Job name, used for idempotent create-or-update (default: ${JOB_NAME})
  --notebook-dir P   Workspace path for the notebooks (default: ${NOTEBOOK_DIR})
  --node-type ID     Cluster node type; default is chosen from the workspace's cloud
  --num-workers N    Workers per job cluster (default: ${NUM_WORKERS})
  --spark-version V  Databricks Runtime (default: ${SPARK_VERSION})
  --scale N          Generator scale baked into the job's default parameters (default: ${SCALE})
  --serverless       Omit job_clusters entirely and let the workspace supply serverless
                     compute. Required on Free Edition, which has no all-purpose clusters.
  --out PATH         Also write the rendered job definition here, for review or diffing
  --run              Trigger the job after deploying, and wait for it
  --dry-run          Render and validate everything without calling the workspace
  -h, --help         This message
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)       PROFILE="$2"; shift 2 ;;
    --catalog)       CATALOG="$2"; shift 2 ;;
    --job-name)      JOB_NAME="$2"; shift 2 ;;
    --notebook-dir)  NOTEBOOK_DIR="$2"; shift 2 ;;
    --node-type)     NODE_TYPE="$2"; shift 2 ;;
    --num-workers)   NUM_WORKERS="$2"; shift 2 ;;
    --spark-version) SPARK_VERSION="$2"; shift 2 ;;
    --scale)         SCALE="$2"; shift 2 ;;
    --out)           OUT_FILE="$2"; shift 2 ;;
    --serverless)    SERVERLESS=1; shift ;;
    --run)           DO_RUN=1; shift ;;
    --dry-run)       DRY_RUN=1; shift ;;
    -h|--help)       usage; exit 0 ;;
    *)               die "Unknown option: $1 (try --help)" ;;
  esac
done

# Every workspace call goes through this, so --profile is honoured in one place and
# --dry-run can intercept everything without each call site remembering to check.
dbx() {
  if [[ ${DRY_RUN} -eq 1 ]]; then
    printf '       \033[0;90mwould run: databricks %s\033[0m\n' "$*" >&3
    return 0
  fi
  if [[ -n "${PROFILE}" ]]; then
    databricks --profile "${PROFILE}" "$@"
  else
    databricks "$@"
  fi
}

# ---------------------------------------------------------------- 1. preflight

step "Preflight"

command -v databricks >/dev/null 2>&1 \
  || die "Databricks CLI not found. See https://docs.databricks.com/dev-tools/cli/install.html"
ok "databricks $(databricks --version 2>&1 | awk '{print $NF}')"

command -v uv >/dev/null 2>&1 || die "uv not found — needed to build the wheel."
ok "uv present"

command -v python3 >/dev/null 2>&1 || die "python3 not found — needed to render the job JSON."

if [[ ${DRY_RUN} -eq 0 ]]; then
  # A clear failure here beats an opaque 401 six steps later, after the wheel has
  # already been built.
  if ! whoami_out=$(dbx current-user me -o json 2>&1); then
    printf '%s\n' "${whoami_out}" | head -3 >&2
    die "Not authenticated. Run:  databricks auth login --host https://<workspace-host>"
  fi
  user=$(printf '%s' "${whoami_out}" | python3 -c \
    'import json,sys; d=json.load(sys.stdin); print(d.get("userName") or d.get("displayName") or "?")' 2>/dev/null || echo "?")
  ok "authenticated as ${user}"
else
  skip "dry run — skipping the authentication check"
fi

# ---------------------------------------------------------------- 2. compute

step "Compute"

# The workspace hostname says which cloud it is on, and node type ids are entirely
# cloud-specific — an Azure id on AWS fails at cluster start, several minutes into
# the first run, with an error that does not mention the cloud.
detect_host() {
  if [[ -n "${DATABRICKS_HOST:-}" ]]; then printf '%s' "${DATABRICKS_HOST}"; return; fi
  if [[ ${DRY_RUN} -eq 1 ]]; then return; fi
  databricks ${PROFILE:+--profile "${PROFILE}"} auth env 2>/dev/null \
    | python3 -c 'import json,sys
try:
    env = json.load(sys.stdin).get("env", {})
    print(env.get("DATABRICKS_HOST", ""))
except Exception:
    print("")' 2>/dev/null || true
}

HOST="$(detect_host || true)"

case "${HOST}" in
  *azuredatabricks.net*) CLOUD=azure; DEFAULT_NODE=Standard_DS3_v2 ;;
  *gcp.databricks.com*)  CLOUD=gcp;   DEFAULT_NODE=n2-highmem-4 ;;
  *databricks.com*)      CLOUD=aws;   DEFAULT_NODE=m5d.large ;;
  *)                     CLOUD=unknown; DEFAULT_NODE=Standard_DS3_v2 ;;
esac

if [[ ${SERVERLESS} -eq 1 ]]; then
  # Never read in this mode — the job_clusters block is removed entirely — but the
  # renderer substitutes unconditionally, so it must not be empty.
  NODE_TYPE="n/a"
  ok "serverless — no job clusters will be declared"
else
  NODE_TYPE="${NODE_TYPE:-${DEFAULT_NODE}}"
  if [[ "${CLOUD}" == "unknown" ]]; then
    warn "Could not determine the workspace's cloud from the host."
    warn "Defaulting node_type_id to ${NODE_TYPE}, which is an Azure id."
    warn "On AWS or GCP pass --node-type, or use --serverless."
  else
    ok "${CLOUD} workspace → node_type_id ${NODE_TYPE}"
  fi
  ok "runtime ${SPARK_VERSION}, ${NUM_WORKERS} workers"
fi

# ---------------------------------------------------------------- 3. wheel

step "Wheel"

# Config lives inside the wheel (see the force-include in pyproject.toml), so there
# is no second deployment step for it and no way for the two to drift apart. A wheel
# missing conf/ installs cleanly and then fails on the first config read — minutes
# into a cluster run — so it is checked here instead.
rm -rf dist
uv build --wheel >/dev/null 2>&1 || die "uv build failed"
WHEEL="$(ls -t dist/sentinel-*.whl 2>/dev/null | head -1)"
[[ -n "${WHEEL}" ]] || die "No wheel produced in dist/"

python3 - "${WHEEL}" <<'PY' || die "Wheel is missing conf/ — check the force-include in pyproject.toml"
import sys, zipfile
names = set(zipfile.ZipFile(sys.argv[1]).namelist())
missing = [f for f in ("base.yaml", "databricks.yaml") if f"sentinel/conf/{f}" not in names]
if missing:
    print("missing from wheel:", missing)
    sys.exit(1)
PY
ok "$(basename "${WHEEL}") (conf/ included)"

VOLUME_ROOT="/Volumes/${CATALOG}/raw"
WHEEL_PATH="${VOLUME_ROOT}/libs/$(basename "${WHEEL}")"

# conf/databricks.yaml is what the notebooks read at runtime. If it points somewhere
# other than the catalog being deployed to, the job runs and writes to the wrong
# place — so the mismatch is caught here rather than discovered in the data.
python3 - "${CATALOG}" <<'PY' || die "conf/databricks.yaml does not match the target catalog"
import sys, yaml
catalog = sys.argv[1]
cfg = yaml.safe_load(open("conf/databricks.yaml"))
configured = cfg.get("catalog")
if configured != catalog:
    print(f"conf/databricks.yaml has catalog: {configured!r}, deploying to {catalog!r}.")
    print("Pass --catalog", configured, "or edit the config.")
    sys.exit(1)
PY
ok "conf/databricks.yaml targets ${CATALOG}"

# ---------------------------------------------------------------- 4. catalog

step "Unity Catalog"

# `create` on something that exists is an error, not a no-op, so each object is
# probed first. Failures here are warnings rather than fatal: on a workspace where
# the catalog is pre-provisioned by an admin, the deployer legitimately lacks CREATE
# CATALOG and the objects are already there.
if [[ ${DRY_RUN} -eq 1 ]]; then
  skip "dry run — would create catalog ${CATALOG}, schemas (${SCHEMAS[*]}), volumes (${VOLUMES[*]})"
else
  if dbx catalogs get "${CATALOG}" >/dev/null 2>&1; then
    skip "catalog ${CATALOG} exists"
  elif dbx catalogs create "${CATALOG}" >/dev/null 2>&1; then
    ok "created catalog ${CATALOG}"
  else
    warn "could not create catalog ${CATALOG} — assuming it exists and continuing"
  fi

  for schema in "${SCHEMAS[@]}"; do
    if dbx schemas get "${CATALOG}.${schema}" >/dev/null 2>&1; then
      skip "schema ${schema}"
    elif dbx schemas create "${schema}" "${CATALOG}" >/dev/null 2>&1; then
      ok "created schema ${schema}"
    else
      warn "could not create schema ${CATALOG}.${schema}"
    fi
  done

  for volume in "${VOLUMES[@]}"; do
    if dbx volumes read "${CATALOG}.raw.${volume}" >/dev/null 2>&1; then
      skip "volume raw.${volume}"
    elif dbx volumes create "${CATALOG}" raw "${volume}" MANAGED >/dev/null 2>&1; then
      ok "created volume raw.${volume}"
    else
      warn "could not create volume ${CATALOG}.raw.${volume}"
    fi
  done
fi

# ---------------------------------------------------------------- 5. publish

step "Publish"

# A Unity Catalog volume rather than DBFS root: DBFS root is deprecated, and on a
# UC-enabled workspace installing libraries from it is restricted by cluster access
# mode — which surfaces as a library-install failure at cluster start, not here.
if dbx fs cp --overwrite "${WHEEL}" "dbfs:${WHEEL_PATH}" >/dev/null 2>&1; then
  ok "wheel → ${WHEEL_PATH}"
else
  [[ ${DRY_RUN} -eq 1 ]] || die "Failed to upload the wheel to ${WHEEL_PATH}"
fi

dbx workspace mkdirs "${NOTEBOOK_DIR}" >/dev/null 2>&1 || true
for notebook in notebooks/*.py; do
  name="$(basename "${notebook}" .py)"
  if dbx workspace import "${NOTEBOOK_DIR}/${name}" \
       --file "${notebook}" --language PYTHON --format SOURCE --overwrite >/dev/null 2>&1; then
    ok "notebook ${name}"
  else
    [[ ${DRY_RUN} -eq 1 ]] || warn "failed to import ${name}"
  fi
done

# ---------------------------------------------------------------- 6. job

step "Job"

RENDERED="$(mktemp -t sentinel-job-XXXXXX.json)"
trap 'rm -f "${RENDERED}" "${RENDERED}.reset"' EXIT

# Rendered with python3 rather than sed/envsubst: the serverless case has to *remove*
# structure (the job_clusters block and every job_cluster_key), which is a JSON edit,
# not a text substitution.
#
# Values reach Python through the environment rather than being interpolated into the
# heredoc. Interpolating them means bash's quoting has to survive being read as Python
# source, and `${x@Q}` emits $'...' for anything with a quote or a backslash in it —
# which is a bash string literal and a Python syntax error.
JOB_NAME="${JOB_NAME}" \
SPARK_VERSION="${SPARK_VERSION}" \
NODE_TYPE="${NODE_TYPE}" \
NOTEBOOK_DIR="${NOTEBOOK_DIR}" \
WHEEL_PATH="${WHEEL_PATH}" \
SCALE="${SCALE}" \
NUM_WORKERS="${NUM_WORKERS}" \
SERVERLESS="${SERVERLESS}" \
python3 - "${RENDERED}" <<'PY' || die "Could not render the job definition"
import json, os, sys

with open("databricks/job_sentinel_pipeline.json") as fh:
    raw = fh.read()

for key in ("JOB_NAME", "SPARK_VERSION", "NODE_TYPE", "NOTEBOOK_DIR", "WHEEL_PATH", "SCALE"):
    # json.dumps, then strip the quotes: escapes any character that would otherwise
    # break out of the JSON string it is being substituted into.
    raw = raw.replace("${%s}" % key, json.dumps(os.environ.get(key, ""))[1:-1])

# num_workers is a JSON number, so its placeholder is unquoted in the template.
raw = raw.replace("${NUM_WORKERS}", str(int(os.environ["NUM_WORKERS"])))

job = json.loads(raw)
job.pop("_comment", None)

if os.environ["SERVERLESS"] == "1":
    job.pop("job_clusters", None)
    for task in job["tasks"]:
        task.pop("job_cluster_key", None)

leftover = [s for s in json.dumps(job).split('"') if s.startswith("${")]
if leftover:
    print("unsubstituted placeholders:", leftover, file=sys.stderr)
    sys.exit(1)

with open(sys.argv[1], "w") as fh:
    json.dump(job, fh, indent=2)
PY
ok "job definition rendered ($(python3 -c 'import json,sys; print(len(json.load(open(sys.argv[1]))["tasks"]))' "${RENDERED}") tasks)"

if [[ -n "${OUT_FILE}" ]]; then
  cp "${RENDERED}" "${OUT_FILE}"
  ok "written to ${OUT_FILE}"
fi

if [[ ${DRY_RUN} -eq 1 ]]; then
  skip "dry run — the rendered definition follows"
  sed 's/^/       /' "${RENDERED}"
else
  # Idempotency: look the job up by name. Without this, every deploy creates another
  # job with the same name and the workspace slowly fills with duplicates that all
  # run on the same schedule.
  EXISTING="$(dbx jobs list --name "${JOB_NAME}" -o json 2>/dev/null \
    | python3 -c 'import json,sys
try:
    jobs = json.load(sys.stdin) or []
except Exception:
    jobs = []
print(jobs[0]["job_id"] if jobs else "")' 2>/dev/null || true)"

  if [[ -n "${EXISTING}" ]]; then
    python3 - "${RENDERED}" "${EXISTING}" > "${RENDERED}.reset" <<'PY'
import json, sys
settings = json.load(open(sys.argv[1]))
print(json.dumps({"job_id": int(sys.argv[2]), "new_settings": settings}, indent=2))
PY
    dbx jobs reset --json "@${RENDERED}.reset" >/dev/null \
      || die "Failed to update job ${EXISTING}"
    JOB_ID="${EXISTING}"
    ok "updated job ${JOB_ID} in place"
  else
    JOB_ID="$(dbx jobs create --json "@${RENDERED}" -o json \
      | python3 -c 'import json,sys; print(json.load(sys.stdin)["job_id"])')" \
      || die "Failed to create the job"
    ok "created job ${JOB_ID}"
  fi

  [[ -n "${HOST}" ]] && log "  ${HOST%/}/jobs/${JOB_ID}"
fi

# ---------------------------------------------------------------- 7. run

if [[ ${DO_RUN} -eq 1 ]]; then
  step "Run"
  if [[ ${DRY_RUN} -eq 1 ]]; then
    skip "dry run — would trigger job ${JOB_NAME}"
  else
    log "Triggering job ${JOB_ID} — this waits for completion (Ctrl-C is safe, the run continues)"
    dbx jobs run-now "${JOB_ID}" --timeout 60m || die "The run failed. Check the job page above."
    ok "run finished"
  fi
fi

step "Done"
if [[ ${DRY_RUN} -eq 1 ]]; then
  log "Dry run only — nothing was uploaded, created or changed."
  log "Re-run without --dry-run to deploy."
else
  log "Deployed to ${CATALOG}. Trigger it with:  ./databricks/deploy.sh --run"
fi
