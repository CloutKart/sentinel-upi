#!/usr/bin/env bash
#
# Run Project Sentinel locally, end to end, in one command.
#
#   ./run.sh                  # everything: pipeline if needed, export, dashboard
#   ./run.sh --fresh          # wipe and regenerate first
#   ./run.sh --serve-only     # skip the pipeline, just re-export and serve
#   ./run.sh --scale 0.1      # a smaller dataset (default 1.0, ~200k transactions)
#   ./run.sh --build          # production build + preview instead of the dev server
#
# There are two halves, and only one of them is a server.
#
#   Backend   the Spark pipeline: generate -> landing -> bronze -> silver -> gold,
#             writing Delta tables under ./data. It runs, finishes and exits. There
#             is deliberately no API service — the dashboard is fed by a static
#             export, so nothing has to stay up for the page to work.
#
#   Frontend  the Vite dashboard, reading JSON that `sentinel-web-export` wrote out
#             of the Gold layer.
#
# The first run does the setup (venv, JDK, npm) and takes a few minutes. Later runs
# skip whatever is already in place.

set -euo pipefail

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ---------------------------------------------------------------- output

step() { printf '\n\033[1m▸ %s\033[0m\n' "$*"; }
ok()   { printf '       \033[0;32m✓\033[0m %s\n' "$*"; }
skip() { printf '       \033[0;90m·\033[0m %s\n' "$*"; }
warn() { printf '\033[0;33m[warn]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[0;31m[error]\033[0m %s\n' "$*" >&2; exit 1; }

# ---------------------------------------------------------------- options

SCALE="1.0"
FRESH=0
SERVE_ONLY=0
BUILD=0
PORT=""
NO_SERVE=0

usage() {
  # Print the header comment, stopping at the first line that is not one, so the
  # help text cannot drift out of sync with a hardcoded line range.
  awk 'NR>2 { if (!/^#/) exit; sub(/^# ?/, ""); print }' "${BASH_SOURCE[0]}"
  cat <<EOF

Options:
  --scale N      Generator scale; 1.0 is ~200k transactions (default: ${SCALE})
  --fresh        Delete generated data and every layer, then rebuild from scratch
  --serve-only   Skip the pipeline; re-export the existing Gold layer and serve
  --build        Production build served on 4173, instead of the dev server on 5173
  --port N       Override the port
  --no-serve     Run the pipeline and export, but do not start a server
  -h, --help     This message
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --scale)      SCALE="$2"; shift 2 ;;
    --fresh)      FRESH=1; shift ;;
    --serve-only) SERVE_ONLY=1; shift ;;
    --build)      BUILD=1; shift ;;
    --port)       PORT="$2"; shift 2 ;;
    --no-serve)   NO_SERVE=1; shift ;;
    -h|--help)    usage; exit 0 ;;
    *)            die "Unknown option: $1 (try --help)" ;;
  esac
done

PORT="${PORT:-$([[ ${BUILD} -eq 1 ]] && echo 4173 || echo 5173)}"

# Spark 3.5 supports JDK 8/11/17 and Fedora ships 25/26 as system Java. The Makefile
# exports this too; setting it here means the script works when invoked directly.
export JAVA_HOME="${JAVA_HOME:-${HOME}/.local/jdks/jdk-17}"

# ---------------------------------------------------------------- 1. toolchain

step "Toolchain"

command -v uv >/dev/null 2>&1 || die "uv not found — https://docs.astral.sh/uv/getting-started/"

if [[ ! -d .venv ]]; then
  printf '       installing the Python environment and a project-local JDK 17...\n'
  make setup >/dev/null 2>&1 || die "make setup failed — run it directly to see why"
  ok "python environment created"
else
  skip "python environment present"
fi

if [[ ! -x "${JAVA_HOME}/bin/java" ]]; then
  make jdk >/dev/null 2>&1 || die "could not install a JDK 17 — run \`make jdk\` to see why"
fi
ok "JDK 17 at ${JAVA_HOME}"

if [[ ${NO_SERVE} -eq 0 ]]; then
  command -v npm >/dev/null 2>&1 || die "npm not found — needed for the dashboard. Use --no-serve to skip it."
  if [[ ! -d dashboards/web/node_modules ]]; then
    printf '       installing dashboard dependencies...\n'
    (cd dashboards/web && npm install --no-audit --no-fund >/dev/null 2>&1) \
      || die "npm install failed — run it in dashboards/web to see why"
    ok "dashboard dependencies installed"
  else
    skip "dashboard dependencies present"
  fi
fi

# One Spark start-up costs ~10s, so this is not free — but a JDK/Delta mismatch
# otherwise surfaces as an opaque JVM crash inside a streaming query, minutes later,
# with the real cause nowhere in the traceback.
if [[ ${SERVE_ONLY} -eq 0 ]]; then
  uv run python -m sentinel.doctor >/dev/null 2>&1 \
    || die "Spark cannot start. Run \`make doctor\` to see the failure."
  ok "Spark + Delta verified"
fi

# ---------------------------------------------------------------- 2. backend

GOLD="data/gold/fact_txn_scored"

if [[ ${FRESH} -eq 1 ]]; then
  step "Clean"
  make clean-all >/dev/null 2>&1
  ok "generated data and every layer removed"
fi

if [[ ${SERVE_ONLY} -eq 1 ]]; then
  step "Pipeline"
  [[ -d "${GOLD}" ]] || die "--serve-only, but there is no Gold layer yet. Run without it first."
  skip "skipped — serving the existing Gold layer"
elif [[ -d "${GOLD}" && ${FRESH} -eq 0 ]]; then
  step "Pipeline"
  skip "Gold layer already present — use --fresh to rebuild it"
else
  step "Generate"
  # Not silenced: at scale 1.0 this is the longest step, and the injected-defect
  # counts it prints are the point of the generator.
  uv run sentinel-gen --scale "${SCALE}" || die "generation failed"

  step "Pipeline"
  uv run sentinel-run all || die "the pipeline failed"
fi

# ---------------------------------------------------------------- 3. export

step "Export"
uv run sentinel-web-export >/dev/null 2>&1 || die "export failed — is there a Gold layer?"
ok "Gold layer exported to dashboards/web/public/data"

if [[ ${NO_SERVE} -eq 1 ]]; then
  step "Done"
  printf '       Pipeline and export complete. Start the dashboard with:  ./run.sh --serve-only\n'
  exit 0
fi

# ---------------------------------------------------------------- 4. frontend

step "Dashboard"

# A stale server on the port would leave the browser showing the previous run's data
# while this one reports success — worth failing on rather than working around.
if command -v ss >/dev/null 2>&1 && ss -ltn 2>/dev/null | grep -q ":${PORT} "; then
  die "Port ${PORT} is already in use. Stop that server, or pass --port."
fi

if [[ ${BUILD} -eq 1 ]]; then
  (cd dashboards/web && npm run build >/dev/null 2>&1) || die "the production build failed"
  ok "production build"
  printf '\n       \033[1mhttp://localhost:%s/\033[0m   (Ctrl-C to stop)\n\n' "${PORT}"
  exec npm --prefix dashboards/web run preview -- --port "${PORT}"
else
  printf '\n       \033[1mhttp://localhost:%s/\033[0m   (hot reload; Ctrl-C to stop)\n\n' "${PORT}"
  # exec so Ctrl-C reaches Vite directly rather than orphaning it behind this script.
  exec npm --prefix dashboards/web run dev -- --port "${PORT}"
fi
