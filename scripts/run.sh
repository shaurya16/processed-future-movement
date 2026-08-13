#!/usr/bin/env bash
#
# One-command demo of the whole pipeline.
#
#   ./scripts/run.sh                      # ingest the provided sample (717 records)
#   ./scripts/run.sh path/to/your.txt     # ingest your own fixed-width file
#
# Brings up Kafka, both services and the UI, publishes the file, waits for the
# aggregation to settle, and prints the report. Leaves the stack running so the
# UI can be browsed afterwards.
#
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

FRONTEND_URL="http://localhost:8080"
INGEST_URL="http://localhost:8081/api/v1"
REPORT_URL="http://localhost:8082/api/v1"

bold()  { printf '\033[1m%s\033[0m\n' "$*"; }
step()  { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
warn()  { printf '\033[33m%s\033[0m\n' "$*"; }
die()   { printf '\033[31mError: %s\033[0m\n' "$*" >&2; exit 1; }

case "${1:-}" in
  -h|--help)
    sed -n '2,10p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    exit 0
    ;;
esac

# ---------------------------------------------------------------------------
# Preflight
# ---------------------------------------------------------------------------
step "Checking prerequisites"

command -v docker >/dev/null || die "docker is not installed."
docker info >/dev/null 2>&1 || die "the Docker daemon is not running. Start Docker Desktop and retry."
docker compose version >/dev/null 2>&1 || die "docker compose v2 is required (this is 'docker compose', not 'docker-compose')."
echo "Docker is running."

# ---------------------------------------------------------------------------
# Resolve the input file
# ---------------------------------------------------------------------------
# sample-data/ is bind-mounted read-only into ingestion-service, so the file has
# to live under it to be visible in the container. A file given from elsewhere is
# copied in.
step "Selecting input file"

if [[ -n "${1:-}" ]]; then
  [[ -f "$1" ]] || die "input file not found: $1"
  cp "$1" sample-data/custom-input.txt
  CONTAINER_PATH="/app/sample-data/custom-input.txt"
  echo "Using your file: $1"
  echo "  copied to sample-data/custom-input.txt (mounted into the container)"
else
  CONTAINER_PATH="/app/sample-data/Input.txt"
  echo "Using the provided sample: sample-data/Input.txt"
fi
export INGESTION_FILE_PATH="$CONTAINER_PATH"

# ---------------------------------------------------------------------------
# Clean slate
# ---------------------------------------------------------------------------
# The report is a RUNNING aggregate: a different file produces different
# content-derived transaction IDs, so its records would be added to whatever is
# already there rather than replacing it. Tearing down first is what makes each
# run's numbers mean what you expect.
step "Tearing down any previous run"
docker compose down -v --remove-orphans >/dev/null 2>&1 || true
echo "Previous containers, volumes and Kafka state removed."

# Checked after the teardown, so anything still holding a port is definitely not
# ours and would make the stack fail to start with a confusing error.
if command -v lsof >/dev/null 2>&1; then
  for port in 8080 8081 8082 9092; do
    if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
      die "port $port is in use by another process. Free it and retry."
    fi
  done
  echo "Ports 8080, 8081, 8082 and 9092 are free."
fi

# ---------------------------------------------------------------------------
# Start
# ---------------------------------------------------------------------------
step "Starting the stack (first run builds three images — this can take a few minutes)"
docker compose up -d --build

step "Waiting for ingestion-service to become ready"
deadline=$(( SECONDS + 300 ))
until curl -fs -o /dev/null "${INGEST_URL%/api/v1}/actuator/health/readiness" 2>/dev/null; do
  (( SECONDS < deadline )) || die "ingestion-service did not become ready within 5 minutes. Try: docker compose logs ingestion-service"
  sleep 2
done
echo "ingestion-service is ready."

# processing-service is gated behind wait-for-topic, so it can still be starting.
step "Waiting for processing-service to become ready"
deadline=$(( SECONDS + 300 ))
until curl -fs -o /dev/null "${REPORT_URL%/api/v1}/actuator/health/readiness" 2>/dev/null; do
  (( SECONDS < deadline )) || die "processing-service did not become ready within 5 minutes. Try: docker compose logs processing-service"
  sleep 2
done
echo "processing-service is ready (Kafka Streams is RUNNING)."

# ---------------------------------------------------------------------------
# Ingest
# ---------------------------------------------------------------------------
step "Publishing the file to Kafka"
INGEST_RESPONSE=$(curl -fsS -X POST "$INGEST_URL/ingest") || die "the ingest request failed. Try: docker compose logs ingestion-service"

if command -v jq >/dev/null 2>&1; then
  echo "$INGEST_RESPONSE" | jq '{totalLines, published, skipped, cached, errorCount: ((.errors // []) | length)}'
  ERROR_COUNT=$(echo "$INGEST_RESPONSE" | jq '(.errors // []) | length')
  if (( ERROR_COUNT > 0 )); then
    warn "$ERROR_COUNT line(s) could not be parsed. First few:"
    echo "$INGEST_RESPONSE" | jq -r '(.errors // [])[:5][] | "  line \(.lineNumber): \(.reason)"'
    echo "  (bad lines are skipped and reported; they do not stop the rest of the file)"
  fi
else
  echo "$INGEST_RESPONSE"
fi

# ---------------------------------------------------------------------------
# Wait for the aggregation to converge
# ---------------------------------------------------------------------------
# Rows appearing in the report is NOT a sufficient "done" signal: Kafka Streams
# materialises the table incrementally, so every key can be present while its
# totals are still being folded. Waiting for the output to stop changing is the
# file-agnostic way to know aggregation has actually settled.
step "Waiting for the aggregation to settle"
stable=0
previous=""
deadline=$(( SECONDS + 120 ))
while (( stable < 3 )); do
  (( SECONDS < deadline )) || die "the report did not settle within 2 minutes. Try: docker compose logs processing-service"
  sleep 2
  current=$(curl -fsS "$REPORT_URL/report/csv" 2>/dev/null || echo "")
  if [[ -n "$current" && "$current" == "$previous" ]]; then
    stable=$(( stable + 1 ))
  else
    stable=0
  fi
  previous="$current"
done
echo "Report is stable."

# ---------------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------------
step "Report (Output.csv format)"
# A large input can produce hundreds of groups; dumping them all buries the
# summary that follows. The full report is one curl away.
ROW_COUNT=$(( $(wc -l <<<"$previous") - 1 ))
if (( ROW_COUNT > 20 )); then
  head -n 21 <<<"$previous"
  echo "  ... and $(( ROW_COUNT - 20 )) more rows — full report: $REPORT_URL/report/csv"
else
  echo "$previous"
fi
echo
echo "$ROW_COUNT (client, product) group(s)."

if [[ -z "${1:-}" ]]; then
  if diff -q <(sort <<<"$previous" | tr -d '\r') <(sort sample-output/Output.csv | tr -d '\r') >/dev/null 2>&1; then
    bold "This matches sample-output/Output.csv exactly — the expected result for the provided sample."
  else
    warn "This does NOT match sample-output/Output.csv. That is unexpected for the sample input."
  fi
fi

step "Done"
cat <<EOF
  UI            $FRONTEND_URL
  Report JSON   $REPORT_URL/report
  Report CSV    $REPORT_URL/report/csv
  Ingest status $INGEST_URL/ingest/status

  Run again with your own file:  ./scripts/run.sh path/to/your-file.txt
  Stop everything:               docker compose down -v
EOF
