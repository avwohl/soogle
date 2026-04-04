#!/usr/bin/env bash
# weekly.bash — Run paid-API scrapers (SERPAPI), then daily.bash.
# SERPAPI free tier: 100 searches/month, so weekly is ~25/run.
#
# Usage: ./weekly.bash [2>&1 | tee -a logs/weekly.log]

set -euo pipefail
cd "$(dirname "$0")"

export SOOGLE_DB_PASS="${SOOGLE_DB_PASS:-xrain}"

PYTHON="python -m scrape"
LOG_PREFIX="[weekly $(date +%Y-%m-%d/%H:%M)]"

log() { echo "$LOG_PREFIX $*"; }

log "=== Starting weekly scrape ==="

# --- SERPAPI-based scrapers ---

if [ -z "${SERPAPI_KEY:-}" ]; then
    log "ERROR: SERPAPI_KEY not set. Export it first."
    exit 1
fi

log "Web discovery (serpapi)"
$PYTHON discover serpapi || log "WARN: discover serpapi failed"

log "YouTube videos"
$PYTHON youtube || log "WARN: youtube failed"

# --- Run the full daily pipeline (free scrapers + processing) ---

log "Running daily.bash"
exec bash "$(dirname "$0")/daily.bash"
