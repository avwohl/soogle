#!/usr/bin/env bash
# daily.bash — Run free scrapers and all processing phases.
# Meant to be run via cron or manually. Does not use SERPAPI_KEY.
#
# Usage: ./daily.bash [2>&1 | tee -a logs/daily.log]

set -euo pipefail
cd "$(dirname "$0")"

export SOOGLE_DB_PASS="${SOOGLE_DB_PASS:-xrain}"

PYTHON="python -m scrape"
LOG_PREFIX="[daily $(date +%Y-%m-%d/%H:%M)]"

log() { echo "$LOG_PREFIX $*"; }

log "=== Starting daily scrape ==="

# --- Free scrapers ---

log "GitHub (incremental)"
$PYTHON github --incremental || log "WARN: github failed"

log "Web sources (squeaksource, smalltalkhub, rosettacode, vskb)"
$PYTHON web all || log "WARN: web all failed"

log "Custom scrapers (squeakmap, sourceforge, launchpad, lukas_renggli)"
$PYTHON custom all || log "WARN: custom all failed"

# --- Processing phases ---

log "Process scrape_raw into packages"
$PYTHON process || log "WARN: process failed"

log "Analyze new domains (requires ANTHROPIC_API_KEY)"
if [ -n "${ANTHROPIC_API_KEY:-}" ]; then
    $PYTHON analyze || log "WARN: analyze failed"
else
    log "SKIP: ANTHROPIC_API_KEY not set, skipping analyze"
fi

log "LLM review of new packages (requires ANTHROPIC_API_KEY)"
if [ -n "${ANTHROPIC_API_KEY:-}" ]; then
    $PYTHON llm-review || log "WARN: llm-review failed"
    $PYTHON video-review || log "WARN: video-review failed"
else
    log "SKIP: ANTHROPIC_API_KEY not set, skipping llm-review / video-review"
fi

log "Status"
$PYTHON status

log "=== Daily scrape complete ==="
