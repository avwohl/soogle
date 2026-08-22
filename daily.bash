#!/usr/bin/env bash
# daily.bash — Run free scrapers and all processing phases.
# Meant to be run via cron or manually. Does not use SERPAPI_KEY.
#
# Usage: ./daily.bash [2>&1 | tee -a logs/daily.log]

set -euo pipefail
cd "$(dirname "$0")"

# Load .env (DB password, API keys). weekly.bash does this too and exports
# through to here, but daily.bash is also run on its own, and then nothing
# else would supply ANTHROPIC_API_KEY - the LLM phases would silently skip.
if [ -f .env ]; then
    set -a
    # shellcheck disable=SC1091
    source .env
    set +a
fi

PYTHON="python -m scrape"
LOG_PREFIX="[daily $(date +%Y-%m-%d/%H:%M)]"

log() { echo "$LOG_PREFIX $*"; }

# Track whether any phase failed.  Phases keep running on failure (one flaky
# scraper shouldn't abort the whole pipeline) but we exit non-zero at the end
# so the caller (periodic.sh) reports the run as FAILED instead of "ok".
fail=0

log "=== Starting daily scrape ==="

# --- Free scrapers ---

log "GitHub (incremental)"
$PYTHON github --incremental || { log "WARN: github failed"; fail=1; }

log "Web sources (squeaksource, smalltalkhub, rosettacode, vskb)"
$PYTHON web all || { log "WARN: web all failed"; fail=1; }

log "Custom scrapers (squeakmap, sourceforge, launchpad, lukas_renggli)"
$PYTHON custom all || { log "WARN: custom all failed"; fail=1; }

# --- Processing phases ---

log "Process scrape_raw into packages"
$PYTHON process || { log "WARN: process failed"; fail=1; }

log "Analyze new domains (requires ANTHROPIC_API_KEY)"
if [ -n "${ANTHROPIC_API_KEY:-}" ]; then
    $PYTHON analyze || { log "WARN: analyze failed"; fail=1; }
else
    log "SKIP: ANTHROPIC_API_KEY not set, skipping analyze"
fi

log "LLM review of new packages (requires ANTHROPIC_API_KEY)"
if [ -n "${ANTHROPIC_API_KEY:-}" ]; then
    $PYTHON llm-review || { log "WARN: llm-review failed"; fail=1; }
    $PYTHON video-review || { log "WARN: video-review failed"; fail=1; }
else
    log "SKIP: ANTHROPIC_API_KEY not set, skipping llm-review / video-review"
fi

log "Status"
$PYTHON status || { log "WARN: status failed"; fail=1; }

if [ "$fail" -eq 0 ]; then
    log "=== Daily scrape complete ==="
else
    log "=== Daily scrape complete WITH FAILURES (see WARN lines above) ==="
fi
exit "$fail"
