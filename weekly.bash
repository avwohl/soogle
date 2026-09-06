#!/usr/bin/env bash
# weekly.bash — Run paid-API scrapers (SERPAPI), then daily.bash.
# SERPAPI free tier: 100 searches/month, so weekly is ~25/run.
#
# Usage: ./weekly.bash [2>&1 | tee -a logs/weekly.log]

set -euo pipefail
cd "$(dirname "$0")"

# Load .env if present (DB creds, API keys, hCaptcha, email).
if [ -f .env ]; then
    set -a
    # shellcheck disable=SC1091
    source .env
    set +a
fi


# Interpreter.  See the matching comment in daily.bash: this was a bare `python`
# that resolved through a ~/bin symlink which vanished on 2026-09-03, so it is
# an absolute path now.  Exported so the daily.bash run at the end of this
# script uses the same one.
export PYTHON_BIN="${PYTHON_BIN:-/usr/bin/python3}"
PYTHON="$PYTHON_BIN -m scrape"
LOG_PREFIX="[weekly $(date +%Y-%m-%d/%H:%M)]"

log() { echo "$LOG_PREFIX $*"; }

# Preflight: die here, loudly, rather than emitting one "command not found" per
# phase.  Every phase below only WARNs on failure, so a missing interpreter used
# to surface as a wall of unrelated warnings instead of one root cause.
if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
    log "FATAL: interpreter '$PYTHON_BIN' is not on PATH"
    log "FATAL: PATH=$PATH"
    exit 1
fi
if ! deps=$("$PYTHON_BIN" -c 'import requests, pymysql, bs4, anthropic' 2>&1); then
    log "FATAL: $PYTHON_BIN cannot import the required modules:"
    printf '%s\n' "$deps" | sed 's/^/    /'
    log "FATAL: try: $PYTHON_BIN -m pip install -r requirements.txt"
    exit 1
fi

# Track whether any phase failed so the caller (periodic.sh) reports the run
# as FAILED instead of "ok".  Phases keep running on failure.
fail=0

log "=== Starting weekly scrape ==="

# --- User-submitted URLs (cheap; run before paid APIs) ---

log "User submissions"
$PYTHON submissions || { log "WARN: submissions failed"; fail=1; }

# --- SERPAPI-based scrapers ---

if [ -z "${SERPAPI_KEY:-}" ]; then
    log "ERROR: SERPAPI_KEY not set. Export it first."
    exit 1
fi

log "Web discovery (serpapi)"
$PYTHON discover serpapi || { log "WARN: discover serpapi failed"; fail=1; }

log "YouTube videos"
$PYTHON youtube || { log "WARN: youtube failed"; fail=1; }

# --- Run the full daily pipeline (free scrapers + processing) ---
# Don't `exec` here: we need daily.bash's exit status to combine with the
# weekly phases above so a failure in either is reported.
log "Running daily.bash"
bash "$(dirname "$0")/daily.bash" || fail=1

exit "$fail"
