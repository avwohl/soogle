#!/usr/bin/env bash
# weekly.bash — Run paid-API scrapers (SERPAPI), then daily.bash.
# SERPAPI free tier: 100 searches/month, so weekly is ~25/run.
#
# Usage: ./weekly.bash [2>&1 | tee -a logs/weekly.log]

set -euo pipefail
cd "$(dirname "$0")"

# Load .env if present (DB creds, API keys, hCaptcha, email).  An
# already-exported value wins over .env; see load_env.bash for why.
# shellcheck disable=SC1091
source ./load_env.bash

# Interpreter.  uv creates .venv via `uv sync`; we call the venv's python
# directly so each phase has no uv/network overhead.  PYTHON_BIN overrides.
# Exported so the daily.bash run at the end of this script uses the same one.
export PYTHON_BIN="${PYTHON_BIN:-.venv/bin/python}"
PYTHON="$PYTHON_BIN -m scrape"
LOG_PREFIX="[weekly $(date +%Y-%m-%d/%H:%M)]"

log() { echo "$LOG_PREFIX $*"; }

# Preflight: die here, loudly, rather than emitting one "command not found" per
# phase.  Every phase below only WARNs on failure, so a missing interpreter used
# to surface as a wall of unrelated warnings instead of one root cause.
if ! command -v uv >/dev/null 2>&1; then
    log "FATAL: uv is not installed. See https://docs.astral.sh/uv/"
    exit 1
fi
if [ ! -x "$PYTHON_BIN" ] || ! deps=$("$PYTHON_BIN" -c 'import requests, pymysql, bs4, anthropic' 2>&1); then
    log "Setting up .venv (uv sync)"
    uv sync || { log "FATAL: uv sync failed"; exit 1; }
    if [ ! -x "$PYTHON_BIN" ] || ! deps=$("$PYTHON_BIN" -c 'import requests, pymysql, bs4, anthropic' 2>&1); then
        log "FATAL: uv sync did not produce a working $PYTHON_BIN:"
        printf '%s\n' "$deps" | sed 's/^/    /'
        exit 1
    fi
fi

# Track which phases failed so the caller (periodic.sh) reports the run as
# FAILED instead of "ok".  Phases keep running on failure.  The last line
# names them, because periodic.sh mails only the last 200 lines and the WARN
# lines for the phases here come ~500 lines before the end of the run.
failed=()
phase_failed() { log "WARN: $1 failed"; failed+=("$1"); }

log "=== Starting weekly scrape ==="

# --- User-submitted URLs (cheap; run before paid APIs) ---

log "User submissions"
$PYTHON submissions || phase_failed submissions

# --- SERPAPI-based scrapers ---

if [ -z "${SERPAPI_KEY:-}" ]; then
    log "ERROR: SERPAPI_KEY not set. Export it first."
    exit 1
fi

log "Web discovery (serpapi)"
$PYTHON discover serpapi || phase_failed "discover serpapi"

log "YouTube videos"
$PYTHON youtube || phase_failed youtube

# --- Run the full daily pipeline (free scrapers + processing) ---
# Don't `exec` here: we need daily.bash's exit status to combine with the
# weekly phases above so a failure in either is reported.
log "Running daily.bash"
bash "$(dirname "$0")/daily.bash" || failed+=("daily.bash")

if [ ${#failed[@]} -eq 0 ]; then
    log "=== Weekly scrape complete ==="
    exit 0
fi
list=$(printf '%s, ' "${failed[@]}")
log "=== Weekly scrape complete WITH FAILURES in: ${list%, } ==="
exit 1
