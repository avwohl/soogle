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
# An already-exported value wins over .env; see load_env.bash for why.
# shellcheck disable=SC1091
source ./load_env.bash

# Interpreter.  uv creates .venv via `uv sync`; we call the venv's python
# directly so each phase has no uv/network overhead.  PYTHON_BIN overrides.
PYTHON_BIN="${PYTHON_BIN:-.venv/bin/python}"
PYTHON="$PYTHON_BIN -m scrape"
LOG_PREFIX="[daily $(date +%Y-%m-%d/%H:%M)]"

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

# Track which phases failed.  Phases keep running on failure (one flaky
# scraper shouldn't abort the whole pipeline) but we exit non-zero at the end
# so the caller (periodic.sh) reports the run as FAILED instead of "ok".
# The last line names them: periodic.sh mails only the last 200 lines of
# output, and on 2026-09-20 and 09-27 the WARN line and the GitHub 401 behind
# it came ~440 lines before the end, so the FAIL mail showed neither.
failed=()
phase_failed() { log "WARN: $1 failed"; failed+=("$1"); }

log "=== Starting daily scrape ==="

# --- Free scrapers ---

log "GitHub (incremental)"
$PYTHON github --incremental || phase_failed github

log "Web sources (squeaksource, smalltalkhub, rosettacode, vskb)"
$PYTHON web all || phase_failed "web all"

log "Custom scrapers (squeakmap, sourceforge, launchpad, lukas_renggli)"
$PYTHON custom all || phase_failed "custom all"

# --- Processing phases ---

log "Process scrape_raw into packages"
$PYTHON process || phase_failed process

log "Analyze new domains (requires ANTHROPIC_API_KEY)"
if [ -n "${ANTHROPIC_API_KEY:-}" ]; then
    $PYTHON analyze || phase_failed analyze
else
    log "SKIP: ANTHROPIC_API_KEY not set, skipping analyze"
fi

log "LLM review of new packages (requires ANTHROPIC_API_KEY)"
if [ -n "${ANTHROPIC_API_KEY:-}" ]; then
    $PYTHON llm-review || phase_failed llm-review
    $PYTHON video-review || phase_failed video-review
else
    log "SKIP: ANTHROPIC_API_KEY not set, skipping llm-review / video-review"
fi

log "Status"
$PYTHON status || phase_failed status

if [ ${#failed[@]} -eq 0 ]; then
    log "=== Daily scrape complete ==="
    exit 0
fi
list=$(printf '%s, ' "${failed[@]}")
log "=== Daily scrape complete WITH FAILURES in: ${list%, } (see WARN lines above) ==="
exit 1
