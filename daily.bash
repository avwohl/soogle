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

# Interpreter.  Absolute on purpose.  This was a bare `python`, which resolved
# through ~/bin/python -- a symlink carried in the avwohl/bin repo, and ~/bin is
# fifth on wohl's login PATH while /usr/bin is tenth.  That repo is shared with
# a Mac: a cleanup commit there deleted the symlink, a `git reset --hard` landed
# it on this host on 2026-09-03, and the 2026-09-06 weekly run died with
# "python: command not found" in every phase.  An absolute path cannot be
# shadowed by anything earlier on PATH, which is the whole point.
# PYTHON_BIN overrides it for a dev box or a venv.
PYTHON_BIN="${PYTHON_BIN:-/usr/bin/python3}"
PYTHON="$PYTHON_BIN -m scrape"
LOG_PREFIX="[daily $(date +%Y-%m-%d/%H:%M)]"

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
