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
    log "FATAL: try: (cd $PWD && $PYTHON_BIN -m pip install --user --break-system-packages -e .)"
    exit 1
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
