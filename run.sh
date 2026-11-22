#!/usr/bin/env bash
# run.sh - set up once, then run a CAS session. Args pass straight through.
#
#   ./run.sh                # live session (dashboard opens)
#   ./run.sh --test 120     # 2-minute live check during market hours
#   ./run.sh --check        # offline self-test only, no credentials needed
#   ./run.sh --find "NIFTY 50"
#
# Everything is idempotent: the venv is created once, dependencies installed once,
# and .env is never overwritten.
set -euo pipefail
cd "$(dirname "$0")"

PY="${PYTHON:-python3}"
VENV=".venv"
PYBIN="$VENV/bin/python"
STAMP="$VENV/.deps-ok"

say() { printf '\033[1m%s\033[0m\n' "$*"; }
die() { printf '\033[31m%s\033[0m\n' "$*" >&2; exit 1; }

command -v "$PY" >/dev/null 2>&1 || die "python3 not found. Install Python 3.10+ and re-run."

# 1) virtualenv -------------------------------------------------------------
if [ ! -x "$PYBIN" ]; then
  say "creating the virtual environment (once)"
  "$PY" -m venv "$VENV" || die "could not create .venv (is python3-venv installed?)"
fi

# 2) dependencies -----------------------------------------------------------
if [ ! -f "$STAMP" ]; then
  say "installing dependencies (once)"
  "$PYBIN" -m pip install --quiet --upgrade pip
  "$PYBIN" -m pip install --quiet -r requirements.txt
  touch "$STAMP"
fi

# 3) offline self-test ------------------------------------------------------
if [ "${1:-}" = "--check" ]; then
  say "offline self-test (no credentials needed)"
  "$PYBIN" verify.py
  "$PYBIN" stress_test.py | tail -5
  exit 0
fi

# 4) .env -------------------------------------------------------------------
if [ ! -f .env ]; then
  cp .env.example .env
  die "created .env from .env.example — open it, fill MSTOCK_API_KEY and one of
     MSTOCK_TOTP_SECRET / MSTOCK_TOTP / MSTOCK_ACCESS_TOKEN, then run this again."
fi

grep -qE '^[[:space:]]*MSTOCK_API_KEY=.+' .env \
  || die "MSTOCK_API_KEY is empty in .env (trade.mstock.com -> Trading APIs)"

grep -qE '^[[:space:]]*(MSTOCK_TOTP_SECRET|MSTOCK_TOTP|MSTOCK_ACCESS_TOKEN)=.+' .env \
  || die "no session credential in .env: set MSTOCK_TOTP_SECRET (best), MSTOCK_TOTP, or MSTOCK_ACCESS_TOKEN"

# 5) go ---------------------------------------------------------------------
say "starting the session"
exec "$PYBIN" cas_logger.py "$@"
