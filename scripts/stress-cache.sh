#!/usr/bin/env bash
# Stress test of the Redis cache of xpubs, with a report.
#
#   scripts/stress-cache.sh [OUT_DIR] [-- OPTIONS]
#
# Builds haskoin-store and the stress program (stress/Main.hs), and runs it:
# a regtest chain of Bitcoin Core, Valkey and wrk in Podman containers, a
# haskoin-store server with the cache, a few wallets making hundreds of
# thousands of transactions between them, and wrk asking the server about
# their xpubs all the while. Then writes OUT_DIR/report.md with
# scripts/stress-report.py. OUT_DIR defaults to stress-<date>.
#
# OPTIONS go to the stress program; see the defaults with
#   $(stack path --dist-dir)/build/haskoin-store-stress/haskoin-store-stress --help
# for instance "-- --txs 20000" for a short run.
#
# It needs Podman and python3. The first run builds the test image, which
# downloads Ubuntu, Valkey, wrk and Bitcoin Core; later runs reuse it. With
# the defaults it uses up to about 7 GiB of memory: 3 GiB for the node and its
# chain, 1 GiB for Valkey, 2 GiB for the server and its database in /dev/shm.
#
# No container is left behind: the program removes its containers when it
# ends, and this script removes any that carry its label if it was killed,
# and stops the server.
set -euo pipefail

cd "$(dirname "$0")/.."
OUT=stress-$(date +%Y%m%d-%H%M%S)
if [ $# -gt 0 ] && [ "$1" != "--" ]; then OUT=$1; shift; fi
[ "${1:-}" = "--" ] && shift

command -v podman >/dev/null || { echo "Podman is needed" >&2; exit 1; }
command -v python3 >/dev/null || { echo "python3 is needed" >&2; exit 1; }

stack build --bench --no-run-benchmarks
SERVER=$(stack path --local-install-root)/bin/haskoin-store
STRESS=$(stack path --dist-dir)/build/haskoin-store-stress/haskoin-store-stress
mkdir -p "$OUT"

PID=
cleanup() {
  if [ -n "$PID" ] && kill -0 "$PID" 2>/dev/null; then
    # an interrupt lets the program remove its containers and stop the server
    kill -INT "$PID" 2>/dev/null || true
    for _ in $(seq 60); do kill -0 "$PID" 2>/dev/null || break; sleep 0.5; done
    kill -KILL "$PID" 2>/dev/null || true
  fi
  if [ -f "$OUT/server.pid" ]; then
    SPID=$(cat "$OUT/server.pid")
    if grep -q haskoin-store "/proc/$SPID/cmdline" 2>/dev/null; then kill "$SPID" 2>/dev/null || true; fi
  fi
  if [ -n "$PID" ]; then
    podman ps -aq --filter "label=haskoin-store-test=$PID" | xargs -r podman rm --force >/dev/null || true
  fi
}
trap cleanup EXIT
trap 'exit 130' INT TERM

"$STRESS" --server "$SERVER" --out "$OUT" "$@" &
PID=$!
status=0
wait "$PID" || status=$?
if [ "$status" -ne 0 ]; then
  echo "The stress test failed with status $status; see $OUT/server.log" >&2
  exit "$status"
fi
python3 scripts/stress-report.py "$OUT"
