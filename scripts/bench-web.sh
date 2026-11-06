#!/usr/bin/env bash
# Load test of the web API with wrk.
#
# Starts the benchmark server (store/bench/Main.hs) on a synthetic regtest
# chain, waits until it serves, then runs wrk against a fixed list of
# endpoints and prints requests per second and latency percentiles for each.
#
#   scripts/bench-web.sh [BLOCKS] [DURATION] [CONNECTIONS] [-- RTS flags]
#
# The server runs on CPUs 0-2 and wrk on CPU 3, so that they do not compete
# for the same cores. Set REDIS_URL to run with the Redis cache; set
# ENDPOINTS to a regular expression to run only matching endpoint names.
set -euo pipefail

BLOCKS=${1:-1000}
DURATION=${2:-10s}
CONNS=${3:-8}
shift $(( $# < 3 ? $# : 3 ))
[ "${1:-}" = "--" ] && shift
RTS=("$@")
PORT=${PORT:-3999}
ENDPOINTS=${ENDPOINTS:-.}
SERVER_CPUS=${SERVER_CPUS:-0-2}
WRK_CPUS=${WRK_CPUS:-3}

cd "$(dirname "$0")/.."
if [ -z "${BIN:-}" ]; then
  BIN=$(stack --no-nix --system-ghc path --dist-dir 2>/dev/null)
  BIN=store/$BIN/build/haskoin-store-bench/haskoin-store-bench
fi
OUT=$(mktemp)
trap 'kill $SERVER 2>/dev/null || true; rm -f "$OUT" "$OUT.wrk"' EXIT

taskset -c "$SERVER_CPUS" "$BIN" "$PORT" "$BLOCKS" ${REDIS_URL:+"$REDIS_URL"} \
  +RTS -N3 ${RTS[@]+"${RTS[@]}"} -RTS >"$OUT" 2>&1 &
SERVER=$!
until grep -q "serving on port" "$OUT"; do
  kill -0 $SERVER 2>/dev/null || { cat "$OUT"; exit 1; }
  sleep 1
done
until curl -sf "http://127.0.0.1:$PORT/block/best" >/dev/null; do sleep 0.2; done

HOT=$(sed -n 's/^hot address: //p' "$OUT")
XPUB=$(sed -n 's/^xpub: //p' "$OUT")
BEST=$(sed -n 's/^best block: //p' "$OUT")
TXID=$(curl -s "http://127.0.0.1:$PORT/address/$HOT/transactions?limit=1" |
  python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["txid"])')
# The xpub is cached in Redis on its first balance query.
curl -s "http://127.0.0.1:$PORT/xpub/$XPUB" >/dev/null

run() {
  local name=$1 path=$2
  [[ $name =~ $ENDPOINTS ]] || return 0
  # warm up, then measure; also report the CPU time that the server used
  # per request, which does not depend on how well it scales across cores
  taskset -c "$WRK_CPUS" wrk -t1 -c"$CONNS" -d2s "http://127.0.0.1:$PORT$path" >/dev/null
  local t0 t1
  t0=$(cpu_ticks)
  taskset -c "$WRK_CPUS" wrk -t1 -c"$CONNS" -d"$DURATION" --latency \
    "http://127.0.0.1:$PORT$path" >"$OUT.wrk"
  t1=$(cpu_ticks)
  awk -v n="$name" -v ticks=$((t1 - t0)) -v hz="$(getconf CLK_TCK)" '
      /requests in/ {reqs=$1}
      /Requests\/sec/ {rps=$2}
      /^ +50%/ {p50=$2} /^ +99%/ {p99=$2}
      /Non-2xx/ {bad=$NF}
      END {printf "%-16s %9s req/s  p50 %8s  p99 %8s  cpu %7.3f ms/req %s\n",
        n, rps, p50, p99, ticks * 1000 / hz / reqs, (bad ? "non-2xx " bad : "")}' "$OUT.wrk"
}

cpu_ticks() {
  awk '{print $14 + $15}' "/proc/$SERVER/stat"
}

run health "/health"
run block-best "/block/best?notx=true"
run block-latest "/block/latest?notx=true"
run block-txs "/transactions/block/$BEST"
run tx "/transaction/$TXID"
run addr-balance "/address/$HOT/balance"
run addr-txs "/address/$HOT/transactions?limit=100"
run addr-txs-full "/address/$HOT/transactions/full?limit=100"
run addr-unspent "/address/$HOT/unspent?limit=100"
run xpub-summary "/xpub/$XPUB"
run xpub-txs "/xpub/$XPUB/transactions?limit=100"
run xpub-txs-full "/xpub/$XPUB/transactions/full?limit=50"
run xpub-unspent "/xpub/$XPUB/unspent?limit=100"
run binfo-multiaddr "/blockchain/multiaddr?active=$HOT%7C$XPUB&n=50"
run binfo-rawaddr "/blockchain/rawaddr/$HOT?limit=50"
run binfo-unspent "/blockchain/unspent?active=$HOT&limit=250"
