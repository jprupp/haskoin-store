#!/bin/sh
# Go through every world of the model checker of Haskoin.Store.Ingest
# (test/Haskoin/Store/Ingest/Model.hs), without building the store: the
# machine and its model need nothing but GHC and containers, so a change to
# them is checked in a minute. The test suite runs the same worlds
# (Haskoin.Store.IngestSpec).
#
#   scripts/check-ingest.sh          every world
#   scripts/check-ingest.sh 9 10     the worlds of those numbers
#
# The program is built in .stack-work/ingest-model, with a heap of 4 GB at
# most.
set -eu

root=$(cd "$(dirname "$0")/.." && pwd)
out="$root/.stack-work/ingest-model"
mkdir -p "$out"

ghc-9.12.4 -O1 -Wall -rtsopts "-with-rtsopts=-M4g" \
  -XGHC2024 -XDuplicateRecordFields -XNoFieldSelectors -XOverloadedRecordDot -XOverloadedStrings \
  -outputdir "$out/o" -i"$root/src" -i"$root/test" -o "$out/model" \
  "$root/scripts/ingest-model/Main.hs" \
  "$root/src/Haskoin/Store/Ingest.hs" \
  "$root/test/Haskoin/Store/Ingest/Model.hs" > "$out/build.log" 2>&1 \
  || { cat "$out/build.log"; exit 1; }

exec "$out/model" "$@"
