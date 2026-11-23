#!/bin/sh
# Go through every world of the checker of the pool after a move of the
# tip (test/Haskoin/Store/Fleet/LeavingModel.hs), without building the
# store: the decisions and their model of Redis are compiled alone, with
# optimisation, so a change to them is checked in a few minutes. The test
# suite runs the same worlds (Haskoin.Store.Fleet.LeavingSpec).
#
#   scripts/check-pool.sh          every world
#   scripts/check-pool.sh 0 2      the worlds of those numbers
#
# It is compiled with the GHC and the libraries of Stack, since the
# decisions need cereal, which GHC does not come with. The program is
# built in .stack-work/pool-model, with a heap of 4 GB at most.
set -eu

root=$(cd "$(dirname "$0")/.." && pwd)
out="$root/.stack-work/pool-model"
mkdir -p "$out"

cd "$root"
stack exec -- ghc -O1 -Wall -rtsopts "-with-rtsopts=-M4g" \
  -XGHC2024 -XDuplicateRecordFields -XNoFieldSelectors -XOverloadedRecordDot -XOverloadedStrings \
  -outputdir "$out/o" -i"$root/src" -i"$root/test" -o "$out/model" \
  "$root/scripts/pool-model/Main.hs" \
  "$root/src/Haskoin/Store/Fleet/Atomic/Cells.hs" \
  "$root/src/Haskoin/Store/Fleet/Atomic.hs" \
  "$root/src/Haskoin/Store/Fleet/Atomic/Keys.hs" \
  "$root/src/Haskoin/Store/Fleet/Pool/Types.hs" \
  "$root/src/Haskoin/Store/Fleet/Pool/Decide.hs" \
  "$root/src/Haskoin/Store/Fleet/XPub/Decide.hs" \
  "$root/test/Haskoin/Store/Fleet/LeavingModel.hs" > "$out/build.log" 2>&1 \
  || { cat "$out/build.log"; exit 1; }

exec "$out/model" "$@"
