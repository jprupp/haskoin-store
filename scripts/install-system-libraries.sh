#!/usr/bin/env bash
# Install the C libraries that haskoin-store links against, as Ubuntu packages
# them: RocksDB, secp256k1 and zlib, with pkg-config to find them. CI runs it
# for the build with the libraries of the OS; it works on any Ubuntu or Debian.
set -euo pipefail

sudo apt-get update
sudo apt-get install -y \
  librocksdb-dev \
  libsecp256k1-dev \
  pkg-config \
  zlib1g-dev
