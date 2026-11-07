# Pinned: a moving tag can change the RocksDB major version on rebuild
# (bookworm ships 7.8, trixie 9.10, forky 9.11).
FROM debian:trixie-20261027@sha256:2dc419c6d7fe3dad0a2f9fd469c34cc90b0f9ba642d8fde837fe3d7d34a0e7f1
RUN apt update
RUN apt install -y libsecp256k1-dev
RUN apt install -y librocksdb-dev
RUN apt install -y pkg-config
RUN apt install -y curl
RUN curl -sSL https://get.haskellstack.org/ | sh
RUN useradd -rm haskoin
USER haskoin:haskoin
# Create the data directory as haskoin, so a volume mounted on it is
# writable (Docker copies the ownership of an existing directory).
RUN mkdir -p /home/haskoin/.haskoin-store
COPY . /home/haskoin/haskoin-store
WORKDIR /home/haskoin/haskoin-store
RUN stack clean --no-terminal --full
RUN stack build --no-terminal --test --copy-bins
# glibc creates up to 8 malloc arenas per core and rarely returns their
# memory to the OS; RocksDB's allocations spread over them and fragment.
ENV MALLOC_ARENA_MAX=2
EXPOSE 3000/tcp
ENTRYPOINT /home/haskoin/haskoin-store/.local/bin/haskoin-store