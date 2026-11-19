# hsq: the store from the command line

`hsq` asks a haskoin-store server how it is, or for anything in its API, and
prints the answer for a person to read: a summary of `/health` with colours,
or JSON laid out and coloured. It is in this repository as `tools/hsq/hsq`, a
single Python file (3.11 or later, standard library only). It is not part of
the Haskell package on Hackage. Install it by copying it onto your `PATH`, for
example `install -m 755 tools/hsq/hsq /usr/local/bin/hsq`.

## Why

`curl` and `jq` can do all of this, but an operator who asks "is it synced,
and is anything wrong?" does not want to read the JSON of `/health` each time.
Since 1.16.3 a store can also listen only on a Unix socket, which needs
`curl --unix-socket` and a URL that `curl` then ignores. `hsq` remembers where
each network's store is, over a socket, HTTP or HTTPS, and says what it sees in
a few lines.

It is a client for people. Scripts and load balancers should read `/health`
and the API as they are: `hsq --raw PATH` prints the bytes unchanged, but the
summary's layout may change from one version to the next.

## Use

```
hsq [-n NET]                  status summary
hsq [-n NET] -w [SECONDS]     the summary, refreshed (every 5 s by default)
hsq [-n NET] PATH             GET an API path and print the JSON, e.g. hsq /block/best
hsq [-n NET] --raw PATH       the same, without colours or layout
hsq --servers                 the configuration files, and the server of each network
hsq --init                    write a commented configuration file, if there is none
```

`-s SOCKET` and `-u URL` use another server for one run. `HSQ_NET`, `HSQ_URL`
and `HSQ_SOCKET` do the same from the environment. A path that answers 400 or
more makes `hsq` exit with 1. Colours are off when the output is not a
terminal, or with `NO_COLOR` set.

The summary shows the version and the network, a bar of the sync with the
height and the headers, the peers, the mempool, the database (whether writes
flow, the compaction backlog, any throttling), Redis, and the checks of
`/health` that fail. While a store syncs, the checks that a sync is expected to
fail (blocks, last block, last transaction) are left out of that list.

For a store syncing on this machine, the summary also shows blocks and
megabytes a second over the last two minutes, and a rough time left at that
rate in blocks. They are read from the "Best block" lines in the journal of its
systemd unit (a user unit first, then a system one; `haskoin-store-NET` unless
the configuration names another), whether `hsq` reaches the store by its socket
or by a URL. A store on another machine has no lines here, and the line is left
out; but a URL to a remote store, with a store of the same unit syncing here,
would show the local one's speed. Take the time left as a rough guide: block
sizes vary more than a hundredfold over a chain's history, so it can be far off
either way (on one Bitcoin Cash sync it said seven hours for what took an hour
and a quarter).

## Configuration

Two TOML files, read in order: `/etc/hsq/config.toml` for everyone, then
`~/.config/hsq/config.toml` for you (`HSQ_CONFIG` names another). Your file
overrides the global one: a top-level key replaces the global one, and a
network's section is merged key by key. `tools/hsq/config.example.toml` is a
global file for a server that runs four networks behind socket units.

```toml
network = "bch"          # the network when -n is not given

[bch]
socket = "/run/user/1000/haskoin-store/bch.sock"
url    = "https://api.haskoin.com/bch"
unit   = "haskoin-store-bch"   # optional; this is the default
```

A network can have a `url`, a `socket`, or both; the socket is used when both
are set, unless `-u` is given. A network with neither uses
`https://api.haskoin.com/NET`. A URL may have a path prefix, as behind a proxy
that serves several networks.

Run without a file of your own, `hsq` writes one: a commented template that
changes nothing when there is a global file, or else a working example that
uses the public API. A user without a writable home goes on without one.

## Errors

`hsq` says in one line what went wrong: no socket at the path (is the socket
unit running?), no permission to open it (join the group that owns it, or use
`-u URL`), a refused connection, no answer within the time limit, a TLS or DNS
failure, an unknown network, or a URL that is not `http://` or `https://`.
