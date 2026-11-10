# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html)

## [Unreleased]

### Added

- `--fleet` (`FLEET`): servers that share a Redis answer as one. Every
  request is answered from the newest block that all live servers have and
  from one mempool that they share in Redis, so that the same request gets
  the same answer whichever server a load balancer picks, also while a
  block or a transaction is on its way through them.
  - Each server keeps its database as it was at each of its last six
    blocks, reports them under a lease of ten seconds, and offers its
    unconfirmed transactions to the shared pool. The fleet tip moves in one
    step, together with what the blocks in between did to the pool.
  - A server that does not have the block that the fleet answers from says
    503 with `behind-fleet`. One whose health check fails stops reporting,
    and stops counting when its lease runs out.
  - Without Redis a server answers from its own database and `/health` says
    203.
  - `Haskoin.Store.Manager` runs the member (`Store.fleet`), the block
    store takes a view of the database right after each block
    (`BlockStoreConfig.onBest`), and the web server answers through
    `Haskoin.Store.Database.Query.Pooled`.
  - An integration test runs two servers as a fleet against a real node and
    Redis and compares their answers with a payment in the pool and after
    the block that confirms it.
  - `docs/layered-database/FleetCost.hs` times a store alone against the
    same store in a fleet, with 40,000 transactions in the pool: a fleet
    adds about one round trip to Redis to a request. A request is answered
    from the tip that the member last read, which its first read of the
    pool checks; a transaction's body comes with what spends its outputs;
    and a first page is as large as the page asked for.

- A cache of xpubs for a fleet (`Haskoin.Store.Fleet.XPub`), under the
  options of the cache of a server alone: `--redis-min-addrs`,
  `--redis-min-txs` and `--redis-max-xpubs`. Until now `--fleet` answered
  every request about an xpub from the database, with a read of the shared
  pool for each address that it touches.
  - An entry has the chain's part of a wallet at one block, which is the
    same for the whole cache, and the part of the shared pool, which the
    script of the pool keeps as it changes the pool. Transactions and
    outputs of both are in one sorted set each, in the order of the API.
  - The cache answers only while its block is the fleet tip. The server
    that moves the tip brings the cache along, with what the new blocks
    did at the addresses that entries have. If it does not, or after a
    reorganisation, the cache is emptied.
  - The cache is at a move of the fleet tip, which the pool counts, and
    not just at a block: the tip can leave a block and come back to it.
  - Every read of balances runs the scan of the xpub over the entry and
    misses unless it reads exactly the addresses of the entry, so a
    payment in the pool to an unused address cannot leave the gap wrong.
  - Entries are built in the background by the server that missed, and
    extended when an xpub gains addresses.
  - A request about a wallet of 464 addresses and 19,499 transactions
    takes 1.3 to 2.2 milliseconds through it, where a fleet took 5 to 38;
    for a wallet with 400 addresses in the pool, 3 to 4 where it took 5 to
    126 (`docs/layered-database/fleet-cost.txt`).
  - The test of the member compares every answer about a wallet through
    the cache with the uncached one after each step of random histories,
    and the two servers of the integration test do the same over HTTP.
  - StatsD metrics under `fleet.cache.*`: hits, misses, builds and their
    time, builds given up, evictions, and the times that the cache was
    emptied. The log says why the cache was emptied each time, and with
    `--debug` has a line for each entry built and each block that the
    cache was brought to.
- Built with `rocksdb-haskell-jprupp` 2.7.1, and every scan by address
  uses an iterator that RocksDB stops where the address ends, so that a
  scan cannot walk over deleted keys that follow it. The migration reads
  the transactions with an iterator that is asked for the order of them
  all. `haskoin-node` still asks for a binding below 2.7: `stack.yaml`
  lifts that bound, and a build with Cabal waits for `haskoin-node`.
- **`/events` agrees across a fleet.** The servers of a fleet send the
  events of the block and the mempool that they share, read from a stream
  in Redis that the pool's script writes as it makes each change, and no
  longer those of their own nodes. A block is told when the fleet has
  moved to it, with every block that it moved through.
- `haskoin-store --help` names the environment variable of every option.
- `swagger.yaml` documents the binary format: how to ask for it, the
  encoding of each type that the API returns, and what differs from JSON.
- Gauges `fleet.cache.xpubs` and `fleet.cache.addresses`: how many xpubs
  the cache of a fleet holds, and how many addresses between them.

### Changed

- Since version 2.6.4 the binding reads the key and the value of a row of
  an iterator without the cost of a safe foreign call.
  Every list and every count takes between a half and a third of the time:
  the first 100 transactions of a busy address 221 microseconds instead of
  575, and the number of transactions of a wallet with 19,499 of them 34
  milliseconds instead of 78.
- **Bloom filters are on by default**, as was always meant: until the
  binding was fixed, `--bloom` built none. `--no-bloom`, or `BLOOM=false`,
  turns them off; `--bloom` is still accepted. An existing database gets
  filters, and the memory that they take in the block cache, as its table
  files are written again.
- **Transactions that leave the mempool unconfirmed are no longer kept.**
  The API already answered 404 for them, but their bodies stayed in the
  database, marked as deleted, for ever, and each server had its own.
  - A server remembers the last hundred thousand that it deleted, in
    memory, so as not to fetch them again from its peers. The fleet's rule
    for conflicting transactions and the cache of a server alone, which
    both asked the database whether a transaction had been deleted, ask
    that memory instead.
  - The cache of a server alone takes a transaction that Redis has as
    unconfirmed for more than ten minutes, and that the database does not
    have, for deleted and not for one that it has not seen yet.
  - Bodies that version 20 marked as deleted stay in a migrated database
    and are ignored. A page that starts at a deleted transaction is empty.
- Removed reports that no longer describe the store: five intermediate
  stress runs, the measurement of a mempool mirrored in memory, and the
  superseded runs in `docs/layered-database/query-cost.txt`.
  `docs/layered-database.md` is rewritten as a description of what
  exists, a quarter of its length.
- **Database version 22: the record of a confirmed transaction is written
  once**, by the block that confirms it, and is the same on every server
  that has the block, so that what the chain's column families hold
  follows from the chain alone.
  - **Its time is the time of its block.** A server that had seen a
    transaction in its mempool used to keep the time of that when a block
    confirmed it: two servers then gave two values of `time` for the same
    confirmed transaction, which the servers of a fleet must not do. A
    transaction that returns to the mempool when its block leaves the main
    chain has the time of that block there.
  - **The spenders of its outputs are under keys of their own**, in the
    `spender` column family, sixteen outputs to a key (`SpenderKey`).
    They were in the record, which each block that spent one of its
    outputs read and wrote whole again. A spend now reads and writes a
    value of a few dozen bytes and leaves the record alone.
  - Reading a confirmed transaction in full takes a second key: 18
    microseconds where it took 11.5, and 0.8 milliseconds for a hundred
    where it took 0.4. Lists by address and by xpub cost the same, and so
    does writing, on the small records of the measurements.
  - A database of version 21 is migrated the first time the store opens
    it (`migrateRecords`). **It reads every transaction of the database
    and writes nearly every one again**, since nothing lists those that
    need it and most have a spender in them: many hours for a database of
    Bitcoin, and room for a second copy of its transactions until RocksDB
    has compacted the first away. The log says how far it is, and it can
    be interrupted and started again.
  - The tests that run the store through random histories now choose them
    from a database of version 20 that goes through the same
    (`withShadow`). Given a layered database, the chooser found next to
    nothing in the mempool, and the blocks of the tests of the fleet
    confirmed almost none of it.
- **Database version 21: the mempool has an area of its own.** Unconfirmed
  transactions are no longer written into the indices of the chain. They
  are kept apart and applied on top when a query is answered
  (`docs/layered-database.md`), so the chain's data changes only when a
  block is imported or reverted.
  - A database of version 20 is migrated the first time the store opens it,
    and the log says how many mempool transactions were moved. **Older
    stores refuse the database afterwards**: copy a production database
    before upgrading.
  - The block store imports with the rules of `Haskoin.Store.Logic.Layered`.
    A block that spends an output of a transaction that is only in the
    mempool is refused as an orphan; no valid block does that.
  - Each request of the web server reads one snapshot of the database for
    everything it answers, so the parts of a response agree with each
    other. `routerScoped` in `Haskoin.Store.Web.Router` is what gives a
    request its snapshot.
  - `--wipe-mempool` deletes the keys of the mempool area, in less time
    than it took to delete each transaction by the rules, and it no longer
    keeps the wiped transactions as deleted ones.
  - `StoreRead` has a new query, `getAddressesTxCount`.
  - Programs that embed the store and read its database with
    `Haskoin.Store.Database.Query` have to move to
    `Haskoin.Store.Database.Query.Layered`: the old queries do not see the
    mempool of a database of version 21.
- Xpub addresses are derived up to an exact gap: every address up to the last
  used one of each chain, then exactly `--xpub-gap` unused ones, and always
  the first `--xpub-gap-init` addresses. Addresses used to be read 32 at a
  time with the gap checked between batches, so the store looked between 33
  and 64 addresses past the last used one, and started counting again at index
  20. This is why api.haskoin.com saw a payment 34 addresses past the last
  used one, which wallets with a gap of 20 or 32 do not see. Payments that far
  away are no longer seen; raise `--xpub-gap` to see them.
- New Redis cache of xpubs (`Haskoin.Store.Cache`), replacing the old one:
  - Whenever a transaction touches a cached xpub, or a reorganisation changes
    one of its transactions, the store reads the whole xpub again from its
    database and replaces the entry in one Lua script, so cached answers are
    always answers of the database.
  - Everything a response needs is in Redis, transaction bodies included,
    shared between xpubs, with the spenders of their outputs kept up to date.
    A store that has not imported a transaction yet still serves it.
  - Several stores can share one Redis. Each change of a cached xpub is
    claimed and applied once, by the first store to see it; stores that catch
    up find their changes done and do no work. A store never replaces an entry
    that knows more than its database (a higher block, or a transaction it has
    not seen), so stores do not undo each other's work, and every entry is
    written together with the bodies it needs, so a store that is behind can
    serve it in full.
  - The rules for claims and for replacing entries are pure decisions
    (`Haskoin.Store.Cache.Protocol`). A model test runs several stores
    catching up with a chain in random interleavings against a model of
    Redis, and checks that they end with the entry of the tip, that no
    store replaces an entry with one that knows less, that each change is
    written once, and that stores catching up after the first write nothing.
  - A reload writes only the differences between the xpub in the database
    and its entry in Redis, read in chunks first, instead of rewriting the
    whole entry; its script checks that the entry did not change since.
    Rewriting an xpub of 15,000 transactions held Redis for up to 77 ms,
    during which it answered no other client. A store reloads up to four
    xpubs at once.
  - A page of the transactions or unspent outputs of a cached xpub is one
    round trip to Redis: a Lua script finds where it starts, from the
    position of the start transaction in the xpub itself, and reads it.
    It took four or five, and read the whole body of the start
    transaction to find its position.
  - A store marks a cached xpub as used at most every ten seconds, for
    eviction, instead of on every read, which made every read a write.
  - `--redis-min-txs` (`REDIS_MIN_TXS`, default 1000) also caches xpubs
    with at least that many transactions in their addresses, however few
    addresses they have.
  - Keys start with `hs2:<network>:`, so the cache can share a Redis with the
    cache of older versions and with stores of other networks.
  - `--redis-max-xpubs` (`REDIS_MAX_XPUBS`, default 10000) replaces
    `--redis-max-keys`: the least recently used xpubs past this many are
    evicted. `--redis-sync-interval` is removed.
  - StatsD metrics: `cache.hits`, `cache.misses`, `cache.reloads`,
    `cache.stale`, `cache.evictions` and `cache.reload` (milliseconds).
- `n_tx` of `/blockchain/multiaddr` counts every transaction of the shown
  addresses and xpubs, not only those on the page, and `n_tx_filtered` those
  that pass the filter. `n_tx` of `/blockchain/rawaddr` for an xpub is its
  number of transactions, not the size of the page.
- The web server runs directly on WAI and Warp, without Scotty. Routes,
  parameters and passing a request to the next route are in
  `Haskoin.Store.Web.Router`, a small router over WAI; a handler is a plain
  function from the state of the server and the request to a response in
  `IO`, and stops early by throwing an `Except`, which becomes the JSON
  error. Every response carries `Access-Control-Allow-Origin: *`, error
  responses included. The endpoints, their parameters and their responses
  are as before.
- No monad transformers, except conduits, and no functions polymorphic over
  `MonadIO` or `MonadUnliftIO`: everything runs in plain `IO`. Database queries are
  functions that take the `DatabaseReader` (`Haskoin.Store.Database.Query`);
  imports take a `Writer` handle (`Haskoin.Store.Database.Writer`,
  `Haskoin.Store.Logic`); the block store and the manager pass their state
  explicitly. The classes `StoreReadBase` and `StoreReadExtra` are replaced
  by `StoreRead`, a record of the same queries as functions in `IO`, which
  `databaseRead` gives for a database; `getTransaction` and the other
  helpers of `Haskoin.Store.Common` take it as their first argument. The state
  of the web server, which every handler takes as an argument instead of
  reading it from a `ReaderT`, holds the queries to answer with, the cache
  in front of those about xpubs;
  `nocache=true` gives a handler a state whose queries read only the
  database. `withStore`, `runWeb`, `runWebSocket` and the `blockStore*`
  functions are in `IO`. `WriterT`, `ImportM`, the `StoreWrite` class,
  `BlockT`, `CacheT`, `DatabaseReaderT` and `withDB` were removed earlier
  in this release. This changes the library API.
- The package uses the GHC2024 edition, with `DuplicateRecordFields`,
  `NoFieldSelectors`, `OverloadedRecordDot` and `OverloadedStrings` on for
  every module, and explicit deriving strategies.

- haskoin-store-data moves to its own repository,
  [jprupp/haskoin-store-data](https://github.com/jprupp/haskoin-store-data),
  with its history and its own releases, and haskoin-store moves to the root
  of this one; `store/` and `data/` are gone. haskoin-store depends on
  `haskoin-store-data ^>= 1.9`. The package has a full description, PVP
  upper bounds on every dependency, and no `Setup.hs`.
- Legacy addresses are called `legacy` in the `derive` parameter, the default.
  `normal`, the name they had, is still accepted, so old URLs keep working;
  it said nothing about the kind of address. Error messages use the new
  name. Built with haskoin-store-data at commit `b304acd2`.
- The executable gives memory back to the system as soon as its heap no
  longer needs it (`--disable-delayed-os-memory-return` in its default RTS
  options). By default GHC marks such memory free for the kernel to take when
  it runs short, and Linux counts it as resident until then, so the heap of a
  busy store seemed to grow without end, by the size of each peak, with little
  live data. The stress test records the resident GHC heap and how much of it
  is lazily freed. The heap profile and the runs that found this are in
  `stress/results/2026-11-09-memory`, and `scripts/heap-profile-summary.py`
  summarises a heap profile.
- The cache brings an entry up to date from what changed, instead of reading
  the whole xpub from RocksDB and Redis on every change, which the stress test
  showed was almost all the time of a reload, growing to seconds at 100,000
  transactions per xpub. A reload reads the changed transactions, the outputs
  they create and spend at addresses of the xpub, and the balances of its
  addresses, which are compared with Redis's to catch a change the entry
  missed. The whole xpub is still read when it is first cached, when the
  entry is more than a block behind, when an address with transactions enters
  or leaves the gap, when a balance does not match, and every ten minutes.
  Entries keep an index of their unspent outputs by outpoint
  (`x:<id>:uop`), in format version 3; entries of version 2 are upgraded by
  their next reload. New metric `cache.full_reloads`.
- A reload whose balances show changes it was not told of first looks for
  them, instead of reading the whole xpub: the transactions that the database
  has for the addresses whose balances moved, in the mempool or in blocks
  after the one the entry was written at, and the mempool transactions of the
  entry that have since been confirmed or dropped, up to 1,000. Under load the
  database is often ahead of the events being applied, which made about one
  reload in ten of the stress test a full one.
- Bodies that Redis already has, such as those of the transactions that a
  block confirms, are rewritten before the commit of a reload, 200 to a
  script, instead of in it. The commit of a block that confirms a thousand
  transactions of an xpub had about 8,000 arguments and held Redis for 10 to
  30 ms, the only commands of the stress test that took that long.
- The scheduled full reload of a cached xpub comes every five to ten
  minutes, an interval that depends on the xpub, instead of every ten for
  all. Xpubs cached together were read whole together each time: in the full
  stress test, four xpubs of tens of thousands of transactions raised the
  heap from under 100 MiB to 785 MiB for a few seconds every ten minutes.
- `docs/cache-consistency.md` analyses how a cached xpub can differ from the
  database without any check noticing, including spenders of cached
  transactions that no reload repairs, and what would prevent it.
- `docs/layered-database.md`: a design for keeping the mempool out of the
  chain's indices, in an area of its own that is applied on top when a query
  is answered, with the measurements behind it and what it means for the
  cache. The store still runs on the layout of version 20; this adds the
  parts that could be proved ahead of the switch:
  - `Haskoin.Store.Database.Types` has the keys of the mempool area
    (`MemSpenderKey`, `MemAddrKey`, `MemDeletedKey`) and a marker for
    migrations that work in several writes (`MigrationKey`).
  - `Haskoin.Store.Database.Mempool` has the keys that one mempool
    transaction contributes, and what the mempool does to the balance and
    the unspent outputs of an address.
  - `Haskoin.Store.Database.Migrate` moves a database of version 20 to the
    layered layout without a resync: it takes every mempool transaction out
    of the chain's indices and writes it to the mempool area, in atomic
    batches that can be interrupted. Nothing calls it yet.
  - `withLayeredReader` opens a database with the extra column family,
    `mempool-addr`, that the layout needs.
  - Tests build databases with the store's own rules from random histories
    of payments, blocks, replacements, deletions and reorganisations,
    migrate them, and check that every query answers as before, that the
    chain's column families are those of a database that never saw a
    mempool, and that emptying the mempool area leaves the chain's answers.
- `Haskoin.Store.Logic.Layered`: the rules that import and revert blocks
  and add, replace and delete mempool transactions in the layered layout of
  `docs/layered-database.md`, with the functions, results and errors of
  `Haskoin.Store.Logic`. Blocks are written to the chain's column families
  alone, and the mempool to its own area. The store does not use them yet.
  - `Haskoin.Store.Database.Writer` can write the mempool area in the same
    atomic batch as the chain; batches of today's rules write nothing there.
  - Tests give the same random history to both sets of rules, on Bitcoin and
    on Bitcoin Cash: payments, replacements, deletions, blocks in and out of
    order, reorganisations, and transactions and blocks that must be
    refused. Every operation must have the same result and move the same
    transactions in and out of the mempool, every query must answer the same
    after it, and at the end the database of today's rules, once migrated,
    must equal that of the new rules key for key.
  - One difference is on purpose: a block that spends an output of an
    unconfirmed transaction is refused by the new rules, as that output is
    not in the chain. Today it is imported.
- `Haskoin.Store.Database.Query.Layered`: every query of the store for the
  layered layout, with the names and arguments of
  `Haskoin.Store.Database.Query`, and `layeredRead`, which gives them as the
  record of queries that the web server answers with. The store does not use
  them yet.
  - A query takes a `View`, a RocksDB snapshot that only `withView` makes,
    so that the chain and the mempool are read as they were at one moment.
  - Pages are cut from the mempool's part of a list followed by the chain's
    part, by today's rules for starts, offsets and limits, for one address,
    for several, and for extended public keys.
  - Mempool spenders are looked up only for the outputs of a transaction
    that no block spends, in one multi-get for a whole page of transactions.
  - `MemDeletedKeyP` matches deleted transactions by the start of their
    hash, for the short transaction numbers of the Blockchain.info API.
  - Tests ask today's queries and the new ones for pages with random
    limits, offsets and starts, on databases migrated from today's layout,
    on databases built by the rules of the layered layout, and on databases
    upgraded in the middle of a history. They also check that an open view
    goes on answering from its moment while blocks and transactions are
    written.
- A summary for each address that the mempool touches, in the layered
  layout (`MemSummary`, under `MemAddrSumKey`): the number of its mempool
  transactions, and the amounts and numbers of the outputs that the mempool
  pays to it, spends, and both pays and spends. The measurements showed the
  queries of that layout to be too slow for an address with much in the
  mempool, and for any address right after a block, because they scanned
  everything that the mempool has for an address to answer anything.
  - A balance is now two reads by key, and an address with nothing in the
    mempool is found by a key that is not there.
  - Lists of transactions and of unspent outputs read the mempool's entries
    only for addresses that have some, and only as far as the page goes.
  - The rules keep the summary with every transaction that enters or leaves
    the mempool, and the migration writes all of them from the entries that
    it moved. The tests work every summary out again after each operation.
  - Measured again with `QueryCost.hs`: the balance of an address with
    6,216 entries in the mempool takes 11 microseconds instead of 35
    milliseconds, a page of its transactions what it takes in version 20,
    the balances of a wallet of 464 busy addresses 1.9 milliseconds instead
    of 110, and nothing gets slower right after a block. Keeping the
    summaries costs about 110 microseconds for each transaction that passes
    through the mempool. `docs/layered-database.md` has the tables.
- `docs/layered-database/QueryCost.hs` writes one synthetic history into a
  database of each layout and times the rules and the queries of both,
  checking that the answers agree. `docs/layered-database.md` has the
  results and what follows from them: writing costs the same when syncing
  and 14% more for a transaction that passes through the mempool; the new
  queries cost 10 to 15 microseconds more for a request when the mempool
  has nothing for what is asked; and they are too slow, as built, for
  addresses that the mempool touches a lot and for any address right after
  a block. The document proposes a summary for each address in the mempool
  area, read by key, and two changes to the RocksDB binding.
- Documentation: a page that starts at a height has no unconfirmed
  transactions, whatever the height. The comment on `AtBlock` said that a
  height above the best block includes them, and the API reference could be
  read that way. The behaviour is unchanged.
- The stress report breaks the commands that blocked Redis down by script,
  and commits by full and incremental reloads, with their numbers of
  arguments.
- `Haskoin.Store.Fleet`: the rules by which stores that share a Redis agree
  on one block to answer from, the fleet tip, as pure decisions with a model
  test. It is the first part of the design for a fleet that answers as one,
  in `docs/layered-database.md`. Nothing uses it yet.
  `Haskoin.Store.Fleet.Redis` carries the rules out against Redis, with a
  Lua script for each step that has to be one step, and a test that takes
  random sequences of steps through a real Redis and through the rules and
  compares them after each. `Haskoin.Store.Fleet.Pool` has the rules of the
  mempool that such stores share, as of the fleet tip: what may be offered,
  which of two conflicting transactions stays, what leaves and why, and how
  the pool follows the tip forwards and across a reorganisation. A model
  test keeps it valid on top of the tip through random offers, blocks and
  reversals. One more Lua script carries these rules out, moving the tip and
  changing the pool in one step, and is compared with them step by step in a
  real Redis like the others; the comparison found a case that the rules had
  left open, a transaction that comes back from an abandoned block and
  spends what the pool already spends, which now wins. The pool also keeps,
  for each address, what its transactions do there: the transactions and
  the outputs paid to it in the order of a list, the outputs of it that are
  spent, and their sums, so that a balance is one read and a list is read
  by the page, as in a store's own mempool area.
- `Haskoin.Store.Database.Query.Pooled`: the queries of the store with a
  mempool that is not its own on top of the chain, for the shared pool of a
  fleet. `read.lua` and `poolRead` read that pool from Redis, each read
  with a check that the fleet tip has not moved. The tests hold these
  queries to those of the layered layout with the store's own mempool as
  the pool. `fleetAdvance` can empty the pool for a store that is alone and
  does not know the way from the old tip.
- `Haskoin.Store.Fleet.Member`: what a store does as a member of a fleet.
  It keeps a view at each recent block, reports them, moves the fleet tip
  with what the blocks in between did to the pool, offers its mempool, and
  answers from the view at the tip with the pool on top. A test with a real
  Redis takes a store that is alone in its fleet through random histories
  and compares every answer with the store's own after each step. It found
  that a body kept in the pool went stale when a parent was confirmed or
  came back, because a transaction inherits from its unconfirmed parents
  whether it can be replaced by fee; the pool now keeps that apart from the
  body and corrects it as transactions come and go.
- `Haskoin.Store.Database.Views` keeps the database as it was at each recent
  block, a RocksDB snapshot for each, so that a store can answer from the
  block that its fleet agrees on and not only from its newest. Views that
  are no longer needed are let go, and one that a request is reading is
  released when the request is done. Nothing uses it yet.
- A store whose Redis does not answer goes on answering from its database,
  instead of failing every request that the cache would have served. A
  request waits at most two seconds for Redis; after that the store leaves
  Redis alone until a health check finds it again. Meanwhile `/health`
  answers 203, Non-Authoritative Information, with the header
  `Haskoin-Degraded: redis`, when every other check passes: a status in the
  200s, so that load balancers keep the store, and not 200, so that
  monitoring notices. The body of `/health` has a new check, `redis`, for a
  store that has a cache; it does not count towards `ok`. It comes with
  haskoin-store-data 1.9.0, which the store now takes from Hackage.
- Breaking: transactions that were deleted are no longer returned. A
  transaction that was replaced, dropped from the mempool, or was the
  coinbase of a block that left the main chain used to answer by id with
  `"deleted": true`; clients that did not look at that field took it for a
  pending payment. It now gives 404, is left out of lists of transactions
  fetched by id, and a page cannot start at it. The `deleted` field stays in
  responses and is always false. The bodies stay in the database.
- Breaking: parameters are checked. A request with a parameter that its
  endpoint does not accept, such as a misspelt `limt=10` or `pretty` on the
  Blockchain.info endpoints, which never honoured it, gives 400
  `Unknown parameter: …`; and a value that cannot be read, such as
  `notx=yes`, `limit=-1`, `filter=4`, `format=xml` or an unknown currency,
  gives 400 `Invalid value for parameter …`. Both used to be ignored, and
  the client got an answer to a question it did not ask. Requests are
  checked before anything is done. The Blockchain.info endpoints accept
  `format=json`, which their clients often send. Each route of the router
  declares the parameters it accepts, and reading an undeclared one is an
  error of the program, so the two cannot drift apart.
- An xpub asked about with the wrong `derive` gives 400, with a message that
  names the right kind, instead of an empty wallet: when the xpub has no
  transactions with the kind asked for, the store checks the first
  `--xpub-gap-init` addresses of the other kinds. This applies to the `/xpub`
  endpoints and to `/blockchain/rawaddr`. A wallet unused with every kind
  still gets an empty answer, and wallets in use cost nothing more.
- `swagger.yaml` explains the API, not only lists it: an introduction to its
  conventions (amounts, ordering, paging, formats, errors, unconfirmed
  transactions, reorganisations, xpubs, live events, Blockchain.info
  compatibility) and the reasons for them, and a description of every
  endpoint, parameter and field. It documents `/dbstats`, the WebSocket at
  `/events` and the `time` field of the health check, which were missing,
  and corrects the health check examples and the version.
- The README tutorial pages with `height`, the parameter the server reads;
  it said `start`, which the server ignores.
- Every line of code is shorter than 120 columns, `swagger.yaml` included,
  whose long hexadecimal and xpub examples are split into equal pieces that
  parse to the same values. `scripts/format` formats `integration/` and
  `stress/` too.
- Scripts live in files of their own: the Lua scripts of the cache in
  `src/Haskoin/Store/Cache/lua/`, embedded at compile time with
  `file-embed`; the script of wrk in `stress/wrk.lua`, one for every
  endpoint, given its paths on the command line; and the shell steps of CI
  in `scripts/check-local-deps.sh` and `scripts/install-system-libraries.sh`.
  The scripts do the same as before.

### Fixed

- `--bloom` builds Bloom filters. It built none when the store was built
  against RocksDB 6.24 or later, as the Docker image is, because
  `rocksdb-haskell-jprupp` passed the size of the filter to RocksDB with the
  wrong type. Nothing failed; a lookup of a key that is not there just went
  on reading data blocks to find that out. The store is now built with
  `rocksdb-haskell-jprupp` 2.6.3, which has the fix. A store that already
  runs with `--bloom` starts building filters when it is upgraded, for the
  table files written from then on: about 1.25 bytes for each key, held in
  the block cache. Size that on a full database, or turn the option off to
  keep things as they were.
- `--bloom` no longer asks for filters on the two indices by address, which
  are scanned and not read by key. With a filter, a scan of an address that
  has nothing steps over every key deleted since the last flush: 810
  microseconds after 5,000 mempool transactions were deleted, against 13
  without filters. No store was affected, because no filter was ever built;
  with the fixed binding they would have been. The option still builds
  filters for transactions, unspent outputs, blocks, balances and the
  mempool.
- `/blockchain/multiaddr` no longer reads every transaction of the wallet to
  count them. `n_tx` of one xpub is its count (a `ZCARD` when it is cached);
  for several xpubs and addresses, the database counts the distinct
  transactions of all their addresses from the keys of its index. The
  transactions of a page are read in pages of about the size asked for, and
  their bodies a page at a time, instead of one by one; `/blockchain/rawaddr`
  does the same. In the stress test, with xpubs of about 15,000
  transactions, the median time of a multiaddr request went from 3.2 seconds
  to 36 milliseconds, and it no longer grows with the wallet.
- Containers left by an integration test run that was killed are removed
  by the next run, as intended: listing them used a template that Podman
  refuses, so they were never found.
- `--peer HOST[:PORT]` works as documented. It was read as a Haskell
  string, so only `--peer '"HOST:PORT"'`, with the quotes, was accepted.
- The syncing peer is disconnected when it sends a block that was not
  requested, as the documentation of the block store said. The block was
  imported if the node knew its header. The state of the block download is
  now a pure state machine (`Haskoin.Store.BlockStore.Sync`) with property
  tests, which also found that a block listed twice in one request was
  outstanding twice.
- The block store no longer starts a thread for every transaction it
  requests, to forget the request twenty seconds later. Requested and
  received mempool transactions are one pure value
  (`Haskoin.Store.BlockStore.Pending`), with property tests, and old
  requests are forgotten on the clock tick of the block store.
- Build against haskoin-node 2.4.0, from the head of its
  `fix-header-sync-stall` branch. The block store reserves its syncing peer
  as an owner of its own (`reservePeer`, `releasePeer`), so no other part of
  the node can release it; header searches run with `chainRunHeaders`. The
  store stopped following the chain
  when a peer announced new blocks with an inventory message instead of
  headers, which peers do when many blocks are found at once; it stayed at
  the old height until the peer reconnected.
- Mempool transactions that entered the mempool in the same second could be
  listed twice in the transactions of an xpub or a multiaddr wallet, and
  their order differed from page to page. Every merge now orders them as the
  database and the cache store them (`txRefKey`, `unspentKey`).
- `nocache=true` on `/xpub/:xpub/transactions` and `/xpub/:xpub/unspent` no
  longer reads, or fills, the cache.
- `confirmations` of `/blockchain/unspent` no longer wraps around for an
  output in a block newer than the chain of the store.

### Added

- `scripts/stress-cache.sh`, a stress test of the Redis cache: a regtest
  chain, Valkey and wrk in Podman, a haskoin-store server, hundreds of
  thousands of transactions between a few wallets, and wrk on their xpubs
  all the while, with a report (`scripts/stress-report.py`) of reloads,
  lag, resources, the commands that blocked Redis (from its slow log) and
  latencies, and a check of the cached answers at the end. The Podman
  helpers of the integration tests are an internal library (`integration`)
  that the stress program shares, and the test image has wrk in a layer of
  its own.
- `stress/results/2026-11-09/`: the results of a full default stress run,
  kept for comparison with later runs.
- `runWebSocket` serves the API on a socket that already listens. The tests
  bind port 0 and pass the socket, so their web servers get free ports with
  no window in which another program could take them; the containers publish
  their ports on random ports of the host.
- Integration tests against regtest chains of Bitcoin Core and Bitcoin Cash
  Node, with Valkey, in Podman containers built from Ubuntu 26.04 and
  release tarballs checked against pinned hashes, one cached layer per
  download, so nothing is downloaded twice. They are skipped when Podman
  cannot run containers or `HASKOIN_STORE_NO_PODMAN` is set.

## [1.8.1] - 2026-11-07

### Changed

- Documentation: the README explains the purpose and design of the store, with
  installation steps, a full configuration reference, monitoring and a
  tutorial, and the Haddock of every module and function says why it works the
  way it does.
- Build against nqe 0.6.9, rocksdb-haskell-jprupp 2.6.1, rocksdb-query 0.6.1,
  statsd-rupp 0.5.0.2, secp256k1-haskell 2.0.1, haskoin-node 2.3.1 and
  Stackage nightly-2026-11-07.

## [1.8.0] - 2026-11-07

### Changed

- Index the mempool by the time each transaction entered it. `/mempool`,
  `/blockchain/unconfirmed-transactions` and the health check read only the
  page they return, newest first, instead of loading and sorting the whole
  mempool on every request. Data version 20: a version 18 or 19 database is
  migrated when it is opened, with no resync.
- The block store no longer reads the whole mempool to publish mempool
  events, to set the mempool gauge or to find expired transactions. Each
  import reports which transactions it added to the mempool or removed from
  it; pruning reads only the oldest entries of the index. Before, every
  block, every batch of new transactions and every clock tick (several per
  second) read the whole mempool at least once.
- Count the transactions of an xpub without building the list of all of them
  when the cache is not used.
- Keep the derived addresses of the 512 to 1024 most recently queried xpubs
  in memory. Every xpub query derived each address again before reading a
  balance. Addresses depend only on the xpub, so the cache never goes stale.
- Page the unspent outputs of an xpub by merging its addresses in order of
  block, stopping once the page is full, instead of decoding up to offset
  plus limit outputs of every address and sorting them all. The pages are
  the same.
- Count the transactions of an xpub by merging the raw keys of its
  addresses, without decoding keys or reading values.
- Open the iterators of a query over many addresses in one bracket instead
  of one bracket each. Every call into RocksDB walked the stack past one
  frame per address.
- Double the page, up to 1024 rows, when streaming a source by offset, as
  `/blockchain/unspent` does. The rows read and skipped grew with the square
  of the rows returned.
- Check that an xpub is cached in the same Redis round trip as the read of
  its balances, transaction count, or a page of its transactions or unspent
  outputs that does not start at a transaction.
- Serialize a transaction at most twice to compute its id, size and weight,
  and stop looking up its witnesses with a quadratic list index.
- Build on haskoin-core 1.6.1, which encodes CashAddr and Base58 addresses
  faster: about 10% less CPU per request on endpoints that return many
  addresses.
- The executable uses every core of the host by default (`-N` instead of
  `-N4`). Set `+RTS -N<n> -RTS` on the command line to choose a number.
- Read the balances of many addresses, and lists of transactions, with one
  RocksDB multi-get instead of one read per key. `StoreReadBase` has a new
  method, `getManyTxData`, which defaults to reading the transactions one by
  one; `getTransactions` reads many transactions with it.
- Give RocksDB a block cache of 1 GiB by default, shared by all column
  families, and keep their index and filter blocks in it at high priority,
  with those of level 0 pinned. Before, each column family had RocksDB's
  default cache of 32 MiB, and index and filter blocks stayed in memory
  outside any cache for every open file. Set the size with `--block-cache-mb`
  or `BLOCK_CACHE_MB`; 0 restores the RocksDB defaults. `StoreConfig` and
  `withDatabaseReader` take the size in bytes. Requires rocksdb-haskell-jprupp
  2.6.0.

### Added

- `store/bench`: a benchmark server that syncs a synthetic regtest chain and
  serves the web API on it, and times the encoding of a response with the
  argument `micro`. `scripts/bench-web.sh` runs wrk against it and reports
  the CPU time of the server per request.

## [1.7.0] - 2026-11-06

### Changed

- Build on haskoin-core 1.6.0 and haskoin-node 2.3.0, the releases with the
  fixes of the November 2026 security review. Their changelogs list the
  changes to message parsing, header validation and peer handling that the
  store now runs with.
- An output has an address only when the network defines one for its script,
  as in haskoin-core 1.6.0. A SegWit-shaped output has no address on Bitcoin
  Cash. A 256-bit script hash output (`OP_HASH256`, a push of 32 bytes,
  `OP_EQUAL`) has none on Bitcoin. Such an output is no longer indexed under
  an address, and its `address` is `null`. Entries that an older version
  wrote under such an address stay in the database. No request can name them,
  since the address has no text form on the network.
- `POST /transactions` takes a body that holds one transaction and nothing
  else. Bytes after the transaction are refused, as Bitcoin Core does for a
  raw transaction. They used to be ignored.
- `POST /transactions` takes the transaction in the format that peers of the
  network read. A network without SegWit, such as Bitcoin Cash, has the
  format without witness only.
- `GET /peers` gives the `version` of a peer as a signed number, as
  haskoin-core 1.6.0 reads it. A peer that sends version ffffffff has version
  -1, where it used to appear as 4294967295.
- Breaking, in `haskoin-store-data`: `toTransaction` takes the `Network` as
  its first argument, and the `version` field of `PeerInfo` is an `Int32`.
  The binary form of `PeerInfo` is the same bytes as before.
- From haskoin-node 2.3.0: a peer is disconnected when it sends more than
  50000 entries in an `inv` or `getdata` message. On Bitcoin Cash a message
  may be at most 2 MiB, unless it carries block content. Transactions from
  Bitcoin Cash peers are read in the format without witness only.

### Fixed

- A response no longer fails on a transaction with an output that has no
  address on the network. Since haskoin-core 1.5.0 the JSON encoder of such
  an address threw, which cut the response off. One such output is in Bitcoin
  block 211997 (transaction
  `a4bfa8ab6435ae5f25dae9d89e4eb67dfa94283ca751f393c1ddc5a837bbc31b`,
  output 0).

## [1.6.5] - 2026-11-04

### Fixed

- Build on haskoin-core 1.5.1, which evaluates the ASERT difficulty rule at the
  parent block. With 1.5.0 the store rejected a valid block on Bitcoin Cash
  mainnet at height 975899.

## [1.6.4] - 2026-11-04

### Added

- Support Bitcoin testnet4 (`NET=btctest4`), from haskoin-core 1.4.0.
- Option `--block-window` (`BLOCK_WINDOW`) to set how many blocks to request
  from the syncing peer at a time.
- Option `--write-buffer-mb` (`WRITE_BUFFER_MB`) to cap RocksDB memtables
  across all column families, 256 MiB by default (0 = unlimited). Without a
  cap the nine column families can keep up to 1.1 GiB of memtables.
- Option `--max-wal-mb` (`MAX_WAL_MB`), 1024 MiB by default (0 = RocksDB
  default), to bound the RocksDB write-ahead log: once it is larger, the
  column families holding its oldest file are flushed so the file can be
  deleted.
- `/dbstats` starts with the memory RocksDB uses outside the GHC heap:
  memtables and table readers for each column family, and the block cache.
- The mempool is kept in its own column family, one key per transaction,
  instead of as a single list value that every import batch read, loaded
  into a hash table and wrote back whole. Data version 19: a version 18
  database is migrated when it is opened, with no resync.

### Changed

- Replace monad-logger with the fast-logger `Logger` from haskoin-node 2.0.0.
- Request 64 blocks at a time from the syncing peer instead of 500, and ask
  for more once fewer than 16 remain, to bound memory use while catching up.
- haskoin-store-data builds against secp256k1-haskell 2.0.0, which hides the
  raw `PubKey` constructor: `XPubSpec` reads its stored key with `pubKey`,
  which checks it.
- Build on haskoin-core 1.5.0, haskoin-node 2.1.2 and secp256k1-haskell 2.0.0.
  `genesisBlock` no longer takes a context.
- A pay-to-public-key output has no address any more, as in haskoin-core
  1.5.0. Its coins cannot be spent with the input script of the P2PKH address
  of the same key, so they are no longer counted under that address.
- The block store no longer reads the whole mempool four extra times on every
  internal ping: it compares mempool snapshots only when there are pending
  transactions to import or expired ones to delete.
- Run with four capabilities (`-N4`) instead of one per core (`-N`), so that
  memory overhead does not grow with the size of the host. Override with
  `+RTS -N<n> -RTS`.
- Docker image: pin the base image to Debian trixie (RocksDB 9.10) by digest
  instead of `debian:latest`, and run with `MALLOC_ARENA_MAX=2` to limit
  malloc fragmentation from RocksDB.
- Each `/events` client, websocket or HTTP stream, gets a queue of at most
  10,000 events and is disconnected when it fills up, instead of the queue
  growing for as long as the client stays connected. Store events that
  `/events` does not show are no longer queued for its clients.
- A request waiting for another to load an xpub into the cache retries for
  about 30 seconds, with at most 5 seconds between attempts, and then serves
  the xpub from the database. It used to retry forever with a delay that
  doubled each time.

- Continuous integration on GitHub Actions: build, run both test suites,
  and check that the heap stays flat while syncing 6000 regtest blocks.

### Fixed

- Support full-RBF: accept replacements of transactions that do not signal BIP125.
- Correct various space leaks.
- `Unspent` implements `coinInputKind`, new in haskoin-core 1.5.0, from its
  output script, so coin selection with it no longer fails at run time.

### Added

- Support for testnet4 on both networks.

## [1.6.3] - 2026-08-31

### Added

- Allow disabling web server to sync block chain only.

## [1.6.2] - 2026-08-13

### Fixed

- Fix upstream regressions in haskoin-node.

## [1.6.1] - 2026-08-13

### Fixed

- Support latest version of haskoin-node.

## [1.6.0] - 2026-08-12

### Fixed

- Support for Bitcoin Cash 256-bit scripts.
- Address parsing now allows only correct lengths.

## [1.5.16] - 2025-05-08

### Changed

- Move to personal repository.

### Removed

- Disable `NO_SLOW` as an option.

### Added

- Allow to set `NO_XPUB` to disable extended public keys.

## [1.5.14] - 2024-12-04

### Changed

- Move `/dbstats` and `/mempool` to slow endpoints section.

### Added

- Cull old transactions from mempool automatically.

## [1.5.13] - 2024-10-25

### Changed

- Dummy release: update upstream LTS Haskell dependency in `stack.yaml` file.

## [1.5.12] - 2024-07-22

### Added

- Metric to track time to import a new block.
- Parameter `no_compact` allowed for backwards compatiblity.

## [1.5.11] - 2024-05-24

### Fixed

- Parameter `noCompact` corrected for `multiaddr` endpoint.

## [1.5.10] - 2024-05-24

### Fixed

- Prune obsolete transactions from outgoing peer requests.
- Remove unnecessary module imports.

## [1.5.9] - 2024-05-23

### Fixed

- Only sync mempool once for each peer.

## [1.5.8] - 2024-05-23

### Fixed

- Dummy release to bump upstream dependency on `haskoin-node`.

## [1.5.7] - 2024-05-23

### Fixed

- Dummy release to bump upstream dependency on `statsd-rupp`.

## [1.5.6] - 2024-05-22

### Changed

- Allow to toggle RocksDB Bloom filters off.

### Fixed

- Correct command-line option behaviour for some rarely-used parametres.

## [1.5.5] - 2024-05-19

### Changed

- Lock Redis before checking if the database needs to be pruned, rather than after.
- Lock xpub in Redis to avoid computing derivations multiple times.

### Fixed

- Rename redis-min-gap to redis-min-addrs to match environment config variable.

## [1.5.4] - 2024-05-07

### Fixed

- Fix Redis bug where old keys weren't pruned quickly enough.

### Changed

- Remove Nomad-specific environment variable parsing for statsd.

## [1.5.2] - 2024-04-24

### Changed

- Use default GHC garbage collector.

## [1.5.1] - 2024-04-23

### Changed

- Compile with GHC 9.8.2.
- Enable optimisations.

## [1.5.0] - 2024-04-06

### Fixed

- Use an optimised topological sort for importing transactions in bulk.

## [1.4.0] - 2024-03-27

### Changed

- Use default optimisation settings.
- Use default RocksDB parameters.

## [1.3.0] - 2024-03-14

### Changed

- Bump upstream dependencies.

## [1.2.5] - 2023-01-31

### Fixed

- Test suite now working again.

## [1.2.4] - 2023-01-31

### Changed

- Bump upstream dependencies.

## [1.2.3] - 2023-12-06

### Changed

- Transactions received via POST are just forwarded to every peer without delay or validation.
- Bump dependency on NQE to resolve async issue.

## [1.2.2] - 2023-10-21

### Fixed

- Fix comparison function for unspent outputs.

### Changed

- Avoid deprecated `param` function from Scotty.

## [1.2.1] - 2023-10-04

### Fixed

- Correct web exception output format (JSON).
- Correct block path parsing order.

## [1.2.0] - 2023-10-03

### Changed

- Upgrade `scotty` dependency and fix incompatibilities.

## [1.1.0] - 2023-09-15

### Added

- Add cache height stats.
- Add database iterator stats.

### Changed

- Detect xpub gap more strictly.
- Use statsd-rupp instead of ekg-stats.
- All stat reporting keys changed.

### Removed

- Remove unsafeInterleaveIO from Web.hs.
- Remove unnecessary WAI middlewares.
- Remove server and client error reporting per endpoint.

## [1.0.2] - 2023-08-25

### Changed

- Code refactoring and simplification.
- Improve logging output.

### Removed

- Do not impose time limits on REST API responses.

### Fixed

- Remove loop in Blockchain.info unspents function.
- Correct documentation for /blockchain/rawblock endpoint.
- Fix no match in record selector address for coinbases.

## [1.0.1] - 2023-08-03

### Changed

- Add compatibility with latest nightly LTS Haskell.

## [1.0.0] - 2023-07-28

### Changed

- Compatibility with latest haskoin-core and haskoin-node packages.
- Use DuplicateRecordFields and OverloadedRecordDot language extensions.
- Modernize package in a backwards-incompatible manner.
- Refactor package extensively.

## [0.65.10] - 2023-05-12

### Changed

- Revert to older LTS Haskell to attempt to resolve memory leak.

### Removed

- Remove unnecessary lock release.

## [0.65.9] - 2023-05-09

### Changed

- Be more aggressive caching individual transactions.
- Introduce a periodic cache mempool sync task.

## [0.65.8] - 2023-05-08

### Changed

- Perform health check in separate thread.

## [0.65.7] - 2023-05-05

### Changed

- Do not sync mempool to Redis if node is not up-to-date.

## [0.65.6] - 2023-05-04

### Changed

- Improve caching ingestion performance.

## [0.65.5] - 2022-09-19

### Added

- Fee field added to Blockchain.info-style export-history endpoint.

### Changed

- Bump version due to Git mistake.

## [0.65.3] - 2022-04-13

### Added

- IPv6 peer support.

## [0.65.2] - 2022-04-13

### Fixed

- Correct lazy bytestring deserialization algorithm for web client.

## [0.65.1] - 2022-04-13

### Changed

- Simplify function in Cache module.
- Bump depenedencies.

## [0.65.0] - 2022-02-03

### Changed

- Spenders now embedded into tx objects in database for performance.
- Use faster IORef instead of slow TVar when ingesting.
- Use mutable hash tables when ingesting.
- Allow to disable unscalable API endpoints.

## [0.64.19] - 2022-02-01

### Added

- Allow to set query timeouts.

## [0.64.18] - 2022-01-21

### Fixed

- Off-by-one error when importing change addresses to cache.

## [0.64.17] - 2022-01-21

### Fixed

- Add change addresses to cache when necessary.

## [0.64.16] - 2022-01-10

### Fixed

- Correct fee calculation for blocks that do not claim all rewards.

## [0.64.15] - 2022-01-07

### Fixed

- Correct scale of cache pruning code.

## [0.64.14] - 2022-01-07

### Fixed

- Always prune to 80% of max cache size.

## [0.64.13] - 2022-01-07

### Fixed

- Avoid doing a hissy fit on transient cache issue.

## [0.64.12] - 2022-01-07

### Fixed

- Prune a little more cache to allow for new keys.

## [0.64.11] - 2022-01-06

### Fixed

- Prune cache more often.

## [0.64.10] - 2021-12-30

### Changed

- Return empty addresses in xpub balances call.

## [0.64.9] - 2021-12-29

### Fixed

- Avoid notifying same transactions in mempool.

## [0.64.8] - 2021-12-26

### Changed

- Use better Fourmolu format.
- Be more precise when counting retrieved items.

### Fixed

- Avoid transactions duplicated in xpub data.
- Refactor some functions to make them more robust.

## [0.64.7] - 2021-12-26

### Fixed

- Optimisations for various Blockchain endpoints.
- More accurate item counts for various web endpoints.

## [0.64.6] - 2021-12-25

### Fixed

- Record cache index time just once per xpub.

## [0.64.5] - 2021-12-25

### Changed

- Add suffix cached to cache entries to avoid conflicts with data entries.

## [0.64.4] - 2021-12-25

### Fixed
- Bring dots back and avoid stats hacks.

## [0.64.3] - 2021-12-25

### Fixed

- Fix typo.

## [0.64.2] - 2021-12-25

### Changed

- Replace dots for underscores in metrics.

## [0.64.1] - 2021-12-25

### Changed

- Automatic formatting.
- Improve cache debug logging.

### Fixed

- Add item count to 'all' query metrics.

## [0.64.0] - 2021-12-25

### Changed

- Improve metrics.

### Fixed

- Get lock before importing transactions to cache.

## 0.63.0
### Fixed
- Make cache updates lightweight.

## 0.62.1
### Fixed
- Fix idempotent transaction elimination bug.

## 0.62.0
### Added
- Configurable Blockchain price URLs.
- Reuse HTTP connections with wreq sessions.

## 0.61.1
### Changed
- Upstream improvements on Base58 performance.

## 0.61.0
### Fixed
- Fixed recursion bug in Blockchain format conversion function.

## 0.60.0
### Fixed
- Correct notifications subsystem.

## 0.59.0
### Fixed
- Correct internal dependency.

## 0.58.0
### Fixed
- Correct output freeing algorithm.

## 0.57.0
### Changed
- Report errors freeing outputs.

## 0.56.0
### Changed
- Report all deleted mempool transactions.
- Crash on certain data corruption.

## 0.55.0
### Changed
- Always have a previous output object in Blockchain inputs.

## 0.54.0
### Added
- WebSocket support.
- Testnet4 support.

## 0.53.11
### Changed
- Dummy version increase to signal upstream update of Haskoin Core.

## 0.53.10
### Fixed
- Correct test that was reversed in previous version.

## 0.53.9
### Removed
- Mempool is no longer synced by default to fix public Bitcoin Core regression.

## 0.53.8
### Changed
- Put derivations stat inside database.

## 0.53.7
### Added
- Added counters for database retrievals.
- Added counter for xpub derivations.

### Changed
- Removed some buggy or unnecessary stats.

## 0.53.6
### Changed
- Improve web server statistics.

## 0.53.5
### Addded
- Endpoint to delete an xpub from cache.

## 0.53.4
### Added
- Debugging when reverting block.

### Fixed
- Negative (headers - blocks) diff shown incorrectly.
- Infinite loop when removing a block's coinbase transaction.

## 0.53.3
### Added
- Blockchain.info endpoint: `q/pubkeyhash/:addr`.
- Blockchain.info endpoint: `q/getsentbyaddress/:addr`.
- Blockchain.info endpoint: `q/pubkeyaddr/:addr`.
- Configurable request body limit size.

## 0.53.2
### Fixed
- Return output in satoshi for some endpoints.

## 0.53.1
### Fixed
- Return 404 in case of bad address.

## 0.53.0
### Added
- Blockchain.info endpoint: `q/addresstohash/:addr`.
- Blockchain.info endpoint: `q/addrpubkey/:pubkey`.
- Blockchain.info endpoint: `q/hashpubkey/:pubkey`.
- Blockchain.info endpoint: `q/getblockcount`.
- Blockchain.info endpoint: `q/latesthash`.
- Blockchain.info endpoint: `q/bcperblock`.
- Blockchain.info endpoint: `q/txtotalbtcoutput/:txid`.
- Blockchain.info endpoint: `q/txtotalbtcinput/:txid`.
- Blockchain.info endpoint: `q/txfee/:txid`.
- Blockchain.info endpoint: `q/txresult/:txhash/:addr`.
- Blockchain.info endpoint: `q/getreceivedbyaddress/:addr`.
- Blockchain.info endpoint: `q/addressbalance/:addr`.
- Blockchain.info endpoint: `q/addressfirstseen/:addr`.
- Support xpubs in `blockchain/rawaddr` endpoint.

### Fixed
- Various latest blocks endpoint fixes.
- Allow empty parameters in Blockchain.info POST requests.

## 0.52.13
### Fixed
- Upstream fixes for unknown inv types.

## 0.52.12
### Fixed
- Fix limits for legacy endpoints.

## 0.52.11
### Added
- Alias for Blockchain.info raw address endpoint.

### Fixed
- Fix unknown message types without payload upstream.
- Randomise peer timeouts upstream.

## 0.52.10
### Fixed
- Fix binary search algorithm for blocks upstream.

## 0.52.9
### Added
- Add Blockchain.info latest block endpoint.
- Add Blockchain.info unconfirmed-transactions endpoint.
- Add Blockchain.info blocks for one day endpoint.

## 0.52.8
### Fixed
- Fix special case for zero limit.

## 0.52.7
### Fixed
- Fix cache bug when too many transactions in mempool.

## 0.52.6
### Changed
- Use rationals for Blockchain.info export-history endpoint arithmetics.

## 0.52.5
### Added
- Support for Blockchain.info export-history endpoint.

### Fixed
- Use strict bytestring decoder for HTTP API client.

## 0.52.4
### Fixed
- Use Base58 addresses by default in Blockchain.info balance querios.

## 0.52.3
### Fixed
- Errors should also contain CORS headers.

## 0.52.2
### Fixed
- Blockchain.info raw address limit should use `limit` parametre and not `n`.

## 0.52.1
### Changed
- Stream multiple transactions when requested.

### Fixed
- Fix missing unspent outputs.

## 0.52.0
### Changed
- Remove unnecessary performance optimisations.

### Fixed
- Reduce balance retrievals for xpubs.
- Hack UTXO ordering to avoid unwanted behaviours.
- Do not merge conduits when there are too many.

## 0.51.0
### Fixed
- Data type for RequestTooLarge was missing.

## 0.50.3
### Added
- Log HTTP request bodies.

### Removed
- Remove web request timeout.

### Fixed
- Do not ignore unknown start tx anchors in conduits.
- Use fast transaction counting in cache.

### Changed
- Use WAI middlewares to manage stats.

## 0.50.2
### Fixed
- Do not use lazy parsers for incoming data that may throw exceptions.

## 0.50.1
### Fixed
- Do not allow incoming POST requests of unlimited body size.

## 0.50.0
### Added
- Limit number of simultaneous xpubs being cached.

## 0.49.0
### Changed
- Improve conduit merging algorithm.

## 0.48.0
### Added
- Timeouts and token bucket for web requests.
- Configurable price fetch interval.

## 0.47.6
### Fixed
- Fix web.all stat.

## 0.47.5
### Changed
- Simplify stats.

## 0.47.4
### Added
- Implement Blockchain.info block height.
- Implement Blockchain.info raw address.
- Implement Blockchain.info balances.

### Fixed
- Make Blockchain.info unspent endpoint more efficient.
- Make Blockchain.info xpubs endpoint more efficient.

### Changed
- Use compact JSON serialization by default everywhere.

## 0.47.3
### Fixed
- Fix serialization bug with unspents.

## 0.47.2
### Fixed
- Fix serialization bug for health check.

## 0.47.1
### Fixed
- Fix serialization bugs for web data types.

## 0.47.0
### Added
- Support for legacy block endpoints.

### Changed
- Serialization now uses bytes library and does interleaved IO.

## 0.46.6
### Added
- Allow comma-separated values in multiaddr.

## 0.46.5
### Changed
- Throw error when offset exceeded instead of truncating it.

## 0.46.4
### Fixed
- Do not compute balances separately for loose addresses when part of an xpub.

### Changed
- Do not fetch entire transaction list for loose addresses.
- Transaction count in wallet object is just a synonym for filtered tx count.
- Use conduits to prevent over-fetching transactions from database.

## 0.46.3
### Fixed
- Make hex value encoding compatible with Java BigInteger decoder.

## 0.46.2
### Changed
- Allow negative confirmations for compatibility with old API.

## 0.46.1
### Added
- Filters for transactions in multiaddr endpoint.

## 0.46.0
### Fixed
- Release to fix unintentional upload.

## 0.45.0
### Added
- Using a tx_index that results in a txid conflict returns a 409 - Conflict.

## 0.44.0
### Changed
- Numeric txid now 53 bits long and doesn't return transactions when hashes collide.

## 0.43.1
### Fixed
- Do not overcount statistics.

## 0.43.0
### Changed
- Transaction index now simply encodes txid as a long number (sorry JS users).

## 0.42.4
### Changed
- Optimise encoding of JSON data for Blockchain.info API.

## 0.42.3
### Changed
- Narrow down the balance for transactions to onlyShow set.

## 0.42.2
### Changed
- Make unspent algorithm more efficient.

## 0.42.1
### Added
- Support for Blockchain.info API unspent endpoint.

### Fixed
- Fix RBF transactions on Blockchain.info API.

## 0.42.0
### Removed
- No more pruning of old mempool transactions.

## 0.41.4
### Fixed
- Correct error computing wallet balances.

## 0.41.3
### Changed
- Use whole active set when computing results for multiaddr.
- Compute correct count of transactions for wallet.

## 0.41.2
### Changed
- Caching mempool is not much slower when pruning, so reversing that change.

## 0.41.1
### Changed
- Only prune mempool every ten minutes or so.

## 0.41.0
### Removed
- No more cache eviction endpoint.

## 0.40.22
### Added
- More metrics for cache.

## 0.40.21
### Added
- Support HTTP GET for multiaddr endpoint.

## 0.40.20
### Added
- Prefix 'app.' in statsd when using Nomad variables.

## 0.40.19
### Added
- Statsd support and pleny of statistics.

### Removed
- Removed use of sync lock for importing xpubs in cache due to volume.

## 0.40.18
### Added
- New options to control cache lock retry count and delay.

### Changed
- Cache xpubs asyncrhonously.
- Mempool syncs fully every refresh cycle.

## 0.40.17
### Changed
- Delete more than 1000 xpubs from Redis if needed.
- Attempt to get lock ten times in order to index a new xpub.

## 0.40.16
### Fixed
- Fix best block not set.

## 0.40.15
### Fixed
- Do not update cache best block if it is higher.

## 0.40.14
### Fixed
- Always respond to cache pings to avoid getting stuck.

## 0.40.13
### Added
- Be more careful about importing blocks to the cache.

### Removed
- Reduce excessive cache logging.

## 0.40.12
### Fixed
- Correct bad arithmetics in cache cooldown.

## 0.40.11
### Added
- Debug log cache cooldown.

### Fixed
- Unlimited retry lock acquisition now works.
- Make locking tighter

## 0.40.10
### Changed
- Reduce the number of things that cache do when pinged.

## 0.40.9
### Fixed
- Avoid filling the cache actor mailbox with pings.

## 0.40.8
### Fixed
- Correct a typo in cache code.

## 0.40.7
### Changed
- Improve cache mempool sync algorithm.

## 0.40.6
### Changed
- Make cache cooldown a number.

## 0.40.5
### Added
- Log every non-successful query.

## 0.40.4
### Fixed
- Use Redis TTL for cache cooldown.

## 0.40.3
### Fixed
- Mempool no longer gets saved in cache when node is not in sync.

## 0.40.2
### Changed
- Change source of price data to https://blockchain.info/ticker.

## 0.40.1
### Removed
- Do not expose Paths_haskoin_store module explicitly.

## 0.40.0
### Added
- Blockchain.info API compatibility for multiaddr and rawtx endpoints.

### Changed
- Segwit fields are no longer omitted on incompatible networks.

## 0.39.0
### Changed
- Upgrade Haskoin Core to support higher version on witness addresses.

## 0.38.4
### Fixed
- Fix block search by timestamp.

## 0.38.3
### Changed
- Disable low latency garbage collector by default.

## 0.38.2
### Changed
- Use GHC 8.10 and its low latency garbage collector.

## 0.38.1
### Changed
- Optimise algorithm for fork safety check.

## 0.38.0
### Added
- Support for Bitcoin Cash hard fork.

## 0.37.5
### Added
- Configurable cache refresh time.

## 0.37.4
### Fixed
- Now request mempool when connecting even if already in sync.

### Changed
- Add a delay to mempool sync to allow for block headers to catch up.
- Attempt to sync mempool whenever syncing is done or a new peer connects.

## 0.37.3
### Changed
- Correct logic error when deleting superseded transactions.

## 0.37.2
### Changed
- Simplify multiple algorithms to hopefully solve corruption bug.
- Allow to disable mempool transaction indexing via command line.

## 0.37.1
### Changed
- Better locking around caching operations.
- Have a global cooldown on cache mempool updates.

### Fixed
- Fix address unspent retrieval using correct column family.

## 0.37.0
### Added
- Add support for column families and prefixes breaking database backwards compatibility.

## 0.36.2
### Fixed
- Fix another place where mempool sorting was wrong.

## 0.36.1
### Fixed
- Fix wrong sorting of mempool transactions in database.

## 0.36.0
### Changed
- Do not use STM transactions because it is impossible to reason about preloading.
- Make importing blocks after mempool is populated faster.

## 0.35.2
### Changed
- Reduce frequency of cache mempool updates.

## 0.35.1
### Changed
- Remove cache lock retries.
- Do not use lock when caching a new xpub.

## 0.35.0
### Changed
- Use new RocksDB Haskell bindings.
- Make headers - blocks diff configurable in health check.
- Simplify locking in cache.

## 0.34.8
### Changed
- Minor refactoring.
- Only send one mempool message ever.

## 0.34.7
### Changed
- Fix block heights endpoint URL.

## 0.34.6
### Removed
- Remove asynchronous database retrieval permanently due to instability.

## 0.34.5
### Added
- Preload memory before freeing outputs.

## 0.34.4
### Added
- Asynchronous preloading only for blocks.

## 0.34.3
### Removed
- Disable asynchronous preloading to prevent segmentation fault error.

## 0.34.2
### Removed
- No more upper limit for mempool call.

### Fixed
- Health check uses minimum between last block and mempool tx.

## 0.34.1
### Added
- Pending mempool transactions health check.

### Removed
- No more upper limit for mempool call.

## 0.34.0
### Changed
- New format for health check.

## 0.33.1
### Changed
- Upload with updated Cabal files.

## 0.33.0
### Fixed
- Fix outputs showing up as both spent and unspent.

### Changed
- Simplify data structures used to hold uncommitted changes.
- Split StoreRead class in two.
- Improve pre-loading algorithms.

## 0.32.3
### Fixed
- Fix command line argument mappings to env vars.

## 0.32.2
### Fixed
- Do not complain when deleting non-existent unconfirmed txs.

## 0.32.1
### Fixed
- Fix corruption bug when deleting transactions during some reorgs.
- Fix tests on data package.
- Rename upstream address string functions.

## 0.32.0
### Changed
- Concurrently load dependencies when importing transaction data.
- Import each transaction using STM.
- Rework logging.
- Refactor all block chain importing algorithms.
- Use peer locking when importing data.
- Change license to MIT.

### Added
- Make max peer count configurable.

### Fixed
- Prevent premature peer timeouts when importing blocks.

## 0.31.0
### Added
- Add Haskell web client code to data library.
- Use type-safe definitions for web paths and parameters.
- Support JSON pretty encoding and enable by default for some endpoints.
- Allow environment variables for configuration.

### Changed
- Change command-line arguments to better fit GNU style.
- Refactor code to manipulate address balances.
- Improve logging.
- Rate-limit mempool importing.
- Do not add xpubs to cache if node not in sync.
- Do not import transactions to cache if no xpubs in cache.
- Refactor transaction importing logic extremely.
- Refactor memory database and writer.

### Fixed
- Fix BTC double-count transaction bug.
- Add missing xpub derive parameter to xpub evict documentation.

## 0.30.1
### Changed
- Limit mempool API endpoint list.

## 0.30.0
### Changed
- Raw JSON values are now wrapped in an object with a result property.
- Better logging when importing transactions into mempool.

### Fixed
- Transaction limit by block height works now.

## 0.29.3
### Changed
- Revert concurrent retrieval changes from 0.29.1 and 0.29.0 to solve stability
  issues.
- Bitcoin Cash (BCH) replaces BTC as default network in CLI.
- Peer discovery enabled automatically if no peers specified in CLI.
- Use separate Stack configuration file for data package.

## 0.29.2
### Changed
- Increase timeouts for manager and chain health checks.

## 0.29.1
### Added
- Perform xpub operations concurrently.

## 0.29.0
### Changed
- BlockTx renamed to TxRef.
- Remove unnecessary monad transformers to simplify import code.

### Added
- Retrieve balances and unspent outputs concurrently when importing.
- Coin instance for Unspent in data package.

## 0.28.0
### Changed
- Change the way that limits, start, and offset values are handled.

### Fixed
- Fix incorrect limit responses.

## 0.27.0
### Added
- Complete support for offset.

### Changed
- Improve monad stack for cache.

## 0.26.6
### Added
- Support for offset in all endpoints where it makes sense.

## 0.26.5
### Fixed
- Sort after using the nub function or use Data.List.nub in exceptional cases.

## 0.26.4
### Changed
- Don't do certain validity checks on confirmed transactions.

### Fixed
- Do not crash when syncing the blockchain from scratch on a populated cache.

## 0.26.3
### Changed
- Replace nub function everywhere with faster version written using hash sets.

### Fixed
- Fix shared cache locking.

## 0.26.2
### Changed
- Simplify cache block import code.
- Improve cache import performance.

## 0.26.1
### Fixed
- Do not call zadd or zrem in Redis with incorrect argument count.

### Changed
- Do not use slow Transaction type in Logic.
- Improve logging while importing blocks.
- Better transaction import algorithm.

## 0.26.0
### Changed
- Add information to some binary types for feature parity with JSON.

## 0.25.4
### Changed
- Hide segwit data on non-segwit networks.

### Fixed
- Fix work which now always serialises as a number literal.

## 0.25.3
### Changed
- Fix version display on health check.

## 0.25.2
### Changed
- Fix wrong version of Haskoin Store Data dependency.

## 0.25.1
### Changed
- Improve code organisation.
- Split data definitions and serialisation into its own package.

## 0.24.0
### Changed
- Use mocking to simulate peers instead of connecting to the network while testing.
- Depend on latest version of Haskoin Core for faster JSON serialisation.
- Clean up confusing JSON encoding/decoding codebase.

## 0.23.24
### Changed
- Implement faster JSON encoding using toEncoding from the Aeson library.

## 0.23.23
### Changed
- Do not read full mempool so often from cache code.

## 0.23.22
### Changed
- Depend on latest Haskoin Node that fixes debugging regression.

## 0.23.21
### Changed
- Reduce non-debug log verbosity.

## 0.23.20
### Changed
- Better Haskoin Node logging.

## 0.23.19
### Changed
- Use set peer timeout instead of constant in block store. 

## 0.23.18
### Changed
- Allow setting peer timeouts in command line.

### Fixed
- Bump Haskoin Node dependency to fix another premature timeout condition.

## 0.23.17
### Fixed
- Touch syncing peer whenever we process one of its blocks to avoid premature timeout.

## 0.23.16
### Changed
- Better algorithm to avoid importing transaction multiple times in cache.
- Depend on latest Haskoin Node.

## 0.23.15
### Changed
- Do not storm the cache with mempool transactions.

## 0.23.13
### Fixed
- Do not ignore deleted incoming transactions.

## 0.23.12
### Fixed
- Do not do RBF checks when replacing a mempool transaction with a confirmed one.

## 0.23.11
### Fixed
- Streamline mempool transaction importing.

## 0.23.10
### Fixed
- Fix transactions not recorded in cache mempool set.
- Fix transactions being downloaded multiple times.

### Removed
- Do not store orphan transactions in database.

### Changed
- Use sets for incoming transactions instead of lists.
- Do not do anything to the cache if there are no xpubs in it.

## 0.23.9
### Fixed
- Wiping mempool fixed.

## 0.23.8
### Added
- Chunk transactions to be deleted from mempool.

## 0.23.7
### Added
- Mempool improvements.

## 0.23.6
### Added
- Ability to wipe mempool at start.
- Improvements to mempool processing code.

## 0.23.5
### Changed
- Tighten the locking loop to avoid slow cache building.

## 0.23.5
### Fixed
- Wrong error in cache when acquiring lock.

## 0.23.4
### Added
- Add extra debug logging for cache code.

### Fixed
- Fix a bug with xpub growing algorithm.

### Changed
- Use locks instead of transactions to update cache.

## 0.23.3
### Fixed
- Reduce contention when many instances of Haskoin Store share a cache.

## 0.23.2
### Added
- Allow retrieving xpub data without using cache.

## 0.23.1
### Added
- Allow xpub eviction from cache via API.
- Clarify cache address addition code.

### Fixed
- Balances were incorrectly computed in cache when new transactions arrived.

## 0.23.0
### Added
- Support for Redis transactions.
- Use a smaller initial gap for empty xpubs.

### Changed
- Remove custom JSON encoding class.
- Refactor and code simplification.

## 0.22.5
### Fixed
- Cache was being completely pruned.

## 0.22.4
### Fixed
- Cache now prunes correctly.

## 0.22.3
### Fixed
- Bug was making cache get stuck when pruning.

## 0.22.2
### Changed
- More efficient algorithms for caching and cache misses.
- Better debug logging of cache hits and misses.

## 0.22.1
### Added
- More debug logging for cache hits.

### Fixed
- Bug using maximum against empty list.

## 0.22.0
### Changed
- Extreme code refactoring.
- Move all code to Haskoin and drop Network from modules.

### Added
- Use Redis pipelining when importing multiple transactions into cache.
- Implement configurable LRU for Redis cache.
- Import xpubs directly into cache from web worker thread when a key is requested.

### Removed
- Only expose a few modules to external API.

## 0.21.7
### Changed
- Improve build configuration.

### Fixed
- Use minimum used addresses instead of minimum index for xpub cache decision.

## 0.21.6
### Fixed
- Fix missing xpub unspent outputs when using cache.

## 0.21.5
### Added
- Only store xpubs in cache if they have more than a threshold addresses used.

## 0.21.4
### Fixed
- Fix shared cache case where head is set beyond header chain by another node.

## 0.21.3
### Fixed
- Fix bug where best head was not being registered in cache.
- Fix best head in cache being decoded incorrectly.

## 0.21.2
### Added
- Complete support for Redis xpub cache.

## 0.21.1
### Fixed
- Latest version of secp256k1-haskell works with Debian 9.

## 0.21.0
### Fixed
- Fix output of web API calls when issuing limits with offsets.

### Changed
- Massive refactoring of entire codebase.

### Added
- Work in progress Redis caching for extended public keys.

## 0.20.2
### Changed
- Filter xpub address balances on web API to show only addresses that have been used.

### Removed
- Remove paths and addresses from xpub summary.

## 0.20.1
### Changed
- Refactor code greatly.
- Depend on new Haskoin Store package to avoid missing tx broadcasts.
- Merge StoreStream class into StoreRead.
- Move former streaming functions to use lists instead of conduits.
- Remove excessively verbose debugging.

## 0.20.0
### Added
- Set minimum version bounds for some dependencies.
- Now compatible with GHC 8.8.
- Extended key caching system using Redis.

### Changed
- Massively refactored codebase.
- Less verbose debug logging.

### Removed
- Removed conduits for faster queries.

## 0.19.6
### Added
- Use a parallel strategy to compute key derivations.

### Fixed
- Do not fail health check upon transaction timeout while syncing.

### Changed
- Do not use conduits for xpub balance streams.
- Multiple minor refactorings.

## 0.19.5
### Changed
- Minor refactor to block import code.

### Fixed
- Minor fix to transaction timeout check.

## 0.19.4
### Fixed
- Clarify and correct health check algorithm.

## 0.19.3
### Changed
- Add address transactions to cache.
- Improve multi-address transaction retrieval algorithms.

## 0.19.2
### Removed
- Cache-Control header turned out to be unnecessary.

### Fixed
- Fix some minor errors in web module.

## 0.19.1
### Added
- Set Cache-Control header to no-cache.

## 0.19.0
### Changed
- Store mempool in single key/value pair.

## 0.18.11
### Changed
- Do not stream mempool.

## 0.18.10
### Removed
- Disable timeout checks for testnets.

## 0.18.9
### Added
- Endpoint to locate a block by unix timestamp.

### Removed
- No more persistence for peers due to dependency on newest haskoin-node.

## 0.18.8
### Added
- Transaction and block timeouts for health check.
- Raw blocks.

## 0.18.7
### Fixed
- Missing tranasctions on xpub listings.

## 0.18.6
### Removed
- Follow Stack advise removing `-O2` GHC option.

## 0.18.5
### Added
- Compatibility with SegWit on extended public key endpoints.

### Changed
- Fix syncing peer not reset after timeout.
- Use simpler monad for streaming data.

## 0.18.4
### Changed
- Bump Haskoin Node to fix peers not stored and excessively verbose logging.

## 0.18.3
### Changed
- Configurable HTTP request logging. Disabled by default.

## 0.18.2
### Changed
- Fix for memory leak.

## 0.18.1
### Removed
- Remove transaction count from xpub summary object.

## 0.18.0
### Changed
- Simplified limits and start point handling on web API.
- Made transaction streaming algorithm faster for xpub transactions.
- Extended public key summary output contains all addresses that have received transactions.

### Added
- Fine-grained control for maximum limits via command line options.
- Transaction hash as starting point.
- Block hash as starting point.
- Timestamp as starting point.
- Configurable xpub gap limit.
- Transaction count added to xpub summary.
- UTXO count added to xpub summary.

### Removed
- Mempool endpoint now has no limits or offsets and always returns full list.
- Extended public key summary output no longer includes any transactions.
- Offsets not allowed for transaction lists involving multiple addresses or xpubs.
- Confusing block position parameter no longer part of web API.

## 0.17.2
### Changed
- Stream address balances and unspent outputs.
- Add configurable max value for limit and offset which defaults at 10000.

## 0.17.1
### Changed
- When posting a transaction to the network, timeout is now five seconds.
- Improve error message when transaction post timeout reached.
- Remove obsolete `not found` error for transaction post.
- Endpoints for retrieving blocks now do streaming for better performance.
- Improve Swagger API documentation.

## 0.17.0
### Added
- Endpoints for retrieving block transactions.
- Endpoint for retrieving set of latest blocks.

### Changed
- Use standardized JSON and binary serialization schemes for raw transaction endpoints.

## 0.16.6
### Changed
- Now logging info messages too by default.
- Consolidated web logging in middleware.

### Removed
- UUIDs for web requests.

## 0.16.5
### Removed
- Remove concurrency from xpub balance requests to prevent RocksDB segfaults.

## 0.16.4
### Removed
- Remove concurrency from requests using iterators to prevent RocksDB from segfaulting.

## 0.16.3
### Added
- Debugging information for web API.

## 0.16.2
### Changed
- Debugging disabled by default (use `--debug` to enable).

## 0.16.1
### Added
- Cache mempool transactions.
- Improve initial syncing performance.

## 0.16.0
### Added
- Orphan transaction support.
- Full address balance cache in RocksDB.
- Full unspent output cache in RocksDB.

### Changed
- Significantly refactor code.
- Move web stuff to its own module.
- Change types related to databases.
- Make xpub balance, transaction and unspent queries fetch data in parallel.

## 0.15.2
### Added
- Internal data types to support orphan transactions.

### Changed
- Do not spam block actor with pings.
- Fix balance/unspent cache not reverting when importing fails.
- Fix transaction sorting algorithm not including transaction position information.
- Fix conflicting mempool transaction preventing block from importing.

## 0.15.1
### Changed
- Fix duplicate coinbase transaction id bug.

## 0.15.0
### Removed
- Removed `PreciseUnixTime` data type.

### Changed
- Use 64 bits for Unix time representation.
- Data model now uses simplified Unix time representation.

## 0.14.9
### Added
- Last external/change index information to xpub summary object.

## 0.14.8
### Added
- Endpoint for xpub summaries.
- Endpoints for full transactions.
- Ability to query by offset/limit.
- More API documentation.

## 0.14.7
### Added
- Debug information for `publishTx`.

### Changed
- Transaction publishing no longer requests mempool.
- Fixed serialization for `TxId` freezing the entire program by infinitely recursing.

## 0.14.6
### Changed
- Enable full threading again as it was deemed not responsible for the freezing behaviour.

## 0.14.5
### Changed
- Enable threading but leave it at a single thread to be able to open more than 1024 files.

## 0.14.4
### Changed
- Target LTS Haskell 13.20 and disable threading in new attempt to fix freezing bug.

## 0.14.3
### Changed
- Remove `-O2`.

## 0.14.2
### Changed
- Target LTS Haskell 12.26 to attempt to fix freezing bug.

## 0.14.1
### Added
- Extra debugging around code that freezes.

### Changed
- Bump dependency on `haskoin-node`.

## 0.14.0
### Removed
- Dump slow protobuf serialization.

### Added
- Add custom serialization.
- Extra debug logging.

### Changed
- Bump `haskoin-core` and `haskoin-node`.

## 0.13.1
### Changed
- Bump `haskoin-node` in `stack.yaml`.
- Do not send empty `getdata` messages.

## 0.13.0
### Added
- Primitive content negotiation for web exceptions.
- Protobuf support for errors.
- Protobuf support for tranasction ids.

### Changed
- Protobuf format changed in non-backwards-compatible manner for transaction ids.

## 0.12.0
### Added
- Support for binary serialization using Protocol Buffers.
- New endpoints for binary raw transactions (not hex-encoded).

### Changed
- Services field now a hex string instead of a number to avoid overflowing signed 64-bit integer.
- Flatten list of block data objects when responding to request for multiple block heights.
- Errors now reported in plain text without container JSON object.
- Transaction broadcasts are responded to with transaction id in plaintext hex (no JSON).
- Remove database snapshots to improve performance.

## 0.11.2
### Changed
- Fix duplicate mempool transaction announcements in event stream.

## 0.11.1
### Removed
- Removed latest block time check.

## 0.11.0
### Changed
- Improve post transactions endpoint.

## 0.10.1
### Changed
- Fix bug where transaction lists from multiple addresses would sort incorrectly.
- Address gap reduced to 20.

## 0.10.0
### Removed
- Remove addresses from transaction lists.
- No longer use container objects for xpub transactions.

## 0.9.3
### Added
- Permissive CORS headers to allow queries from any domain.
- Improved documentation using real-world examples from the BCH testnet.

## 0.9.2
### Added
- HTTP JSON API switch to turn off transaction list when retrieving blocks.

## 0.9.1
### Added
- Total block fees.
- Total block outputs.
- Block subsidy.

### Changed
- Do not consider the blocks less one block away from headers as out of sync in health check.
- Health check now returns HTTP 503 when not OK or out of sync.

## 0.9.0
### Added
- Version to health check output.
- Block weight for segwit.
- Transaction weight for segwit.

## 0.8.1
### Added
- Health check endpoint.

## 0.8.0
### Added
- Limits and skips.
- Add timestamps to transactions.
- Add transaction count to address balance object.
- Add Merkle root to block data.
- Total funds received by an address now shows up in balance.
- Balances for any address that ever had funds show up in xpub endpoints.

### Changed
- Data model update.
- Performance improvement for xpub calls.
- Transactions are returned in reverse mempool/block order (highest or most recent first).
- Balance objects do not get deleted from database ever.

## 0.6.9
### Changed
- Reduce number of coinbase checks to 10,000 ancestors.

## 0.6.8
### Changed
- Further optimize coinbase after height checks.

## 0.6.7
### Changed
- Impose restrictions on recursion for coinbase after height checks.

## 0.6.6
### Added
- Check whether a transaction can be traced back to a coinbase after certain height.

## 0.6.5
### Changed
- Delete transactions in reverse topological order when reverting a block.

## 0.6.4
### Changed
- Do not fail silently when importing orphan transactions into the mempool.

## 0.6.3
### Changed
- Dummy release to bump haskoin-node in stack.yaml.

## 0.6.2
### Changed
- Correct bug where coinbase transactions weren't properly flagged.

## 0.6.1
### Changed
- Compatibility with Bitcoin Cash hard fork.
- Various bug fixes.

## 0.6.0
### Added
- Address balance cache in memory.

### Changed
- Simplify data model further.
- Fix bug importing outputs with UTXO cache.
- Unspent balances cannot be negative.

## 0.5.0
### Added
- Add UTXO cache in memory.
- Get transactions with witness data in segwit networks.

### Changed
- Paths for derivations in xpubs is a list and no longer a string.
- Various bug fixes.

## 0.4.2
### Removed
- Remove extended public key itself from output of relevant endpoints to save bandwidth.

## 0.4.1
### Changed
- Fix bug when deleting coinbase transactions.
- Extended public key API support.

## 0.4.0
### Changed
- Generate events for mempool transactions.
- Respond with entire block data when querying blocks by height.

## 0.3.1
### Changed
- Do not import transactions to mempool while not synced.
- Only sync mempool against a single peer.
- Allow duplicate transactions to fix re-introduced sync bug.

## 0.3.0
### Added
- Update dependencies.
- Keep orphan blocks and deleted transactions in database.
- Add a `mainchain` field for block data and a `deleted` field for transactions.
- Stream records for performance.
- Show witness data for transaction inputs in SegWit network.
- Support RBF in SegWit network.

### Changed
- Refactor all data access code away from actor.
- Refactor import logic away from actor.
- Abstract data access using typeclasses.
- Implement data access using clean layered architecture.
- Make most of import logic code pure.
- Database now in `db` as opposed to `blocks` directory.
- Use latest `haskoin-node`.

### Removed
- Remove some data from peer information output.
- Remove full transaction from address transaction data.
- Remove limits from address transaction data.
- Remove block data from previous output.
- Remove spender from JSON response when output not spent.
- Remove block hash from block reference.

## 0.2.3
### Removed
- Do not send transaction notifications if a block is being imported.

## 0.2.2
### Added
- Peer information endpoint.

### Changed
- Update `haskoin-node`.

## 0.2.1
### Changed
- Fix tests

## 0.2.0
### Added
- Documentation everywhere.
- Ability to retrieve address transactions.

### Changed
- New versions of NQE and Haskoin Node upstream.
- Improve and simplify API.
- Multi-element endpoints return arrays of arrays.
- Database snapshots for all queries are now mandatory.

### Removed
- Retrieving unspent and spent outputs for an address.
- Redundant API endpoints for multiple elements.

## 0.1.3
### Changed
- Fix a bug with transaction notifications.
- Improve handling orphan transactions.

## 0.1.2
### Changed
- Specify dependencies better.

## 0.1.1
### Changed
- Dependency `secp256k1` is now `secp256k1-haskell`.

## 0.1.0
### Added
- New `CHANGELOG.md` file.
- Bitcoin (BTC) and Bitcoin Cash (BCH) compatibility.
- RocksDB database.
- Mempool support.
- HTTP streaming for events.
- CashAddr support.
- Bech32 support.
- Rate limits.

### Changed
- Split out of former `haskoin` repository.
- Use hpack and `package.yaml`.

### Removed
- Removed Stylish Haskell configuration file.
- Remvoed `haskoin-core` and `haskoin-wallet` packages from this repository.
