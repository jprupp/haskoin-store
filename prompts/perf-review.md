You are doing a performance review of the haskoin-store HTTP API. The goal is a server that answers each request faster (lower p50 and p99 latency) and handles more requests per second on the same hardware, without changing what the API returns.

## Context
- Repo: `haskoin-store`. It has two packages: `data/` (shared types, JSON/binary serialization, `WebCommon`, `WebClient`) and `store/` (the server).
- The hot path is `store/src/Haskoin/Store/Web.hs` (Scotty on Warp). It calls into:
  - `Database/Reader.hs` (RocksDB reads through `rocksdb-query` and `rocksdb-haskell`)
  - `Cache.hs` (Redis cache, mostly for xpub queries)
  - `Common.hs` (the `StoreReadBase`/`StoreReadExtra` classes and their default implementations)
  - `data/src/Haskoin/Store/Data.hs` (ToJSON, `Serial` and `Binary` instances for the response types)
- The executable is built with `-threaded -rtsopts -with-rtsopts=-N4` (see `store/package.yaml`). Server startup and settings are in `store/app/Main.hs`.
- Sibling repos (`haskoin-core`, `rocksdb-haskell`, `rocksdb-query`, `haskoin-node`) are in scope if a bottleneck is really in them.

## What to do

### Phase 1: Map the request path
For every endpoint in `Web.hs`, trace the full path: request parsing, store reads, cache lookups, response encoding, and how the response is written to the socket. Write down which endpoints are likely heaviest: address, xpub, multi-address and multi-tx queries, block, mempool, and anything that streams or paginates.

### Phase 2: Look for problems in these categories
Be concrete and cite `file:line` for each finding.
1. **Database access**
   - N+1 lookups, such as fetching txs one by one where a batched `multiGet` would work.
   - Repeated reads of the same key within one request.
   - Missing or misused iterators or prefix seeks.
   - Unnecessary full decodes when only part of a value is needed.
   - Snapshot or iterator lifetimes that cost more than they need to.
   - RocksDB options: block cache size, bloom filters, `readOptions`, `fillCache`, compression on hot column families.
2. **Laziness and allocation**
   - Space leaks, lazy accumulators, and thunks built up across conduits or folds.
   - `String`/`Text`/`ByteString` conversions (`cs` is everywhere; check whether any sit on hot paths).
   - Lists used where `Vector`, `Seq` or `HashMap` would fit better.
   - `nub` and other O(n²) operations.
   - Missing strictness annotations or `INLINE`/`SPECIALIZE` on hot polymorphic code, especially mtl-style class-constrained functions that may not specialize.
3. **Serialization**
   - Aeson: is `toEncoding` implemented directly, or does it go through `toJSON` to `Value` first?
   - Pretty-printing cost, and whether it is applied when not requested.
   - Binary and serial encoders: builders versus strict concatenation.
   - Whether responses are fully materialized before sending when they could be streamed with `stream`/`Builder`.
   - Content negotiation overhead.
4. **Caching**
   - Redis round trips: pipelining versus sequential calls, and keys fetched one at a time.
   - Lock contention and the cache-refresh logic in `Cache.hs`.
   - Whether some hot, cheap-to-cache results (best block, chain tip, recent blocks, health, peers) are recomputed on every request.
   - Whether an in-process cache (for example a TVar or LRU map invalidated on new block or tx events) would remove a whole layer of work.
5. **Concurrency and server config**
   - Warp settings: timeouts, `setFdCacheDuration`, `setServerName`, HTTP keep-alive, whether gzip middleware runs on already-compressed or tiny responses.
   - Global `MVar`/`TVar` contention in request handling or metrics.
   - Blocking calls in the request thread.
   - Per-request `getPOSIXTime` or stats overhead.
   - RTS flags: `-N`, `-A`/`-n` allocation area, `--nonmoving-gc` or `-qg`, `-I0`. Is a hardcoded `-N4` right?
6. **Limits and pathological queries**
   - Requests whose cost grows without bound, such as huge xpub gaps, large `limit`/`offset`, or multi-address lists.
   - Whether offset pagination scans and discards rows it could seek past.
   - Whether limits are enforced before the expensive work starts.

### Phase 3: Measure before changing anything
- Build with `stack build --fast` only for checking. Use optimized builds for any measurement.
- Set up a repeatable benchmark. Use `wrk`, `oha` or `bombardier` against a locally running instance if a node and database can be brought up. Otherwise write `criterion`/`tasty-bench` microbenchmarks for the encoders and handler functions using the test fixtures in `store/test` and `data/test`.
- Profile with `+RTS -s` and, if feasible, `-p`/`-hT` or eventlog/`ghc-debug` to confirm where time and allocation actually go. Do not optimize on speculation. Rank findings by measured or clearly reasoned impact.

### Phase 4: Report, then implement
1. Produce a ranked list of findings. For each one give:
   - location
   - the problem
   - why it is slow, with the complexity or allocation argument
   - the proposed fix
   - expected impact (high, medium or low)
   - risk
2. Implement the high-impact, low-risk fixes first, one logical change per commit, each with before and after numbers where you measured them.
3. Preserve exact API behaviour. JSON field names and ordering where clients may depend on it, binary formats, status codes, and pagination semantics must not change. `stack test` must pass after every commit. If a fix would change output, flag it and do not implement it.
4. Do not add a dependency unless the gain is substantial, and justify it if you do.
5. Finish with a summary table:
   - finding
   - status (fixed, proposed or rejected)
   - measured or estimated gain

   Also list follow-ups that need operational changes, such as RocksDB tuning, Redis deployment or RTS flags in the Dockerfile.

Be skeptical of your own findings. For each one, check that the code path is actually hot and the fix actually helps before claiming a win.
