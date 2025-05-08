# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html)

## [1.5.16]

### Changed

- Move to personal repository.

## [1.5.15]

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
