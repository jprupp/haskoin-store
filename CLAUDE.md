# Instructions for Claude

## Build and test

- Build: `haskell-dev build`
- All tests: `haskell-dev test`
- One spec: `haskell-dev test Fleet.Redis` (a word of the module name;
  `--match "/Haskoin.Store.Integration/Regtest Bitcoin Core/"` narrows it)
- Documentation: `haskell-dev docs`
- `haskell-dev` is a script of [jprupp/tools](https://github.com/jprupp/tools),
  shared with the other packages, and not a file of this repository. It must
  be on the `PATH`. Its settings for this package are in `haskell-dev.toml`.
  A change to the script itself is a change to jprupp/tools.
- `stack build`, `stack test`, `stack haddock` and `stack bench` are the
  release build. Do not mix them with `haskell-dev` in one session. With
  no source changed, Stack keeps the objects of the other flags, so a
  `stack build` or `stack test` right after `haskell-dev build` runs code
  without optimisation; the next change to a source compiles everything
  again, and `stack bench` compiles with optimisation.
- `haskell-dev` builds without optimisation. Never time the store with it:
  the benchmarks run with `stack bench`, as the README says.
- The tests take about 200 seconds in eight processes (65 before the
  tests of the mempool against real nodes, 160 before those of the
  pool letting go after a move), against 140 to 210
  with every spec module in a process of its own. Most of it is
  `Haskoin.Store.IntegrationSpec`, whose five environments are groups at the
  top of the module, so that each runs in its own process; keep a new
  environment at the top too. The longest is the store of Bitcoin Core
  whose node never stops taking transactions (`unceasing` in
  `Haskoin.Store.MempoolSpec`); of the environments, Bitcoin Cash Node
  takes about 64 seconds. Eight processes at once use about 1.2 GB more than one, with the
  containers. `Haskoin.Store.DownloadSpec` has the downloads of a store from
  real nodes, a group with a pod of its own for each test.
- Tests name no address or port of the machine. A node, Valkey or any other
  program runs in a container of podman-test, reached through a Unix socket
  of its pod; nothing uses a Bitcoin Core or a Bitcoin Cash Node, or another
  program such as openssl or curl, of the machine. A web server of the tests
  listens on a Unix socket (`listening` in `IntegrationSpec`), and a peer in
  memory is named `unix:NAME`. What an honest node does is tested against a
  real one, not a mock. SELinux lets the tests connect to a socket that a
  container made, not a container connect to one that the tests made: such
  a container runs with `--security-opt label=disable` (`tlsTest`).
- `Haskoin.Store.MempoolSpec` and `Haskoin.Store.NodeAxiomsSpec` are the
  tests of the path into the shared mempool against Bitcoin Core and
  Bitcoin Cash Node: a store that must end with its node's mempool, and
  what the path assumes of a node. Each test is a group with a pod of its
  own. They read `/mempool?limit=1000000`: the default page is 100.
- A store that does not sync with its node in time, which happened a
  few times on Bitcoin Cash Node without being understood yet, prints what
  the node and the store say of their sync and the last 400 lines of the
  log of the store, whatever `HASKOIN_TEST_LOG` says (`syncDiagnostics`).
  Read them before running such a failure again.
- In one run of four, a test failed with `InsecureConnectionDenied`, an
  exception of the HTTP client, and passed when run again with the same
  seed. Its cause is not known. Run it again before taking such a failure
  for a fault of the change.

## Concurrency

Every protocol that runs at once, in a store or across a fleet, is a pure
machine, a decision of `Haskoin.Store.Fleet.Atomic` or a Lua script that
decides it in one call, and `docs/concurrency.md` says which, who owns
each piece of shared state, and why no interleaving breaks it. A change to one of them updates that
document, and adds the property that the change keeps to the machine's
spec. Change shared state only from its owner, or in one STM transaction
that reads what the decision depends on.

What a write to Redis does to one cell is said once, in
`Haskoin.Store.Fleet.Atomic.Cells`, for the model of Redis and for what a
decision sees of its own writes. Liquid Haskell checks that module with
the others (`cabal build` in `proofs/`): a change to it, or a new kind of
write, is not done until that passes. Its bytes are a type of any kind:
Liquid Haskell does not load `ByteString` here.

The shared mempool in Redis is changed only by the decisions of
`Haskoin.Store.Fleet.Pool.Decide`, and the cache of extended public keys
by those of `Haskoin.Store.Fleet.XPub.Decide`: Haskell that asks Redis
questions and lists writes, which `lua/atomic.lua` carries out only if
every answer is still the same. Do not write a rule of the fleet in Lua: a script that
fails after its first write leaves Redis half changed. A decision imports
`Haskoin.Store.Fleet.Atomic.Keys`, where a key has its kind in its type
and a write of another kind does not compile, and never the writes of
`Haskoin.Store.Fleet.Atomic`; a new key is named, with its kind, in the
record of keys of its module.

A decision is carried out only if nothing that it read has changed, which
makes it right and does not make it happen: one that reads much loses
every race to those that read little. The move of the tip has the floor
for that (`docs/concurrency.md`, "The floor"). A new decision that writes
to the pool or to the cache gives way to it (`floorHeld`), and to what a
move marked as leaving and has not taken out yet (`leavingHeld`,
"After the move"); a new read of the pool or of the cache has to name
the marked transactions in its way, in `read.lua` or `xpub.lua`, and the
test of that in `DecideSpec` (`reading`) has to read it. A change to
any of this is not done until `scripts/check-pool.sh` passes: it goes
through every pool, block and order of taking out of its small worlds,
in half a minute, without building the store. A new writer is added to
those that it tries (`others` in `Haskoin.Store.Fleet.LeavingModel`), a
new read to what it reads (`aims`), and each with a fault put in that
the checker finds. A decision that
reads much needs a turn of its own. Test a decision at the size and under
the traffic of a full block: `crowded` in `Haskoin.Store.Fleet.DecideSpec`
and `unceasing` in `Haskoin.Store.MempoolSpec` are the examples. A test
that lets the node go quiet before it looks does not see a store that
cannot keep up. Test it too at the time that it takes on a slow machine,
with leases of ten seconds as stores have them: `longMove` carries a move
out twelve seconds after it was decided. A test whose move takes under a
second sees no limit of ten. `Haskoin.Store.Fleet.DecideSpec`
and `Haskoin.Store.Fleet.XPubDecideSpec` hold the decisions to the
scripts that they replaced (`pool.lua` and `xpub.lua` in
`test/Haskoin/Store/Fleet`); a change to a decision makes one fail, and
has to say there what differs and why.

## The path into the shared mempool

`docs/mempool-ingestion.md` says what the path from a node's mempool to
the pool must guarantee (T1 to T7), what it assumes (A1 to A8), and how
each claim is established. The path is one pure machine,
`Haskoin.Store.Ingest`. A change to it is not done until:

- `scripts/check-ingest.sh` passes: the model checker goes through every
  order of events of its small worlds, in about a minute, without building
  the store. A new rule of the machine gets a check in
  `Haskoin.Store.Ingest.Model`, and a planted bug that the check finds.
- Liquid Haskell passes: `cabal build` in `proofs/`, with GHCup's GHC
  9.12.4 and `z3` on the `PATH`, in about eighty seconds and 1 to 1.3 GiB of
  memory. `Haskoin.Store.Ingest` has the option `--eliminate=none`
  for that, and no other module has it. A comment there says why:
  without it the module took 6.5 GiB, and for some changes, a
  function that never ends among them, all the memory there is with
  no verdict. It reached 19 GiB
  once and had the kernel kill another program. Run it under a limit
  all the same, such as
  `systemd-run --user --scope -p MemoryMax=4G -p MemorySwapMax=512M`:
  a run that the limit stops is a failure to look into, and not a
  limit to raise. CI runs it too, in a job of its own (`Liquid
  Haskell` in `.github/workflows/ci.yml`), with the libraries of
  `proofs/cabal.project.freeze` and the release of z3 named there: a
  change of either is made in both places, after the proofs pass with
  it here. A function that is not exported and that nothing
  calls is not checked. It checks the
  refinement types of `Haskoin.Store.Ingest.Ledger`, the only module that
  can change where a transaction id is, and that every function of the
  machine is total and ends. Liquid Haskell does not see through the dot
  of a record field: what it has to reason about is read by matching. It
  checks a module again only when GHC compiles it again. A function that
  reads the same thing many times through a function call can make the
  check take hours: bind it once. The same build checks
  `Haskoin.Store.Fleet.Atomic.Cells` (`docs/concurrency.md`).
- The document says what is now proved, checked, sampled or only tested.
  Never write "proved" for what is tested.

Do not fix a failure of this path seen on a server by trying a build
there. Reproduce it in the model or in `Haskoin.Store.MempoolSpec` first.

## Keep the documentation up to date with every change

Every change to this repository must update all the documentation it affects,
in the same commit or pull request. A change is not finished until its
documentation is. Before you finish, check each of these and update what the
change touches:

- `README.md`: features, installation steps, examples, configuration,
  tutorials, module overview and caveats. Code examples must still compile
  against the new code, and version numbers in examples must name the current
  release.
- `CHANGELOG.md`: describe the change under the next version.
- `swagger.yaml`: the API reference. Update it whenever an endpoint, a
  parameter, a response field or an error changes.
- The configuration tables in `README.md`: keep them in step with the options
  and environment variables in `app/Main.hs`.
- Haddock in `src` and `app`.

The data types, their encodings and the API client are in the separate
haskoin-store-data package and repository. A change to them is made there,
released, and then picked up here.
- Module headers and declaration comments: add Haddock for every new
  declaration, and fix comments that the change made wrong.

If a change needs no documentation update, say so explicitly when you report
the change, with the reason.

## How to write documentation here

Documentation explains the purpose and spirit of the code, not only its
behaviour. For every module and every significant type or function, say what
problem it solves and why it is designed the way it is, including the trade-offs
and the alternatives it rejects. A reader should come away knowing when to use
it and when not to. Keep to the structure the README already has, and put new
material in the section where a reader would look for it.

Write it so that it can be read at speaking speed by someone who has not
read the code. The documents of `docs/` were once accurate and hard to
follow, for their author too. `docs/tip-first.md` is the model:

- Start with the problem, in everyday words, before any name of the code.
  A page of `docs/` opens with "In short" and "Words used here".
- Define every word of the project where it is first used, and use it for
  one thing only: "the tip", "a decision", "the floor". The words that
  the pages share are in `docs/README.md`.
- Short sentences, one idea each, the subject first. What happens in
  order is a numbered list, and a comparison a small table.
- Say what is and why. How it was found belongs in the commit, unless it
  is the reason.
- Say how a claim is known with the four words of `docs/README.md`:
  proved, checked, sampled, tested.
- A new page is added to the table of `docs/README.md`.

## Line length

Every line of code must be shorter than 120 columns. This covers Haskell
source, tests, `package.yaml` and cabal files, CI workflows, scripts, and the
code examples in the documentation. Break long expressions, type signatures
and import lists over several lines rather than letting them run on.

- **Long strings:** split them over several lines in the least cluttered way
  that works. Pick the form that leaves the fewest quotes, backslashes and
  operators on the page:
  - `unwords [...]` or `unlines [...]` for messages and prose, one piece per
    line;
  - string gaps for text that has to stay a single literal, with a backslash
    at the end of one line and another at the start of the next:

    ```haskell
    msg =
      "The first part of a long message, \
      \and the rest of it."
    ```

  - `<>` only when neither of those reads better.
- **Long constants:** split them too, even when they have no natural break,
  such as hexadecimal test vectors, keys, hashes and base64 data. Cut them into
  pieces of equal width so that they line up, and join them with string gaps
  or `mconcat`.
- **Exceptions:** sometimes a line cannot be split because of a limitation of
  the language or of a tool, for example a generated file such as
  `stack.yaml.lock`, a URL or other value that a tool reads only on one line,
  or a format with no way to continue a line. Leave such lines long, but keep
  them rare, and add a comment explaining why where the format allows one.

## Embedded scripts

Put multi-line scripts in their own files whenever possible, rather than
embedding them in other files: for example, move a long `run:` block of a
GitHub Actions workflow into a script under `scripts/` and call it from the
workflow. A script in its own file can be read, linted and run on its own.

When a script has to stay embedded, lay it out to be read: one command per
line, indentation that shows its structure, a comment for any step that is not
obvious, and no long chains of commands joined with `&&` or `;` on one line.

## Checks

Build the documentation with `haskell-dev docs` and make sure it has no
warnings about broken markup. The command ends with a check of the comments:
a change must add nothing to what that check reports. It reports 165
problems on master since the rules of the shared mempool are decided in
Haskell, all older than the check: 49 ambiguous names, 17 names out of
scope, 77 links to names that their module does not export, 6 links to
pages or anchors that do not exist, 3 namespace letters written right
after a parenthesis, and 13 slashes to write `\/`. Keep the
lines after `Documentation check` in a file before the change, and compare
with `diff` after it. CI builds the documentation too, with `stack haddock`,
both with the OS libraries and with `stack --nix`.

List the lines of 120 columns or more with
`git ls-files '*.hs' '*.yaml' '*.cabal' '*.md' | xargs grep -n '.\{120,\}'`,
and fix every one that is code and is not one of the exceptions above.
