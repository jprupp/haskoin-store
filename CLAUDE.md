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
- The tests take about 65 seconds in eight processes, against 140 to 210
  with every spec module in a process of its own. Most of it is
  `Haskoin.Store.IntegrationSpec`, whose five environments are groups at the
  top of the module, so that each runs in its own process; keep a new
  environment at the top too. The longest, Bitcoin Cash Node, takes about 64
  seconds. Eight processes at once use about 1.2 GB more than one, with the
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
machine or a Lua script that decides it in one call, and
`docs/concurrency.md` says which, who owns each piece of shared state, and
why no interleaving breaks it. A change to one of them updates that
document, and adds the property that the change keeps to the machine's
spec. Change shared state only from its owner, or in one STM transaction
that reads what the decision depends on.

## The path into the shared mempool

`docs/mempool-ingestion.md` says what the path from a node's mempool to
the pool must guarantee (T1 to T5), what it assumes (A1 to A6), and how
each claim is established. The path is one pure machine,
`Haskoin.Store.Ingest`. A change to it is not done until:

- `scripts/check-ingest.sh` passes: the model checker goes through every
  order of events of its small worlds, in about a minute, without building
  the store. A new rule of the machine gets a check in
  `Haskoin.Store.Ingest.Model`, and a planted bug that the check finds.
- Liquid Haskell passes: `cabal build` in `proofs/`, with GHCup's GHC
  9.12.4 and `z3` on the `PATH`.
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
a change must add nothing to what that check reports. It reports 174
problems on master after the concurrency fixes of 2.0.0, all older than the
check: 49 ambiguous names, 17 names out of scope, 79 links to names that
their module does not export, 12 links to pages or anchors that do not
exist, 4 namespace letters
written right after a parenthesis, and 13 slashes to write `\/`. Keep the
lines after `Documentation check` in a file before the change, and compare
with `diff` after it. CI builds the documentation too, with `stack haddock`,
both with the OS libraries and with `stack --nix`.

List the lines of 120 columns or more with
`git ls-files '*.hs' '*.yaml' '*.cabal' '*.md' | xargs grep -n '.\{120,\}'`,
and fix every one that is code and is not one of the exceptions above.
