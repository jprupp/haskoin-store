-- |
-- Module      : Haskoin.Store.Integration.Image
-- Copyright   : (c) Haskoin Developers
-- License     : MIT
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : POSIX
--
-- The container image of the integration tests, built from pinned and
-- verified sources only.
--
-- The tests use no third-party images. The image starts from the official
-- Ubuntu 26.04 image, pinned by the digest of its multi-architecture index,
-- installs Valkey (the Redis-compatible server Ubuntu ships) from the Ubuntu archive at a fixed snapshot date (apt checks
-- the signatures of the archive; the certificates needed to reach the
-- snapshot service over HTTPS come from the regular archive first), and
-- downloads Bitcoin Core and Bitcoin Cash
-- Node from their official release locations, checking each tarball against
-- a SHA-256 hash pinned here. A tarball that does not match stops the build.
--
-- The hashes were taken from the signed checksum files of each release and
-- checked against the release signing keys of each project when they were
-- pinned:
--
-- * Bitcoin Core 32.1: @SHA256SUMS@ from bitcoincore.org, with good
--   signatures from eight of the builders whose keys are in the
--   @bitcoin-core/guix.sigs@ repository;
-- * Bitcoin Cash Node 29.2.0: @SHA256SUMS.29.2.0.asc.*@ from the
--   @bitcoin-cash-node/announcements@ repository, with good signatures from
--   two maintainers whose keys are in @contrib/gitian-signing/pubkeys.txt@.
--
-- To move to new releases, change the versions and hashes below after
-- checking them the same way. The tag of the image is a hash of the whole
-- recipe, so a change builds a new image and the old one is no longer used.
module Haskoin.Store.Integration.Image
  ( testImage,
    coreBin,
    bchnBin,
    valkeyBin,
  )
where

import Data.ByteString.Char8 qualified as C8
import Data.List (intercalate)
import Data.Serialize (encode)
import Data.Text qualified as T
import Haskoin (encodeHex, sha256)
import System.Exit (ExitCode (..))
import System.Process (readProcessWithExitCode)
import UnliftIO.Temporary (withSystemTempDirectory)

-- | Ubuntu 26.04, by the digest of its multi-architecture index.
base :: String
base = "docker.io/library/ubuntu@sha256:6a06111c3b0a8600feadb270aec771dc239bf407b25625f93f44ab00e11f4d3c"

-- | Date of the snapshot of the Ubuntu archive to install packages from, so
-- that the same versions are installed every time.
snapshot :: String
snapshot = "20261101T000000Z"

-- | Bitcoin Core release and the hashes of its Linux tarballs for x86_64 and
-- aarch64.
coreVersion, coreAmd64, coreArm64 :: String
coreVersion = "32.1"
coreAmd64 = "61545c17c3120fb7412f896d248b11993af04ec8817aad3649c55b2afb044963"
coreArm64 = "7ed37c7373eb6aa3e3f72842596a1afeba1b223717033cade9842b25773e1922"

-- | Bitcoin Cash Node release and the hashes of its Linux tarballs for x86_64
-- and aarch64.
bchnVersion, bchnAmd64, bchnArm64 :: String
bchnVersion = "29.2.0"
bchnAmd64 = "23bbe9ff749136a9b832a2a59023ce76a5391322c1ea2f4f24b5b91f035081bc"
bchnArm64 = "27e177e5a07dc78c2c0d18798ea2e3925111955509ecdc476e828e7ce8f46383"

-- | Daemon of Bitcoin Core in the image.
coreBin :: String
coreBin = "/opt/bitcoin-core/bin/bitcoind"

-- | Daemon of Bitcoin Cash Node in the image.
bchnBin :: String
bchnBin = "/opt/bitcoin-cash-node/bin/bitcoind"

-- | Valkey server in the image.
valkeyBin :: String
valkeyBin = "/usr/bin/valkey-server"

-- | Recipe of the image.
--
-- Every download is its own layer, ordered from the one that changes least
-- often: the packages of the snapshot, then Bitcoin Core, then Bitcoin Cash
-- Node. Podman keeps the layers it builds and reuses each one whose
-- instruction, and those before it, did not change, so moving one node to a
-- new release downloads only that release. Nothing in the image depends on
-- the time it is built.
containerfile :: String
containerfile =
  unlines $
    [ "FROM " <> base,
      "ARG TARGETARCH",
      -- the snapshot archive is only served over HTTPS, so first get the
      -- certificates to reach it from the regular archive (over HTTP, with
      -- the packages checked by the signatures of the archive), then install
      -- everything, the certificates included, from the snapshot
      "RUN apt-get update --error-on=any \\",
      " && apt-get install -y --no-install-recommends ca-certificates \\",
      " && apt-get update --error-on=any --snapshot " <> snapshot <> " \\",
      " && apt-get install -y --no-install-recommends --snapshot " <> snapshot <> " ca-certificates curl valkey-server \\",
      " && rm -rf /var/lib/apt/lists/*"
    ]
      <> release
        "/opt/bitcoin-core"
        ("https://bitcoincore.org/bin/bitcoin-core-" <> coreVersion <> "/bitcoin-" <> coreVersion <> "-$a-linux-gnu.tar.gz")
        coreAmd64
        coreArm64
      <> release
        "/opt/bitcoin-cash-node"
        ("https://github.com/bitcoin-cash-node/bitcoin-cash-node/releases/download/v" <> bchnVersion <> "/bitcoin-cash-node-" <> bchnVersion <> "-$a-linux-gnu.tar.gz")
        bchnAmd64
        bchnArm64
  where
    -- download a release for the architecture of the image, check its hash,
    -- and unpack it; @$a@ in the URL stands for the architecture
    release dir url amd64 arm64 =
      [ "RUN set -eu; \\",
        " case \"$TARGETARCH\" in \\",
        "  amd64) a=x86_64; h=" <> amd64 <> ";; \\",
        "  arm64) a=aarch64; h=" <> arm64 <> ";; \\",
        "  *) echo \"unsupported architecture $TARGETARCH\"; exit 1;; \\",
        " esac; \\",
        " curl -fsSL -o /tmp/release.tar.gz \"" <> url <> "\"; \\",
        " echo \"$h  /tmp/release.tar.gz\" | sha256sum -c -; \\",
        " mkdir -p " <> dir <> "; \\",
        " tar -xzf /tmp/release.tar.gz -C " <> dir <> " --strip-components=1; \\",
        " rm /tmp/release.tar.gz"
      ]

-- | Tag of the image: a hash of its recipe.
tag :: String
tag =
  "localhost/haskoin-store-test:"
    <> take 16 (T.unpack (encodeHex (encode (sha256 (C8.pack containerfile)))))

-- | Build the image unless it exists already, and return its tag. The first
-- build downloads the base image, the packages and the two releases, and may
-- take a few minutes; later builds of a changed recipe reuse the layers that
-- did not change ('containerfile'). Images and layers are never removed by
-- the tests. The build may use 1 GiB of memory.
testImage :: IO String
testImage = do
  (exists, _, _) <- readProcessWithExitCode "podman" ["image", "exists", tag] ""
  case exists of
    ExitSuccess -> return tag
    _ -> withSystemTempDirectory "haskoin-store-image" $ \ctx -> do
      (code, out, err) <-
        readProcessWithExitCode
          "podman"
          ["build", "--layers", "--memory", "1g", "--tag", tag, "--file", "-", ctx]
          containerfile
      case code of
        ExitSuccess -> return tag
        _ ->
          fail $
            intercalate
              "\n"
              ["Could not build the test image " <> tag <> ":", lastLines out, lastLines err]
  where
    lastLines = unlines . reverse . take 30 . reverse . lines
