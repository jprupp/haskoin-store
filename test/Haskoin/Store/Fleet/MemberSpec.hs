module Haskoin.Store.Fleet.MemberSpec (spec) where

import Control.Monad (foldM_, void, when)
import Data.Either (isRight)
import Data.Serialize (encode)
import Haskoin (btcRegTest, prepareContext)
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Database.Query.Pooled (pooledRead)
import Haskoin.Store.Database.Views (closeViews)
import Haskoin.Store.Fleet.Member
import Haskoin.Store.Fleet.Redis (FleetKeys (..), poolWithdraw)
import Haskoin.Store.Fleet.RedisSpec (withRedis)
import Haskoin.Store.Histories
import Haskoin.Store.Integration.Podman (podmanUsable)
import Test.Hspec
import Test.Hspec.QuickCheck (modifyMaxSuccess)
import Test.QuickCheck (forAll, ioProperty, property, vectorOf)
import UnliftIO (withSystemTempDirectory)

spec :: Spec
spec = prepareContext $ \ctx -> do
  usable <- runIO podmanUsable
  describe "member of a fleet" $ case usable of
    Left why -> it "answers as the store does when it is the whole fleet" (pendingWith why)
    Right () -> aroundAll withRedis $ do
      modifyMaxSuccess (const 20) $
        it "answers as the store does when it is the whole fleet" $ \(conn, fresh) ->
          -- A store alone in its fleet, through a random history of
          -- payments, replacements, blocks and reorganisations. After each
          -- step it takes its turn as a member, and then every answer that
          -- it gives from the view at the fleet tip with the pool of Redis
          -- on top must be the answer that it gives from its own database.
          property . forAll (vectorOf 60 anyNumber) $ \seeds -> ioProperty $
            withSystemTempDirectory "haskoin-store-member-" $ \dir ->
              openLayered btcRegTest ctx dir $ \db -> do
                keys <- fresh
                let config = MemberConfig {prefix = keys.prefix, name = "a", lease = 60000, depth = 6, expiry = 0}
                    best = Q.getBestBlock db >>= maybe (fail "no best block") return
                m <- newMember config conn db
                hist0 <- start layeredRules db
                best >>= noteBest m
                memberTick m
                let turn hist seed =
                      plan db hist seed >>= \case
                        (hist', Nothing) -> return hist'
                        (hist', Just op) -> do
                          out <- apply layeredRules db op
                          let hist'' = record db hist' op out
                          when (isRight out.result) $ case op of
                            Mine _ _ -> best >>= noteBest m
                            Revert _ -> best >>= noteBest m
                            -- a store deletes what is too old, and the pool
                            -- does the same by its own clock
                            Delete _ -> void (poolWithdraw conn keys 32 (map encode out.removed))
                            Pay _ _ -> return ()
                          memberTick m
                          own <- layered db (ask hist'')
                          answering m (\r pool -> ask hist'' (pooledRead r pool)) >>= \case
                            Answered shared -> (show op, shared) `shouldBe` (show op, own)
                            Alone -> expectationFailure "the fleet has no tip"
                            Behind -> expectationFailure "the store has no view of the fleet tip"
                          return hist''
                foldM_ turn hist0 seeds
                closeViews m.views
