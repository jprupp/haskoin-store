module Haskoin.Store.Fleet.MemberSpec (spec) where

import Control.Monad (foldM_, void, when)
import Data.Default (def)
import Data.Either (isRight)
import Data.IORef (modifyIORef', newIORef, readIORef)
import Data.Serialize (encode)
import Data.Text qualified as T
import Haskoin (btcRegTest, prepareContext, txHash)
import Haskoin.Node (LogLevel (..))
import Haskoin.Store.Common (Limits (..), Start (..), StoreRead)
import Haskoin.Store.Database.Query qualified as Q
import Haskoin.Store.Database.Query.Layered (From (..))
import Haskoin.Store.Database.Query.Pooled (pooledRead)
import Haskoin.Store.Database.Views (closeViews)
import Haskoin.Store.Fleet.Member
import Haskoin.Store.Fleet.Redis (FleetKeys (..), poolWithdraw)
import Haskoin.Store.Fleet.RedisSpec (withRedis)
import Haskoin.Store.Fleet.XPub
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
                let config =
                      MemberConfig
                        { prefix = keys.prefix,
                          name = "a",
                          lease = 60000,
                          depth = 6,
                          expiry = 0,
                          cacheMax = 0,
                          cacheMinAddrs = 0,
                          cacheMinTxs = 0,
                          stats = Nothing,
                          note = \_ _ -> return ()
                        }
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
                          -- the store tells its member what it deletes
                          memberForget m out.removed
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
      modifyMaxSuccess (const 20) $
        it "answers about a wallet from the cache as it does from the database" $ \(conn, fresh) ->
          -- The same store and histories, with the cache of extended
          -- public keys on. After each step, every question about the
          -- wallet of the histories is asked of the view at the fleet tip
          -- with the pool on top, and again with the cache in front: the
          -- answers must be the same, whether the cache has the wallet,
          -- has it with other addresses, or does not have it. The wallet
          -- is cached after some steps and not after others, so that
          -- blocks and payments find it in the cache and out of it. Right
          -- after it is cached, the cache must be what answers, and only
          -- to a caller that has the wallet with as many addresses.
          --
          -- At one step in five the fleet tip is moved by the same store
          -- taking its turn without the cache, as a store does that stops
          -- between moving the tip and bringing the cache along. The
          -- cache is then at another block than the tip, and must not
          -- answer.
          property . forAll (vectorOf 60 anyNumber) $ \seeds -> ioProperty $
            withSystemTempDirectory "haskoin-store-member-" $ \dir ->
              openLayered btcRegTest ctx dir $ \db -> do
                keys <- fresh
                said <- newIORef []
                let config =
                      MemberConfig
                        { prefix = keys.prefix,
                          name = "a",
                          lease = 60000,
                          depth = 6,
                          expiry = 0,
                          cacheMax = 10,
                          cacheMinAddrs = 0,
                          cacheMinTxs = 0,
                          stats = Nothing,
                          note = \level line -> modifyIORef' said ((level, line) :)
                        }
                    best = Q.getBestBlock db >>= maybe (fail "no best block") return
                    key = wallet ctx
                m <- newMember config conn db
                -- the same store, as it takes its turns without the cache
                bare <- newMember (config {cacheMax = 0} :: MemberConfig) conn db
                hits <- newIORef (0 :: Int)
                hist0 <- start layeredRules db
                best >>= \b -> noteBest m b >> noteBest bare b
                memberTick m
                let tipNow = memberTip m >>= maybe (fail "the fleet has no tip") return
                    inFleet :: (StoreRead -> StoreRead -> IO a) -> IO a
                    inFleet action = do
                      tip <- tipNow
                      answeringCached m tip (\plain cached _ -> action plain cached)
                        >>= maybe (fail "the store has no view of the fleet tip") return
                    turn hist seed =
                      plan db hist seed >>= \case
                        (hist', Nothing) -> return hist'
                        (hist', Just op) -> do
                          out <- apply layeredRules db op
                          let hist'' = record db hist' op out
                          memberForget m out.removed
                          memberForget bare out.removed
                          when (isRight out.result) $ case op of
                            Mine _ _ -> best >>= \b -> noteBest m b >> noteBest bare b
                            Revert _ -> best >>= \b -> noteBest m b >> noteBest bare b
                            Delete _ -> void (poolWithdraw conn keys 32 (map encode out.removed))
                            Pay _ _ -> return ()
                          if seed `mod` 5 == 0
                            then do
                              -- the tip moves, if the step made a block,
                              -- and the cache stays where it was: not in
                              -- every history, but in most
                              memberTick bare
                              -- the member with the cache learns the tip
                              memberTick m
                            else memberTick m
                          let made = map txHash hist''.made
                              pick xs = xs !! (fromIntegral seed `mod` length xs)
                              starts =
                                [Nothing, Just (AtBlock (fromIntegral seed `mod` 8))]
                                  <> [Just (AtTx (pick made)) | not (null made)]
                              limits =
                                [ Limits {limit, offset, start = from}
                                | from <- starts,
                                  (limit, offset) <- [(0, 0), (2, 0), (3, fromIntegral seed `mod` 3)]
                                ]
                              asked =
                                [WalletBalances, WalletCount]
                                  <> map WalletTxs limits
                                  <> map WalletUnspents limits
                          (direct, cached) <- inFleet $ \plain cached ->
                            (,) <$> mapM (answer plain) asked <*> mapM (answer cached) asked
                          (show op, differences asked direct cached) `shouldBe` (show op, [])
                          -- cache the wallet after two steps of three
                          when (seed `mod` 3 /= 0) $ do
                            tip <- tipNow
                            built <- answeringAt m tip (\r pool _ -> cacheBuild conn keys tip r pool 1 key)
                            at <- cacheTip conn keys
                            if at /= Just tip
                              then do
                                -- the cache is behind the tip: nothing is
                                -- written to it, and it does not answer
                                built `shouldSatisfy` (`elem` [Just Abandoned, Just (Built 0)])
                                cachedEntry conn keys tip 20 32 1 key `shouldReturn` Miss
                              else do
                                built `shouldSatisfy` \case
                                  Just (Built _) -> True
                                  _ -> False
                                cachedEntry conn keys tip 20 32 1 key >>= \case
                                  Hit e -> do
                                    modifyIORef' hits (+ 1)
                                    let (n0, n1) = e.sizes
                                        isHit = \case
                                          Hit _ -> True
                                          _ -> False
                                    cachedTxs conn keys tip key (n0, n1) Top def >>= (`shouldSatisfy` isHit)
                                    cachedCount conn keys tip key (n0, n1) >>= (`shouldSatisfy` isHit)
                                    -- not for a caller with other addresses
                                    cachedTxs conn keys tip key (n0 + 1, n1) Top def `shouldReturn` Miss
                                    cachedCount conn keys tip key (n0, n1 + 1) `shouldReturn` Miss
                                    let more = e {sizes = (n0 + 1, n1)}
                                    cachedUnspents conn keys tip key more Top def `shouldReturn` Miss
                                  other -> expectationFailure ("the cache did not answer: " <> show other)
                            (direct', cached') <- inFleet $ \plain through ->
                              (,) <$> mapM (answer plain) asked <*> mapM (answer through) asked
                            (show op, differences asked direct' cached') `shouldBe` (show op, [])
                          return hist''
                foldM_ turn hist0 seeds
                -- the least recently used entries go when there are too
                -- many, and nothing of them is left
                memberTick m
                memberTick m
                tip <- tipNow
                cacheTip conn keys `shouldReturn` Just tip
                _ <- answeringAt m tip (\r pool _ -> cacheBuild conn keys tip r pool 1 key)
                cacheEvict conn keys 1 `shouldReturn` 0
                cacheEvict conn keys 0 `shouldReturn` 1
                cachedEntry conn keys tip 20 32 1 key `shouldReturn` Miss
                cacheHave conn keys key `shouldReturn` (0, 0)
                closeViews m.views
                closeViews bare.views
                -- what the member logged: that it emptied the cache, and
                -- why, as information, and nothing else above debugging
                logged <- readIORef said
                let emptied = [line | (LevelInfo, line) <- logged]
                [() | (level, _) <- logged, level /= LevelInfo, level /= LevelDebug] `shouldBe` []
                emptied `shouldSatisfy` all ("Emptied the xpub cache at block " `T.isPrefixOf`)
                readIORef hits >>= (`shouldSatisfy` (> 10))
