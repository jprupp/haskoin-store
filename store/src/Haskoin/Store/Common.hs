{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE NoFieldSelectors #-}

module Haskoin.Store.Common
  ( Limits (..),
    Start (..),
    StoreReadBase (..),
    StoreReadExtra (..),
    StoreWrite (..),
    StoreEvent (..),
    getActiveBlock,
    getActiveTxData,
    getDefaultBalance,
    getTransaction,
    getNumTransaction,
    blockAtOrAfter,
    blockAtOrBefore,
    blockAtOrAfterMTP,
    xPubSummary,
    deriveAddresses,
    deriveFunction,
    deOffset,
    applyLimits,
    applyLimitsC,
    applyLimit,
    applyLimitC,
    sortTxs,
    nub',
    microseconds,
    streamThings,
    joinDescStreams,
    loopForever,
  )
where

import Conduit
  ( ConduitT,
    await,
    dropC,
    mapC,
    sealConduitT,
    takeC,
    yield,
    ($$++),
  )
import Control.Monad.Trans (lift)
import Control.Monad.Trans.Maybe (MaybeT (..), runMaybeT)
import Control.Monad.Trans.Reader (runReaderT)
import Data.ByteString (ByteString)
import Data.Default (Default (..))
import Data.Graph qualified as G
import Data.HashMap.Strict qualified as M
import Data.HashSet qualified as H
import Data.Hashable (Hashable)
import Data.Map.Strict qualified as Map
import Data.Maybe (mapMaybe)
import Data.Time.Clock.System
  ( getSystemTime,
    systemNanoseconds,
    systemSeconds,
  )
import Data.Word (Word32, Word64)
import Haskoin
import Haskoin.Node (Chain, Peer)
import Haskoin.Store.Data
  ( Balance (..),
    BlockData (..),
    DeriveType (..),
    Spender,
    Transaction (..),
    TxData (..),
    TxRef (..),
    UnixTime,
    Unspent (..),
    XPubBal (..),
    XPubSpec (..),
    XPubSummary (..),
    XPubUnspent (..),
    nullBalance,
    toTransaction,
    zeroBalance,
  )
import UnliftIO (MonadIO, liftIO)

type DeriveAddr = XPubKey -> KeyIndex -> Address

type Offset = Word32

type Limit = Word32

data Start
  = AtTx !TxHash
  | AtBlock !BlockHeight
  deriving (Eq, Show)

data Limits = Limits
  { limit :: !Word32,
    offset :: !Word32,
    start :: !(Maybe Start)
  }
  deriving (Eq, Show)

defaultLimits :: Limits
defaultLimits = Limits {limit = 0, offset = 0, start = Nothing}

instance Default Limits where
  def = defaultLimits

class (Monad m) => StoreReadBase m where
  getNetwork :: m Network
  getCtx :: m Ctx
  getBestBlock :: m (Maybe BlockHash)
  getBlocksAtHeight :: BlockHeight -> m [BlockHash]
  getBlock :: BlockHash -> m (Maybe BlockData)
  getTxData :: TxHash -> m (Maybe TxData)
  getSpender :: OutPoint -> m (Maybe Spender)
  getBalance :: Address -> m (Maybe Balance)
  getUnspent :: OutPoint -> m (Maybe Unspent)
  getMempool :: m [(UnixTime, TxHash)]

class (StoreReadBase m) => StoreReadExtra m where
  getAddressesTxs :: [Address] -> Limits -> m [TxRef]
  getAddressesUnspents :: [Address] -> Limits -> m [Unspent]
  getInitialGap :: m Word32
  getMaxGap :: m Word32
  getNumTxData :: Word64 -> m [TxData]
  getBalances :: [Address] -> m [Balance]
  getAddressTxs :: Address -> Limits -> m [TxRef]
  getAddressUnspents :: Address -> Limits -> m [Unspent]
  xPubBals :: XPubSpec -> m [XPubBal]
  xPubUnspents :: XPubSpec -> [XPubBal] -> Limits -> m [XPubUnspent]
  xPubTxs :: XPubSpec -> [XPubBal] -> Limits -> m [TxRef]
  xPubTxCount :: XPubSpec -> [XPubBal] -> m Word32

class StoreWrite m where
  setBest :: BlockHash -> m ()
  insertBlock :: BlockData -> m ()
  setBlocksAtHeight :: [BlockHash] -> BlockHeight -> m ()
  insertTx :: TxData -> m ()
  insertAddrTx :: Address -> TxRef -> m ()
  deleteAddrTx :: Address -> TxRef -> m ()
  insertAddrUnspent :: Address -> Unspent -> m ()
  deleteAddrUnspent :: Address -> Unspent -> m ()
  addToMempool :: TxHash -> UnixTime -> m ()
  deleteFromMempool :: TxHash -> m ()
  setBalance :: Balance -> m ()
  insertUnspent :: Unspent -> m ()
  deleteUnspent :: OutPoint -> m ()

getActiveBlock :: (StoreReadExtra m) => BlockHash -> m (Maybe BlockData)
getActiveBlock bh =
  getBlock bh >>= \case
    Just b | b.main -> return (Just b)
    _ -> return Nothing

getActiveTxData :: (StoreReadBase m) => TxHash -> m (Maybe TxData)
getActiveTxData th =
  getTxData th >>= \case
    Just td | not td.deleted -> return (Just td)
    _ -> return Nothing

getDefaultBalance :: (StoreReadBase m) => Address -> m Balance
getDefaultBalance a =
  getBalance a >>= \case
    Nothing -> return $ zeroBalance a
    Just b -> return b

deriveAddresses :: DeriveAddr -> XPubKey -> Word32 -> [(Word32, Address)]
deriveAddresses derive xpub start = map (\i -> (i, derive xpub i)) [start ..]

deriveFunction :: Ctx -> DeriveType -> DeriveAddr
deriveFunction ctx DeriveNormal i = fst . deriveAddr ctx i
deriveFunction ctx DeriveP2SH i = fst . deriveCompatWitnessAddr ctx i
deriveFunction ctx DeriveP2WPKH i = fst . deriveWitnessAddr ctx i

xPubSummary :: XPubSpec -> [XPubBal] -> XPubSummary
xPubSummary _xspec xbals =
  XPubSummary
    { confirmed = sum (map (.balance.confirmed) bs),
      unconfirmed = sum (map (.balance.unconfirmed) bs),
      received = rx,
      utxo = uc,
      change = ch,
      external = ex
    }
  where
    bs = filter (not . nullBalance . (.balance)) xbals
    ex = foldl max 0 [i | XPubBal {path = [0, i]} <- bs]
    ch = foldl max 0 [i | XPubBal {path = [1, i]} <- bs]
    uc = sum [b.balance.utxo | b <- bs]
    xt = [b | b@XPubBal {path = [0, _]} <- bs]
    rx = sum [b.balance.received | b <- xt]

getTransaction ::
  (StoreReadBase m) => TxHash -> m (Maybe Transaction)
getTransaction h = do
  ctx <- getCtx
  fmap (toTransaction ctx) <$> getTxData h

getNumTransaction ::
  (StoreReadExtra m) => Word64 -> m [Transaction]
getNumTransaction i =
  getCtx >>= \ctx ->
    map (toTransaction ctx) <$> getNumTxData i

blockAtOrAfter ::
  (MonadIO m, StoreReadExtra m) =>
  Chain ->
  UnixTime ->
  m (Maybe BlockData)
blockAtOrAfter ch q = runMaybeT $ do
  net <- lift getNetwork
  x <- MaybeT $ liftIO $ runReaderT (firstGreaterOrEqual net f) ch
  MaybeT $ getBlock $ headerHash x.header
  where
    f x =
      let t = fromIntegral x.header.timestamp
       in return $ t `compare` q

blockAtOrBefore ::
  (MonadIO m, StoreReadExtra m) =>
  Chain ->
  UnixTime ->
  m (Maybe BlockData)
blockAtOrBefore ch q = runMaybeT $ do
  net <- lift getNetwork
  x <- MaybeT $ liftIO $ runReaderT (lastSmallerOrEqual net f) ch
  MaybeT $ getBlock $ headerHash x.header
  where
    f x =
      let t = fromIntegral x.header.timestamp
       in return $ t `compare` q

blockAtOrAfterMTP ::
  (MonadIO m, StoreReadExtra m) =>
  Chain ->
  UnixTime ->
  m (Maybe BlockData)
blockAtOrAfterMTP ch q = runMaybeT $ do
  net <- lift getNetwork
  x <- MaybeT $ liftIO $ runReaderT (firstGreaterOrEqual net f) ch
  MaybeT $ getBlock $ headerHash x.header
  where
    f x = do
      t <- fromIntegral <$> mtp x
      return $ t `compare` q

-- | Events that the store can generate.
data StoreEvent
  = StoreBestBlock !BlockHash
  | StoreMempoolNew !TxHash
  | StoreMempoolDelete !TxHash
  | StorePeerConnected !Peer
  | StorePeerDisconnected !Peer
  | StorePeerPong !Peer !Word64
  | StoreTxAnnounce !Peer ![TxHash]
  | StoreTxReject !Peer !TxHash !RejectCode !ByteString

applyLimits :: Limits -> [a] -> [a]
applyLimits Limits {..} = applyLimit limit . applyOffset offset

applyOffset :: Offset -> [a] -> [a]
applyOffset = drop . fromIntegral

applyLimit :: Limit -> [a] -> [a]
applyLimit 0 = id
applyLimit l = take (fromIntegral l)

deOffset :: Limits -> Limits
deOffset l = case l.limit of
  0 -> l {offset = 0}
  _ -> l {limit = l.limit + l.offset, offset = 0}

applyLimitsC :: (Monad m) => Limits -> ConduitT i i m ()
applyLimitsC Limits {..} = applyOffsetC offset >> applyLimitC limit

applyOffsetC :: (Monad m) => Offset -> ConduitT i i m ()
applyOffsetC = dropC . fromIntegral

applyLimitC :: (Monad m) => Limit -> ConduitT i i m ()
applyLimitC 0 = mapC id
applyLimitC l = takeC (fromIntegral l)

sortTxs :: [Tx] -> [(Word32, Tx)]
sortTxs txs = ts
  where
    hm :: M.HashMap TxHash Int
    hm = M.fromList $ zip (map txHash txs) [0 ..]
    ns :: [(Tx, Int, [Int])]
    ns =
      let is tx = nub' $ map (.outpoint.hash) tx.inputs
          hs tx = mapMaybe (`M.lookup` hm) (is tx)
          tp i tx = (tx, i, hs tx)
       in zipWith tp [0 ..] txs
    (g, n, _) = G.graphFromEdges ns
    vs = G.reverseTopSort g
    ts =
      let f (tx, i, _) = (fromIntegral i, tx)
       in map (f . n) vs

nub' :: (Hashable a) => [a] -> [a]
nub' = H.toList . H.fromList

-- | A 'forever' that stays a flat loop in any monad. 'forever' sequences
-- with @*>@; monad-logger's @LoggingT@ leaves @*>@ at its default
-- @(id <$ a) <*> b@ and @ReaderT@ defines @>>@ as @*>@, so a loop that is
-- polymorphic in its monad and not specialised by GHC keeps an @<*>@
-- continuation frame and two thunks per iteration for its whole lifetime.
-- Sequencing with @>>=@ only is a tail call in all these stacks.
loopForever :: (Monad m) => m a -> m b
loopForever action = go
  where
    go = action >>= \_ -> go
{-# INLINE loopForever #-}

microseconds :: (MonadIO m) => m Integer
microseconds =
  let f t =
        toInteger (systemSeconds t) * 1000000
          + toInteger (systemNanoseconds t) `div` 1000
   in liftIO $ f <$> getSystemTime

streamThings ::
  (Monad m) =>
  (Limits -> m [a]) ->
  Maybe (a -> TxHash) ->
  Limits ->
  ConduitT () a m ()
streamThings getit gettx limits =
  lift (getit limits) >>= \case
    [] -> return ()
    ls -> mapM_ yield ls >> go limits (last ls)
  where
    h l x = case gettx of
      Just g -> Just l {offset = 1, start = Just (AtTx (g x))}
      Nothing -> case l.limit of
        0 -> Nothing
        _ -> Just l {offset = l.offset + l.limit}
    go l x = case h l x of
      Nothing -> return ()
      Just l' ->
        lift (getit l') >>= \case
          [] -> return ()
          ls -> mapM_ yield ls >> go l' (last ls)

joinDescStreams ::
  (Monad m, Ord a) =>
  [ConduitT () a m ()] ->
  ConduitT () a m ()
joinDescStreams xs = do
  let ss = map sealConduitT xs
  go Nothing =<< g ss
  where
    j (x, y) = (,[x]) <$> y
    g ss =
      let l = mapMaybe j <$> lift (traverse ($$++ await) ss)
       in Map.fromListWith (++) <$> l
    go m mp = case Map.lookupMax mp of
      Nothing -> return ()
      Just (x, ss) -> do
        case m of
          Nothing -> yield x
          Just x'
            | x == x' -> return ()
            | otherwise -> yield x
        mp1 <- g ss
        let mp2 = Map.deleteMax mp
            mp' = Map.unionWith (++) mp1 mp2
        go (Just x) mp'
