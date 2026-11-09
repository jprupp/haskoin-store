{-# LANGUAGE RecordWildCards #-}
{-# OPTIONS_GHC -Wno-orphans #-}

-- |
-- Generators of the data of the store, for property tests. Collections of
-- large values (transactions, blocks, inputs, outputs) are kept to three
-- elements: a round trip of one element tests the encoding as well as a round
-- trip of a hundred, and big values made the tests take minutes.
module Haskoin.Store.Gen
  ( small,
    small1,
    arbitraryTxData,
    arbitraryStoreInput,
    arbitraryStoreOutput,
    arbitraryTransaction,
    arbitraryXPubSpec,
    arbitraryDeriveType,
    arbitraryBalance,
    arbitraryUnspent,
    arbitraryBlockData,
    arbitraryXPubBal,
    arbitraryXPubUnspent,
    arbitraryBinfoMultiAddr,
    arbitraryBinfoRawAddr,
    arbitraryBinfoBalance,
    arbitraryBinfoBlock,
    arbitraryBinfoTx,
    arbitraryBinfoTxInput,
    arbitraryBinfoTxOutput,
    arbitraryBinfoXPubPath,
    arbitraryBinfoUnspent,
    arbitraryBinfoUnspents,
    arbitraryBinfoMempool,
  )
where

import Data.ByteString qualified as B
import Data.Maybe (isJust)
import Data.String.Conversions (cs)
import Haskoin
import Haskoin.Store.Data
import Haskoin.Util.Arbitrary
import Test.QuickCheck

-- | Up to three values.
small :: Gen a -> Gen [a]
small g = choose (0, 3) >>= (`vectorOf` g)

-- | One to three values.
small1 :: Gen a -> Gen [a]
small1 g = choose (1, 3) >>= (`vectorOf` g)

instance Arbitrary BlockRef where
  arbitrary =
    oneof [BlockRef <$> arbitrary <*> arbitrary, MemRef <$> arbitrary]

instance Arbitrary Prev where
  arbitrary = Prev <$> arbitraryBS1 <*> arbitrary

arbitraryTxData :: Ctx -> Gen TxData
arbitraryTxData ctx =
  TxData
    <$> arbitrary
    <*> arbitraryTx btc ctx
    <*> arbitrary
    <*> arbitrary
    <*> arbitrary
    <*> arbitrary
    <*> arbitrary

arbitraryStoreInput :: Network -> Gen StoreInput
arbitraryStoreInput net = do
  store <-
    oneof
      [ StoreCoinbase
          <$> arbitraryOutPoint
          <*> arbitrary
          <*> arbitraryBS1
          <*> listOf arbitraryBS1,
        StoreInput
          <$> arbitraryOutPoint
          <*> arbitrary
          <*> arbitraryBS1
          <*> arbitraryBS1
          <*> arbitrary
          <*> listOf arbitraryBS1
          <*> arbitraryMaybe (arbitraryAddress net)
      ]
  let res
        | net.segWit = store
        | otherwise = witless store
  return res
  where
    witless StoreInput {..} = StoreInput {witness = [], ..}
    witless StoreCoinbase {..} = StoreCoinbase {witness = [], ..}

instance Arbitrary Spender where
  arbitrary = Spender <$> arbitraryTxHash <*> arbitrary

arbitraryStoreOutput :: Network -> Gen StoreOutput
arbitraryStoreOutput net =
  StoreOutput
    <$> arbitrary
    <*> arbitraryBS1
    <*> arbitrary
    <*> arbitraryMaybe (arbitraryAddress net)

arbitraryTransaction :: Network -> Gen Transaction
arbitraryTransaction net =
  Transaction
    <$> arbitrary
    <*> arbitrary
    <*> arbitrary
    <*> small1 (arbitraryStoreInput net)
    <*> small1 (arbitraryStoreOutput net)
    <*> arbitrary
    <*> arbitrary
    <*> arbitrary
    <*> arbitraryTxHash
    <*> arbitrary
    <*> arbitrary
    <*> arbitrary

instance Arbitrary PeerInfo where
  arbitrary =
    PeerInfo
      <$> (cs <$> listOf arbitraryUnicodeChar)
      <*> listOf arbitraryPrintableChar
      <*> arbitrary
      <*> arbitrary
      <*> arbitrary

instance Arbitrary BlockHealth where
  arbitrary =
    BlockHealth
      <$> arbitrary
      <*> arbitrary
      <*> arbitrary

instance Arbitrary TimeHealth where
  arbitrary =
    TimeHealth
      <$> arbitrary
      <*> arbitrary

instance Arbitrary CountHealth where
  arbitrary =
    CountHealth
      <$> arbitrary
      <*> arbitrary

instance Arbitrary MaxHealth where
  arbitrary =
    MaxHealth
      <$> arbitrary
      <*> arbitrary

instance Arbitrary HealthCheck where
  arbitrary =
    HealthCheck
      <$> arbitrary
      <*> arbitrary
      <*> arbitrary
      <*> arbitrary
      <*> arbitrary
      <*> arbitrary
      <*> arbitrary
      <*> arbitrary

instance Arbitrary RejectCode where
  arbitrary =
    elements
      [ RejectMalformed,
        RejectInvalid,
        RejectObsolete,
        RejectDuplicate,
        RejectNonStandard,
        RejectDust,
        RejectInsufficientFee,
        RejectCheckpoint
      ]

arbitraryXPubSpec :: Network -> Ctx -> Gen XPubSpec
arbitraryXPubSpec net ctx = XPubSpec <$> arbitraryXPubKey ctx <*> arbitraryDeriveType net

arbitraryDeriveType :: Network -> Gen DeriveType
arbitraryDeriveType net =
  if net.segWit
    then elements [DeriveNormal, DeriveP2SH, DeriveP2WPKH]
    else return DeriveNormal

instance Arbitrary TxId where
  arbitrary = TxId <$> arbitraryTxHash

instance Arbitrary TxRef where
  arbitrary = TxRef <$> arbitrary <*> arbitraryTxHash

arbitraryBalance :: Network -> Gen Balance
arbitraryBalance net =
  Balance
    <$> arbitraryAddress net
    <*> arbitrary
    <*> arbitrary
    <*> arbitrary
    <*> arbitrary
    <*> arbitrary

arbitraryUnspent :: Network -> Gen Unspent
arbitraryUnspent net =
  Unspent
    <$> arbitrary
    <*> arbitraryOutPoint
    <*> arbitrary
    <*> arbitraryBS1
    <*> arbitraryMaybe (arbitraryAddress net)

instance Arbitrary BlockData where
  arbitrary =
    BlockData
      <$> arbitrary
      <*> arbitrary
      <*> (fromInteger <$> suchThat arbitrary (0 <=))
      <*> arbitraryBlockHeader
      <*> arbitrary
      <*> arbitrary
      <*> listOf1 arbitraryTxHash
      <*> arbitrary
      <*> arbitrary
      <*> arbitrary

arbitraryBlockData :: Network -> Gen BlockData
arbitraryBlockData net = do
  dat@BlockData {..} <- arbitrary
  return $ if net.segWit then dat else BlockData {weight = 0, ..}

instance (Arbitrary a) => Arbitrary (GenericResult a) where
  arbitrary = GenericResult <$> arbitrary

instance (Arbitrary a) => Arbitrary (RawResult a) where
  arbitrary = RawResult <$> arbitrary

instance (Arbitrary a) => Arbitrary (RawResultList a) where
  arbitrary = RawResultList <$> arbitrary

arbitraryXPubBal :: Network -> Gen XPubBal
arbitraryXPubBal net = XPubBal <$> arbitrary <*> arbitraryBalance net

arbitraryXPubUnspent :: Network -> Gen XPubUnspent
arbitraryXPubUnspent net = XPubUnspent <$> arbitraryUnspent net <*> arbitrary

instance Arbitrary XPubSummary where
  arbitrary =
    XPubSummary
      <$> arbitrary
      <*> arbitrary
      <*> arbitrary
      <*> arbitrary
      <*> arbitrary
      <*> arbitrary

instance Arbitrary Event where
  arbitrary =
    oneof
      [ EventBlock <$> arbitraryBlockHash,
        EventTx <$> arbitraryTxHash
      ]

instance Arbitrary Except where
  arbitrary =
    oneof
      [ return ThingNotFound,
        return ServerError,
        return BadRequest,
        UserError <$> arbitrary,
        StringError <$> arbitrary,
        TxIndexConflict <$> listOf1 arbitraryTxHash,
        return ServerTimeout
      ]

---------------------------------------
-- Blockchain.info API Compatibility --
---------------------------------------

instance Arbitrary BinfoTxId where
  arbitrary =
    oneof
      [ BinfoTxIdHash <$> arbitraryTxHash,
        BinfoTxIdIndex <$> arbitrary
      ]

arbitraryBinfoMultiAddr :: Network -> Ctx -> Gen BinfoMultiAddr
arbitraryBinfoMultiAddr net ctx = do
  b <- arbitraryBinfoBalance net ctx
  let addresses = [b]
  wallet <- arbitrary
  txs <- small (arbitraryBinfoTx net ctx)
  info <- arbitrary
  recommendFee <- arbitrary
  let cashAddr = isJust net.cashAddrPrefix
  return BinfoMultiAddr {..}

arbitraryBinfoRawAddr :: Network -> Ctx -> Gen BinfoRawAddr
arbitraryBinfoRawAddr net ctx = do
  address <-
    oneof
      [ BinfoAddr <$> arbitraryAddress net,
        BinfoXpub <$> arbitraryXPubKey ctx
      ]
  balance <- arbitrary
  ntx <- arbitrary
  utxo <- arbitrary
  received <- arbitrary
  sent <- arbitrary
  txs <- small (arbitraryBinfoTx net ctx)
  return $ BinfoRawAddr {..}

instance Arbitrary BinfoShortBal where
  arbitrary = BinfoShortBal <$> arbitrary <*> arbitrary <*> arbitrary

arbitraryBinfoBalance :: Network -> Ctx -> Gen BinfoBalance
arbitraryBinfoBalance net ctx = do
  address <- arbitraryAddress net
  txs <- arbitrary
  received <- arbitrary
  sent <- arbitrary
  balance <- arbitrary
  xpub <- arbitraryXPubKey ctx
  external <- arbitrary
  change <- arbitrary
  elements [BinfoAddrBalance {..}, BinfoXPubBalance {..}]

instance Arbitrary BinfoWallet where
  arbitrary = do
    balance <- arbitrary
    txs <- arbitrary
    filtered <- arbitrary
    received <- arbitrary
    sent <- arbitrary
    return BinfoWallet {..}

arbitraryBinfoBlock :: Network -> Ctx -> Gen BinfoBlock
arbitraryBinfoBlock net ctx = do
  hash <- arbitraryBlockHash
  version <- arbitrary
  prev <- arbitraryBlockHash
  merkle <- (.get) <$> arbitraryTxHash
  timestamp <- arbitrary
  bits <- arbitrary
  next <- small arbitraryBlockHash
  ntx <- arbitrary
  fee <- arbitrary
  nonce <- arbitrary
  size <- arbitrary
  index <- arbitrary
  main <- arbitrary
  height <- arbitrary
  weight <- arbitrary
  txs <- small (arbitraryBinfoTx net ctx)
  return BinfoBlock {..}

arbitraryBinfoTx :: Network -> Ctx -> Gen BinfoTx
arbitraryBinfoTx net ctx = do
  txid <- arbitraryTxHash
  version <- arbitrary
  inputs <- small1 (arbitraryBinfoTxInput net ctx)
  outputs <- small1 (arbitraryBinfoTxOutput net ctx)
  let inputCount = fromIntegral $ length inputs
      outputCount = fromIntegral $ length outputs
  size <- arbitrary
  weight <- arbitrary
  fee <- arbitrary
  relayed <- cs <$> listOf arbitraryUnicodeChar
  locktime <- arbitrary
  index <- arbitrary
  doubleSpend <- arbitrary
  rbf <- arbitrary
  timestamp <- arbitrary
  blockIndex <- arbitrary
  blockHeight <- arbitrary
  balance <- arbitrary
  return BinfoTx {..}

arbitraryBinfoTxInput :: Network -> Ctx -> Gen BinfoTxInput
arbitraryBinfoTxInput net ctx = do
  sequence <- arbitrary
  witness <- B.pack <$> listOf arbitrary
  script <- B.pack <$> listOf arbitrary
  index <- arbitrary
  output <- arbitraryBinfoTxOutput net ctx
  return BinfoTxInput {..}

arbitraryBinfoTxOutput :: Network -> Ctx -> Gen BinfoTxOutput
arbitraryBinfoTxOutput net ctx = do
  typ <- arbitrary
  spent <- arbitrary
  value <- arbitrary
  index <- arbitrary
  txidx <- arbitrary
  script <- B.pack <$> listOf arbitrary
  spenders <- arbitrary
  address <- arbitraryMaybe (arbitraryAddress net)
  xpub <- arbitraryMaybe $ arbitraryBinfoXPubPath net ctx
  return BinfoTxOutput {..}

instance Arbitrary BinfoSpender where
  arbitrary = do
    txidx <- arbitrary
    input <- arbitrary
    return BinfoSpender {..}

arbitraryBinfoXPubPath :: Network -> Ctx -> Gen BinfoXPubPath
arbitraryBinfoXPubPath _net ctx = do
  key <- arbitraryXPubKey ctx
  deriv <- arbitrarySoftPath
  return BinfoXPubPath {..}

instance Arbitrary BinfoInfo where
  arbitrary = do
    connected <- arbitrary
    conversion <- arbitrary
    fiat <- arbitrary
    crypto <- arbitrary
    head <- arbitrary
    return BinfoInfo {..}

instance Arbitrary BinfoBlockInfo where
  arbitrary = do
    hash <- arbitraryBlockHash
    height <- arbitrary
    timestamp <- arbitrary
    index <- arbitrary
    return BinfoBlockInfo {..}

instance Arbitrary BinfoSymbol where
  arbitrary = do
    code <- cs <$> listOf1 arbitraryUnicodeChar
    symbol <- cs <$> listOf1 arbitraryUnicodeChar
    name <- cs <$> listOf1 arbitraryUnicodeChar
    conversion <- arbitrary
    after <- arbitrary
    local <- arbitrary
    return BinfoSymbol {..}

instance Arbitrary BinfoRate where
  arbitrary = BinfoRate <$> arbitrary <*> arbitrary <*> arbitrary

instance Arbitrary BinfoTicker where
  arbitrary = do
    fifteen <- arbitrary
    sell <- arbitrary
    buy <- arbitrary
    last <- arbitrary
    symbol <- cs <$> listOf1 arbitraryUnicodeChar
    return BinfoTicker {..}

instance Arbitrary BinfoHistory where
  arbitrary = do
    date <- cs <$> listOf1 arbitraryUnicodeChar
    time <- cs <$> listOf1 arbitraryUnicodeChar
    typ <- cs <$> listOf1 arbitraryUnicodeChar
    amount <- arbitrary
    valueThen <- arbitrary
    valueNow <- arbitrary
    rateThen <- arbitrary
    txid <- arbitraryTxHash
    fee <- arbitrary
    return BinfoHistory {..}

arbitraryBinfoUnspent :: Network -> Ctx -> Gen BinfoUnspent
arbitraryBinfoUnspent net ctx = do
  txid <- arbitraryTxHash
  index <- arbitrary
  script <- B.pack <$> listOf arbitrary
  value <- arbitrary
  confirmations <- arbitrary
  txidx <- arbitrary
  xpub <- arbitraryMaybe $ arbitraryBinfoXPubPath net ctx
  return BinfoUnspent {..}

arbitraryBinfoUnspents :: Network -> Ctx -> Gen BinfoUnspents
arbitraryBinfoUnspents net ctx =
  BinfoUnspents <$> small (arbitraryBinfoUnspent net ctx)

instance Arbitrary BinfoHeader where
  arbitrary =
    BinfoHeader
      <$> arbitraryBlockHash
      <*> arbitrary
      <*> arbitrary
      <*> arbitrary
      <*> arbitrary

arbitraryBinfoMempool :: Network -> Ctx -> Gen BinfoMempool
arbitraryBinfoMempool net ctx =
  BinfoMempool <$> small (arbitraryBinfoTx net ctx)

instance Arbitrary BinfoBlockInfos where
  arbitrary = BinfoBlockInfos <$> small arbitrary
