{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ImportQualifiedPost #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE NoFieldSelectors #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Haskoin.Store.DataSpec (spec, arbitraryDeriveType) where

import Control.Arrow (second)
import Control.Monad (forM_)
import Data.Aeson (FromJSON (..))
import Data.ByteString qualified as B
import Data.Maybe (isJust)
import Data.String.Conversions (cs)
import Haskoin
import Haskoin.Store.Data
import Haskoin.Util
import Haskoin.Util.Arbitrary
import Test.Hspec
import Test.QuickCheck

identityTests :: Network -> Ctx -> IdentityTests
identityTests net ctx =
  IdentityTests
    { readTests = [],
      marshalTests = [],
      jsonTests =
        [ JsonBox (arbitrary :: Gen TxRef),
          JsonBox (arbitrary :: Gen BlockRef),
          JsonBox (arbitrary :: Gen Spender),
          JsonBox (arbitrary :: Gen XPubSummary),
          JsonBox (arbitrary :: Gen HealthCheck),
          JsonBox (arbitrary :: Gen Event),
          JsonBox (arbitrary :: Gen TxId),
          JsonBox (arbitrary :: Gen PeerInfo),
          JsonBox (arbitrary :: Gen (GenericResult XPubSummary)),
          JsonBox (arbitrary :: Gen (RawResult BlockData)),
          JsonBox (arbitrary :: Gen (RawResultList BlockData)),
          JsonBox (arbitrary :: Gen Except),
          JsonBox (arbitrary :: Gen BinfoWallet),
          JsonBox (arbitrary :: Gen BinfoSymbol),
          JsonBox (arbitrary :: Gen BinfoBlockInfo),
          JsonBox (arbitrary :: Gen BinfoInfo),
          JsonBox (arbitrary :: Gen BinfoSpender),
          JsonBox (arbitrary :: Gen BinfoRate),
          JsonBox (arbitrary :: Gen BinfoTicker),
          JsonBox (arbitrary :: Gen BinfoTxId),
          JsonBox (arbitrary :: Gen BinfoShortBal),
          JsonBox (arbitrary :: Gen BinfoHistory),
          JsonBox (arbitrary :: Gen BinfoHeader),
          JsonBox (arbitrary :: Gen BinfoBlockInfos)
        ],
      serialTests =
        [ SerialBox (arbitraryDeriveType net),
          SerialBox (arbitraryXPubSpec net ctx),
          SerialBox (arbitrary :: Gen BlockRef),
          SerialBox (arbitrary :: Gen TxRef),
          SerialBox (arbitraryBalance net),
          SerialBox (arbitraryUnspent net),
          SerialBox (arbitrary :: Gen BlockData),
          SerialBox (arbitraryStoreInput net),
          SerialBox (arbitrary :: Gen Spender),
          SerialBox (arbitraryStoreOutput net),
          SerialBox (arbitrary :: Gen Prev),
          SerialBox (arbitraryTxData ctx :: Gen TxData),
          SerialBox (arbitraryTransaction net),
          SerialBox (arbitraryXPubBal net),
          SerialBox (arbitraryXPubUnspent net),
          SerialBox (arbitrary :: Gen XPubSummary),
          SerialBox (arbitrary :: Gen HealthCheck),
          SerialBox (arbitrary :: Gen Event),
          SerialBox (arbitrary :: Gen TxId),
          SerialBox (arbitrary :: Gen PeerInfo),
          SerialBox (arbitrary :: Gen (GenericResult BlockData)),
          SerialBox (arbitrary :: Gen (RawResult BlockData)),
          SerialBox (arbitrary :: Gen (RawResultList BlockData))
        ],
      marshalJsonTests =
        [ MarshalJsonBox (withNet net arbitraryBalance),
          MarshalJsonBox (withNet net arbitraryStoreOutput),
          MarshalJsonBox (withNet net arbitraryUnspent),
          MarshalJsonBox (withNet net arbitraryXPubBal),
          MarshalJsonBox (withNet net arbitraryXPubUnspent),
          MarshalJsonBox (withNet net arbitraryStoreInput),
          MarshalJsonBox (withNet net arbitraryBlockData),
          MarshalJsonBox (withNet net arbitraryTransaction),
          MarshalJsonBox (withNetCtx net ctx arbitraryBinfoMultiAddr),
          MarshalJsonBox (withNetCtx net ctx arbitraryBinfoBalance),
          MarshalJsonBox (withNetCtx net ctx arbitraryBinfoBlock),
          MarshalJsonBox (withNetCtx net ctx arbitraryBinfoTx),
          MarshalJsonBox (withNetCtx net ctx arbitraryBinfoTxInput),
          MarshalJsonBox (withNetCtx net ctx arbitraryBinfoTxOutput),
          MarshalJsonBox (withNetCtx net ctx arbitraryBinfoXPubPath),
          MarshalJsonBox (withNetCtx net ctx arbitraryBinfoUnspent),
          MarshalJsonBox (withNetCtx net ctx (\net _ -> listOf $ arbitraryBinfoBlock net ctx)),
          MarshalJsonBox (withNetCtx net ctx arbitraryBinfoRawAddr),
          MarshalJsonBox (withNetCtx net ctx arbitraryBinfoMempool)
        ]
    }

withNetCtx :: Network -> Ctx -> (Network -> Ctx -> Gen a) -> Gen ((Network, Ctx), a)
withNetCtx net ctx g = do
  x <- g net ctx
  return ((net, ctx), x)

withNet :: Network -> (Network -> Gen a) -> Gen (Network, a)
withNet net g = do
  x <- g net
  return (net, x)

spec :: Spec
spec = forM_ allNets $ \net -> prepareContext (testIdentity . identityTests net)

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
    <*> listOf (arbitraryStoreInput net)
    <*> listOf (arbitraryStoreOutput net)
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
  txs <- listOf $ arbitraryBinfoTx net ctx
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
  txs <- listOf $ arbitraryBinfoTx net ctx
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
  next <- listOf arbitraryBlockHash
  ntx <- arbitrary
  fee <- arbitrary
  nonce <- arbitrary
  size <- arbitrary
  index <- arbitrary
  main <- arbitrary
  height <- arbitrary
  weight <- arbitrary
  txs <- resize 5 $ listOf $ arbitraryBinfoTx net ctx
  return BinfoBlock {..}

arbitraryBinfoTx :: Network -> Ctx -> Gen BinfoTx
arbitraryBinfoTx net ctx = do
  txid <- arbitraryTxHash
  version <- arbitrary
  inputs <- resize 5 $ listOf1 $ arbitraryBinfoTxInput net ctx
  outputs <- resize 5 $ listOf1 $ arbitraryBinfoTxOutput net ctx
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
arbitraryBinfoXPubPath net ctx = do
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
  fmap BinfoUnspents $ listOf $ arbitraryBinfoUnspent net ctx

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
  fmap BinfoMempool $ listOf $ arbitraryBinfoTx net ctx

instance Arbitrary BinfoBlockInfos where
  arbitrary = BinfoBlockInfos <$> arbitrary
