-- |
-- Laws that every encoding of the data of the store must obey, as specs that
-- take a generator. One line per type in a test table, instead of a block of
-- near-identical code per type and encoding.
module Haskoin.Store.Laws
  ( binaryLaws,
    jsonLaws,
    marshalLaws,
  )
where

import Data.Aeson (FromJSON, ToJSON, Value)
import Data.Aeson qualified as A
import Data.Aeson.Encoding (encodingToLazyByteString)
import Data.Aeson.Types (parseMaybe)
import Data.Binary (Binary)
import Data.Binary qualified as Binary
import Data.ByteString.Lazy qualified as BL
import Data.Bytes.Get (runGetS)
import Data.Bytes.Put (runPutS)
import Data.Bytes.Serial (Serial (..))
import Data.Proxy (Proxy (..))
import Data.Serialize (Serialize)
import Data.Serialize qualified as Cereal
import Data.Typeable (Typeable, typeRep)
import Haskoin.Util (MarshalJSON (..))
import Test.Hspec (Spec)
import Test.Hspec.QuickCheck (prop)
import Test.QuickCheck (Gen, forAll, (.&&.), (===))

-- | Name of the type of a generator, for the description of a test.
name :: forall a. (Typeable a) => Gen a -> String
name _ = show (typeRep (Proxy @a))

-- | The value comes back from its bytes, and the three binary classes
-- ('Serial', cereal and binary) agree on the bytes.
binaryLaws :: forall a. (Eq a, Show a, Typeable a, Serial a, Serialize a, Binary a) => Gen a -> Spec
binaryLaws g = prop (name g) . forAll g $ \x ->
  let bytes = runPutS (serialize x)
   in runGetS deserialize bytes === Right x
        .&&. Cereal.encode x === bytes
        .&&. BL.toStrict (Binary.encode x) === bytes
        .&&. Cereal.decode bytes === Right x

-- | The value comes back from its JSON, and the fast encoder ('A.toEncoding')
-- writes the same JSON as 'A.toJSON': the two are written by hand and could
-- drift apart.
jsonLaws :: forall a. (Eq a, Show a, Typeable a, ToJSON a, FromJSON a) => Gen a -> Spec
jsonLaws g = prop (name g) . forAll g $ \x ->
  A.fromJSON (A.toJSON x) === A.Success x
    .&&. (A.decode (A.encode x) :: Maybe Value) === Just (A.toJSON x)

-- | Like 'jsonLaws', for JSON that depends on the network.
marshalLaws :: forall s a. (Eq a, Show a, Typeable a, MarshalJSON s a) => s -> Gen a -> Spec
marshalLaws s g = prop (name g) . forAll g $ \x ->
  parseMaybe (unmarshalValue s) (marshalValue s x) === Just x
    .&&. (A.decode (encodingToLazyByteString (marshalEncoding s x)) :: Maybe Value)
      === Just (marshalValue s x)
