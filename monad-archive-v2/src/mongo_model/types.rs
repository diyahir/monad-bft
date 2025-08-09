use std::ops::{Deref, DerefMut};

use alloy_primitives::{
    hex::{FromHex, ToHexExt},
    FixedBytes,
};
use alloy_rlp::{Decodable, Encodable};
use mongodb::bson::{self, doc, Bson};
use serde::{Deserialize, Serialize};

use crate::{prelude::*, versioned::Versioned};

/// Wrapper around u64 to allow for bson serialization/deserialization as i64
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub(crate) struct BlockNumber(pub u64);

impl BlockNumber {
    pub(super) fn document(&self) -> bson::Document {
        doc! {
            "block_number": self.0 as i64,
        }
    }
}

impl Deref for BlockNumber {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for BlockNumber {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Into<Bson> for BlockNumber {
    fn into(self) -> Bson {
        Bson::Int64(self.0 as i64)
    }
}

impl Serialize for BlockNumber {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_i64(self.0 as i64)
    }
}

impl<'de> Deserialize<'de> for BlockNumber {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let i = i64::deserialize(deserializer)?;
        Ok(BlockNumber(i as u64))
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageMode {
    Inline,
    Ref,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct HeaderDoc {
    #[serde(rename = "_id")]
    pub block_number: BlockNumber,
    pub block_hash: HexBlockHash,
    pub tx_hashes: Vec<HexTxHash>,
    pub tx_count: u32,
    pub storage_mode: StorageMode,

    // RLP encoded evm header data
    #[serde(
        serialize_with = "serialize_header",
        deserialize_with = "deserialize_header"
    )]
    pub header_data: Header,
}

impl HeaderDoc {
    pub(super) fn key(block_number: BlockNumber) -> bson::Document {
        doc! {
            "_id": block_number,
        }
    }

    pub(super) fn to_key(&self) -> bson::Document {
        doc! {
            "_id": self.block_number,
        }
    }
}

#[derive(Debug)]
pub(crate) enum InlineOrRef {
    Ref { byte_start: u32, byte_end: u32 },
    Inline(Vec<u8>),
}

impl InlineOrRef {
    pub fn expect_inline(&self) -> Result<&[u8], ReaderError> {
        match self {
            InlineOrRef::Inline(bytes) => Ok(&bytes),
            InlineOrRef::Ref { .. } => Err(ReaderError::InvalidData(eyre!("Tx is not inline"))),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TxDoc {
    #[serde(rename = "_id")]
    pub tx_hash: HexTxHash,
    pub block_number: BlockNumber,
    pub block_hash: HexBlockHash,
    pub tx_index: u32,

    pub tx_key: InlineOrRef,
    pub rx_key: InlineOrRef,
    pub trace_key: InlineOrRef,

    pub log_prefixes: Vec<LogPrefixes>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TxDocBodyRxProj {
    #[serde(rename = "_id")]
    pub tx_hash: HexTxHash,

    pub tx_key: InlineOrRef,
    pub rx_key: InlineOrRef,
}

impl TxDocBodyRxProj {
    pub(super) fn projection() -> bson::Document {
        doc! {
            "_id": 1,
            "tx_key": 1,
            "rx_key": 1,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TxDocBodyTraceProj {
    #[serde(rename = "_id")]
    pub tx_hash: HexTxHash,
    pub block_number: BlockNumber,
    pub tx_index: u32,

    pub tx_key: InlineOrRef,
    pub trace_key: InlineOrRef,
}

impl TxDocBodyTraceProj {
    pub(super) fn projection() -> bson::Document {
        doc! {
            "_id": 1,
            "tx_index": 1,
            "block_number": 1,
            "tx_key": 1,
            "trace_key": 1,
        }
    }
}

impl TxDoc {
    pub(super) fn key(tx_hash: HexTxHash) -> bson::Document {
        doc! {
            "_id": tx_hash.0.encode_hex(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct LogPrefixes {
    pub address: HexPrefix,
    pub topic_0: HexPrefix,
    pub topic_1: Option<HexPrefix>,
    pub topic_2: Option<HexPrefix>,
    pub topic_3: Option<HexPrefix>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct LatestDoc {
    pub block_number: BlockNumber,
}

impl LatestDoc {
    pub(super) fn key() -> bson::Document {
        doc! {
            "_id": "latest",
        }
    }
}

impl Into<Bson> for LatestDoc {
    fn into(self) -> Bson {
        doc! {
            "block_number": self.block_number.0 as i64,
        }
        .into()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct BlockNumberDoc {
    pub block_number: BlockNumber,
}

pub(super) fn serialize_header<S>(header: &Header, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    // todo: bson fast path with direct encoding into internal buffer
    let mut buf = Vec::new();
    header.encode(&mut buf);
    buf.serialize(serializer)
}

pub(super) fn deserialize_header<'de, D>(deserializer: D) -> Result<Header, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let b = bson::Bson::deserialize(deserializer)?;
    match b {
        bson::Bson::Binary(bin) => {
            Ok(Header::decode(&mut bin.bytes.as_slice()).map_err(serde::de::Error::custom)?)
        }
        _ => Err(serde::de::Error::custom("Invalid header format")),
    }
}

pub type HexTxHash = HexBytes<32>;
pub type HexBlockHash = HexBytes<32>;
/// 4 bytes prefix of a hex string
pub type HexPrefix = HexBytes<4>;

pub(super) fn hex_prefix<const N: usize>(bytes: &FixedBytes<N>) -> HexPrefix {
    let mut buf = [0u8; 4];
    buf[0..4].copy_from_slice(&bytes.0[0..4]);
    HexBytes(FixedBytes::from(buf))
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct HexBytes<const N: usize>(pub FixedBytes<N>);

impl Into<Bson> for HexBytes<32> {
    fn into(self) -> Bson {
        Bson::String(self.0.encode_hex())
    }
}

impl<const N: usize> From<FixedBytes<N>> for HexBytes<N> {
    fn from(bytes: FixedBytes<N>) -> Self {
        HexBytes(bytes)
    }
}

impl<const N: usize> Deref for HexBytes<N> {
    type Target = FixedBytes<N>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<const N: usize> DerefMut for HexBytes<N> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<const N: usize> Serialize for HexBytes<N> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.0.encode_hex())
    }
}

impl<'de, const N: usize> Deserialize<'de> for HexBytes<N> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(HexBytes(
            FixedBytes::from_hex(&s).map_err(serde::de::Error::custom)?,
        ))
    }
}

impl Serialize for InlineOrRef {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            InlineOrRef::Ref {
                byte_start,
                byte_end,
            } => {
                // with left padding to 8 digits
                serializer.serialize_str(&format!("{:08x}:{:08x}", byte_start, byte_end))
            }
            InlineOrRef::Inline(bytes) => serializer.serialize_bytes(bytes),
        }
    }
}

impl<'de> Deserialize<'de> for InlineOrRef {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let b = bson::Bson::deserialize(deserializer)?;
        match b {
            bson::Bson::Binary(bin) => Ok(InlineOrRef::Inline(bin.bytes)),
            bson::Bson::String(s) => {
                let mut iter = s.split(':');
                let first: &str = iter
                    .next()
                    .ok_or_else(|| serde::de::Error::custom("Invalid object key format"))?;
                let second: &str = iter
                    .next()
                    .ok_or_else(|| serde::de::Error::custom("Invalid object key format"))?;
                let byte_start =
                    u32::from_str_radix(first, 16).map_err(serde::de::Error::custom)?;
                let byte_end = u32::from_str_radix(second, 16).map_err(serde::de::Error::custom)?;
                Ok(InlineOrRef::Ref {
                    byte_start,
                    byte_end,
                })
            }
            _ => Err(serde::de::Error::custom("Invalid object key format")),
        }
    }
}

impl<T: Versioned> From<T> for InlineOrRef {
    fn from(value: T) -> Self {
        InlineOrRef::Inline(value.to_bytes())
    }
}

impl From<mongodb::error::Error> for ReaderError {
    fn from(e: mongodb::error::Error) -> Self {
        use mongodb::error::ErrorKind;
        match &*e.kind {
            ErrorKind::InvalidTlsConfig { .. } | ErrorKind::ServerSelection { .. } => {
                ReaderError::NetworkError(e.into())
            }
            ErrorKind::BsonDeserialization(..)
            | ErrorKind::BsonSerialization(..)
            | ErrorKind::InvalidArgument { .. }
            | ErrorKind::InvalidResponse { .. } => ReaderError::InvalidData(e.into()),
            _ => ReaderError::Other(e.into()),
        }
    }
}

impl From<mongodb::error::Error> for WriterError {
    fn from(e: mongodb::error::Error) -> Self {
        use mongodb::error::ErrorKind;
        match &*e.kind {
            ErrorKind::InvalidTlsConfig { .. } | ErrorKind::ServerSelection { .. } => {
                WriterError::NetworkError(e.into())
            }
            ErrorKind::BsonDeserialization(..) | ErrorKind::BsonSerialization(..) => {
                WriterError::EncodeError(e.into())
            }
            _ => WriterError::Other(e.into()),
        }
    }
}
