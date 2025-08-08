use alloy_primitives::{hex::ToHexExt, Address, TxHash};
use alloy_rlp::{Decodable, Encodable, RlpDecodable, RlpEncodable};
use alloy_rpc_types::{Log, Topic};
use bytes::BufMut;
use mongodb::bson::{self, Bson};
use serde::{Deserialize, Serialize};

use crate::prelude::*;

pub type Tx = TxEnvelopeWithSender;
pub type TxReceipt = ReceiptWithLogIndex;
pub type TxTrace = Vec<u8>;
pub type Block = AlloyBlock<Tx, Header>;
pub type BlockReceipts = Vec<TxReceipt>;
pub type BlockTraces = Vec<TxTrace>;
pub type BlockRange = RangeInclusive<u64>;

pub struct BlockData {
    pub block: Block,
    pub receipts: BlockReceipts,
    pub traces: BlockTraces,
}

pub struct TxData {
    pub tx: Tx,
    pub receipt: TxReceipt,
    pub trace: TxTrace,
}

pub fn make_block(header: Header, transactions: Vec<Tx>) -> Block {
    Block {
        header,
        body: BlockBody {
            transactions,
            ommers: Vec::new(),
            withdrawals: Some(alloy_eips::eip4895::Withdrawals::default()),
        },
    }
}

#[derive(RlpEncodable)]
struct VersionedDataRef<'a, T: Encodable> {
    version: u8,
    data: &'a T,
}

#[derive(RlpDecodable)]
struct VersionedData {
    version: u8,
    data: Vec<u8>,
}

pub trait Versioned: Sized {
    const VERSION: u8;
    /// RLP encode a versioned object to bytes.
    fn to_bytes(&self) -> Vec<u8>;

    /// RLP decode a versioned object from bytes.
    fn from_bytes(bytes: &[u8]) -> Result<Self>;

    fn to_bson(&self) -> Bson {
        Bson::Binary(bson::Binary {
            subtype: bson::spec::BinarySubtype::Generic,
            bytes: self.to_bytes(),
        })
    }
}

impl Versioned for Tx {
    const VERSION: u8 = 0;

    fn to_bytes(&self) -> Vec<u8> {
        let mut rlp_buf = Vec::with_capacity(1024);
        VersionedDataRef {
            version: Self::VERSION,
            data: self,
        }
        .encode(&mut rlp_buf);
        rlp_buf
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, eyre::Error> {
        let VersionedData { version, data } = VersionedData::decode(&mut &bytes[..])?;
        match version {
            0 => {
                let tx = Tx::decode(&mut &data[..])?;
                Ok(tx)
            }
            _ => Err(alloy_rlp::Error::Custom("Invalid tx storage repr data").into()),
        }
    }
}

impl Versioned for TxReceipt {
    const VERSION: u8 = 0;

    fn to_bytes(&self) -> Vec<u8> {
        let mut rlp_buf = Vec::with_capacity(1024);
        VersionedDataRef {
            version: Self::VERSION,
            data: self,
        }
        .encode(&mut rlp_buf);
        rlp_buf
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, eyre::Error> {
        let VersionedData { version, data } = VersionedData::decode(&mut &bytes[..])?;
        match version {
            0 => {
                let tx = TxReceipt::decode(&mut &data[..])?;
                Ok(tx)
            }
            _ => Err(alloy_rlp::Error::Custom("Invalid tx storage repr data").into()),
        }
    }
}

// impl TxStorageRepr {
//     pub async fn convert(self, _model: &impl Reader) -> Tx {
//         match self {
//             Self::V0(tx) => tx,
//         }
//     }

//     pub fn serialize(&self) -> Vec<u8> {
//         let mut rlp_buf = Vec::with_capacity(1024);
//         self.encode(&mut rlp_buf);
//         rlp_buf
//     }
// }

// // impl Encodable for TxStorageRepr {
// //     fn encode(&self, out: &mut dyn BufMut) {
// //         match self {
// //             Self::V0(tx) => {
// //                 VersionedDataRef {
// //                     version: 0,
// //                     data: tx,
// //                 }
// //                 .encode(out);
// //             }
// //         }
// //     }
// // }

// impl Decodable for TxStorageRepr {
//     fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
//         let VersionedData { version, data } = VersionedData::decode(buf)?;
//         match version {
//             0 => Ok(Self::V0(data)),
//             _ => Err(alloy_rlp::Error::Custom("Invalid tx storage repr data")),
//         }
//     }
// }

// impl RxStorageRepr {
//     pub async fn convert(self, _model: &impl Reader) -> TxReceipt {
//         match self {
//             Self::V0(rx) => rx,
//         }
//     }

//     pub fn serialize(&self) -> Vec<u8> {
//         match self {
//             Self::V0(rx) => {
//                 let mut rlp_buf = Vec::with_capacity(1024);
//                 rlp_buf.push(0); // version
//                 rx.encode(&mut rlp_buf);
//                 rlp_buf
//             }
//         }
//     }
// }

// impl TryFrom<&[u8]> for TxStorageRepr {
//     type Error = eyre::Error;

//     fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
//         if value.len() < 2 {
//             return Err(eyre::eyre!("Invalid tx storage repr data: too short"));
//         }
//         let ty = value[0];
//         let data = &value[1..];

//         match ty {
//             0 => {
//                 let tx = Tx::decode(&mut &data[..])
//                     .wrap_err_with(|| format!("Invalid tx storage repr data: {}", ty))?;
//                 Ok(Self::V0(tx))
//             }
//             _ => Err(eyre::eyre!("Invalid tx storage repr type: {}", ty)),
//         }
//     }
// }

// impl TryFrom<&[u8]> for RxStorageRepr {
//     type Error = eyre::Error;

//     fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
//         if value.len() < 2 {
//             return Err(eyre::eyre!("Invalid rx storage repr data: too short"));
//         }
//         let ty = value[0];
//         let data = &value[1..];

//         match ty {
//             0 => {
//                 let rx = TxReceipt::decode(&mut &data[..])
//                     .wrap_err_with(|| format!("Invalid rx storage repr data: {}", ty))?;
//                 Ok(Self::V0(rx))
//             }
//             _ => Err(eyre::eyre!("Invalid rx storage repr type: {}", ty)),
//         }
//     }
// }

pub type ReaderResult<T> = Result<T, ReaderError>;

pub enum ReaderError {
    BlockNotFound(u64),
    TxNotFound(TxHash),
    InvalidData(eyre::Error),
    NetworkError(eyre::Error),
    Other(eyre::Error),
}

impl From<eyre::Error> for ReaderError {
    fn from(error: eyre::Error) -> Self {
        Self::Other(error)
    }
}

impl std::fmt::Display for ReaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BlockNotFound(block_number) => write!(f, "Block not found: {}", block_number),
            Self::TxNotFound(tx_hash) => write!(f, "Tx not found: {}", tx_hash.encode_hex()),
            Self::InvalidData(error) => write!(f, "Invalid data: {}", error),
            Self::NetworkError(error) => write!(f, "Network error: {}", error),
            Self::Other(error) => write!(f, "Other error: {}", error),
        }
    }
}

impl std::fmt::Debug for ReaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BlockNotFound(block_number) => write!(f, "Block not found: {}", block_number),
            Self::TxNotFound(tx_hash) => write!(f, "Tx not found: {}", tx_hash.encode_hex()),
            Self::InvalidData(error) => write!(f, "Invalid data: {:?}", error),
            Self::NetworkError(error) => write!(f, "Network error: {:?}", error),
            Self::Other(error) => write!(f, "Other error: {:?}", error),
        }
    }
}

impl std::error::Error for ReaderError {}

pub type WriterResult = Result<(), WriterError>;

pub enum WriterError {
    EncodeError(eyre::Error),
    NetworkError(eyre::Error),
    Other(eyre::Error),
}

impl std::fmt::Display for WriterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EncodeError(error) => write!(f, "Encode error: {}", error),
            Self::NetworkError(error) => write!(f, "Network error: {}", error),
            Self::Other(error) => write!(f, "Other error: {}", error),
        }
    }
}

impl std::fmt::Debug for WriterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EncodeError(error) => write!(f, "Encode error: {:?}", error),
            Self::NetworkError(error) => write!(f, "Network error: {:?}", error),
            Self::Other(error) => write!(f, "Other error: {:?}", error),
        }
    }
}

impl std::error::Error for WriterError {}

pub struct EthGetLogsQuery {
    pub block_range: BlockRange,
    pub address: Vec<Address>,
    pub topics: Vec<Topic>,
}

pub trait BlockReader {
    async fn get_latest(&self) -> ReaderResult<Option<u64>>;

    async fn get_block_header(&self, block_number: u64) -> ReaderResult<Header>;
    async fn get_block(&self, block_number: u64) -> ReaderResult<Block>;
    async fn get_block_receipts(&self, block_number: u64) -> ReaderResult<BlockReceipts>;
    async fn get_block_traces(&self, block_number: u64) -> ReaderResult<BlockTraces>;
    async fn get_block_data(&self, block_number: u64) -> ReaderResult<BlockData>;

    async fn block_exists(&self, block_number: u64) -> ReaderResult<bool>;
    async fn list_blocks(&self, range: BlockRange) -> ReaderResult<Vec<u64>>;
}

pub trait Reader: BlockReader {
    async fn get_tx(&self, tx_hash: TxHash) -> ReaderResult<Tx>;
    async fn get_tx_receipt(&self, tx_hash: TxHash) -> ReaderResult<TxReceipt>;
    async fn get_tx_trace(&self, tx_hash: TxHash) -> ReaderResult<TxTrace>;
    async fn get_tx_data(&self, tx_hash: TxHash) -> ReaderResult<TxData>;

    async fn tx_exists(&self, tx_hash: TxHash) -> ReaderResult<bool>;

    async fn eth_logs(&self, query: EthGetLogsQuery) -> ReaderResult<Vec<Log>>;
}

pub trait Writer {
    async fn update_latest(&self, block_number: u64) -> WriterResult;

    async fn write_block_data(&self, data: BlockData) -> WriterResult;
}
