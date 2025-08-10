
use crate::{
    errors::KVError,
    model::BlockReader,
    prelude::*,
};

pub mod reader;
pub mod writer;

pub mod stores;
pub use stores::memory::MemoryStore;

const DIGITS_U64: u8 = 20;
const DIGITS_U32: u8 = 10;

pub type KVResult<T> = Result<T, KVError>;

pub trait ObjectStore: Send + Sync + 'static {
    async fn get(&self, key: &str) -> KVResult<Bytes>;
    async fn get_range(&self, key: &str, range: RangeInclusive<u32>) -> KVResult<Bytes>;
    async fn put(&self, key: &str, value: Bytes) -> KVResult<()>;
    async fn delete(&self, key: &str) -> KVResult<()>;
    async fn list(&self, prefix: &str) -> KVResult<Vec<String>>;
    async fn list_range(&self, prefix: &str, range: RangeInclusive<u64>) -> KVResult<Vec<u64>>;
    async fn exists(&self, key: &str) -> KVResult<bool>;
    // async fn is_stale(&self, key: &str, version: u64) -> ObjectStoreResult<bool>;
}

pub trait ObjectStoreReader: ObjectStore + BlockReader {
    async fn get_tx_range(
        &self,
        block_number: u64,
        tx_hash: TxHash,
        byte_range: RangeInclusive<u32>,
    ) -> ReaderResult<Tx>;
    async fn get_tx_receipt_range(
        &self,
        block_number: u64,
        byte_range: RangeInclusive<u32>,
    ) -> ReaderResult<TxReceipt>;
    async fn get_tx_trace_range(
        &self,
        block_number: u64,
        byte_range: RangeInclusive<u32>,
    ) -> ReaderResult<TxTrace>;
}
