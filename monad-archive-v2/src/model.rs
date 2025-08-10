use alloy_primitives::{Address, TxHash};
use alloy_rpc_types::{Log, Topic};

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

pub struct EthGetLogsQuery {
    pub block_range: BlockRange,
    /// Set of addresses to filter logs for.
    pub address: Vec<Address>,
    /// Max 4 topics to filter logs for.
    pub topics: Vec<Topic>,
}

pub struct TxData {
    pub tx: Tx,
    pub receipt: TxReceipt,
    pub trace: TxTrace,
}

pub trait BlockReader: Send + Sync + 'static {
    async fn get_latest(&self) -> ReaderResult<u64>;

    async fn get_block_header(&self, block_number: u64) -> ReaderResult<Header>;
    async fn get_block(&self, block_number: u64) -> ReaderResult<Block>;
    async fn get_block_receipts(&self, block_number: u64) -> ReaderResult<BlockReceipts>;
    async fn get_block_traces(&self, block_number: u64) -> ReaderResult<BlockTraces>;
    async fn get_block_data(&self, block_number: u64) -> ReaderResult<BlockData>;

    async fn block_exists(&self, block_number: u64) -> ReaderResult<bool>;
    async fn list_blocks(&self, range: BlockRange) -> ReaderResult<Vec<u64>>;
}

pub trait Reader: BlockReader + Send + Sync + 'static {
    async fn get_tx(&self, tx_hash: TxHash) -> ReaderResult<Tx>;
    async fn get_tx_receipt(&self, tx_hash: TxHash) -> ReaderResult<TxReceipt>;
    async fn get_tx_trace(&self, tx_hash: TxHash) -> ReaderResult<TxTrace>;
    async fn get_tx_data(&self, tx_hash: TxHash) -> ReaderResult<TxData>;

    async fn tx_exists(&self, tx_hash: TxHash) -> ReaderResult<bool>;
}

pub trait EthGetLogsReader: Reader {
    async fn eth_logs(&self, query: EthGetLogsQuery) -> ReaderResult<Vec<Log>>;
}

pub trait Writer: Send + Sync + 'static {
    async fn update_latest(&self, block_number: u64) -> WriterResult;

    async fn write_block_data(&self, data: BlockData) -> WriterResult;
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

pub type ReaderResult<T> = Result<T, ReaderError>;
pub type WriterResult = Result<(), WriterError>;
