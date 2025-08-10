use alloy_rlp::Decodable;

use super::*;
use crate::{
    model::{make_block, BlockData, BlockReader},
    versioned::{Versioned, VersionedError},
    wire_reprs::{TxList, TxReceiptList, TxTraceList},
};

// Key prefixes
const BLOCK_HEADER_PREFIX: &str = "block_header/";
const BLOCK_RECEIPTS_PREFIX: &str = "block_receipts/";
const BLOCK_TRACES_PREFIX: &str = "block_traces/";
const BLOCK_BODY_PREFIX: &str = "block_body/";

// Const Keys
const LATEST_KEY: &str = "latest";

// Key functions
fn header_key(number: u64) -> String {
    format!("{BLOCK_HEADER_PREFIX}{number:20}")
}

fn receipts_key(number: u64) -> String {
    format!("{BLOCK_RECEIPTS_PREFIX}{number:20}")
}

fn traces_key(number: u64) -> String {
    format!("{BLOCK_TRACES_PREFIX}{number:20}")
}

fn body_key(number: u64) -> String {
    format!("{BLOCK_BODY_PREFIX}{number:20}")
}

impl<S: ObjectStore> BlockReader for S {
    async fn get_latest(&self) -> ReaderResult<u64> {
        let latest = self.get(LATEST_KEY).await?;

        let latest: &[u8] = latest.as_ref();
        let latest = str::from_utf8(latest).expect("latest key is not utf8");
        let latest = latest
            .parse::<u64>()
            .map_err(VersionedError::other("failed to parse latest key"))?;
        Ok(latest)
    }

    async fn get_block_header(&self, block_number: u64) -> ReaderResult<Header> {
        let buf = self.get(&header_key(block_number)).await?;
        let decoded = Header::decode(&mut buf.as_ref());
        decoded.map_err(ReaderError::rlp_decode("Header"))
    }

    async fn get_block(&self, block_number: u64) -> ReaderResult<Block> {
        let header = self.get_block_header(block_number).await?;
        let body_key = body_key(block_number);

        let txs_bytes = self.get(&body_key).await?;
        let txs = TxList::from_bytes(&txs_bytes)?;

        Ok(make_block(header, txs.0))
    }

    async fn get_block_receipts(&self, block_number: u64) -> ReaderResult<BlockReceipts> {
        let receipts_bytes = self.get(&receipts_key(block_number)).await?;
        let receipts = TxReceiptList::from_bytes(&receipts_bytes)?;
        Ok(receipts.0)
    }

    async fn get_block_traces(&self, block_number: u64) -> ReaderResult<BlockTraces> {
        let traces_bytes = self.get(&traces_key(block_number)).await?;
        let traces = TxTraceList::from_bytes(&traces_bytes)?;
        Ok(traces.0)
    }

    async fn get_block_data(&self, block_number: u64) -> ReaderResult<BlockData> {
        let (block, receipts, traces) = try_join!(
            self.get_block(block_number),
            self.get_block_receipts(block_number),
            self.get_block_traces(block_number),
        )?;
        Ok(BlockData {
            block,
            receipts,
            traces,
        })
    }

    async fn block_exists(&self, block_number: u64) -> ReaderResult<bool> {
        let key = header_key(block_number);
        Ok(self.exists(&key).await?)
    }

    async fn list_blocks(&self, range: BlockRange) -> ReaderResult<Vec<u64>> {
        self.list_range(BLOCK_HEADER_PREFIX, range)
            .await
            .map_err(Into::into)
    }
}

impl<S: ObjectStore> ObjectStoreReader for S {
    async fn get_tx_range(
        &self,
        block_number: u64,
        tx_hash: TxHash,
        byte_range: RangeInclusive<u32>,
    ) -> ReaderResult<Tx> {
        let tx_bytes = self.get_range(&body_key(block_number), byte_range).await?;
        // TODO: Figure out versioned decode for this
        let tx = Tx::decode(&mut tx_bytes.as_ref()) // fmt
            .map_err(ReaderError::rlp_decode("Tx"))?;
        if tx.tx.tx_hash() != &tx_hash {
            return Err(ReaderError::InvalidData(
                eyre::eyre!(
                    "tx hash mismatch: expected {:?}, got {:?}",
                    tx_hash,
                    tx.tx.tx_hash(),
                )
                .into(),
            ));
        }
        Ok(tx)
    }

    async fn get_tx_receipt_range(
        &self,
        block_number: u64,
        byte_range: RangeInclusive<u32>,
    ) -> ReaderResult<TxReceipt> {
        let receipts_bytes = self
            .get_range(&receipts_key(block_number), byte_range)
            .await?;
        // TODO: Figure out versioned decode for this
        TxReceipt::decode(&mut receipts_bytes.as_ref()) // fmt
            .map_err(ReaderError::rlp_decode("TxReceipt"))
    }

    async fn get_tx_trace_range(
        &self,
        block_number: u64,
        byte_range: RangeInclusive<u32>,
    ) -> ReaderResult<TxTrace> {
        let traces_bytes = self
            .get_range(&traces_key(block_number), byte_range)
            .await?;
        // TODO: Figure out versioned decode for this
        TxTrace::decode(&mut traces_bytes.as_ref()) // fmt
            .map_err(ReaderError::rlp_decode("TxTrace"))
    }
}
