// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use alloy_consensus::{Header as RlpHeader, Transaction as _};
use alloy_eips::BlockNumberOrTag;
use alloy_primitives::{Bloom, FixedBytes, U256};
use alloy_rpc_types::{
    Block, BlockTransactions, Filter, FilterBlockOption, FilteredParams, Header, Log, Transaction,
    TransactionReceipt,
};
use futures::{stream, StreamExt, TryStreamExt};
use itertools::{Either, Itertools};
use monad_archive::{
    model::BlockDataReader,
    prelude::{ArchiveReader, Context, ContextCompat, IndexReader, TxEnvelopeWithSender},
};
use monad_triedb_utils::triedb_env::{
    BlockHeader, BlockKey, FinalizedBlockKey, TransactionLocation, Triedb,
};
use monad_types::SeqNum;
use tracing::{debug, error, trace, warn};

use crate::{
    chainstate::buffer::{block_height_from_tag, ChainStateBuffer},
    eth_json_types::{BlockTagOrHash, BlockTags, FixedData, MonadLog, Quantity},
    handlers::eth::{
        block::block_receipts,
        txn::{parse_tx_receipt, FilterError},
    },
    jsonrpc::{ArchiveErrorExt, JsonRpcError, JsonRpcResult},
};

pub mod buffer;

#[derive(Clone)]
pub struct ChainState<T> {
    buffer: Option<Arc<ChainStateBuffer>>,
    triedb_env: T,
    archive_reader: Option<ArchiveReader>,
}

#[derive(Debug)]
pub enum ChainStateError {
    Triedb(String),
    Archive(String),
    ResourceNotFound,
}

impl From<monad_archive::prelude::Report> for ChainStateError {
    fn from(e: monad_archive::prelude::Report) -> Self {
        // Log with debug to get error chain, but return only top level error in response
        error!("Archive Error: {e:?}");
        ChainStateError::Archive(e.to_string())
    }
}

pub fn get_block_key_from_tag<T: Triedb>(triedb_env: &T, tag: BlockTags) -> BlockKey {
    match tag {
        BlockTags::Number(n) => triedb_env.get_block_key(SeqNum(n.0)),
        BlockTags::Latest => triedb_env.get_latest_voted_block_key(),
        BlockTags::Safe => triedb_env.get_latest_voted_block_key(),
        BlockTags::Finalized => BlockKey::Finalized(triedb_env.get_latest_finalized_block_key()),
    }
}

impl<T: Triedb> ChainState<T> {
    pub fn new(
        buffer: Option<Arc<ChainStateBuffer>>,
        triedb_env: T,
        archive_reader: Option<ArchiveReader>,
    ) -> Self {
        ChainState {
            buffer,
            triedb_env,
            archive_reader,
        }
    }

    pub fn get_latest_block_number(&self) -> u64 {
        if let Some(buffer) = &self.buffer {
            buffer.get_latest_safe_voted_block_num()
        } else {
            self.triedb_env.get_latest_voted_block_key().seq_num().0
        }
    }

    pub async fn get_transaction_receipt(
        &self,
        hash: [u8; 32],
    ) -> Result<TransactionReceipt, ChainStateError> {
        let latest_block_key = get_block_key_from_tag(&self.triedb_env, BlockTags::Latest);
        if let Some(TransactionLocation {
            tx_index,
            block_num,
        }) = self
            .triedb_env
            .get_transaction_location_by_hash(latest_block_key, hash)
            .await
            .map_err(ChainStateError::Triedb)?
        {
            let block_key = self.triedb_env.get_block_key(SeqNum(block_num));
            if let Some(receipt) =
                get_receipt_from_triedb(&self.triedb_env, block_key, tx_index).await?
            {
                return Ok(receipt);
            }
        }

        // try archive if transaction hash not found and archive reader specified
        if let Some(archive_reader) = &self.archive_reader {
            if let Some(tx_data) = archive_reader.get_tx_indexed_data(&hash.into()).await? {
                let receipt = crate::handlers::eth::txn::parse_tx_receipt(
                    tx_data.header_subset.base_fee_per_gas,
                    Some(tx_data.header_subset.block_timestamp),
                    tx_data.header_subset.block_hash,
                    tx_data.tx,
                    tx_data.header_subset.gas_used,
                    tx_data.receipt,
                    tx_data.header_subset.block_number,
                    tx_data.header_subset.tx_index,
                );

                return Ok(receipt);
            }
        }

        Err(ChainStateError::ResourceNotFound)
    }

    pub async fn get_transaction_with_block_and_index(
        &self,
        block: BlockTagOrHash,
        index: u64,
    ) -> Result<Transaction, ChainStateError> {
        match block {
            BlockTagOrHash::BlockTags(block) => {
                if let Some(buffer) = &self.buffer {
                    let height = block_height_from_tag(buffer, &block);
                    if let Some(tx) = buffer.get_transaction_by_location(height, index) {
                        return Ok(tx);
                    }
                }

                let block_key = get_block_key_from_tag(&self.triedb_env, block);
                if let Some(tx) =
                    get_transaction_from_triedb(&self.triedb_env, block_key, index).await?
                {
                    return Ok(tx);
                }

                // try archive if block header not found and archive reader specified
                if let (Some(archive_reader), BlockKey::Finalized(FinalizedBlockKey(block_num))) =
                    (&self.archive_reader, block_key)
                {
                    if let Some(block) = archive_reader.try_get_block_by_number(block_num.0).await?
                    {
                        if let Some(tx) = block.body.transactions.get(index as usize) {
                            return Ok(parse_tx_content(
                                block.header.hash_slow(),
                                block.header.number,
                                block.header.base_fee_per_gas,
                                tx.clone(),
                                index,
                            ));
                        }
                    }
                }
            }
            BlockTagOrHash::Hash(hash) => {
                if let Some(buffer) = &self.buffer {
                    if let Some(blk) = buffer.get_block_by_hash(&hash) {
                        if let Some(tx) =
                            buffer.get_transaction_by_location(blk.header.number, index)
                        {
                            return Ok(tx);
                        }
                    }
                }

                let latest_block_key = get_block_key_from_tag(&self.triedb_env, BlockTags::Latest);
                if let Some(block_num) = self
                    .triedb_env
                    .get_block_number_by_hash(latest_block_key, hash.0)
                    .await
                    .map_err(ChainStateError::Triedb)?
                {
                    let block_key = self.triedb_env.get_block_key(SeqNum(block_num));
                    if let Some(tx) =
                        get_transaction_from_triedb(&self.triedb_env, block_key, index).await?
                    {
                        return Ok(tx);
                    }
                }

                // try archive if block hash not found and archive reader specified
                if let Some(archive_reader) = &self.archive_reader {
                    if let Some(block) =
                        archive_reader.try_get_block_by_hash(&hash.0.into()).await?
                    {
                        if let Some(tx) = block.body.transactions.get(index as usize) {
                            return Ok(parse_tx_content(
                                hash.0.into(),
                                block.header.number,
                                block.header.base_fee_per_gas,
                                tx.clone(),
                                index,
                            ));
                        }
                    }
                }
            }
        }

        Err(ChainStateError::ResourceNotFound)
    }

    pub async fn get_transaction(&self, hash: [u8; 32]) -> Result<Transaction, ChainStateError> {
        if let Some(buffer) = &self.buffer {
            if let Some(tx) = buffer.get_transaction_by_hash(&FixedData(hash)) {
                return Ok(tx);
            }
        }

        let latest_block_key = get_block_key_from_tag(&self.triedb_env, BlockTags::Latest);
        if let Some(TransactionLocation {
            tx_index,
            block_num,
        }) = self
            .triedb_env
            .get_transaction_location_by_hash(latest_block_key, hash)
            .await
            .map_err(ChainStateError::Triedb)?
        {
            let block_key = self.triedb_env.get_block_key(SeqNum(block_num));
            if let Some(tx) =
                get_transaction_from_triedb(&self.triedb_env, block_key, tx_index).await?
            {
                return Ok(tx);
            };
        }

        // try archive if transaction hash not found and archive reader specified
        if let Some(archive_reader) = &self.archive_reader {
            if let Some((tx, header_subset)) = archive_reader.get_tx(&hash.into()).await? {
                return Ok(parse_tx_content(
                    header_subset.block_hash,
                    header_subset.block_number,
                    header_subset.base_fee_per_gas,
                    tx,
                    header_subset.tx_index,
                ));
            }
        }

        Err(ChainStateError::ResourceNotFound)
    }

    pub async fn get_block_header(
        &self,
        block: BlockTagOrHash,
    ) -> Result<alloy_consensus::Header, ChainStateError> {
        match &block {
            BlockTagOrHash::BlockTags(tag) => {
                if let Some(buffer) = &self.buffer {
                    let height = block_height_from_tag(buffer, tag);
                    if let Some(block) = buffer.get_block_by_height(height) {
                        return Ok(block.header.inner);
                    }
                }
            }
            BlockTagOrHash::Hash(hash) => {
                if let Some(buffer) = &self.buffer {
                    if let Some(block) = buffer.get_block_by_hash(hash) {
                        return Ok(block.header.inner);
                    }
                }
            }
        };

        let block_key = match &block {
            BlockTagOrHash::BlockTags(tag) => {
                Some(get_block_key_from_tag(&self.triedb_env, tag.clone()))
            }
            BlockTagOrHash::Hash(hash) => {
                let latest_block_key = get_block_key_from_tag(&self.triedb_env, BlockTags::Latest);

                self.triedb_env
                    .get_block_number_by_hash(latest_block_key, hash.0)
                    .await
                    .map_err(ChainStateError::Triedb)?
                    .map(|block_num| self.triedb_env.get_block_key(SeqNum(block_num)))
            }
        };

        if let Some(block_key) = block_key {
            if let Some(header) = self
                .triedb_env
                .get_block_header(block_key)
                .await
                .map_err(ChainStateError::Triedb)?
            {
                return Ok(header.header);
            }
        };

        if let Some(archive_reader) = &self.archive_reader {
            match block {
                BlockTagOrHash::BlockTags(BlockTags::Number(n)) => {
                    if let Some(block) = archive_reader.try_get_block_by_number(n.0).await? {
                        return Ok(block.header);
                    }
                }
                BlockTagOrHash::Hash(hash) => {
                    if let Some(block) = archive_reader
                        .try_get_block_by_hash(&FixedBytes(hash.0))
                        .await?
                    {
                        return Ok(block.header);
                    }
                }
                _ => {}
            }
        }

        Err(ChainStateError::ResourceNotFound)
    }

    pub async fn get_block(
        &self,
        block: BlockTagOrHash,
        return_full_txns: bool,
    ) -> Result<Block, ChainStateError> {
        if let Some(buffer) = &self.buffer {
            match &block {
                BlockTagOrHash::BlockTags(tag) => {
                    let height = block_height_from_tag(buffer, tag);
                    if let Some(mut block) = buffer.get_block_by_height(height) {
                        if !return_full_txns {
                            block.transactions = block.transactions.into_hashes();
                        }
                        return Ok(block);
                    }
                }
                BlockTagOrHash::Hash(hash) => {
                    if let Some(mut block) = buffer.get_block_by_hash(hash) {
                        if !return_full_txns {
                            block.transactions = block.transactions.into_hashes();
                        }
                        return Ok(block);
                    }
                }
            }
        }

        let block_key = match &block {
            BlockTagOrHash::BlockTags(tag) => {
                Some(get_block_key_from_tag(&self.triedb_env, tag.clone()))
            }
            BlockTagOrHash::Hash(hash) => {
                let latest_block_key = get_block_key_from_tag(&self.triedb_env, BlockTags::Latest);

                self.triedb_env
                    .get_block_number_by_hash(latest_block_key, hash.0)
                    .await
                    .map_err(ChainStateError::Triedb)?
                    .map(|block_num| self.triedb_env.get_block_key(SeqNum(block_num)))
            }
        };

        if let Some(block_key) = block_key {
            if let Some(header) = self
                .triedb_env
                .get_block_header(block_key)
                .await
                .map_err(ChainStateError::Triedb)?
            {
                if let Ok(transactions) = self.triedb_env.get_transactions(block_key).await {
                    return Ok(parse_block_content(
                        header.hash,
                        header.header,
                        transactions,
                        return_full_txns,
                    ));
                }
            }
        }

        if let Some(archive_reader) = &self.archive_reader {
            match block {
                BlockTagOrHash::BlockTags(BlockTags::Number(n)) => {
                    if let Some(block) = archive_reader.try_get_block_by_number(n.0).await? {
                        return Ok(parse_block_content(
                            block.header.hash_slow(),
                            block.header,
                            block.body.transactions,
                            return_full_txns,
                        ));
                    }
                }
                BlockTagOrHash::Hash(hash) => {
                    if let Some(block) = archive_reader
                        .try_get_block_by_hash(&FixedBytes(hash.0))
                        .await?
                    {
                        return Ok(parse_block_content(
                            block.header.hash_slow(),
                            block.header,
                            block.body.transactions,
                            return_full_txns,
                        ));
                    }
                }
                _ => {}
            }
        }

        Err(ChainStateError::ResourceNotFound)
    }

    /// Returns raw transaction receipts for a block.
    pub async fn get_raw_receipts(
        &self,
        block: BlockTags,
    ) -> Result<Vec<alloy_consensus::ReceiptEnvelope>, ChainStateError> {
        let block_key = get_block_key_from_tag(&self.triedb_env, block);
        if let Ok(receipts) = self.triedb_env.get_receipts(block_key).await {
            let receipts: Vec<alloy_consensus::ReceiptEnvelope> = receipts
                .into_iter()
                .map(|receipt_with_log_index| receipt_with_log_index.receipt)
                .collect();
            return Ok(receipts);
        };

        if let (Some(archive_reader), BlockKey::Finalized(FinalizedBlockKey(block_num))) =
            (&self.archive_reader, block_key)
        {
            if let Some(receipts) = archive_reader.try_get_block_receipts(block_num.0).await? {
                let receipts: Vec<alloy_consensus::ReceiptEnvelope> = receipts
                    .into_iter()
                    .map(|receipt_with_log_index| receipt_with_log_index.receipt)
                    .collect();
                return Ok(receipts);
            }
        }

        Err(ChainStateError::ResourceNotFound)
    }

    /// Returns transaction receipts mapped to their block and transaction info.
    pub async fn get_block_receipts(
        &self,
        block: BlockTagOrHash,
    ) -> Result<Vec<crate::eth_json_types::MonadTransactionReceipt>, ChainStateError> {
        if let Ok(block_key) = crate::handlers::eth::block::get_block_key_from_tag_or_hash(
            &self.triedb_env,
            block.clone(),
        )
        .await
        {
            if let Some(header) = self
                .triedb_env
                .get_block_header(block_key)
                .await
                .map_err(ChainStateError::Triedb)?
            {
                // if block header is present but transactions are not, the block is statesynced
                if let Ok(transactions) = self.triedb_env.get_transactions(block_key).await {
                    if let Ok(receipts) = self.triedb_env.get_receipts(block_key).await {
                        let block_receipts = crate::handlers::eth::block::map_block_receipts(
                            transactions,
                            receipts,
                            &header.header,
                            header.hash,
                            crate::eth_json_types::MonadTransactionReceipt,
                        )
                        .map_err(|_| ChainStateError::ResourceNotFound)?;
                        return Ok(block_receipts);
                    }
                }
            }
        }
        // try archive if header or transactions not found and archive reader specified
        if let Some(archive_reader) = &self.archive_reader {
            let block = match block {
                BlockTagOrHash::BlockTags(tag) => {
                    match get_block_key_from_tag(&self.triedb_env, tag) {
                        BlockKey::Finalized(FinalizedBlockKey(block_num)) => {
                            archive_reader.try_get_block_by_number(block_num.0).await?
                        }
                        BlockKey::Proposed(_) => None,
                    }
                }
                BlockTagOrHash::Hash(hash) => {
                    archive_reader.try_get_block_by_hash(&hash.0.into()).await?
                }
            };
            if let Some(block) = block {
                if let Some(receipts_with_log_index) = archive_reader
                    .try_get_block_receipts(block.header.number)
                    .await?
                {
                    let block_receipts = crate::handlers::eth::block::map_block_receipts(
                        block.body.transactions,
                        receipts_with_log_index,
                        &block.header,
                        block.header.hash_slow(),
                        crate::eth_json_types::MonadTransactionReceipt,
                    )
                    .map_err(|_| ChainStateError::ResourceNotFound)?;
                    return Ok(block_receipts);
                }
            }
        }

        Err(ChainStateError::ResourceNotFound)
    }

    pub async fn get_logs(
        &self,
        filter: Filter,
        max_block_range: u64,
        use_eth_get_logs_index: bool,
        dry_run_get_logs_index: bool,
        max_finalized_block_cache_len: u64,
    ) -> JsonRpcResult<Vec<MonadLog>> {
        let latest_block_number = self.get_latest_block_number();

        let (from_block, to_block) = match filter.block_option {
            FilterBlockOption::Range {
                from_block,
                to_block,
            } => {
                let into_block_tag = |block: Option<BlockNumberOrTag>| -> BlockTags {
                    match block {
                        None => BlockTags::default(),
                        Some(b) => match b {
                            BlockNumberOrTag::Number(q) => BlockTags::Number(Quantity(q)),
                            _ => BlockTags::Latest,
                        },
                    }
                };
                let from_block_tag = into_block_tag(from_block);
                let to_block_tag = into_block_tag(to_block);

                let from_block = get_block_key_from_tag(&self.triedb_env, from_block_tag);
                let to_block = get_block_key_from_tag(&self.triedb_env, to_block_tag);

                (
                    from_block.seq_num().0,
                    std::cmp::min(to_block.seq_num().0, latest_block_number),
                )
            }
            FilterBlockOption::AtBlockHash(block_hash) => {
                let latest_block_key = get_block_key_from_tag(&self.triedb_env, BlockTags::Latest);

                let block = self
                    .triedb_env
                    .get_block_number_by_hash(latest_block_key, block_hash.into())
                    .await
                    .map_err(|e| {
                        warn!("Error getting block number by hash: {e:?}");
                        JsonRpcError::internal_error("could not get block hash".to_string())
                    })?;

                let block_num = match block {
                    Some(block_num) => block_num,
                    None => {
                        // retry from archive reader if block hash not available in triedb
                        // TODO: This is ridiculously inefficient, we should be using the archive direct support for
                        //       eth_getLogs via block_hash instead
                        if let Some(archive_reader) = &self.archive_reader {
                            if let Some(block) =
                                archive_reader.try_get_block_by_hash(&block_hash).await?
                            {
                                block.header.number
                            } else {
                                return Ok(vec![]);
                            }
                        } else {
                            return Ok(vec![]);
                        }
                    }
                };

                (block_num, block_num)
            }
        };

        if from_block > to_block {
            return Err(FilterError::InvalidBlockRange.into());
        }
        if to_block - from_block > max_block_range {
            return Err(FilterError::RangeTooLarge.into());
        }

        // Only use index if no blocks are cached, otherwise use triedb + cache
        let to_block_outside_cache = to_block + max_finalized_block_cache_len < latest_block_number;
        // Determine if the request actually filters any logs.
        // We only want to use the index if the query constrains the result set.
        // This is the case when either:
        //  * at least one address is provided, or
        //  * at least one topic filter set is non‑empty (i.e. it contains a value to match on).
        let has_filters = !filter.address.is_empty() || filter.topics.iter().any(|t| !t.is_empty());

        if use_eth_get_logs_index
            && self.archive_reader.is_some()
            && to_block_outside_cache
            && has_filters
        {
            let archive_reader = self.archive_reader.as_ref().unwrap();
            trace!("Using eth_getLogs index");
            match get_logs_with_index(archive_reader, from_block, to_block, &filter).await {
                Ok(logs) => {
                    return Ok(logs.into_iter().map(MonadLog).collect());
                }
                Err(e) => {
                    debug!(
                    "Error getting logs with index. Falling back to unindexed method. Error: {e:?}"
                );
                }
            }
        }

        let address_filter = FilteredParams::address_filter(&filter.address);
        let topics_filter = FilteredParams::topics_filter(&filter.topics);

        let filter_match = |bloom: Bloom| -> bool {
            FilteredParams::matches_address(bloom, &address_filter)
                && FilteredParams::matches_topics(bloom, &topics_filter)
        };

        let filtered_params = FilteredParams::new(Some(filter.clone()));

        let block_range = from_block..=to_block;

        let triedb_stream = stream::iter(block_range)
            .map(|block_num| {
                let block_key = self.triedb_env.get_block_key(SeqNum(block_num));

                async move {
                    if let Some(header) = self
                        .triedb_env
                        .get_block_header(block_key)
                        .await
                        .map_err(JsonRpcError::internal_error)?
                    {
                        if filter_match(header.header.logs_bloom) {
                            // try fetching from triedb
                            if let Ok(transactions) =
                                self.triedb_env.get_transactions(block_key).await
                            {
                                let bloom_receipts = self
                                    .triedb_env
                                    .get_receipts(block_key)
                                    .await
                                    .map_err(JsonRpcError::internal_error)?;
                                // successfully fetched from triedb
                                Ok(Either::Left((header, transactions, bloom_receipts)))
                            } else {
                                // header exists but not transactions, block is statesynced
                                // pass block number to try for archive
                                Ok(Either::Right(block_num))
                            }
                        } else {
                            Ok(Either::Left((header, vec![], vec![])))
                        }
                    } else {
                        Ok(Either::Right(block_num)) // pass block number to try for archive
                    }
                }
            })
            .buffered(10);

        let data = triedb_stream
            .map(|result| {
                async move {
                    match result {
                        Ok(Either::Left(data)) => Ok(data), // successfully fetched from triedb
                        Ok(Either::Right(block_num)) => {
                            // fallback and try fetching from archive
                            if let Some(archive_reader) = &self.archive_reader {
                                fetch_from_archive(archive_reader, block_num, filter_match).await
                            } else {
                                Err(JsonRpcError::internal_error(
                                    "error getting block header from triedb and archive".into(),
                                ))
                            }
                        }
                        Err(err) => Err(err),
                    }
                }
            })
            .buffered(100)
            .try_collect::<Vec<_>>()
            .await?;

        let receipt_logs = data
            .iter()
            .map(|(header, transactions, bloom_receipts)| {
                block_receipts(
                    transactions.to_vec(),
                    bloom_receipts.to_vec(),
                    &header.header,
                    header.hash,
                )
            })
            .flatten_ok()
            .map_ok(|receipt| {
                let logs = match receipt.inner {
                    alloy_consensus::ReceiptEnvelope::Legacy(receipt_with_bloom)
                    | alloy_consensus::ReceiptEnvelope::Eip2930(receipt_with_bloom)
                    | alloy_consensus::ReceiptEnvelope::Eip1559(receipt_with_bloom)
                    | alloy_consensus::ReceiptEnvelope::Eip4844(receipt_with_bloom)
                    | alloy_consensus::ReceiptEnvelope::Eip7702(receipt_with_bloom) => {
                        receipt_with_bloom.receipt.logs
                    }
                    _ => unreachable!(),
                };

                logs.into_iter().filter(|log: &Log| {
                    !(filtered_params.filter.is_some()
                        && (!filtered_params.filter_address(&log.address())
                            || !filtered_params.filter_topics(log.topics())))
                })
            })
            .flatten_ok()
            .map_ok(MonadLog)
            .collect::<Result<Vec<_>, _>>()?;

        if dry_run_get_logs_index {
            let non_indexed =
                HashSet::from_iter(receipt_logs.iter().map(|monad_log| &monad_log.0).cloned());
            if let Some(archive_reader) = self.archive_reader.clone() {
                tokio::spawn(async move {
                    if let Err(e) = check_dry_run_get_logs_index(
                        archive_reader,
                        from_block,
                        to_block,
                        filter,
                        non_indexed,
                    )
                    .await
                    {
                        warn!("Error checking dry run get logs index: {e:?}");
                    }
                });
            }
        }

        Ok(receipt_logs)
    }
}

async fn fetch_from_archive(
    archive_reader: &ArchiveReader,
    block_num: u64,
    filter_match: impl Fn(Bloom) -> bool,
) -> JsonRpcResult<(
    BlockHeader,
    Vec<TxEnvelopeWithSender>,
    Vec<monad_archive::prelude::ReceiptWithLogIndex>,
)> {
    let block = archive_reader
        .get_block_by_number(block_num)
        .await
        .to_jsonrpc_error("Error getting block by number")?;

    if !filter_match(block.header.logs_bloom) {
        return Ok((
            BlockHeader {
                hash: block.header.hash_slow(),
                header: block.header,
            },
            vec![],
            vec![],
        ));
    }

    let bloom_receipts = archive_reader
        .get_block_receipts(block_num)
        .await
        .to_jsonrpc_error("Error getting block receipts")?;
    Ok((
        BlockHeader {
            hash: block.header.hash_slow(),
            header: block.header,
        },
        block.body.transactions,
        bloom_receipts,
    ))
}

async fn check_dry_run_get_logs_index(
    archive_reader: ArchiveReader,
    from_block: u64,
    to_block: u64,
    filter: Filter,
    non_indexed: HashSet<Log>,
) -> monad_archive::prelude::Result<()> {
    let indexed = get_logs_with_index(&archive_reader, from_block, to_block, &filter)
        .await
        .map(HashSet::from_iter)
        .wrap_err("Error getting logs with index")?;

    let group_by = |mut map: HashMap<_, _>, log: &Log| {
        let Some(block_number) = log.block_number else {
            return map;
        };
        let Some(transaction_hash) = log.transaction_hash else {
            return map;
        };
        map.entry(block_number)
            .or_insert_with(|| Vec::with_capacity(2))
            .push(transaction_hash.to_string());
        map
    };

    let non_indexed_only = non_indexed
        .difference(&indexed)
        .fold(HashMap::new(), group_by);
    let indexed_only = indexed
        .difference(&non_indexed)
        .fold(HashMap::new(), group_by);

    if non_indexed_only.is_empty() && indexed_only.is_empty() {
        debug!("Indexed and non-indexed logs are identical");
    } else {
        let non_indexed_only_json = serde_json::to_string(&non_indexed_only)?;
        let indexed_only_json = serde_json::to_string(&indexed_only)?;
        warn!(
            non_indexed_only = non_indexed_only_json,
            indexed_only = indexed_only_json,
            "Index and non-index logs are not identical"
        );
    }

    Ok(())
}

async fn get_logs_with_index(
    reader: &ArchiveReader,
    from_block: u64,
    to_block: u64,
    filter: &Filter,
) -> monad_archive::prelude::Result<Vec<Log>> {
    let log_index = reader
        .log_index
        .as_ref()
        .wrap_err("Log index reader not present")?;

    let latest_indexed_tx = reader
        .get_latest_indexed()
        .await?
        .wrap_err("Latest indexed tx not found")?;

    if latest_indexed_tx < to_block {
        monad_archive::prelude::bail!(
            "Latest indexed tx is less than to_block. {}, {}",
            latest_indexed_tx,
            to_block
        );
    }

    let filtered_params = FilteredParams::new(Some(filter.clone()));

    // Note: we an limit returned (and queried!) data by using `query_logs_index_streamed`
    // and take_while we're under the response size limit
    let potential_matches = log_index
        .query_logs(from_block, to_block, filter.address.iter(), &filter.topics)
        .await?;
    let potential_matches = potential_matches.try_collect::<Vec<_>>().await?;

    Ok(potential_matches
        .into_iter()
        .flat_map(|tx_data| {
            let receipt = parse_tx_receipt(
                tx_data.header_subset.base_fee_per_gas,
                Some(tx_data.header_subset.block_timestamp),
                tx_data.header_subset.block_hash,
                tx_data.tx,
                tx_data.header_subset.gas_used,
                tx_data.receipt,
                tx_data.header_subset.block_number,
                tx_data.header_subset.tx_index,
            );
            receipt
                .inner
                .logs()
                .iter()
                .filter(|log: &&Log| {
                    !(filtered_params.filter.is_some()
                        && (!filtered_params.filter_address(&log.address())
                            || !filtered_params.filter_topics(log.topics())))
                })
                .cloned()
                .collect::<Vec<_>>()
        })
        .collect())
}

fn parse_block_content(
    block_hash: FixedBytes<32>,
    header: RlpHeader,
    transactions: Vec<TxEnvelopeWithSender>,
    return_full_txns: bool,
) -> Block {
    // parse transactions
    let transactions = if return_full_txns {
        let txs = transactions
            .into_iter()
            .enumerate()
            .map(|(idx, tx)| {
                parse_tx_content(
                    block_hash,
                    header.number,
                    header.base_fee_per_gas,
                    tx,
                    idx as u64,
                )
            })
            .collect();

        BlockTransactions::Full(txs)
    } else {
        BlockTransactions::Hashes(transactions.iter().map(|tx| *tx.tx.tx_hash()).collect())
    };

    // NOTE: no withdrawals currently in monad-bft
    Block {
        header: Header {
            total_difficulty: Some(header.difficulty),
            hash: block_hash,
            size: Some(U256::from(header.size())),
            inner: header,
        },
        transactions,
        uncles: vec![],
        withdrawals: None,
    }
}

pub fn parse_tx_content(
    block_hash: FixedBytes<32>,
    block_number: u64,
    base_fee: Option<u64>,
    tx: TxEnvelopeWithSender,
    tx_index: u64,
) -> Transaction {
    // unpack transaction
    let sender = tx.sender;
    let tx = tx.tx;

    // effective gas price is calculated according to eth json rpc specification
    let effective_gas_price = tx.effective_gas_price(base_fee);

    Transaction {
        inner: tx,
        from: sender,
        block_hash: Some(block_hash),
        block_number: Some(block_number),
        effective_gas_price: Some(effective_gas_price),
        transaction_index: Some(tx_index),
    }
}

#[tracing::instrument(level = "debug")]
async fn get_transaction_from_triedb<T: Triedb>(
    triedb_env: &T,
    block_key: BlockKey,
    tx_index: u64,
) -> Result<Option<Transaction>, ChainStateError> {
    let header = match triedb_env
        .get_block_header(block_key)
        .await
        .map_err(ChainStateError::Triedb)?
    {
        Some(header) => header,
        None => return Ok(None),
    };

    match triedb_env
        .get_transaction(block_key, tx_index)
        .await
        .map_err(ChainStateError::Triedb)?
    {
        Some(tx) => Ok(Some(parse_tx_content(
            header.hash,
            header.header.number,
            header.header.base_fee_per_gas,
            tx,
            tx_index,
        ))),
        None => Ok(None),
    }
}

#[tracing::instrument(level = "debug")]
async fn get_receipt_from_triedb<T: Triedb>(
    triedb_env: &T,
    block_key: BlockKey,
    tx_index: u64,
) -> Result<Option<TransactionReceipt>, ChainStateError> {
    let header = match triedb_env
        .get_block_header(block_key)
        .await
        .map_err(ChainStateError::Triedb)?
    {
        Some(header) => header,
        None => return Ok(None),
    };

    let tx = match triedb_env
        .get_transaction(block_key, tx_index)
        .await
        .map_err(ChainStateError::Triedb)?
    {
        Some(tx) => tx,
        None => return Ok(None),
    };

    match triedb_env
        .get_receipt(block_key, tx_index)
        .await
        .map_err(ChainStateError::Triedb)?
    {
        Some(receipt) => {
            // Get the previous receipt's cumulative gas used to calculate gas used
            let gas_used = if tx_index > 0 {
                match triedb_env
                    .get_receipt(block_key, tx_index - 1)
                    .await
                    .map_err(ChainStateError::Triedb)?
                {
                    Some(prev_receipt) => {
                        receipt.receipt.cumulative_gas_used()
                            - prev_receipt.receipt.cumulative_gas_used()
                    }
                    None => return Err(ChainStateError::Triedb("error getting receipt".into())),
                }
            } else {
                receipt.receipt.cumulative_gas_used()
            };

            let receipt = crate::handlers::eth::txn::parse_tx_receipt(
                header.header.base_fee_per_gas,
                Some(header.header.timestamp),
                header.hash,
                tx,
                gas_used,
                receipt,
                block_key.seq_num().0,
                tx_index,
            );

            Ok(Some(receipt))
        }
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use alloy_eips::BlockNumberOrTag;
    use alloy_rpc_types::{Filter, FilterBlockOption};
    use monad_archive::{
        prelude::{ArchiveReader, BlockDataArchive, IndexReaderImpl, TxIndexArchiver},
        test_utils::{mock_block, mock_rx, mock_tx, MemoryStorage},
    };
    use monad_triedb_utils::mock_triedb::MockTriedb;

    use crate::{
        chainstate::ChainState,
        eth_json_types::{BlockTagOrHash, BlockTags, FixedData, Quantity},
    };

    #[tokio::test]
    async fn test_archive_fallback() {
        let mut mock_triedb = MockTriedb::default();
        mock_triedb.set_latest_block(1000);

        let primary = MemoryStorage::new("primary");
        let fallback = MemoryStorage::new("fallback");

        let primary_bdr = BlockDataArchive::new(primary.clone());
        let fallback_bdr = BlockDataArchive::new(fallback.clone());
        let primary = TxIndexArchiver::new(primary, primary_bdr.clone(), 1000);

        let tx = mock_tx(123);
        let block = mock_block(10, vec![tx.clone()]);
        let receipts = mock_rx(100, 10);

        primary_bdr.archive_block(block.clone()).await.unwrap();
        primary_bdr
            .archive_receipts(vec![receipts.clone()], 10)
            .await
            .unwrap();
        primary
            .index_block(
                mock_block(10, vec![tx.clone()]),
                vec![vec![]],
                vec![receipts.clone()],
                None,
            )
            .await
            .unwrap();

        let reader = ArchiveReader::new(primary_bdr.clone(), primary.reader, None, None)
            .with_fallback(
                Some(ArchiveReader::new(
                    fallback_bdr.clone(),
                    IndexReaderImpl::new(fallback.clone(), fallback_bdr),
                    None,
                    None,
                )),
                None,
                None,
            );

        let chain_state = ChainState::new(None, mock_triedb, Some(reader));

        let block_hash = block.header.hash_slow().0;

        let found = chain_state
            .get_block(BlockTagOrHash::Hash(FixedData(block_hash)), false)
            .await;
        assert!(found.is_ok());

        let found = chain_state
            .get_block(
                BlockTagOrHash::BlockTags(BlockTags::Number(Quantity(10))),
                false,
            )
            .await;
        assert!(found.is_ok());

        chain_state
            .get_block_header(BlockTagOrHash::Hash(FixedData(block_hash)))
            .await
            .unwrap();
        chain_state
            .get_block_header(BlockTagOrHash::BlockTags(BlockTags::Number(Quantity(10))))
            .await
            .unwrap();
        assert!(found.is_ok());

        chain_state
            .get_transaction(tx.tx.tx_hash().0)
            .await
            .unwrap();

        chain_state
            .get_transaction_receipt(tx.tx.tx_hash().0)
            .await
            .unwrap();

        chain_state
            .get_block_receipts(BlockTagOrHash::Hash(FixedData(block_hash)))
            .await
            .unwrap();

        chain_state
            .get_block_receipts(BlockTagOrHash::BlockTags(BlockTags::Number(Quantity(10))))
            .await
            .unwrap();

        chain_state
            .get_transaction_with_block_and_index(
                BlockTagOrHash::Hash(crate::eth_json_types::FixedData(block_hash)),
                0,
            )
            .await
            .unwrap();

        chain_state
            .get_transaction_with_block_and_index(
                BlockTagOrHash::BlockTags(BlockTags::Number(Quantity(10))),
                0,
            )
            .await
            .unwrap();

        chain_state
            .get_raw_receipts(BlockTags::Number(Quantity(10)))
            .await
            .unwrap();

        let filter = Filter {
            block_option: FilterBlockOption::Range {
                from_block: Some(BlockNumberOrTag::Number(10)),
                to_block: Some(BlockNumberOrTag::Number(10)),
            },
            ..Default::default()
        };
        let logs = chain_state
            .get_logs(filter, 1, false, false, 1)
            .await
            .unwrap();
        assert!(!logs.is_empty());

        let filter = Filter {
            block_option: FilterBlockOption::AtBlockHash(block_hash.into()),
            ..Default::default()
        };
        let logs = chain_state
            .get_logs(filter, 1, false, false, 1)
            .await
            .unwrap();
        assert!(!logs.is_empty());
    }
}
