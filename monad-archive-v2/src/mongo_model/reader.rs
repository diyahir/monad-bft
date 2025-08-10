use alloy_primitives::hex::ToHexExt;
use mongodb::bson::{self, doc, Document};
use serde::de::DeserializeOwned;

use crate::{
    errors::ROK,
    model::{make_block, BlockData, BlockReader},
    mongo_model::StorageMode,
    prelude::*,
    versioned::Versioned,
};

use super::{
    BlockNumber, BlockNumberDoc, HeaderDoc, InlineOrRef, LatestDoc, MongoImpl, TxDoc,
    TxDocBodyRxProj, TxDocBodyTraceProj,
};

impl<BR: ObjectStoreReader> MongoImpl<BR> {
    fn validate_inline_storage_mode<'a>(
        &self,
        header: &HeaderDoc,
        key_iter: impl ExactSizeIterator<Item = &'a InlineOrRef>,
    ) -> ReaderResult<()> {
        let num_docs = key_iter.len();
        let num_refs = key_iter
            .filter(|key| matches!(key, InlineOrRef::Ref { .. }))
            .count();

        if num_docs != header.tx_count as usize {
            return Err(ReaderError::InvalidData(
                eyre::eyre!(
                    "block {} tx_count mismatch - header={} docs={}",
                    header.block_number.0,
                    header.tx_count,
                    num_docs
                )
                .into(),
            ));
        }

        match header.storage_mode {
            StorageMode::Inline => {
                if num_refs != 0 {
                    return Err(ReaderError::InvalidData(
                        eyre::eyre!(
                            "block {} header storage_mode is inline but num_refs={}",
                            header.block_number.0,
                            num_refs
                        )
                        .into(),
                    ));
                }
            }
            StorageMode::Ref => {
                if num_refs != num_docs {
                    return Err(ReaderError::InvalidData(
                        eyre::eyre!(
                            "block {} header storage_mode is ref but num_refs={} != num_docs={}",
                            header.block_number.0,
                            num_refs,
                            num_docs
                        )
                        .into(),
                    ));
                }
            }
        }

        Ok(())
    }

    async fn get_header_doc(&self, block_number: u64) -> ReaderResult<HeaderDoc> {
        self.headers
            .find_one(HeaderDoc::key(BlockNumber(block_number)))
            .await?
            .ok_or_else(|| ReaderError::not_found(format!("header {}", block_number)))
    }

    async fn get_tx_docs<T: DeserializeOwned + Send + Sync>(
        &self,
        block_number: u64,
        projection: Option<bson::Document>,
    ) -> ReaderResult<Vec<T>> {
        let coll = self.txs.clone_with_type::<T>();
        let find = coll
            .find(BlockNumber(block_number).document())
            .sort(doc! { "tx_index": 1 });
        let find = match projection {
            Some(projection) => find.projection(projection),
            None => find,
        };
        find.await?
            .try_collect::<Vec<_>>()
            .await
            .map_err(ReaderError::from)
    }
}

impl<BR: ObjectStoreReader> BlockReader for MongoImpl<BR> {
    async fn get_latest(&self) -> ReaderResult<u64> {
        let latest = self
            .db
            .collection::<LatestDoc>("latest")
            .find_one(LatestDoc::key())
            .await?;
        match latest {
            Some(doc) => Ok(doc.block_number.0),
            None => Err(ReaderError::not_found("latest")),
        }
    }

    async fn get_block_header(&self, block_number: u64) -> ReaderResult<Header> {
        let header = self.get_header_doc(block_number).await?;
        Ok(header.header_data)
    }

    async fn get_block(&self, block_number: u64) -> ReaderResult<Block> {
        // Load header and validate ingest invariants
        let (header, txn_projs) = try_join!(
            self.get_header_doc(block_number),
            self.get_tx_docs::<TxDocBodyRxProj>(block_number, Some(TxDocBodyRxProj::projection()))
        )?;

        // Validate ingest invariants
        let key_iter = txn_projs.iter().map(|proj| &proj.tx_key);
        self.validate_inline_storage_mode(&header, key_iter)?;

        Ok(match header.storage_mode {
            StorageMode::Inline => {
                let transactions = txn_projs
                    .iter()
                    .map(|proj| {
                        let bytes = proj.tx_key.expect_inline()?;
                        Tx::from_bytes(&bytes).map_err(ReaderError::from)
                    })
                    .collect::<ReaderResult<Vec<_>>>()?;
                make_block(header.header_data, transactions)
            }
            StorageMode::Ref => {
                let block = self.block_reader.get_block(block_number).await?;
                block
            }
        })
    }

    async fn get_block_receipts(&self, block_number: u64) -> ReaderResult<BlockReceipts> {
        // Load header first and validate ingest invariants
        let (header, txn_projs) = try_join!(
            self.get_header_doc(block_number),
            self.get_tx_docs::<TxDocBodyRxProj>(block_number, Some(TxDocBodyRxProj::projection()))
        )?;

        let key_iter = txn_projs.iter().map(|proj| &proj.rx_key);
        self.validate_inline_storage_mode(&header, key_iter)?;

        match header.storage_mode {
            StorageMode::Inline => txn_projs
                .iter()
                .map(|proj| {
                    let buf = proj.rx_key.expect_inline()?;
                    TxReceipt::from_bytes(&buf).map_err(ReaderError::from)
                })
                .collect(),
            StorageMode::Ref => self.block_reader.get_block_receipts(block_number).await,
        }
    }

    async fn get_block_traces(&self, block_number: u64) -> ReaderResult<BlockTraces> {
        // TODO: optimize this to query the txs collection instead of the block reader
        self.block_reader.get_block_traces(block_number).await
    }

    async fn get_block_data(&self, block_number: u64) -> ReaderResult<BlockData> {
        // todo: optimize this to use a single query
        let (block, receipts, traces) = try_join!(
            self.get_block(block_number),
            self.get_block_receipts(block_number),
            self.get_block_traces(block_number)
        )?;

        Ok(BlockData {
            block,
            receipts,
            traces,
        })
    }

    async fn block_exists(&self, block_number: u64) -> ReaderResult<bool> {
        let headers_proj = self.headers.clone_with_type::<Document>();
        Ok(headers_proj
            .find_one(HeaderDoc::key(BlockNumber(block_number)))
            .projection(doc! { "_id": 1 })
            .await?
            .is_some())
    }

    async fn list_blocks(&self, range: BlockRange) -> ReaderResult<Vec<u64>> {
        let headers_proj = self.headers.clone_with_type::<BlockNumberDoc>();
        let block_numbers = headers_proj
            .find(doc! {
                "block_number": {
                    "$gte": *range.start() as i64,
                    "$lte": *range.end() as i64,
                },
            })
            .projection(doc! { "block_number": 1 })
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        Ok(block_numbers
            .into_iter()
            .map(|doc| doc.block_number.0)
            .collect())
    }
}

impl<BR: ObjectStoreReader> Reader for MongoImpl<BR> {
    async fn get_tx(&self, tx_hash: TxHash) -> ReaderResult<crate::model::Tx> {
        let tx_docs = self.txs.clone_with_type::<TxDocBodyRxProj>();
        let tx_doc = tx_docs
            .find_one(TxDoc::key(tx_hash.into()))
            .projection(TxDocBodyRxProj::projection())
            .await?
            .ok_or(ReaderError::not_found(tx_hash.encode_hex()))?;

        match tx_doc.tx_key {
            InlineOrRef::Inline(bytes) => Ok(Tx::from_bytes(&bytes)?),
            InlineOrRef::Ref(range) => {
                self.block_reader
                    .get_tx_range(tx_doc.block_number.0, tx_hash, range)
                    .await
            }
        }
    }

    async fn get_tx_receipt(&self, tx_hash: TxHash) -> ReaderResult<TxReceipt> {
        let tx_docs = self.txs.clone_with_type::<TxDocBodyRxProj>();
        let tx_doc = tx_docs
            .find_one(TxDoc::key(tx_hash.into()))
            .projection(TxDocBodyRxProj::projection())
            .await?
            .ok_or(ReaderError::not_found(tx_hash.encode_hex()))?;

        match tx_doc.rx_key {
            InlineOrRef::Inline(bytes) => Ok(TxReceipt::from_bytes(&bytes)?),
            InlineOrRef::Ref(range) => {
                self.block_reader
                    .get_tx_receipt_range(tx_doc.block_number.0, range)
                    .await
            }
        }
    }

    async fn get_tx_trace(&self, tx_hash: TxHash) -> ReaderResult<TxTrace> {
        let tx_docs = self.txs.clone_with_type::<TxDocBodyTraceProj>();
        let tx_doc = tx_docs
            .find_one(TxDoc::key(tx_hash.into()))
            .projection(TxDocBodyTraceProj::projection())
            .await?
            .ok_or(ReaderError::not_found(tx_hash.encode_hex()))?;

        match tx_doc.trace_key {
            InlineOrRef::Inline(bytes) => Ok(bytes),
            InlineOrRef::Ref(range) => {
                self.block_reader
                    .get_tx_trace_range(tx_doc.block_number.0, range)
                    .await
            }
        }
    }

    async fn get_tx_data(&self, tx_hash: TxHash) -> ReaderResult<TxData> {
        let Some(tx_doc) = self.txs.find_one(TxDoc::key(tx_hash.into())).await? else {
            return Err(ReaderError::not_found(tx_hash.encode_hex()));
        };

        let block_number = tx_doc.block_number.0;

        let tx = async {
            ROK(match tx_doc.tx_key {
                InlineOrRef::Inline(bytes) => Tx::from_bytes(&bytes)?,
                InlineOrRef::Ref(range) => {
                    self.block_reader
                        .get_tx_range(block_number, tx_hash, range)
                        .await?
                }
            })
        };

        let receipt = async {
            ROK(match tx_doc.rx_key {
                InlineOrRef::Inline(bytes) => TxReceipt::from_bytes(&bytes)?,
                InlineOrRef::Ref(range) => {
                    self.block_reader
                        .get_tx_receipt_range(block_number, range)
                        .await?
                }
            })
        };

        let trace = async {
            ROK(match tx_doc.trace_key {
                InlineOrRef::Inline(bytes) => bytes,
                InlineOrRef::Ref(range) => {
                    self.block_reader
                        .get_tx_trace_range(block_number, range)
                        .await?
                }
            })
        };

        let (tx, receipt, trace) = try_join!(tx, receipt, trace)?;

        Ok(TxData { tx, receipt, trace })
    }

    async fn tx_exists(&self, tx_hash: TxHash) -> ReaderResult<bool> {
        let id_proj = self.txs.clone_with_type::<Document>();
        Ok(id_proj
            .find_one(TxDoc::key(tx_hash.into()))
            .projection(doc! { "_id": 1 })
            .await?
            .is_some())
    }
}
