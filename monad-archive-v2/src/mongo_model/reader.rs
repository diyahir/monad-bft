use mongodb::{
    bson::{self, doc, Document},
    Client,
};
use serde::de::DeserializeOwned;

use crate::{
    model::{make_block, BlockData, BlockReader, Tx},
    mongo_model::{MongoImplInternal, StorageMode},
    prelude::*,
    versioned::Versioned,
};

use super::{
    BlockNumber, BlockNumberDoc, HeaderDoc, InlineOrRef, LatestDoc, MongoImpl, TxDoc,
    TxDocBodyRxProj, TxDocBodyTraceProj,
};

impl<BR: BlockReader> MongoImpl<BR> {
    pub async fn new(
        uri: String,
        replica_name: String,
        block_reader: BR,
        max_retries: usize,
    ) -> Result<Self> {
        let client = Client::with_uri_str(uri)
            .await
            .wrap_err("Failed to connect to MongoDB")?;
        let db = client.database(&replica_name);

        let txs = db.collection("txs");
        let headers = db.collection("headers");
        // Document type is different but in same collection as headers
        let latest = headers.clone_with_type::<LatestDoc>();

        Ok(Self {
            inner: Arc::new(MongoImplInternal {
                client,
                db,
                replica_name,
                headers,
                latest,
                txs,
                block_reader,
                max_retries,
            }),
        })
    }

    async fn get_header_doc(&self, block_number: u64) -> ReaderResult<HeaderDoc> {
        self.headers
            .find_one(HeaderDoc::key(BlockNumber(block_number)))
            .await?
            .ok_or(ReaderError::BlockNotFound(block_number))
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

impl<BR: BlockReader> BlockReader for MongoImpl<BR> {
    async fn get_latest(&self) -> ReaderResult<Option<u64>> {
        let latest = self
            .db
            .collection::<LatestDoc>("latest")
            .find_one(LatestDoc::key())
            .await?;
        Ok(latest.map(|doc| doc.block_number.0))
    }

    async fn get_block_header(&self, block_number: u64) -> ReaderResult<Header> {
        let header = self.get_header_doc(block_number).await?;
        Ok(header.header_data)
    }

    async fn get_block(&self, block_number: u64) -> ReaderResult<Block> {
        // Load header first and validate ingest invariants
        let header = self.get_header_doc(block_number).await?;
        if header.storage_mode != StorageMode::Inline {
            // For now we only support inline path here
            return Err(ReaderError::InvalidData(eyre!(
                "block {} storage_mode is not inline",
                block_number
            )));
        }

        // Fetch tx bodies + receipts inline
        let txn_projs = self
            .get_tx_docs::<TxDocBodyRxProj>(block_number, Some(TxDocBodyRxProj::projection()))
            .await?;

        if txn_projs.len() as u32 != header.tx_count {
            return Err(ReaderError::InvalidData(eyre!(
                "block {} tx_count mismatch - header={} found={}",
                block_number,
                header.tx_count,
                txn_projs.len()
            )));
        }

        // Decode tx bodies - safe to assume inline after storage_mode check
        let transactions = txn_projs
            .iter()
            .map(|tx| {
                let bytes = tx.tx_key.expect_inline()?;
                Tx::from_bytes(&bytes).map_err(ReaderError::from)
            })
            .collect::<ReaderResult<Vec<_>>>()?;

        Ok(make_block(header.header_data, transactions))
    }

    async fn get_block_receipts(&self, block_number: u64) -> ReaderResult<BlockReceipts> {
        let txn_projs = self
            .get_tx_docs::<TxDocBodyRxProj>(block_number, Some(TxDocBodyRxProj::projection()))
            .await?;

        let num_refs = txn_projs
            .iter()
            .filter(|proj| matches!(proj.rx_key, InlineOrRef::Ref { .. }))
            .count();

        debug_assert!(
            num_refs == txn_projs.len() || num_refs == 0,
            "All txs must be inline or all must be refs, not a mix"
        );

        if num_refs == 0 {
            txn_projs
                .iter()
                .map(|proj| {
                    let buf = proj.rx_key.expect_inline()?;
                    TxReceipt::from_bytes(&buf).map_err(ReaderError::from)
                })
                .collect()
        } else if num_refs == txn_projs.len() {
            self.block_reader.get_block_receipts(block_number).await
        } else {
            Err(eyre!("All txs must be inline or all must be refs, not a mix").into())
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

impl<BR: BlockReader> Reader for MongoImpl<BR> {
    async fn get_tx(&self, tx_hash: TxHash) -> ReaderResult<crate::model::Tx> {
        let tx_docs = self.txs.clone_with_type::<TxDocBodyRxProj>();
        let tx_doc = tx_docs
            .find_one(TxDoc::key(tx_hash.into()))
            .projection(TxDocBodyRxProj::projection())
            .await?
            .ok_or(ReaderError::TxNotFound(tx_hash))?;
        let bytes = tx_doc.tx_key.expect_inline()?;
        Ok(Tx::from_bytes(&bytes)?)
    }

    async fn get_tx_receipt(&self, tx_hash: TxHash) -> ReaderResult<TxReceipt> {
        let tx_docs = self.txs.clone_with_type::<TxDocBodyRxProj>();
        let tx_doc = tx_docs
            .find_one(TxDoc::key(tx_hash.into()))
            .projection(TxDocBodyRxProj::projection())
            .await?
            .ok_or(ReaderError::TxNotFound(tx_hash))?;
        let bytes = tx_doc.rx_key.expect_inline()?;
        Ok(TxReceipt::from_bytes(&bytes)?)
    }

    async fn get_tx_trace(&self, tx_hash: TxHash) -> ReaderResult<TxTrace> {
        let tx_docs = self.txs.clone_with_type::<TxDocBodyTraceProj>();
        let tx_doc = tx_docs
            .find_one(TxDoc::key(tx_hash.into()))
            .projection(TxDocBodyTraceProj::projection())
            .await?
            .ok_or(ReaderError::TxNotFound(tx_hash))?;
        match tx_doc.trace_key {
            InlineOrRef::Inline(bytes) => Ok(bytes),
            InlineOrRef::Ref { .. } => {
                // TODO: use start and end to do a range s3 get
                let block_number = tx_doc.block_number.0;
                let tx_index = tx_doc.tx_index as usize;
                let mut traces = self.block_reader.get_block_traces(block_number).await?;
                let trace = traces
                    .get_mut(tx_index)
                    .ok_or(ReaderError::TxNotFound(tx_hash))?;
                Ok(std::mem::take(trace))
            }
        }
    }

    async fn get_tx_data(&self, tx_hash: TxHash) -> ReaderResult<TxData> {
        let Some(tx_doc) = self.txs.find_one(TxDoc::key(tx_hash.into())).await? else {
            return Err(ReaderError::TxNotFound(tx_hash));
        };

        // TODO: handle evicted txn bodies and rxs
        let tx_body = tx_doc.tx_key.expect_inline()?;
        let tx = Tx::from_bytes(&tx_body)?;
        let rx_buf = tx_doc.rx_key.expect_inline()?;
        let receipt = TxReceipt::from_bytes(&rx_buf)?;

        let trace = match tx_doc.trace_key {
            InlineOrRef::Inline(bytes) => bytes,
            InlineOrRef::Ref { .. } => {
                let block_number = tx_doc.block_number.0;
                let tx_index = tx_doc.tx_index as usize;
                let mut traces = self.block_reader.get_block_traces(block_number).await?;
                let trace = traces
                    .get_mut(tx_index)
                    .ok_or(ReaderError::TxNotFound(tx_hash))?;
                std::mem::take(trace)
            }
        };

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

    async fn eth_logs(&self, _query: EthGetLogsQuery) -> ReaderResult<Vec<alloy_rpc_types::Log>> {
        todo!("TODO: implement eth_logs")
    }
}
