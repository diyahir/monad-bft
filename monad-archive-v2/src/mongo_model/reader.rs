use std::{
    future::IntoFuture,
    ops::{Deref, DerefMut},
};

use alloy_primitives::{
    hex::{FromHex, ToHexExt},
    FixedBytes,
};
use alloy_rlp::{Decodable, Encodable};
use aws_sdk_dynamodb::types::builders::DeleteReplicationGroupMemberActionBuilder;
use mongodb::{
    bson::{self, doc, Bson, Document},
    options::ReplaceOneModel,
    Client, Collection, Database,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    model::{make_block, BlockData, BlockReader, Tx, Versioned},
    prelude::*,
};

use super::{
    BlockNumber, BlockNumberDoc, HeaderDoc, InlineOrRef, LatestDoc, TxDoc, TxDocBodyRxProj,
    TxDocBodyTraceProj, MongoImpl,
};

impl<BR: BlockReader> MongoImpl<BR> {
    pub async fn new(uri: String, replica_name: String, block_reader: BR) -> Result<Self> {
        let client = Client::with_uri_str(uri)
            .await
            .wrap_err("Failed to connect to MongoDB")?;
        let db = client.database(&replica_name);
        let headers = db.collection("headers");
        let txs = db.collection("txs");

        Ok(Self {
            client,
            db,
            replica_name,
            headers,
            txs,
            block_reader,
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
        let header = self.get_header_doc(block_number);
        let txn_projs =
            self.get_tx_docs::<TxDocBodyRxProj>(block_number, Some(TxDocBodyRxProj::projection()));

        let (header, txn_projs) = try_join!(header, txn_projs)?;

        if header.evicted {
            todo!("Haven't implemented evicted blocks yet")
        }

        // We can assume that the txs are not evicted because we checked the header
        // This means they are inline
        let transactions = txn_projs
            .iter()
            .map(|tx| {
                let bytes = tx.tx_key.expect_inline()?;
                Tx::from_bytes(&bytes)
            })
            .collect::<Result<Vec<_>, _>>()?;

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
