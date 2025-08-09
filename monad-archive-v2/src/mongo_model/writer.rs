use std::future::IntoFuture;

use futures::FutureExt;
use mongodb::{
    options::{BulkWriteOptions, WriteModel},
    ClientSession,
};

use crate::{model::BlockData, mongo_model::mtransaction::with_mtransaction, prelude::*};

use super::*;

impl<BR: BlockReader> Writer for MongoImpl<BR> {
    async fn update_latest(&self, block_number: u64) -> WriterResult {
        self.db
            .collection::<LatestDoc>("latest")
            .replace_one(
                LatestDoc::key(),
                LatestDoc {
                    block_number: BlockNumber(block_number),
                },
            )
            .upsert(true)
            .await
            .map_err(|e| WriterError::NetworkError(e.into()))?;
        Ok(())
    }

    // UPDATED: atomic ingest with mtransaction + commit bit
    async fn write_block_data(&self, data: BlockData) -> WriterResult {
        let BlockData {
            block,
            receipts,
            traces,
        } = data;

        // Hard length checks to prevent silent truncation
        let tx_len = block.body.transactions.len();
        if receipts.len() != tx_len || traces.len() != tx_len {
            return Err(WriterError::EncodeError(eyre!(
                "tx_count mismatch - txs={} receipts={} traces={}",
                tx_len,
                receipts.len(),
                traces.len()
            )));
        }

        // Prebuild tx docs and bulk models outside the txn window
        let block_hash = block.header.hash_slow();
        let block_number = block.header.number;
        let tx_docs_iter = block
            .body
            .transactions()
            .zip(receipts)
            .zip(traces)
            .enumerate()
            .map(|(tx_index, ((tx, receipt), trace))| TxDoc {
                tx_index: tx_index as u32,
                tx_hash: (*tx.tx.tx_hash()).into(),
                block_number: BlockNumber(block_number),
                block_hash: block_hash.into(),
                tx_key: InlineOrRef::Inline(tx.to_bytes()),
                rx_key: InlineOrRef::Inline(receipt.to_bytes()),
                trace_key: InlineOrRef::Inline(trace),
                log_prefixes: receipt
                    .receipt
                    .logs()
                    .iter()
                    .map(|log| {
                        let topics = log.data.topics();
                        LogPrefixes {
                            address: hex_prefix(&log.address.0),
                            topic_0: hex_prefix(&topics[0]),
                            topic_1: topics.get(1).map(hex_prefix),
                            topic_2: topics.get(2).map(hex_prefix),
                            topic_3: topics.get(3).map(hex_prefix),
                        }
                    })
                    .collect(),
            });

        let tx_models: Vec<WriteModel> = tx_docs_iter
            .map(|tx| {
                self.txs
                    .replace_one_model(TxDoc::key(tx.tx_hash), tx)
                    .map(|mut m| {
                        m.upsert = Some(true);
                        m.into()
                    })
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| WriterError::EncodeError(e.into()))?;

        // Header to write only in the final chunk's transaction
        let header_doc = HeaderDoc {
            block_number: BlockNumber(block_number),
            block_hash: block_hash.into(),
            tx_hashes: block
                .body
                .transactions()
                .map(|tx| (*tx.tx.tx_hash()).into())
                .collect(),
            header_data: block.header,
            tx_count: tx_len as u32,
            storage_mode: StorageMode::Inline,
        };

        const CHUNK_SIZE: usize = 1000;
        const MAX_RETRIES: usize = 5;
        let total_chunks = (tx_models.len() + CHUNK_SIZE - 1) / CHUNK_SIZE;

        for i in 0..total_chunks {
            let start = i * CHUNK_SIZE;
            let end = (start + CHUNK_SIZE).min(tx_models.len());

            // OWN the models for this chunk so nothing borrows `tx_models`
            let models_chunk: Vec<_> = tx_models[start..end].to_vec();

            with_mtransaction(
                &self.client,
                Arc::new(models_chunk),
                MAX_RETRIES,
                move |session, models_chunk| {
                    Box::pin(async move {
                        session
                            .client()
                            // TODO: avoid materializing the models_chunk and
                            //       instead create it on-demand to avoid cloning on happy path
                            .bulk_write((*models_chunk).clone())
                            .ordered(false)
                            .session(&mut *session)
                            .await
                            .map(|_| ())
                    })
                },
            )
            .await
            .map_err(|e| WriterError::NetworkError(e.into()))?;
        }

        self.with_mtransaction(
            Arc::new(header_doc), // line break to avoid long closure
            move |session, model, header_doc| {
                Box::pin(async move {
                    model
                        .headers
                        .replace_one(header_doc.to_key(), header_doc)
                        .upsert(true)
                        .session(&mut *session)
                        .await?;
                    Ok::<_, mongodb::error::Error>(())
                })
            },
        )
        .await?;

        Ok(())
    }
}
