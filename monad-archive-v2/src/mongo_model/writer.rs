use futures::FutureExt;
use mongodb::{error::Error as MongoError, options::WriteModel, ClientSession};

use crate::{model::BlockData, prelude::*};

use super::*;

impl<BR: BlockReader> MongoImpl<BR> {
    async fn update_latest_with_session(
        &self,
        block_number: u64,
        sesh: Option<&mut ClientSession>,
    ) -> Result<(), MongoError> {
        let mut builder = self
            .latest
            .replace_one(
                LatestDoc::key(),
                LatestDoc {
                    block_number: BlockNumber(block_number),
                },
            )
            .upsert(true);

        if let Some(sesh) = sesh {
            builder = builder.session(sesh);
        }
        builder.await?;
        Ok(())
    }
}

impl<BR: BlockReader> Writer for MongoImpl<BR> {
    async fn update_latest(&self, block_number: u64) -> WriterResult {
        Ok(self.update_latest_with_session(block_number, None).await?)
    }

    async fn write_block_data(&self, data: BlockData) -> WriterResult {
        let BlockData {
            block,
            receipts,
            traces,
        } = data;

        // Hard length checks to prevent silent truncation
        let tx_len = block.body.transactions.len();
        if receipts.len() != tx_len || traces.len() != tx_len {
            return Err(WriterError::InconsistentTxRxTraceLengths {
                tx_len: tx_len as u32,
                rx_len: receipts.len() as u32,
                trace_len: traces.len() as u32,
            });
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
            .collect::<Result<Vec<_>, _>>()?;

        // Header to write only in the final chunk's transaction
        let header_doc = Arc::new(HeaderDoc {
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
        });

        const CHUNK_SIZE: usize = 1000;
        let total_chunks = (tx_models.len() + CHUNK_SIZE - 1) / CHUNK_SIZE;

        for i in 0..total_chunks {
            let start = i * CHUNK_SIZE;
            let end = (start + CHUNK_SIZE).min(tx_models.len());

            // OWN the models for this chunk so nothing borrows `tx_models`
            let chunk: Vec<_> = tx_models[start..end].to_vec();

            self.with_mtransaction(
                Arc::new(chunk), // fmt: line break
                move |sesh, _, chunk| {
                    async move {
                        sesh.client()
                            .bulk_write((*chunk).clone())
                            .ordered(false)
                            .session(&mut *sesh)
                            .await
                    }
                    .boxed()
                },
            )
            .await?;
        }

        self.with_mtransaction(
            header_doc, // fmt: line break
            |sesh, db, header_doc| {
                async move {
                    db.headers
                        .replace_one(header_doc.to_key(), header_doc)
                        .upsert(true)
                        .session(&mut *sesh)
                        .await?;

                    db.update_latest_with_session(block_number, Some(sesh))
                        .await
                }
                .boxed()
            },
        )
        .await?;

        Ok(())
    }
}
