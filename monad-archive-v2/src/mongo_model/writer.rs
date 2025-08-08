use crate::{model::BlockData, prelude::*};

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

    async fn write_block_data(&self, data: BlockData) -> WriterResult {
        // Write header + tx hashes to headers collection
        let BlockData {
            block,
            receipts,
            traces,
        } = data;
        let block_hash = block.header.hash_slow();
        let block_number = block.header.number;
        self.headers
            .replace_one(
                HeaderDoc::key(BlockNumber(block_number)),
                HeaderDoc {
                    block_number: BlockNumber(block_number),
                    block_hash: block_hash.into(),
                    tx_hashes: block
                        .body
                        .transactions()
                        .map(|tx| (*tx.tx.tx_hash()).into())
                        .collect(),
                    header_data: block.header,
                    evicted: false,
                },
            )
            .upsert(true)
            .await?;

        // Create tx docs for each tx, rx, and trace
        let tx_docs = block
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
                // Create log prefixes for each log
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

        // Create a bulk write model for each tx doc
        // Collect to short-circuit on error
        let models = tx_docs
            .map(|tx| {
                self.txs
                    .replace_one_model(TxDoc::key(tx.tx_hash), tx)
                    .map(|mut model| {
                        model.upsert = Some(true);
                        model
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;

        self.client.bulk_write(models).await?;

        Ok(())
    }
}
