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

use std::collections::{BTreeSet, HashMap};

use eyre::{Context, Result};
use futures::future::try_join_all;
use monad_archive::{cli::ArchiveArgs, prelude::*};
use serde::{Deserialize, Serialize};

// Constants for S3 key prefixes defining the storage organization
pub const LATEST_CHECKED_PREFIX: &str = "latest_checked";
pub const FAULTS_CHUNK_PREFIX: &str = "faults_chunk";
pub const GOOD_BLOCKS_PREFIX: &str = "good_blocks";
pub const REPLICAS_KEY: &str = "replicas/list";

/// Generates S3 key for tracking the latest block checked per replica
pub fn latest_checked_key(replica: &str) -> String {
    format!("{}/{}", LATEST_CHECKED_PREFIX, replica)
}

/// Generates S3 key for storing faults for a specific replica and block range
pub fn faults_chunk_key(replica: &str, starting_block_num: u64) -> String {
    format!("{}/{}/{}", FAULTS_CHUNK_PREFIX, replica, starting_block_num)
}

/// Generates S3 key for storing valid block references for a block range
pub fn good_blocks_key(starting_block_num: u64) -> String {
    format!("{}/{}", GOOD_BLOCKS_PREFIX, starting_block_num)
}

/// Main model for the archive checker system
///
/// Manages access to S3 storage and block data readers for each replica
#[derive(Clone)]
pub struct CheckerModel {
    pub store: KVStoreErased,
    pub block_data_readers: Arc<HashMap<String, BlockDataArchive>>,
}

impl CheckerModel {
    /// Creates a new checker model with all configured replicas
    pub async fn new(
        store: impl Into<KVStoreErased>,
        metrics: &Metrics,
        init_replicas: Option<HashSet<ArchiveArgs>>,
    ) -> Result<Self> {
        let store = store.into();
        let block_data_readers =
            Arc::new(Self::load_replicas(&store, metrics, init_replicas).await?);
        Ok(Self {
            store,
            block_data_readers,
        })
    }

    /// Stores replica configuration in S3
    pub async fn set_replica_args(
        s3: &impl KVStore,
        replica_args: &HashSet<ArchiveArgs>,
    ) -> Result<()> {
        // Serialize the replica args list
        let data =
            serde_json::to_vec(&replica_args).wrap_err("Failed to serialize replica list")?;

        // Store in S3
        s3.put(REPLICAS_KEY, data)
            .await
            .wrap_err("Failed to store replica list in S3")
    }

    /// Retrieves replica configuration from S3
    async fn get_replica_args(s3: &impl KVStore) -> Result<HashSet<ArchiveArgs>> {
        match s3.get(REPLICAS_KEY).await? {
            Some(data) => {
                serde_json::from_slice(&data).wrap_err("Failed to deserialize replica list")
            }
            None => Ok(HashSet::new()),
        }
    }

    /// Initializes and loads all configured replicas
    async fn load_replicas(
        s3: &impl KVStore,
        metrics: &Metrics,
        init_replicas: Option<HashSet<ArchiveArgs>>,
    ) -> Result<HashMap<String, BlockDataArchive>> {
        let mut readers = HashMap::new();

        // Get the list of replicas from S3
        let mut replica_args = Self::get_replica_args(s3).await?;

        // Initialize replica args bucket
        // Handle errors cases
        if let Some(init_replicas) = init_replicas {
            if replica_args.is_empty() {
                Self::set_replica_args(s3, &init_replicas).await?;
                replica_args = init_replicas;
            } else if init_replicas != replica_args {
                bail!("s3 replicas is non-empty and does not match init replica arg");
            } else if init_replicas.is_empty() {
                bail!(
                    "Init replicas set, no s3 replicas pre-configured but init replicas is empty"
                );
            } else {
                info!("Init replicas set, but already initialized. You can remove that cli arg");
            }
        }

        // Create a reader for each replica
        for args in replica_args {
            let replica_name = args.replica_name();
            let reader = args.build_block_data_archive(metrics).await?;
            readers.insert(replica_name, reader);
        }

        Ok(readers)
    }

    /// Returns the minimum latest checked block across all replicas
    pub async fn min_latest_checked(&self) -> Result<u64> {
        let all_latest_checkeds = try_join_all(
            self.block_data_readers
                .keys()
                .map(|key| self.get_latest_checked_for_replica(key)),
        )
        .await?;

        all_latest_checkeds
            .into_iter()
            .min()
            .context("There must be at least 1 replica")
    }

    /// Returns the latest block available for checking
    pub async fn latest_to_check(&self) -> Result<u64> {
        let latest_per_replica =
            try_join_all(self.block_data_readers.values().map(|archive| async {
                archive
                    .get_latest(LatestKind::Uploaded)
                    .await
                    .map(|latest| latest.unwrap_or(0) - 1000)
            }))
            .await?;

        Ok(latest_per_replica
            .into_iter()
            .max()
            .expect("Checker has at least one replica"))
    }

    /// Gets the latest checked block for a specific replica
    pub async fn get_latest_checked_for_replica(&self, replica: &str) -> Result<u64> {
        let key = latest_checked_key(replica);
        match self.store.get(&key).await? {
            Some(data) => {
                let block_num = String::from_utf8(data.to_vec())?
                    .parse::<u64>()
                    .wrap_err("Failed to parse block number")?;
                Ok(block_num)
            }
            None => Ok(0), // Default to 0 if not found
        }
    }

    /// Updates the latest checked block for a specific replica
    pub async fn set_latest_checked_for_replica(
        &self,
        replica: &str,
        block_num: u64,
    ) -> Result<()> {
        let key = latest_checked_key(replica);
        self.store
            .put(&key, block_num.to_string().into_bytes())
            .await
            .wrap_err("Failed to set latest checked for replica")?;
        Ok(())
    }

    pub async fn find_chunk_starts_with_faults_by_replica(
        &self,
        replica: &str,
        start: Option<u64>,
        end: Option<u64>,
    ) -> Result<BTreeSet<u64>> {
        // Calculate the common prefix for optimization if possible
        let key_prefix = format!("{}/{}", FAULTS_CHUNK_PREFIX, replica);
        let keys = self.store.scan_prefix(&key_prefix).await?;

        let chunks = keys
            .into_iter()
            .map(|key| {
                key.split('/')
                    .nth(2)
                    .unwrap()
                    .parse::<u64>()
                    .wrap_err("Failed to parse chunk start")
            })
            .collect::<Result<Vec<u64>>>()?
            .into_iter()
            .filter(|chunk_start| {
                // A chunk starting at position X contains blocks from X to X+CHUNK_SIZE-1
                // Include the chunk if it could contain any blocks in the range [start, end]
                use crate::CHUNK_SIZE;

                if let Some(end) = end {
                    // Chunk should start before or at the end block
                    if chunk_start > &end {
                        return false;
                    }
                }

                if let Some(start) = start {
                    // Chunk should contain at least one block >= start
                    // This means chunk_start + CHUNK_SIZE > start
                    if chunk_start + CHUNK_SIZE <= start {
                        return false;
                    }
                }

                true
            })
            .collect::<BTreeSet<u64>>();

        Ok(chunks)
    }

    pub async fn find_chunk_starts_with_faults(&self) -> Result<BTreeSet<u64>> {
        let keys = self.store.scan_prefix(FAULTS_CHUNK_PREFIX).await?;
        let chunks = keys
            .into_iter()
            .map(|key| {
                key.split('/')
                    .nth(2)
                    .unwrap()
                    .parse::<u64>()
                    .wrap_err("Failed to parse chunk start")
            })
            .collect::<Result<BTreeSet<u64>>>()?;
        Ok(chunks)
    }

    /// Retrieves faults for a specific replica and block range
    pub async fn get_faults_chunk(
        &self,
        replica: &str,
        starting_block_num: u64,
    ) -> Result<Vec<Fault>> {
        let key = faults_chunk_key(replica, starting_block_num);
        match self.store.get(&key).await? {
            Some(data) => {
                let faults: Vec<Fault> =
                    serde_json::from_slice(&data).wrap_err("Failed to deserialize faults")?;
                Ok(faults)
            }
            None => Ok(Vec::new()), // Return empty vec if not found
        }
    }

    pub async fn get_faults_chunks_all_replicas(
        &self,
        starting_block_num: u64,
    ) -> Result<HashMap<String, Vec<Fault>>> {
        let mut handles = Vec::new();
        for replica in self.block_data_readers.keys() {
            let replica = replica.clone();
            let model = self.clone();
            handles.push(tokio::spawn(async move {
                model.get_faults_chunk(&replica, starting_block_num).await
            }));
        }
        let results = try_join_all(handles).await?;
        self.block_data_readers
            .keys()
            .zip(results)
            .map(|(replica, result)| Ok((replica.clone(), result?)))
            .collect()
    }

    /// Retrieves faults object for a specific replica and block range
    pub async fn delete_faults_chunk(&self, replica: &str, starting_block_num: u64) -> Result<()> {
        let key = faults_chunk_key(replica, starting_block_num);
        self.store.delete(&key).await
    }

    /// Stores faults for a specific replica and block range
    pub async fn set_faults_chunk(
        &self,
        replica: &str,
        starting_block_num: u64,
        faults: Vec<Fault>,
    ) -> Result<()> {
        let key = faults_chunk_key(replica, starting_block_num);
        let data = serde_json::to_vec(&faults).wrap_err("Failed to serialize faults")?;

        self.store
            .put(&key, data)
            .await
            .wrap_err("Failed to set faults chunk")
    }

    /// Retrieves the good blocks reference for a block range
    pub async fn get_good_blocks(&self, starting_block_num: u64) -> Result<GoodBlocks> {
        let key = good_blocks_key(starting_block_num);
        match self.store.get(&key).await? {
            Some(data) => {
                serde_json::from_slice(&data).wrap_err("Failed to deserialize good blocks")
            }
            // Return empty GoodBlocks if not found
            None => Ok(GoodBlocks {
                block_num_to_replica: HashMap::new(),
            }),
        }
    }

    /// Stores the good blocks reference for a block range
    pub async fn set_good_blocks(
        &self,
        starting_block_num: u64,
        good_blocks: GoodBlocks,
    ) -> Result<()> {
        let key = good_blocks_key(starting_block_num);
        let data = serde_json::to_vec(&good_blocks).wrap_err("Failed to serialize good blocks")?;

        self.store
            .put(&key, data)
            .await
            .wrap_err("Failed to set good blocks")?;
        Ok(())
    }

    /// Fetches block data from a specific replica for a single block
    pub async fn fetch_block_data_for_replica(
        &self,
        block_num: u64,
        replica_name: &str,
    ) -> Option<(Block, BlockReceipts, BlockTraces)> {
        let reader = self.block_data_readers.get(replica_name).unwrap();
        // Try to get all the data for this block from this replica
        let block_result = try_join!(
            reader.try_get_block_by_number(block_num),
            reader.try_get_block_receipts(block_num),
            reader.try_get_block_traces(block_num)
        );

        // Return the result (or None if any part failed)
        match block_result {
            Ok((Some(block), Some(receipts), Some(traces))) => Some((block, receipts, traces)),
            Ok(_) => None,
            Err(e) => {
                info!(
                    "Failed to fetch block {} from {}: {}",
                    block_num, replica_name, e
                );
                None
            }
        }
    }
}
/// Types of faults that can be detected in blocks
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub enum FaultKind {
    /// Block data is missing from the replica
    MissingBlock,
    /// Block number in the header doesn't match expected block number
    InvalidBlockNumber { expected: u64, actual: u64 },
    /// Number of receipts doesn't match transaction count
    ReceiptCountMismatch {
        tx_count: usize,
        receipt_count: usize,
    },
    /// Number of traces doesn't match transaction count
    TraceCountMismatch { tx_count: usize, trace_count: usize },
    /// Block data is inconsistent with the majority of replicas
    InconsistentBlock(InconsistentBlockReason),
    // Additional fault types can be added here
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InconsistentBlockReason {
    Header,
    /// Parent hash doesn't match the hash of the parent block
    InvalidParentHash,
    /// Transaction root in header doesn't match calculated root
    InvalidTransactionRoot,
    /// Receipts root in header doesn't match calculated root
    InvalidReceiptsRoot,
    BodyContents,
    BodyLen,
    ReceiptsContents,
    ReceiptsLen,
    TracesContents,
    TracesOutputDiffers,
    TracesLen,
    Unknown,
    /// No consensus could be reached among replicas
    NoConsensus,
}

impl InconsistentBlockReason {
    pub fn metric_name(&self) -> &'static str {
        match self {
            InconsistentBlockReason::Header => "inconsistent_header",
            InconsistentBlockReason::InvalidParentHash => "invalid_parent_hash",
            InconsistentBlockReason::InvalidTransactionRoot => "invalid_transaction_root",
            InconsistentBlockReason::InvalidReceiptsRoot => "invalid_receipts_root",
            InconsistentBlockReason::BodyContents => "inconsistent_body_contents",
            InconsistentBlockReason::BodyLen => "inconsistent_body_len",
            InconsistentBlockReason::ReceiptsContents => "inconsistent_receipts_contents",
            InconsistentBlockReason::ReceiptsLen => "inconsistent_receipts_len",
            InconsistentBlockReason::TracesContents => "inconsistent_traces_contents",
            InconsistentBlockReason::TracesOutputDiffers => "inconsistent_traces_output_differs",
            InconsistentBlockReason::TracesLen => "inconsistent_traces_len",
            InconsistentBlockReason::Unknown => "unknown",
            InconsistentBlockReason::NoConsensus => "no_consensus",
        }
    }
}

impl std::fmt::Display for InconsistentBlockReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "InconsistentBlockReason: {}",
            match self {
                InconsistentBlockReason::Header => "Header",
                InconsistentBlockReason::InvalidParentHash => "Invalid Parent Hash",
                InconsistentBlockReason::InvalidTransactionRoot => "Invalid Transaction Root",
                InconsistentBlockReason::InvalidReceiptsRoot => "Invalid Receipts Root",
                InconsistentBlockReason::BodyContents => "Body Contents",
                InconsistentBlockReason::BodyLen => "Body Length",
                InconsistentBlockReason::ReceiptsContents => "Receipts Contents",
                InconsistentBlockReason::ReceiptsLen => "Receipts Length",
                InconsistentBlockReason::TracesContents => "Traces Contents",
                InconsistentBlockReason::TracesOutputDiffers => "Traces Output Differs",
                InconsistentBlockReason::TracesLen => "Traces Length",
                InconsistentBlockReason::Unknown => "Unknown",
                InconsistentBlockReason::NoConsensus => "No Consensus",
            }
        )
    }
}

impl FaultKind {
    pub fn variant_name(&self) -> &'static str {
        match self {
            FaultKind::MissingBlock => "MissingBlock",
            FaultKind::InvalidBlockNumber { .. } => "InvalidBlockNumber",
            FaultKind::ReceiptCountMismatch { .. } => "ReceiptCountMismatch",
            FaultKind::TraceCountMismatch { .. } => "TraceCountMismatch",
            FaultKind::InconsistentBlock(_) => "InconsistentBlock",
        }
    }

    pub fn metric_name(&self) -> &'static str {
        match self {
            FaultKind::MissingBlock => "missing_block",
            FaultKind::InvalidBlockNumber { .. } => "invalid_block_number",
            FaultKind::ReceiptCountMismatch { .. } => "receipt_count_mismatch",
            FaultKind::TraceCountMismatch { .. } => "trace_count_mismatch",
            FaultKind::InconsistentBlock(reason) => reason.metric_name(),
        }
    }
}

/// Represents a fault detected in a specific block from a specific replica
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Fault {
    pub block_num: u64,
    pub replica: String,
    pub fault: FaultKind,
}

/// Maps block numbers to the replica with the canonical version
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct GoodBlocks {
    pub block_num_to_replica: HashMap<u64, String>,
}

#[cfg(test)]
mod tests {
    use monad_archive::kvstore::memory::MemoryStorage;

    use super::*;

    async fn setup_test_model() -> CheckerModel {
        let store: KVStoreErased = MemoryStorage::new("test-store").into();
        let block_data_readers = Arc::new(HashMap::new());
        CheckerModel {
            store,
            block_data_readers,
        }
    }

    #[tokio::test]
    async fn test_find_chunk_starts_with_faults_empty() {
        let model = setup_test_model().await;

        let result = model.find_chunk_starts_with_faults().await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_find_chunk_starts_with_faults_single_replica() {
        let model = setup_test_model().await;

        // Add some fault chunks for a single replica
        let faults = vec![
            Fault {
                block_num: 100,
                replica: "replica1".to_string(),
                fault: FaultKind::MissingBlock,
            },
            Fault {
                block_num: 101,
                replica: "replica1".to_string(),
                fault: FaultKind::MissingBlock,
            },
        ];

        model
            .set_faults_chunk("replica1", 100, faults)
            .await
            .unwrap();

        let result = model.find_chunk_starts_with_faults().await.unwrap();
        assert_eq!(result.len(), 1);
        assert!(result.contains(&100));
    }

    #[tokio::test]
    async fn test_find_chunk_starts_with_faults_multiple_replicas() {
        let model = setup_test_model().await;

        // Add fault chunks for multiple replicas with overlapping and non-overlapping chunks
        let faults1 = vec![Fault {
            block_num: 100,
            replica: "replica1".to_string(),
            fault: FaultKind::MissingBlock,
        }];
        let faults2 = vec![Fault {
            block_num: 200,
            replica: "replica2".to_string(),
            fault: FaultKind::MissingBlock,
        }];
        let faults3 = vec![Fault {
            block_num: 100,
            replica: "replica3".to_string(),
            fault: FaultKind::InconsistentBlock(InconsistentBlockReason::Header),
        }];

        model
            .set_faults_chunk("replica1", 100, faults1)
            .await
            .unwrap();
        model
            .set_faults_chunk("replica2", 200, faults2)
            .await
            .unwrap();
        model
            .set_faults_chunk("replica3", 100, faults3)
            .await
            .unwrap();

        let result = model.find_chunk_starts_with_faults().await.unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.contains(&100));
        assert!(result.contains(&200));
    }

    #[tokio::test]
    async fn test_find_chunk_starts_with_faults_multiple_chunks_same_replica() {
        let model = setup_test_model().await;

        // Add multiple fault chunks for the same replica
        let faults1 = vec![Fault {
            block_num: 0,
            replica: "replica1".to_string(),
            fault: FaultKind::MissingBlock,
        }];
        let faults2 = vec![Fault {
            block_num: 1000,
            replica: "replica1".to_string(),
            fault: FaultKind::MissingBlock,
        }];
        let faults3 = vec![Fault {
            block_num: 2000,
            replica: "replica1".to_string(),
            fault: FaultKind::MissingBlock,
        }];

        model
            .set_faults_chunk("replica1", 0, faults1)
            .await
            .unwrap();
        model
            .set_faults_chunk("replica1", 1000, faults2)
            .await
            .unwrap();
        model
            .set_faults_chunk("replica1", 2000, faults3)
            .await
            .unwrap();

        let result = model.find_chunk_starts_with_faults().await.unwrap();
        assert_eq!(result.len(), 3);
        assert!(result.contains(&0));
        assert!(result.contains(&1000));
        assert!(result.contains(&2000));
    }

    #[tokio::test]
    async fn test_find_chunk_starts_with_faults_large_block_numbers() {
        let model = setup_test_model().await;

        // Test with large block numbers
        let large_block_start = 1_000_000_000u64;
        let faults = vec![Fault {
            block_num: large_block_start,
            replica: "replica1".to_string(),
            fault: FaultKind::MissingBlock,
        }];

        model
            .set_faults_chunk("replica1", large_block_start, faults)
            .await
            .unwrap();

        let result = model.find_chunk_starts_with_faults().await.unwrap();
        assert_eq!(result.len(), 1);
        assert!(result.contains(&large_block_start));
    }

    #[tokio::test]
    async fn test_find_chunk_starts_with_faults_after_delete() {
        let model = setup_test_model().await;

        // Add fault chunks
        let faults1 = vec![Fault {
            block_num: 100,
            replica: "replica1".to_string(),
            fault: FaultKind::MissingBlock,
        }];
        let faults2 = vec![Fault {
            block_num: 200,
            replica: "replica2".to_string(),
            fault: FaultKind::MissingBlock,
        }];

        model
            .set_faults_chunk("replica1", 100, faults1)
            .await
            .unwrap();
        model
            .set_faults_chunk("replica2", 200, faults2)
            .await
            .unwrap();

        // Verify both chunks exist
        let result = model.find_chunk_starts_with_faults().await.unwrap();
        assert_eq!(result.len(), 2);

        // Delete one chunk
        model.delete_faults_chunk("replica1", 100).await.unwrap();

        // Verify only one chunk remains
        let result = model.find_chunk_starts_with_faults().await.unwrap();
        assert_eq!(result.len(), 1);
        assert!(result.contains(&200));
        assert!(!result.contains(&100));
    }

    #[tokio::test]
    async fn test_find_chunk_starts_with_faults_empty_faults_list() {
        let model = setup_test_model().await;

        // Add an empty faults list (this might happen after all faults are resolved)
        let empty_faults: Vec<Fault> = vec![];
        model
            .set_faults_chunk("replica1", 300, empty_faults)
            .await
            .unwrap();

        // The chunk should still be found even if it's empty
        let result = model.find_chunk_starts_with_faults().await.unwrap();
        assert_eq!(result.len(), 1);
        assert!(result.contains(&300));
    }

    #[tokio::test]
    async fn test_find_chunk_starts_with_faults_mixed_fault_types() {
        let model = setup_test_model().await;

        // Add chunks with different fault types
        let faults_missing = vec![Fault {
            block_num: 100,
            replica: "replica1".to_string(),
            fault: FaultKind::MissingBlock,
        }];

        let faults_invalid_num = vec![Fault {
            block_num: 200,
            replica: "replica2".to_string(),
            fault: FaultKind::InvalidBlockNumber {
                expected: 200,
                actual: 201,
            },
        }];

        let faults_receipt_mismatch = vec![Fault {
            block_num: 300,
            replica: "replica3".to_string(),
            fault: FaultKind::ReceiptCountMismatch {
                tx_count: 5,
                receipt_count: 3,
            },
        }];

        let faults_trace_mismatch = vec![Fault {
            block_num: 400,
            replica: "replica4".to_string(),
            fault: FaultKind::TraceCountMismatch {
                tx_count: 5,
                trace_count: 4,
            },
        }];

        let faults_inconsistent = vec![Fault {
            block_num: 500,
            replica: "replica5".to_string(),
            fault: FaultKind::InconsistentBlock(InconsistentBlockReason::InvalidParentHash),
        }];

        model
            .set_faults_chunk("replica1", 100, faults_missing)
            .await
            .unwrap();
        model
            .set_faults_chunk("replica2", 200, faults_invalid_num)
            .await
            .unwrap();
        model
            .set_faults_chunk("replica3", 300, faults_receipt_mismatch)
            .await
            .unwrap();
        model
            .set_faults_chunk("replica4", 400, faults_trace_mismatch)
            .await
            .unwrap();
        model
            .set_faults_chunk("replica5", 500, faults_inconsistent)
            .await
            .unwrap();

        let result = model.find_chunk_starts_with_faults().await.unwrap();
        assert_eq!(result.len(), 5);
        assert!(result.contains(&100));
        assert!(result.contains(&200));
        assert!(result.contains(&300));
        assert!(result.contains(&400));
        assert!(result.contains(&500));
    }

    #[tokio::test]
    async fn test_find_chunk_starts_with_faults_by_replica_no_filter() {
        let model = setup_test_model().await;

        // Add faults for multiple replicas
        let faults1 = vec![Fault {
            block_num: 100,
            replica: "replica1".to_string(),
            fault: FaultKind::MissingBlock,
        }];
        let faults2 = vec![Fault {
            block_num: 200,
            replica: "replica2".to_string(),
            fault: FaultKind::MissingBlock,
        }];
        let faults3 = vec![Fault {
            block_num: 300,
            replica: "replica1".to_string(),
            fault: FaultKind::MissingBlock,
        }];

        model
            .set_faults_chunk("replica1", 100, faults1)
            .await
            .unwrap();
        model
            .set_faults_chunk("replica2", 200, faults2)
            .await
            .unwrap();
        model
            .set_faults_chunk("replica1", 300, faults3)
            .await
            .unwrap();

        // Query for replica1 without range filter
        let result = model
            .find_chunk_starts_with_faults_by_replica("replica1", None, None)
            .await
            .unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.contains(&100));
        assert!(result.contains(&300));

        // Query for replica2 without range filter
        let result = model
            .find_chunk_starts_with_faults_by_replica("replica2", None, None)
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        assert!(result.contains(&200));

        // Query for non-existent replica
        let result = model
            .find_chunk_starts_with_faults_by_replica("replica3", None, None)
            .await
            .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_find_chunk_starts_with_faults_by_replica_with_start() {
        let model = setup_test_model().await;

        // Add faults at different chunk starts
        let faults1 = vec![Fault {
            block_num: 100,
            replica: "replica1".to_string(),
            fault: FaultKind::MissingBlock,
        }];
        let faults2 = vec![Fault {
            block_num: 200,
            replica: "replica1".to_string(),
            fault: FaultKind::MissingBlock,
        }];
        let faults3 = vec![Fault {
            block_num: 300,
            replica: "replica1".to_string(),
            fault: FaultKind::MissingBlock,
        }];

        model
            .set_faults_chunk("replica1", 100, faults1)
            .await
            .unwrap();
        model
            .set_faults_chunk("replica1", 200, faults2)
            .await
            .unwrap();
        model
            .set_faults_chunk("replica1", 300, faults3)
            .await
            .unwrap();

        // Query with start = 200
        // Chunk 100 contains blocks 100-1099, which includes blocks >= 200
        // So all three chunks should be included
        let result = model
            .find_chunk_starts_with_faults_by_replica("replica1", Some(200), None)
            .await
            .unwrap();
        assert_eq!(result.len(), 3);
        assert!(result.contains(&100)); // Contains blocks 200-1099
        assert!(result.contains(&200)); // Contains blocks 200-1199
        assert!(result.contains(&300)); // Contains blocks 300-1299
    }

    #[tokio::test]
    async fn test_find_chunk_starts_with_faults_by_replica_with_end() {
        let model = setup_test_model().await;

        // Add faults at different chunk starts
        let faults1 = vec![Fault {
            block_num: 100,
            replica: "replica1".to_string(),
            fault: FaultKind::MissingBlock,
        }];
        let faults2 = vec![Fault {
            block_num: 200,
            replica: "replica1".to_string(),
            fault: FaultKind::MissingBlock,
        }];
        let faults3 = vec![Fault {
            block_num: 300,
            replica: "replica1".to_string(),
            fault: FaultKind::MissingBlock,
        }];

        model
            .set_faults_chunk("replica1", 100, faults1)
            .await
            .unwrap();
        model
            .set_faults_chunk("replica1", 200, faults2)
            .await
            .unwrap();
        model
            .set_faults_chunk("replica1", 300, faults3)
            .await
            .unwrap();

        // Query with end = 200
        let result = model
            .find_chunk_starts_with_faults_by_replica("replica1", None, Some(200))
            .await
            .unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.contains(&100));
        assert!(result.contains(&200));
        assert!(!result.contains(&300));
    }

    #[tokio::test]
    async fn test_find_chunk_starts_with_faults_by_replica_with_range() {
        let model = setup_test_model().await;

        // Add faults at proper chunk boundaries (multiples of CHUNK_SIZE=1000)
        let faults1 = vec![Fault {
            block_num: 0,
            replica: "replica1".to_string(),
            fault: FaultKind::MissingBlock,
        }];
        let faults2 = vec![Fault {
            block_num: 1000,
            replica: "replica1".to_string(),
            fault: FaultKind::MissingBlock,
        }];
        let faults3 = vec![Fault {
            block_num: 2000,
            replica: "replica1".to_string(),
            fault: FaultKind::MissingBlock,
        }];
        let faults4 = vec![Fault {
            block_num: 3000,
            replica: "replica1".to_string(),
            fault: FaultKind::MissingBlock,
        }];
        let faults5 = vec![Fault {
            block_num: 4000,
            replica: "replica1".to_string(),
            fault: FaultKind::MissingBlock,
        }];

        model
            .set_faults_chunk("replica1", 0, faults1)
            .await
            .unwrap();
        model
            .set_faults_chunk("replica1", 1000, faults2)
            .await
            .unwrap();
        model
            .set_faults_chunk("replica1", 2000, faults3)
            .await
            .unwrap();
        model
            .set_faults_chunk("replica1", 3000, faults4)
            .await
            .unwrap();
        model
            .set_faults_chunk("replica1", 4000, faults5)
            .await
            .unwrap();

        // Query with range [1500, 3500]
        // This should include chunks:
        // - 1000 (contains blocks 1000-1999, includes 1500-1999)
        // - 2000 (contains blocks 2000-2999)
        // - 3000 (contains blocks 3000-3999, includes 3000-3500)
        let result = model
            .find_chunk_starts_with_faults_by_replica("replica1", Some(1500), Some(3500))
            .await
            .unwrap();
        assert_eq!(result.len(), 3);
        assert!(!result.contains(&0)); // Doesn't overlap with [1500, 3500]
        assert!(result.contains(&1000)); // Contains blocks 1500-1999
        assert!(result.contains(&2000)); // Contains blocks 2000-2999
        assert!(result.contains(&3000)); // Contains blocks 3000-3500
        assert!(!result.contains(&4000)); // Doesn't overlap with [1500, 3500]
    }

    #[tokio::test]
    async fn test_find_chunk_starts_with_faults_by_replica_large_numbers() {
        let model = setup_test_model().await;

        // Test with large block numbers to verify prefix optimization
        let chunk_starts = vec![
            1_000_000_000u64,
            1_000_001_000u64,
            1_100_000_000u64,
            2_000_000_000u64,
        ];

        for chunk_start in &chunk_starts {
            let faults = vec![Fault {
                block_num: *chunk_start,
                replica: "replica1".to_string(),
                fault: FaultKind::MissingBlock,
            }];
            model
                .set_faults_chunk("replica1", *chunk_start, faults)
                .await
                .unwrap();
        }

        // Query with range that should use prefix optimization
        let result = model
            .find_chunk_starts_with_faults_by_replica(
                "replica1",
                Some(1_000_000_000),
                Some(1_100_000_000),
            )
            .await
            .unwrap();

        assert_eq!(result.len(), 3);
        assert!(result.contains(&1_000_000_000));
        assert!(result.contains(&1_000_001_000));
        assert!(result.contains(&1_100_000_000));
        assert!(!result.contains(&2_000_000_000));
    }

    #[tokio::test]
    async fn test_find_chunk_starts_with_faults_by_replica_with_prefix_optimization() {
        let model = setup_test_model().await;

        // Add faults in a range where prefix optimization helps
        // Chunks 100000, 101000, 102000, ..., 109000 all start with "10"
        for i in 0..10 {
            let chunk_start = 100_000 + i * 1000;
            let faults = vec![Fault {
                block_num: chunk_start,
                replica: "replica1".to_string(),
                fault: FaultKind::MissingBlock,
            }];
            model
                .set_faults_chunk("replica1", chunk_start, faults)
                .await
                .unwrap();
        }

        // Also add some chunks that shouldn't match
        let faults_outside = vec![Fault {
            block_num: 200_000,
            replica: "replica1".to_string(),
            fault: FaultKind::MissingBlock,
        }];
        model
            .set_faults_chunk("replica1", 200_000, faults_outside)
            .await
            .unwrap();

        // Query with a range that should use prefix optimization
        let result = model
            .find_chunk_starts_with_faults_by_replica("replica1", Some(100_500), Some(109_500))
            .await
            .unwrap();

        // Should find all chunks in the 100000-109000 range
        assert_eq!(result.len(), 10);
        for i in 0..10 {
            assert!(result.contains(&(100_000 + i * 1000)));
        }
        assert!(!result.contains(&200_000));
    }
}
