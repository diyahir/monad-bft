use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    fs::{self, File},
    io::{ErrorKind, Read, Seek, SeekFrom, Write},
    marker::PhantomData,
    path::PathBuf,
    pin::Pin,
    task::{Context, Poll},
};

use alloy_rlp::{Decodable, Encodable};
use executable_block::{format_block_id, ExecutableBlock};
use futures::Stream;
use monad_block_persist::{BlockPersist, FileBlockPersist};
use monad_blocksync::messages::message::{
    BlockSyncBodyResponse, BlockSyncHeadersResponse, BlockSyncResponseMessage,
};
use monad_consensus_types::{
    block::{BlockRange, ConsensusBlockHeader, ConsensusFullBlock, ExecutionProtocol},
    ledger::OptimisticCommit,
    payload::{ConsensusBlockBody, ConsensusBlockBodyId, EthExecutionProtocol},
    signature_collection::SignatureCollection,
};
use monad_crypto::{
    certificate_signature::{CertificateSignaturePubKey, CertificateSignatureRecoverable},
    hasher::{Hasher, HasherType},
};
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{BlockSyncEvent, LedgerCommand, MonadEvent};
use monad_types::{BlockId, Hash, Round, SeqNum, GENESIS_BLOCK_ID};
use reth_primitives::TransactionSigned;
use tracing::{info, trace};

pub mod executable_block;

/// Protocol parameters that go into Eth block header
pub struct EthHeaderParam {
    pub gas_limit: u64,
}

/// A ledger for committed Ethereum blocks
/// Blocks are RLP encoded and written to their own individual file, named by the block
/// number
pub struct MonadBlockFileLedger<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    eth_block_path: PathBuf,
    bft_block_persist: FileBlockPersist<ST, SCT, EthExecutionProtocol>,
    header_param: EthHeaderParam,

    metrics: ExecutorMetrics,
    last_commit: Option<(SeqNum, Round)>,

    block_cache_size: usize,
    block_cache: HashMap<BlockId, ConsensusFullBlock<ST, SCT, EthExecutionProtocol>>,
    block_payload_cache: HashMap<ConsensusBlockBodyId, ConsensusBlockBody<EthExecutionProtocol>>,
    block_cache_index: BTreeMap<Round, (BlockId, ConsensusBlockBodyId)>,

    wal: File,
    wal_path: PathBuf,

    fetches_tx:
        tokio::sync::mpsc::UnboundedSender<BlockSyncResponseMessage<ST, SCT, EthExecutionProtocol>>,
    fetches: tokio::sync::mpsc::UnboundedReceiver<
        BlockSyncResponseMessage<ST, SCT, EthExecutionProtocol>,
    >,

    phantom: PhantomData<ST>,
}

const GAUGE_EXECUTION_LEDGER_NUM_COMMITS: &str = "monad.execution_ledger.num_commits";
const GAUGE_EXECUTION_LEDGER_NUM_TX_COMMITS: &str = "monad.execution_ledger.num_tx_commits";
const GAUGE_EXECUTION_LEDGER_BLOCK_NUM: &str = "monad.execution_ledger.block_num";

impl<ST, SCT> MonadBlockFileLedger<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    pub fn new(
        eth_block_path: PathBuf,
        bft_block_path: PathBuf,
        payload_path: PathBuf,
        header_param: EthHeaderParam,
    ) -> Self {
        match fs::create_dir(&eth_block_path) {
            Ok(_) => (),
            Err(e) if e.kind() == ErrorKind::AlreadyExists => (),
            Err(e) => panic!("{}", e),
        }
        match fs::create_dir(&bft_block_path) {
            Ok(_) => (),
            Err(e) if e.kind() == ErrorKind::AlreadyExists => (),
            Err(e) => panic!("{}", e),
        }
        match fs::create_dir(&payload_path) {
            Ok(_) => (),
            Err(e) if e.kind() == ErrorKind::AlreadyExists => (),
            Err(e) => panic!("{}", e),
        }

        let wal_path = {
            let mut wal_path = eth_block_path.clone();
            wal_path.push("wal");
            wal_path
        };
        let mut wal = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&wal_path)
            .expect("failed to open WAL");
        let wal_len = wal.metadata().expect("failed to get wal metadata").len();
        const event_len: u64 = 33; // FIXME don't hardcode
        wal.set_len(wal_len / event_len * event_len)
            .expect("failed to set wal len");
        let num_events = wal_len / event_len;

        let bft_block_persist = FileBlockPersist::new(bft_block_path, payload_path);

        let mut last_commit = None;
        for event_idx in (0..num_events).rev() {
            wal.seek(SeekFrom::Start(event_idx * event_len))
                .expect("failed to seek to event in wal");
            let mut buf = [0_u8; event_len as usize];
            wal.read_exact(&mut buf)
                .expect("failed to read event from wal");
            let block_id = BlockId(Hash(buf[1..].try_into().expect("blockid not 32 bytes")));

            if buf[0] == 1
            // committed, FIXME const
            {
                let bft_block = bft_block_persist
                    .read_bft_header(&block_id)
                    .expect("failed to find bft block");
                last_commit = Some((bft_block.seq_num, bft_block.round));
                break;
            }
        }

        wal.seek(SeekFrom::End(0))
            .expect("failed to seek to end of wal");

        let (fetches_tx, fetches) = tokio::sync::mpsc::unbounded_channel();
        Self {
            eth_block_path: eth_block_path.clone(),
            bft_block_persist,
            header_param,

            metrics: Default::default(),
            last_commit,

            // FIXME this currently needs to be bigger than max statesync
            // buffer size, cuz committed proposed block must exist in
            // cache
            block_cache_size: 1_000, // TODO configurable

            block_cache: Default::default(),
            block_payload_cache: Default::default(),
            block_cache_index: Default::default(),

            wal,
            wal_path,

            fetches_tx,
            fetches,

            phantom: PhantomData,
        }
    }

    pub fn last_commit(&self) -> Option<SeqNum> {
        let (last_commit_seq_num, _) = self.last_commit?;
        Some(last_commit_seq_num)
    }

    fn update_cache(&mut self, monad_block: ConsensusFullBlock<ST, SCT, EthExecutionProtocol>) {
        let block_id = monad_block.get_id();
        let payload_id = monad_block.get_body_id();
        let block_round = monad_block.get_round();

        let maybe_removed = self
            .block_cache_index
            .insert(block_round, (block_id, payload_id));
        if let Some((block_id, payload_id)) = maybe_removed {
            self.block_cache.remove(&block_id);
            self.block_payload_cache.remove(&payload_id);
        }

        if self.block_cache_index.len() > self.block_cache_size {
            let (evicted_round, (block_id, payload_id)) =
                self.block_cache_index.pop_first().expect("nonempty");
            let last_commit_round = self
                .last_commit
                .map(|(_, last_commit_round)| last_commit_round);
            assert!(evicted_round < last_commit_round.unwrap_or(Round(u64::MAX)));
            self.block_cache.remove(&block_id);
            self.block_payload_cache.remove(&payload_id);
        }

        // insert at the end in case payload got evicted
        self.block_payload_cache
            .insert(payload_id, monad_block.body().clone());
        self.block_cache.insert(block_id, monad_block);
    }

    fn ledger_block_file_path(&self, bft_block_id: &[u8; 32]) -> PathBuf {
        let mut file_path = PathBuf::from(&self.eth_block_path);
        file_path.push(format_block_id(bft_block_id));
        file_path
    }

    fn try_write_executable_block(&self, block: &ExecutableBlock) -> std::io::Result<()> {
        let file_path = self.ledger_block_file_path(block.bft_block_id());

        if file_path.exists() {
            return Ok(());
        }

        let mut block_bytes = Vec::default();
        block.encode(&mut block_bytes);

        let mut file_path_wip = file_path.clone();
        file_path_wip.set_extension("wip");
        std::fs::write(&file_path_wip, &block_bytes)?;
        std::fs::rename(&file_path_wip, &file_path)?;
        Ok(())
    }

    fn mark_proposed(&mut self, bft_block_id: &[u8; 32]) -> std::io::Result<()> {
        let mut event: [u8; 33] = [0_u8; 33]; // FIXME
        event[0] = 0; // FIXME
        event[1..].copy_from_slice(bft_block_id);
        self.wal.write_all(&event)?;
        self.wal.flush()?;
        Ok(())
    }

    fn mark_committed(&mut self, bft_block_id: &[u8; 32]) -> std::io::Result<()> {
        let mut event: [u8; 33] = [0_u8; 33]; // FIXME
        event[0] = 1; // FIXME
        event[1..].copy_from_slice(bft_block_id);
        self.wal.write_all(&event)?;
        self.wal.flush()?;
        Ok(())
    }

    fn mark_verified(&mut self, bft_block_id: &[u8; 32]) -> std::io::Result<()> {
        let mut event: [u8; 33] = [0_u8; 33]; // FIXME
        event[0] = 2; // FIXME
        event[1..].copy_from_slice(bft_block_id);
        self.wal.write_all(&event)?;
        self.wal.flush()?;
        Ok(())
    }

    fn write_bft_block(&self, full_block: &ConsensusFullBlock<ST, SCT, EthExecutionProtocol>) {
        // unwrap because failure to persist a finalized block is fatal error

        // write payload first so that header always points to payload that exists
        self.bft_block_persist
            .write_bft_body(full_block.body())
            .unwrap();
        self.bft_block_persist
            .write_bft_header(full_block.header())
            .unwrap();
    }

    fn ledger_fetch_headers(
        &self,
        block_range: BlockRange,
    ) -> BlockSyncHeadersResponse<ST, SCT, EthExecutionProtocol> {
        let mut next_block_id = block_range.last_block_id;

        let mut headers = VecDeque::new();
        while (headers.len() as u64) < block_range.max_blocks.0 {
            // TODO add max number of headers to read
            let block_header = if let Some(cached_block) = self.block_cache.get(&next_block_id) {
                cached_block.header().clone()
            } else if let Ok(block) = self.bft_block_persist.read_bft_header(&next_block_id) {
                block
            } else {
                trace!(?block_range, "requested headers not available in ledger");
                return BlockSyncHeadersResponse::NotAvailable(block_range);
            };

            next_block_id = block_header.get_parent_id();
            headers.push_front(block_header);
        }

        trace!(?block_range, "found requested headers in ledger");
        BlockSyncHeadersResponse::Found((block_range, headers.into()))
    }

    fn ledger_fetch_payload(
        &self,
        payload_id: ConsensusBlockBodyId,
    ) -> BlockSyncBodyResponse<EthExecutionProtocol> {
        if let Some(cached_payload) = self.block_payload_cache.get(&payload_id) {
            // payload in cache
            trace!(?payload_id, "found requested payload in ledger cache");
            BlockSyncBodyResponse::Found(cached_payload.clone())
        } else if let Ok(payload) = self.bft_block_persist.read_bft_body(&payload_id) {
            // payload read from block persist
            trace!(
                ?payload_id,
                "found requested payload in ledger blockpersist"
            );
            BlockSyncBodyResponse::Found(payload)
        } else {
            trace!(?payload_id, "requested payload not available in ledger");
            BlockSyncBodyResponse::NotAvailable(payload_id)
        }
    }
}

impl<ST, SCT> Executor for MonadBlockFileLedger<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    type Command = LedgerCommand<ST, SCT, EthExecutionProtocol>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            match command {
                LedgerCommand::LedgerClearWal => {
                    if self.wal.metadata().expect("can't read meta").len() > 0 {
                        let timestamp = std::time::UNIX_EPOCH
                            .elapsed()
                            .expect("failed to get duration since epoch");
                        let _ = std::fs::rename(
                            &self.wal_path,
                            format!(
                                "{}.{}",
                                self.wal_path
                                    .to_str()
                                    .expect("wal_path is not valid unicode"),
                                timestamp.as_millis()
                            ),
                        );
                    }
                    self.wal = File::options()
                        .read(true)
                        .write(true)
                        .create(true)
                        .truncate(true)
                        .open(&self.wal_path)
                        .expect("failed to open WAL");
                }
                LedgerCommand::LedgerCommit(OptimisticCommit::Proposed(block)) => {
                    let block_id = block.get_id();
                    let block_round = block.get_round();
                    let executable_block = ExecutableBlock::from(&block);

                    // this can panic because failure to persist a block is fatal error
                    self.write_bft_block(&block);

                    self.update_cache(block);

                    if self
                        .last_commit
                        .is_some_and(|(_last_commit_seq_num, last_commit_round)| {
                            block_round <= last_commit_round
                        })
                    {
                        // we can't repropose stuff that's already finalized
                        continue;
                    }
                    // write executable block to ledger
                    self.try_write_executable_block(&executable_block).unwrap();

                    self.mark_proposed(&block_id.0 .0).unwrap();
                }
                LedgerCommand::LedgerCommit(OptimisticCommit::Committed(block_id)) => {
                    self.metrics[GAUGE_EXECUTION_LEDGER_NUM_COMMITS] += 1;

                    let block = self
                        .block_cache
                        .get(&block_id)
                        .expect("must have committed proposed block (in cache)");
                    let block_round = block.get_round();
                    let executable_block = ExecutableBlock::from(block);
                    let num_tx = executable_block.num_tx() as u64;
                    let block_num = executable_block.number();
                    info!(num_tx, block_num, "committed block");
                    self.metrics[GAUGE_EXECUTION_LEDGER_NUM_TX_COMMITS] += num_tx;
                    self.metrics[GAUGE_EXECUTION_LEDGER_BLOCK_NUM] = executable_block.number();

                    if self
                        .last_commit
                        .is_some_and(|(_last_commit_seq_num, last_commit_round)| {
                            block_round <= last_commit_round
                        })
                    {
                        // we can't recommit stuff that's already finalized
                        continue;
                    }

                    self.last_commit = Some((block.get_seq_num(), block.get_round()));

                    self.mark_committed(&block_id.0 .0).unwrap();
                }
                LedgerCommand::LedgerCommit(OptimisticCommit::Verified(block_id)) => {
                    self.mark_verified(&block_id.0 .0).unwrap();
                }
                LedgerCommand::LedgerFetchHeaders(block_range) => {
                    // TODO cap max concurrent LedgerFetch? DOS vector
                    let fetches_tx = self.fetches_tx.clone();
                    let response = BlockSyncResponseMessage::HeadersResponse(
                        self.ledger_fetch_headers(block_range),
                    );
                    fetches_tx
                        .send(response)
                        .expect("failed to write to fetches_tx");
                }
                LedgerCommand::LedgerFetchPayload(payload_id) => {
                    let fetches_tx = self.fetches_tx.clone();
                    let response = BlockSyncResponseMessage::PayloadResponse(
                        self.ledger_fetch_payload(payload_id),
                    );
                    fetches_tx
                        .send(response)
                        .expect("failed to write to fetches_tx");
                }
            }
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        self.metrics.as_ref().into()
    }
}

impl<ST, SCT> Stream for MonadBlockFileLedger<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    type Item = MonadEvent<ST, SCT, EthExecutionProtocol>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.fetches.poll_recv(cx).map(|response| {
            let response = response.expect("fetches_tx never dropped");
            Some(MonadEvent::BlockSyncEvent(BlockSyncEvent::SelfResponse {
                response,
            }))
        })
    }
}
