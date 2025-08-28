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
    collections::{BTreeMap, VecDeque},
    task::{Poll, Waker},
};

use alloy_consensus::{transaction::Recovered, TxEnvelope};
use alloy_rlp::Decodable;
use bytes::Bytes;
use futures::Stream;
use monad_consensus_types::block::{
    BlockPolicy, MockExecutionBody, MockExecutionProposedHeader, MockExecutionProtocol,
    ProposedExecutionInputs,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_block_policy::EthBlockPolicy;
use monad_eth_txpool::{EthTxPool, EthTxPoolEventTracker, EthTxPoolMetrics};
use monad_eth_types::EthExecutionProtocol;
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{MempoolEvent, MonadEvent, TxPoolCommand};
use monad_state_backend::StateBackend;
use monad_types::ExecutionProtocol;
use monad_validator::signature_collection::SignatureCollection;

pub trait MockableTxPool:
    Executor<
        Command = TxPoolCommand<
            Self::Signature,
            Self::SignatureCollection,
            Self::ExecutionProtocol,
            Self::BlockPolicy,
            Self::StateBackend,
        >,
    > + Stream<Item = Self::Event>
    + Unpin
{
    type Signature: CertificateSignatureRecoverable;
    type SignatureCollection: SignatureCollection<
        NodeIdPubKey = CertificateSignaturePubKey<Self::Signature>,
    >;
    type ExecutionProtocol: ExecutionProtocol;
    type BlockPolicy: BlockPolicy<
        Self::Signature,
        Self::SignatureCollection,
        Self::ExecutionProtocol,
        Self::StateBackend,
    >;
    type StateBackend: StateBackend<Self::Signature, Self::SignatureCollection>;

    type Event;

    fn ready(&self) -> bool;

    fn send_transaction(&mut self, tx: Bytes);
}

impl<T: MockableTxPool + ?Sized> MockableTxPool for Box<T> {
    type Signature = T::Signature;
    type SignatureCollection = T::SignatureCollection;
    type ExecutionProtocol = T::ExecutionProtocol;
    type BlockPolicy = T::BlockPolicy;
    type StateBackend = T::StateBackend;

    type Event = T::Event;

    fn ready(&self) -> bool {
        (**self).ready()
    }

    fn send_transaction(&mut self, tx: Bytes) {
        (**self).send_transaction(tx);
    }
}

pub struct MockTxPoolExecutor<ST, SCT, EPT, BPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    SBT: StateBackend<ST, SCT>,
{
    // This field is only populated when the execution protocol is EthExecutionProtocol
    eth: Option<(EthTxPool<ST, SCT, SBT>, BPT, SBT)>,

    events: VecDeque<MempoolEvent<ST, SCT, EPT>>,
    waker: Option<Waker>,

    metrics: EthTxPoolMetrics,
    executor_metrics: ExecutorMetrics,
}

impl<ST, SCT, BPT, SBT> Default for MockTxPoolExecutor<ST, SCT, MockExecutionProtocol, BPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend<ST, SCT>,
{
    fn default() -> Self {
        Self {
            eth: None,

            events: VecDeque::default(),
            waker: None,

            metrics: EthTxPoolMetrics::default(),
            executor_metrics: ExecutorMetrics::default(),
        }
    }
}

impl<ST, SCT, SBT> MockTxPoolExecutor<ST, SCT, EthExecutionProtocol, EthBlockPolicy<ST, SCT>, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend<ST, SCT>,
{
    pub fn new(block_policy: EthBlockPolicy<ST, SCT>, state_backend: SBT) -> Self {
        Self {
            eth: Some((EthTxPool::default_testing(), block_policy, state_backend)),

            events: VecDeque::default(),
            waker: None,

            metrics: EthTxPoolMetrics::default(),
            executor_metrics: ExecutorMetrics::default(),
        }
    }
}

impl<ST, SCT, BPT, SBT> Executor for MockTxPoolExecutor<ST, SCT, MockExecutionProtocol, BPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<ST, SCT, MockExecutionProtocol, SBT>,
    SBT: StateBackend<ST, SCT>,
{
    type Command = TxPoolCommand<ST, SCT, MockExecutionProtocol, BPT, SBT>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            match command {
                TxPoolCommand::CreateProposal {
                    epoch,
                    round,
                    seq_num,
                    high_qc,
                    round_signature,
                    last_round_tc,
                    fresh_proposal_certificate,
                    tx_limit: _,
                    proposal_gas_limit: _,
                    proposal_byte_limit: _,
                    beneficiary: _,
                    timestamp_ns,
                    extending_blocks: _,
                    delayed_execution_results,
                } => {
                    self.events.push_back(MempoolEvent::Proposal {
                        epoch,
                        round,
                        seq_num,
                        high_qc,
                        timestamp_ns,
                        round_signature,
                        base_fee: monad_tfm::base_fee::MIN_BASE_FEE,
                        base_fee_trend: monad_tfm::base_fee::GENESIS_BASE_FEE_TREND,
                        base_fee_moment: monad_tfm::base_fee::GENESIS_BASE_FEE_MOMENT,
                        delayed_execution_results,
                        proposed_execution_inputs: ProposedExecutionInputs {
                            header: MockExecutionProposedHeader::default(),
                            body: MockExecutionBody::default(),
                        },
                        last_round_tc,
                        fresh_proposal_certificate,
                    });

                    if let Some(waker) = self.waker.take() {
                        waker.wake();
                    }
                }
                TxPoolCommand::BlockCommit(_) | TxPoolCommand::Reset { .. } => {}
                TxPoolCommand::InsertForwardedTxs { .. } => {
                    unimplemented!(
                        "MockTxPoolExecutor should never recieve txs with MockExecutionProtocol"
                    );
                }
                TxPoolCommand::EnterRound { .. } => {}
            }
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        ExecutorMetricsChain::default()
    }
}

impl<ST, SCT, SBT> Executor
    for MockTxPoolExecutor<ST, SCT, EthExecutionProtocol, EthBlockPolicy<ST, SCT>, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend<ST, SCT>,
{
    type Command = TxPoolCommand<ST, SCT, EthExecutionProtocol, EthBlockPolicy<ST, SCT>, SBT>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        let (pool, block_policy, state_backend) = self.eth.as_mut().unwrap();

        let mut events = BTreeMap::default();
        let mut event_tracker = EthTxPoolEventTracker::new(&self.metrics, &mut events);

        for command in commands {
            match command {
                TxPoolCommand::CreateProposal {
                    epoch,
                    round,
                    seq_num,
                    high_qc,
                    round_signature,
                    last_round_tc,
                    fresh_proposal_certificate,
                    tx_limit,
                    proposal_gas_limit,
                    proposal_byte_limit,
                    beneficiary,
                    timestamp_ns,
                    extending_blocks,
                    delayed_execution_results,
                } => {
                    let (base_fee, base_fee_trend, base_fee_moment) =
                        block_policy.compute_base_fee(&extending_blocks);

                    let proposed_execution_inputs = pool
                        .create_proposal(
                            &mut event_tracker,
                            seq_num,
                            base_fee,
                            tx_limit,
                            proposal_gas_limit,
                            proposal_byte_limit,
                            beneficiary,
                            timestamp_ns,
                            round_signature.clone(),
                            extending_blocks,
                            block_policy,
                            state_backend,
                        )
                        .expect("proposal succeeds");

                    self.events.push_back(MempoolEvent::Proposal {
                        epoch,
                        round,
                        seq_num,
                        high_qc,
                        timestamp_ns,
                        round_signature,
                        base_fee,
                        base_fee_trend,
                        base_fee_moment,
                        delayed_execution_results,
                        proposed_execution_inputs,
                        last_round_tc,
                        fresh_proposal_certificate,
                    });

                    if let Some(waker) = self.waker.take() {
                        waker.wake();
                    }
                }
                TxPoolCommand::BlockCommit(committed_blocks) => {
                    for committed_block in committed_blocks {
                        BlockPolicy::<ST, SCT, EthExecutionProtocol, SBT>::update_committed_block(
                            block_policy,
                            &committed_block,
                        );
                        pool.update_committed_block(&mut event_tracker, committed_block);
                    }
                }
                TxPoolCommand::Reset {
                    last_delay_committed_blocks,
                } => {
                    BlockPolicy::<ST, SCT, EthExecutionProtocol, SBT>::reset(
                        block_policy,
                        last_delay_committed_blocks.iter().collect(),
                    );
                    pool.reset(&mut event_tracker, last_delay_committed_blocks);
                }
                TxPoolCommand::InsertForwardedTxs { sender: _, txs } => {
                    pool.insert_txs(
                        &mut event_tracker,
                        block_policy,
                        state_backend,
                        txs.into_iter()
                            .filter_map(|raw_tx| {
                                let tx = TxEnvelope::decode(&mut raw_tx.as_ref()).ok()?;
                                let signer = tx.recover_signer().ok()?;
                                Some(Recovered::new_unchecked(tx, signer))
                            })
                            .collect(),
                        false,
                        |_| {},
                    );
                }
                // TODO: add chain config to MockTxPoolExecutor if we're testing
                // param forking with it
                TxPoolCommand::EnterRound { .. } => {}
            }
        }

        self.metrics.update(&mut self.executor_metrics);
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        ExecutorMetricsChain::default().push(&self.executor_metrics)
    }
}

impl<ST, SCT, EPT, BPT, SBT> Stream for MockTxPoolExecutor<ST, SCT, EPT, BPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
    BPT: BlockPolicy<ST, SCT, EPT, SBT>,
    SBT: StateBackend<ST, SCT>,

    Self: Unpin,
{
    type Item = MonadEvent<ST, SCT, EPT>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(Some(MonadEvent::MempoolEvent(event)));
        }

        self.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl<ST, SCT, BPT, SBT> MockableTxPool
    for MockTxPoolExecutor<ST, SCT, MockExecutionProtocol, BPT, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    BPT: BlockPolicy<ST, SCT, MockExecutionProtocol, SBT>,
    SBT: StateBackend<ST, SCT>,

    Self: Executor<Command = TxPoolCommand<ST, SCT, MockExecutionProtocol, BPT, SBT>> + Unpin,
{
    type Signature = ST;
    type SignatureCollection = SCT;
    type ExecutionProtocol = MockExecutionProtocol;
    type BlockPolicy = BPT;
    type StateBackend = SBT;

    type Event = MonadEvent<ST, SCT, MockExecutionProtocol>;

    fn ready(&self) -> bool {
        !self.events.is_empty()
    }

    fn send_transaction(&mut self, _: Bytes) {
        unreachable!(
            "MockTxPoolExecutor does not support send_transaction with MockExecutionProtocol"
        );
    }
}

impl<ST, SCT, SBT> MockableTxPool
    for MockTxPoolExecutor<ST, SCT, EthExecutionProtocol, EthBlockPolicy<ST, SCT>, SBT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend<ST, SCT>,

    Self: Executor<
            Command = TxPoolCommand<ST, SCT, EthExecutionProtocol, EthBlockPolicy<ST, SCT>, SBT>,
        > + Unpin,
{
    type Signature = ST;
    type SignatureCollection = SCT;
    type ExecutionProtocol = EthExecutionProtocol;
    type BlockPolicy = EthBlockPolicy<ST, SCT>;
    type StateBackend = SBT;

    type Event = MonadEvent<ST, SCT, EthExecutionProtocol>;

    fn ready(&self) -> bool {
        !self.events.is_empty()
    }

    fn send_transaction(&mut self, tx: Bytes) {
        let (pool, block_policy, state_backend) = self.eth.as_mut().unwrap();

        let Ok(tx) = TxEnvelope::decode(&mut tx.as_ref()) else {
            panic!("MockableTxPool received invalid tx bytes!");
        };

        let Ok(signer) = tx.recover_signer() else {
            panic!("MockableTxPool received tx with invalid signer");
        };

        let tx = Recovered::new_unchecked(tx, signer);

        pool.insert_txs(
            &mut EthTxPoolEventTracker::new(&self.metrics, &mut BTreeMap::default()),
            block_policy,
            state_backend,
            vec![tx],
            true,
            |tx| {
                self.events.push_back(MempoolEvent::ForwardTxs(vec![
                    alloy_rlp::encode(tx.raw()).into()
                ]));
            },
        );

        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }
}
