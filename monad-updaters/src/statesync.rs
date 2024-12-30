use std::{
    collections::VecDeque,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use futures::Stream;
use monad_consensus_types::{
    block::{ExecutionProtocol, FinalizedHeader},
    signature_collection::SignatureCollection,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor::{Executor, ExecutorMetricsChain};
use monad_executor_glue::{
    MonadEvent, StateSyncCommand, StateSyncEvent, StateSyncNetworkMessage, StateSyncRequest,
    StateSyncResponse, StateSyncUpsert, StateSyncUpsertType, SELF_STATESYNC_VERSION,
};
use monad_state_backend::InMemoryState;
use monad_types::{NodeId, SeqNum, GENESIS_SEQ_NUM};

pub trait MockableStateSync:
    Executor<Command = StateSyncCommand<Self::Signature, Self::ExecutionProtocol>> + Unpin
{
    type Signature: CertificateSignatureRecoverable;
    type SignatureCollection: SignatureCollection<
        NodeIdPubKey = CertificateSignaturePubKey<Self::Signature>,
    >;
    type ExecutionProtocol: ExecutionProtocol;

    fn ready(&self) -> bool;
    fn pop(
        &mut self,
    ) -> Option<MonadEvent<Self::Signature, Self::SignatureCollection, Self::ExecutionProtocol>>;
}

impl<T: MockableStateSync + ?Sized> MockableStateSync for Box<T> {
    type Signature = T::Signature;
    type SignatureCollection = T::SignatureCollection;
    type ExecutionProtocol = T::ExecutionProtocol;

    fn ready(&self) -> bool {
        (**self).ready()
    }
    fn pop(
        &mut self,
    ) -> Option<MonadEvent<Self::Signature, Self::SignatureCollection, Self::ExecutionProtocol>>
    {
        (**self).pop()
    }
}

pub struct MockStateSyncExecutor<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    events: VecDeque<MonadEvent<ST, SCT, EPT>>,

    state_backend: InMemoryState,
    peers: Vec<NodeId<CertificateSignaturePubKey<ST>>>,

    started_execution: bool,
    request: Option<StateSyncRequest>,

    waker: Option<Waker>,
}

impl<ST, SCT, EPT> Executor for MockStateSyncExecutor<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type Command = StateSyncCommand<ST, EPT>;

    fn exec(&mut self, cmds: Vec<Self::Command>) {
        for cmd in cmds {
            if let Some(waker) = self.waker.take() {
                waker.wake();
            }
            match cmd {
                StateSyncCommand::StartExecution => {
                    assert!(!self.started_execution);
                    self.started_execution = true;
                }
                StateSyncCommand::RequestSync(eth_header)
                    if eth_header.seq_num() == GENESIS_SEQ_NUM =>
                {
                    self.events
                        .push_back(MonadEvent::StateSyncEvent(StateSyncEvent::DoneSync(
                            GENESIS_SEQ_NUM,
                        )));
                }
                StateSyncCommand::RequestSync(eth_header) => {
                    assert!(!self.started_execution);
                    assert_eq!(self.request, None);
                    let request = StateSyncRequest {
                        version: SELF_STATESYNC_VERSION,
                        target: eth_header.seq_num().0,
                        from: 0,
                        prefix: 0,
                        prefix_bytes: 1,
                        until: 0,
                        old_target: 0,
                    };
                    self.request = Some(request);
                    self.events.extend(self.peers.iter().map(|peer| {
                        MonadEvent::StateSyncEvent(StateSyncEvent::Outbound(
                            *peer,
                            StateSyncNetworkMessage::Request(request),
                        ))
                    }))
                }
                StateSyncCommand::Message((from, message)) => match message {
                    StateSyncNetworkMessage::Request(request) => {
                        if self.started_execution {
                            let state = self.state_backend.lock().unwrap();
                            if let Some(state) = state.committed_state(&SeqNum(request.target)) {
                                let serialized = serde_json::to_vec(state).unwrap();
                                let response = StateSyncResponse {
                                    version: SELF_STATESYNC_VERSION,
                                    nonce: 0,
                                    response_index: 0,

                                    request,
                                    response: vec![StateSyncUpsert::new(
                                        StateSyncUpsertType::Code,
                                        serialized,
                                    )],
                                    response_n: 1,
                                };
                                self.events.push_back(MonadEvent::StateSyncEvent(
                                    StateSyncEvent::Outbound(
                                        from,
                                        StateSyncNetworkMessage::Response(response),
                                    ),
                                ))
                            }
                        }
                    }
                    StateSyncNetworkMessage::Response(response) => {
                        if !self.started_execution
                            && self
                                .request
                                .is_some_and(|request| request == response.request)
                        {
                            self.request = None;
                            let deserialized =
                                serde_json::from_slice(&response.response[0].data).unwrap();
                            let mut old_state = self.state_backend.lock().unwrap();
                            old_state.reset_state(deserialized);
                            self.events.push_back(MonadEvent::StateSyncEvent(
                                StateSyncEvent::DoneSync(SeqNum(response.request.target)),
                            ));
                        }
                    }
                },
            }
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        Default::default()
    }
}

impl<ST, SCT, EPT> MockStateSyncExecutor<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub fn new(
        state_backend: InMemoryState,
        peers: Vec<NodeId<CertificateSignaturePubKey<ST>>>,
    ) -> Self {
        Self {
            events: Default::default(),

            state_backend,
            peers,

            started_execution: false,
            request: None,

            waker: None,
        }
    }
}

impl<ST, SCT, EPT> MockableStateSync for MockStateSyncExecutor<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type Signature = ST;
    type SignatureCollection = SCT;
    type ExecutionProtocol = EPT;

    fn ready(&self) -> bool {
        !self.events.is_empty()
    }

    fn pop(&mut self) -> Option<MonadEvent<ST, SCT, EPT>> {
        self.events.pop_front()
    }
}

impl<ST, SCT, EPT> Stream for MockStateSyncExecutor<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type Item = MonadEvent<ST, SCT, EPT>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(Some(event));
        }
        self.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}
