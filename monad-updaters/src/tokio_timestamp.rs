use std::{
    marker::PhantomData,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll},
    time::{SystemTime, UNIX_EPOCH},
};

use futures::Stream;
use monad_consensus_types::{block::ExecutionProtocol, signature_collection::SignatureCollection};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{MonadEvent, TimestampCommand};
use tokio::time::{Duration, Interval};

use crate::timestamp::TimestampAdjuster;

pub struct TokioTimestamp<ST, SCT, EPT> {
    /// create timestamp events at this interval
    interval: Interval,
    adjuster: TimestampAdjuster,
    metrics: ExecutorMetrics,
    _phantom: PhantomData<(ST, SCT, EPT)>,
}

impl<ST, SCT, EPT> TokioTimestamp<ST, SCT, EPT> {
    pub fn new(period: Duration, max_delta: u64, adjustment_period: usize) -> Self {
        Self {
            interval: tokio::time::interval(period),
            adjuster: TimestampAdjuster::new(max_delta, adjustment_period),
            metrics: Default::default(),
            _phantom: PhantomData,
        }
    }
}

impl<ST, SCT, EPT> Executor for TokioTimestamp<ST, SCT, EPT> {
    type Command = TimestampCommand;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            match command {
                TimestampCommand::AdjustDelta(t) => self.adjuster.handle_adjustment(t),
            }
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        self.metrics.as_ref().into()
    }
}

impl<ST, SCT, EPT> Stream for TokioTimestamp<ST, SCT, EPT>
where
    Self: Unpin,
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type Item = MonadEvent<ST, SCT, EPT>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.deref_mut();

        match this.interval.poll_tick(cx) {
            Poll::Ready(_) => {
                let start = SystemTime::now();
                let epoch_time = start
                    .duration_since(UNIX_EPOCH)
                    .expect("Clock may have gone backwards");
                let mut t: i64 = epoch_time
                    .as_millis()
                    .try_into()
                    .expect("its not 300 million years since the epoch");
                // t += self.adjuster.get_adjustment();

                if t < 0 {
                    t = 0;
                }

                Poll::Ready(Some(MonadEvent::TimestampUpdateEvent(
                    t.try_into().unwrap(),
                )))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
