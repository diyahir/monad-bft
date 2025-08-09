use std::sync::Arc;

use futures::future::BoxFuture;
use mongodb::{error::Error as MongoError, ClientSession};

use crate::{
    model::{BlockReader, WriterError},
    mongo_model::MongoImpl,
};

impl<BR: BlockReader> MongoImpl<BR> {
    pub(super) async fn with_mtransaction<T, F, R>(
        &self,
        resources: Arc<R>,
        mut work: F,
    ) -> Result<T, WriterError>
    where
        F: for<'s> FnMut(
            &'s mut ClientSession,
            Self,
            Arc<R>,
        ) -> BoxFuture<'s, Result<T, MongoError>>,
        R: Send + Sync + 'static,
    {
        let mut attempt = 0usize;

        loop {
            let mut session = self.client.start_session().await?;
            session.start_transaction().await?;

            match work(&mut session, self.clone(), Arc::clone(&resources)).await {
                Ok(val) => match session.commit_transaction().await {
                    Ok(()) => return Ok(val),
                    Err(e) if is_retryable_error(&e) && attempt < self.max_retries => {
                        attempt += 1;
                    }
                    Err(e) => return Err(WriterError::NetworkError(e.into())),
                },
                Err(e) => {
                    let _ = session.abort_transaction().await;
                    if is_retryable_error(&e) && attempt < self.max_retries {
                        attempt += 1;
                    } else {
                        return Err(WriterError::NetworkError(e.into()));
                    }
                }
            }

            // backoff
            let delay_ms = 25u64.saturating_mul(1u64 << attempt.min(6)); // 25,50,100,200,400,800,1600...
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms.min(1600))).await;
        }
    }
}

/// Internal: classify retryable transaction errors via labels.
fn is_transient_txn_error(e: &MongoError) -> bool {
    e.contains_label(mongodb::error::TRANSIENT_TRANSACTION_ERROR)
}
fn is_unknown_commit_result(e: &MongoError) -> bool {
    e.contains_label(mongodb::error::UNKNOWN_TRANSACTION_COMMIT_RESULT)
}

fn is_retryable_error(e: &MongoError) -> bool {
    is_transient_txn_error(e) || is_unknown_commit_result(e)
}
