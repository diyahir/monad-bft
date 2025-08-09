use std::sync::Arc;

use futures::future::BoxFuture;
use mongodb::{error::Error as MongoError, ClientSession};

use crate::{model::BlockReader, mongo_model::MongoImpl};

const MAX_RETRIES: usize = 5;

impl<BR: BlockReader> MongoImpl<BR> {
    pub(super) async fn with_mtransaction<T, F, R>(
        &self,
        resources: Arc<R>,
        mut work: F,
    ) -> Result<T, MongoError>
    where
        F: for<'s> FnMut(
            &'s mut ClientSession,
            Self,
            Arc<R>,
        ) -> BoxFuture<'s, Result<T, MongoError>>,
        R: Send + Sync + 'static,
    {
        let mut attempt = 0usize;
        let max_retries = MAX_RETRIES;

        loop {
            let mut session = self.client.start_session().await?;
            session.start_transaction().await?;

            match work(&mut session, self.clone(), Arc::clone(&resources)).await {
                Ok(val) => match session.commit_transaction().await {
                    Ok(()) => return Ok(val),
                    Err(e) if is_retryable_error(&e) && attempt < max_retries => {
                        attempt += 1;
                    }
                    Err(e) => return Err(e),
                },
                Err(e) => {
                    let _ = session.abort_transaction().await;
                    if is_retryable_error(&e) && attempt < max_retries {
                        attempt += 1;
                    } else {
                        return Err(e);
                    }
                }
            }

            // backoff
            let delay_ms = 25u64.saturating_mul(1u64 << attempt.min(6)); // 25,50,100,200,400,800,1600...
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms.min(1600))).await;
        }
    }
}

/// Run `work` inside a Mongo transaction with simple exponential backoff retries.
/// Retries on TransientTransactionError and UnknownTransactionCommitResult.
pub(super) async fn with_mtransaction<T, F, R>(
    client: &mongodb::Client,
    resources: Arc<R>,
    max_retries: usize,
    mut work: F,
) -> Result<T, MongoError>
where
    F: for<'s> FnMut(&'s mut ClientSession, Arc<R>) -> BoxFuture<'s, Result<T, MongoError>>,
    R: Send + Sync + 'static,
{
    let mut attempt = 0usize;

    loop {
        let mut session = client.start_session().await?;
        session.start_transaction().await?;

        match work(&mut session, Arc::clone(&resources)).await {
            Ok(val) => match session.commit_transaction().await {
                Ok(()) => return Ok(val),
                Err(e) if is_retryable_error(&e) && attempt < max_retries => {
                    attempt += 1;
                }
                Err(e) => return Err(e),
            },
            Err(e) => {
                let _ = session.abort_transaction().await;
                if is_retryable_error(&e) && attempt < max_retries {
                    attempt += 1;
                } else {
                    return Err(e);
                }
            }
        }

        // backoff
        let delay_ms = 25u64.saturating_mul(1u64 << attempt.min(6)); // 25,50,100,200,400,800,1600...
        tokio::time::sleep(std::time::Duration::from_millis(delay_ms.min(1600))).await;
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
