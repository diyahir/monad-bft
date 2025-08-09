use std::sync::Arc;

use futures::future::BoxFuture;
use mongodb::{error::Error as MongoError, ClientSession};

use crate::{
    model::{BlockReader, WriterError},
    mongo_model::MongoImpl,
};

impl<BR: BlockReader> MongoImpl<BR> {
    /// Run `work` inside a MongoDB multi-document transaction with retries.
    ///
    /// **Example:**
    /// ```ignore
    /// self.with_mtransaction(
    ///     header_doc,
    ///     |sesh, db, header_doc| {
    ///         async move {
    ///             db.headers
    ///                 .replace_one(header_doc.to_key(), header_doc)
    ///                 .upsert(true)
    ///                 .session(&mut *sesh)
    ///                 .await?;
    ///
    ///             db.update_latest_with_session(block_number, Some(sesh))
    ///                 .await
    ///         }
    ///         .boxed()
    ///     },
    /// )
    /// .await?;
    /// ```
    ///
    /// Behavior:
    /// - Starts a client session, begins a transaction, then calls
    ///   `work(session, &self, Arc::clone(&res))`.
    /// - Retries on retryable transaction/commit errors up to `self.max_retries`,
    ///   using capped exponential backoff (25ms doubling up to 1600ms).
    /// - Aborts the transaction on `work` error; commits on success; returns the
    ///   value from `work` on a successful commit.
    ///
    /// Parameters:
    /// - `res`: arbitrary resources made available to each attempt as `Arc<R>`.
    /// - `work`: closure that performs all DB operations using the provided
    ///   `&mut ClientSession` and `&Self`, and returns a `BoxFuture` tied to the
    ///   session lifetime. The closure must be idempotent because it may run
    ///   multiple times on retries.
    ///
    /// Lifetimes:
    /// - The future returned by `work` is bound to the session lifetime. Do not
    ///   move the session; always reborrow it in driver builders, e.g.
    ///   `.session(&mut *session)`.
    ///
    /// Retry semantics:
    /// - Retries on `TransientTransactionError` and `UnknownTransactionCommitResult`.
    /// - Any other error is treated as non-retryable and returned after aborting.
    ///
    /// Guidance:
    /// - Keep transactions short and bounded. Aim for ≤ ~1000 document mutations
    ///   per transaction and split large batches into chunks.
    /// - Do heavy CPU or serialization work before calling this function so the
    ///   transaction window stays small.
    ///
    /// Returns:
    /// - `Ok(T)` with the committed result from `work`, or `Err(WriterError)` if
    ///   the transaction ultimately fails.
    ///
    pub(super) async fn with_mtransaction<T, R, F>(
        &self,
        res: Arc<R>,
        mut work: F,
    ) -> Result<T, WriterError>
    where
        R: Send + Sync + 'static,
        F: for<'s> FnMut(
            &'s mut ClientSession,
            &'s Self,
            Arc<R>,
        ) -> BoxFuture<'s, Result<T, MongoError>>,
    {
        let mut attempt = 0;

        loop {
            let mut session = self.client.start_session().await?;
            session.start_transaction().await?;

            match work(&mut session, &self, Arc::clone(&res)).await {
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

            // TODO: use some abstraction for exponential backoff
            let delay_ms = 25_u64.saturating_mul(1 << attempt.min(6));
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
