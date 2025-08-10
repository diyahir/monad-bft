use std::sync::Arc;

use mongodb::{Client, Collection, Database};
use thiserror::Error;

use crate::{errors::NetworkError, model::BlockReader, versioned::Versioned};

mod error_ext;
mod mtransaction;
mod reader;
mod types;
mod writer;

pub use types::*;

pub struct MongoImpl<BR: BlockReader> {
    pub(crate) inner: Arc<MongoImplInternal<BR>>,
}

impl<BR: BlockReader> Clone for MongoImpl<BR> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

pub struct MongoImplInternal<BR: BlockReader> {
    client: Client,
    db: Database,
    replica_name: String,
    latest: Collection<LatestDoc>,
    headers: Collection<HeaderDoc>,
    txs: Collection<TxDoc>,
    block_reader: BR,
    max_retries: usize,
}

impl<BR: BlockReader> std::ops::Deref for MongoImpl<BR> {
    type Target = MongoImplInternal<BR>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<BR: BlockReader> MongoImpl<BR> {
    pub async fn new(
        uri: String,
        replica_name: String,
        block_reader: BR,
        max_retries: usize,
    ) -> Result<Self, NetworkError> {
        let client = Client::with_uri_str(uri)
            .await
            .map_err(NetworkError::other)?;
        let db = client.database(&replica_name);

        let txs = db.collection("txs");
        let headers = db.collection("headers");
        // Document type is different but in same collection as headers
        let latest = headers.clone_with_type::<LatestDoc>();

        Ok(Self {
            inner: Arc::new(MongoImplInternal {
                client,
                db,
                replica_name,
                headers,
                latest,
                txs,
                block_reader,
                max_retries,
            }),
        })
    }
}
