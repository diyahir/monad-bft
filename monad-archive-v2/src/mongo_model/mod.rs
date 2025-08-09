use std::sync::Arc;

use mongodb::{Client, Collection, Database};

use crate::{model::BlockReader, versioned::Versioned};

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
