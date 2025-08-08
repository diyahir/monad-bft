
use mongodb::{
    Client, Collection, Database,
};

use crate::model::{BlockReader, Versioned};

mod reader;
mod writer;
mod types;

pub use types::*;

pub struct MongoImpl<BR: BlockReader> {
    pub client: Client,
    pub db: Database,
    pub replica_name: String,
    pub(crate) headers: Collection<HeaderDoc>,
    pub(crate) txs: Collection<TxDoc>,

    pub block_reader: BR,
}