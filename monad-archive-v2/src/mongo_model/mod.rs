use std::{
    future::IntoFuture,
    ops::{Deref, DerefMut},
};

use alloy_primitives::{
    hex::{FromHex, ToHexExt},
    FixedBytes,
};
use alloy_rlp::{Decodable, Encodable};
use aws_sdk_dynamodb::types::builders::DeleteReplicationGroupMemberActionBuilder;
use mongodb::{
    bson::{self, doc, Bson, Document},
    options::ReplaceOneModel,
    Client, Collection, Database,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    model::{make_block, BlockData, BlockReader, Tx, Versioned},
    prelude::*,
};

mod reader;
mod writer;
mod types;

pub use reader::*;
pub use writer::*;
pub use types::*;

pub struct MongoImpl<BR: BlockReader> {
    pub client: Client,
    pub db: Database,
    pub replica_name: String,
    pub(crate) headers: Collection<HeaderDoc>,
    pub(crate) txs: Collection<TxDoc>,

    pub block_reader: BR,
}