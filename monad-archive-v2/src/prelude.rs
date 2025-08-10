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

pub use std::{
    collections::{HashMap, HashSet},
    ffi::OsString,
    ops::RangeInclusive,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

pub use alloy_consensus::{
    Block as AlloyBlock, BlockBody, Header, ReceiptEnvelope, ReceiptWithBloom,
};
pub use alloy_primitives::{BlockHash, TxHash, U128, U256, U64};
pub use bytes::Bytes;
pub use eyre::{bail, eyre, Context, ContextCompat, OptionExt, Result};
pub use futures::{try_join, StreamExt, TryStream, TryStreamExt};
pub use monad_triedb_utils::triedb_env::{ReceiptWithLogIndex, TxEnvelopeWithSender};
pub use tokio::time::sleep;
pub use tracing::{debug, error, info, warn, Level};

pub use crate::{
    errors::{ReaderError, ReaderResult, WriterError, WriterResult},
    model::{
        Block, BlockRange, BlockReceipts, BlockTraces, EthGetLogsQuery, Reader, Tx, TxData,
        TxReceipt, TxTrace, Writer,
    },
    object_store::{ObjectStore, ObjectStoreReader},
};

/// Spawn a rayon task and wait for it to complete.
pub async fn spawn_rayon_async<F, R>(func: F) -> Result<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let (tx, rx) = tokio::sync::oneshot::channel();
    rayon::spawn(|| {
        let _ = tx.send(func());
    });
    rx.await.map_err(Into::into)
}
