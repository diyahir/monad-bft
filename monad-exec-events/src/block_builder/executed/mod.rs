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

use itertools::Itertools;
use monad_event_ring::{EventDescriptor, EventDescriptorPayload};

use self::state::{BlockReassemblyState, TxnReassemblyState};
use super::{BlockBuilderError, BlockBuilderResult, ReassemblyError};
use crate::{
    ffi::{monad_c_bytes32, monad_exec_txn_start},
    ExecEvent, ExecEventDecoder, ExecEventRef, ExecutedBlock, ExecutedTxn, ExecutedTxnCallFrame,
    ExecutedTxnLog,
};

mod state;

/// Reassembles execution events from event ring event descriptors into full execution blocks.
#[derive(Debug, Default)]
pub struct ExecutedBlockBuilder {
    state: Option<BlockReassemblyState>,
    include_call_frames: bool,
}

impl ExecutedBlockBuilder {
    /// Creates a new [`ExecutedBlockBuilder`].
    pub fn new(include_call_frames: bool) -> Self {
        Self {
            state: None,
            include_call_frames,
        }
    }

    /// Processes the execution event in the provided event descriptor.
    pub fn process_event_descriptor<'ring>(
        &mut self,
        event_descriptor: &EventDescriptor<'ring, ExecEventDecoder>,
    ) -> Option<BlockBuilderResult<ExecutedBlock>> {
        match event_descriptor.try_filter_map(if self.include_call_frames {
            Self::select_block_event_refs::<true>
        } else {
            Self::select_block_event_refs::<false>
        }) {
            EventDescriptorPayload::Payload(Some(exec_event)) => {
                self.process_exec_event(exec_event)
            }
            EventDescriptorPayload::Payload(None) => None,
            EventDescriptorPayload::Expired => {
                self.reset();

                Some(Err(BlockBuilderError::PayloadExpired))
            }
        }
    }

    /// Resets the state of the block builder.
    ///
    /// <div class="warning">
    ///
    /// This method **must** be called before giving [`self`](ExecutedBlockBuilder) an event
    /// descriptor that is out of order. Failing to do so will cause the [`ExecutedBlockBuilder`] to
    /// eventually produce a [`BlockBuilderError::ImplicitDrop`] as the block reassembly will fail.
    ///
    /// See [`BlockBuilderError::ImplicitDrop`] and [`ReassemblyError`] for more details.
    ///
    /// </div>
    pub fn reset(&mut self) {
        self.state = None;
    }

    fn select_block_event_refs<const INCLUDE_CALL_FRAMES: bool>(
        event_ref: ExecEventRef<'_>,
    ) -> Option<ExecEvent> {
        match event_ref {
            ExecEventRef::BlockPerfEvmEnter
            | ExecEventRef::BlockPerfEvmExit
            | ExecEventRef::BlockQC(_)
            | ExecEventRef::BlockFinalized(_)
            | ExecEventRef::BlockVerified(_)
            | ExecEventRef::TxnPerfEvmEnter
            | ExecEventRef::TxnPerfEvmExit => None,

            ExecEventRef::TxnCallFrame { .. } if !INCLUDE_CALL_FRAMES => None,

            event => Some(event.into_owned()),
        }
    }

    fn process_exec_event(
        &mut self,
        exec_event: ExecEvent,
    ) -> Option<BlockBuilderResult<ExecutedBlock>> {
        match exec_event {
            ExecEvent::BlockPerfEvmEnter
            | ExecEvent::BlockPerfEvmExit
            | ExecEvent::BlockQC(_)
            | ExecEvent::BlockFinalized(_)
            | ExecEvent::BlockVerified(_)
            | ExecEvent::TxnPerfEvmEnter
            | ExecEvent::TxnPerfEvmExit => unreachable!(),

            ExecEvent::TxnCallFrame { .. } if !self.include_call_frames => unreachable!(),

            ExecEvent::AccountAccessListHeader { txn_index, header } => {
                let state = self.state.as_mut()?;

                let txn_ref = state
                    .txns
                    .get_mut(TryInto::<usize>::try_into(txn_index).unwrap())
                    .expect("ExecutedBlockBuilder AccountAccessListHeader txn_index within bounds")
                    .as_mut()
                    .expect("ExecutedBlockBuilder AccountAccessListHeader after TxnStart");

                // Prepare account slot for storage keys; header contains access_list_count but we build dynamically
                let account_index = txn_ref.access_list_accounts.len();
                txn_ref.access_list_accounts.push(ffi::monad_c_access_list_entry {
                    address: header.address,
                    storage_key_count: 0,
                });
                txn_ref.access_list_storage_keys.push(Vec::new());

                debug_assert_eq!(account_index + 1, txn_ref.access_list_accounts.len());

                None
            }
            ExecEvent::AccountAccess { txn_index, account_index, access } => {
                let state = self.state.as_mut()?;

                let txn_ref = state
                    .txns
                    .get_mut(TryInto::<usize>::try_into(txn_index).unwrap())
                    .expect("ExecutedBlockBuilder AccountAccess txn_index within bounds")
                    .as_mut()
                    .expect("ExecutedBlockBuilder AccountAccess after TxnStart");

                // Ensure storage vector exists up to account_index
                if txn_ref.access_list_storage_keys.len() <= account_index {
                    txn_ref
                        .access_list_storage_keys
                        .resize_with(account_index + 1, Vec::new);
                }
                if txn_ref.access_list_accounts.len() <= account_index {
                    txn_ref
                        .access_list_accounts
                        .resize(account_index + 1, ffi::monad_c_access_list_entry {
                            address: ffi::monad_c_address { bytes: [0; 20] },
                            storage_key_count: 0,
                        });
                }
                // Set address (AccountAccess provides address); header is already added earlier too
                txn_ref.access_list_accounts[account_index].address = access.address;

                None
            }
            ExecEvent::StorageAccess { txn_index, account_index, storage } => {
                let state = self.state.as_mut()?;

                let txn_ref = state
                    .txns
                    .get_mut(TryInto::<usize>::try_into(txn_index).unwrap())
                    .expect("ExecutedBlockBuilder StorageAccess txn_index within bounds")
                    .as_mut()
                    .expect("ExecutedBlockBuilder StorageAccess after TxnStart");

                if txn_ref.access_list_storage_keys.len() <= account_index {
                    txn_ref
                        .access_list_storage_keys
                        .resize_with(account_index + 1, Vec::new);
                }
                txn_ref.access_list_storage_keys[account_index].push(storage.slot);

                None
            }
            ExecEvent::BlockStart(block_header) => {
                if let Some(dropped_state) = self.state.take() {
                    return Some(Err(BlockBuilderError::ImplicitDrop {
                        block: dropped_state.start,
                        reassembly_error: ReassemblyError::UnterminatedBlock {
                            unexpected_header: block_header,
                        },
                    }));
                }

                let txn_count = block_header.exec_input.txn_count.try_into().unwrap();

                let mut txns = Vec::with_capacity(txn_count);
                txns.resize_with(txn_count, || None);

                self.state = Some(BlockReassemblyState {
                    start: block_header,
                    txns: txns.into_boxed_slice(),
                });

                None
            }
            ExecEvent::BlockReject(_) => {
                let state = self.state.as_mut()?;

                self.reset();

                Some(Err(BlockBuilderError::Rejected))
            }
            ExecEvent::BlockEnd(block_result) => {
                let BlockReassemblyState {
                    start: header,
                    txns,
                } = self.state.take()?;

                Some(Ok(ExecutedBlock {
                    start: header,
                    end: block_result,
                    txns: txns
                        .into_vec()
                        .into_iter()
                        .map(|txn_opt| {
                            txn_opt.expect("ExecutedBlockBuilder received TxnStart for txn")
                        })
                        .map(
                            |TxnReassemblyState {
                                 hash,
                                 sender,
                                 header,
                                 input,
                                 logs,
                                 output,
                                 call_frames,
                                 mut access_list_accounts,
                                 access_list_storage_keys,
                             }| {
                                let output = output
                                    .expect("ExecutedBlockBuilder received TxnEvmOutput for txn");

                                assert_eq!(logs.len(), output.receipt.log_count as usize);

                                // finalize access list counts by assigning per-account storage counts
                                for (idx, keys) in access_list_storage_keys.iter().enumerate() {
                                    if idx < access_list_accounts.len() {
                                        access_list_accounts[idx].storage_key_count = keys.len() as u32;
                                    }
                                }

                                ExecutedTxn {
                                    hash,
                                    sender,
                                    header,
                                    input,
                                    logs: logs.into_boxed_slice(),
                                    output,
                                    call_frames: call_frames.map(|call_frames| {
                                        assert_eq!(
                                            call_frames.len(),
                                            output.call_frame_count as usize
                                        );

                                        Vec::into_boxed_slice(call_frames)
                                    }),
                                    access_list_accounts: access_list_accounts.into_boxed_slice(),
                                    access_list_storage_keys: access_list_storage_keys
                                        .into_iter()
                                        .map(Vec::into_boxed_slice)
                                        .collect(),
                                }
                            },
                        )
                        .collect(),
                }))
            }
            ExecEvent::TxnStart {
                txn_index: index,
                txn_start,
                data_bytes,
                access_list_bytes,
            } => {
                let state = self.state.as_mut()?;

                let monad_exec_txn_start {
                    ingest_epoch_nanos,
                    txn_hash,
                    sender,
                    txn_header,
                } = txn_start;

                let txn_ref = state
                    .txns
                    .get_mut(TryInto::<usize>::try_into(index).unwrap())
                    .expect("ExecutedBlockBuilder TxnStart txn_index within bounds");

                assert!(txn_ref.is_none());

                // Parse access list trailing bytes if access_list_count > 0
                let mut access_list_accounts = Vec::new();
                let mut access_list_storage_keys = Vec::new();
                if txn_header.access_list_count > 0 {
                    let mut cursor = access_list_bytes;
                    for _ in 0..txn_header.access_list_count {
                        // Read one monad_c_access_list_entry
                        let (entry, rest) = crate::events::bytes::split_ref_from_bytes::<ffi::monad_c_access_list_entry>(cursor)
                            .expect("valid access list entry bytes");
                        cursor = rest;
                        let storage_count = entry.storage_key_count as usize;
                        let mut keys = Vec::with_capacity(storage_count);
                        for _ in 0..storage_count {
                            let (key, r2) = crate::events::bytes::split_ref_from_bytes::<monad_c_bytes32>(cursor)
                                .expect("valid storage key bytes");
                            keys.push(*key);
                            cursor = r2;
                        }
                        access_list_accounts.push(*entry);
                        access_list_storage_keys.push(keys);
                    }
                    debug_assert!(cursor.is_empty(), "access_list_bytes had trailing data");
                }

                *txn_ref = Some(TxnReassemblyState {
                    hash: txn_hash,
                    sender,
                    header: txn_header,
                    input: data_bytes,
                    logs: Vec::default(),
                    output: None,
                    call_frames: self.include_call_frames.then(Vec::default),
                    access_list_accounts,
                    access_list_storage_keys,
                });

                None
            }
            ExecEvent::TxnReject { .. } => {
                let state = self.state.as_mut()?;

                self.reset();

                Some(Err(BlockBuilderError::Rejected))
            }
            ExecEvent::TxnEvmOutput { txn_index, output } => {
                let state = self.state.as_mut()?;

                let txn_ref = state
                    .txns
                    .get_mut(TryInto::<usize>::try_into(txn_index).unwrap())
                    .expect("ExecutedBlockBuilder TxnReceipt txn_index within bounds")
                    .as_mut()
                    .expect("ExecutedBlockBuilder TxnReceipt txn_index populated from preceding TxnStart");

                assert!(txn_ref.output.is_none());
                assert!(txn_ref.logs.is_empty());

                if self.include_call_frames {
                    let txn_call_frames = txn_ref
                        .call_frames
                        .as_mut()
                        .expect("ExecutedBlockBuilder TxnReassemblyState call_frames set to Some");

                    assert!(txn_call_frames.is_empty());
                }

                txn_ref.output = Some(output);

                None
            }
            ExecEvent::TxnLog {
                txn_index,
                txn_log,
                topic_bytes,
                data_bytes,
            } => {
                let state = self.state.as_mut()?;

                let txn_ref = state
                    .txns
                    .get_mut(TryInto::<usize>::try_into(txn_index).unwrap())
                    .expect("ExecutedBlockBuilder TxnLog txn_index within bounds")
                    .as_mut()
                    .expect(
                        "ExecutedBlockBuilder TxnLog txn_index populated from preceding TxnStart",
                    );

                assert_eq!(txn_ref.logs.len(), txn_log.index as usize);

                txn_ref.logs.push(ExecutedTxnLog {
                    address: txn_log.address,
                    topic: topic_bytes
                        .into_vec()
                        .into_iter()
                        .chunks(std::mem::size_of::<monad_c_bytes32>())
                        .into_iter()
                        .take(4)
                        .map(|chunk| monad_c_bytes32 {
                            bytes: chunk.collect_vec().try_into().unwrap(),
                        })
                        .collect(),
                    data: data_bytes,
                });

                None
            }
            ExecEvent::TxnCallFrame {
                txn_index,
                txn_call_frame,
                input_bytes,
                return_bytes,
            } => {
                let state = self.state.as_mut()?;

                let txn_ref = state
                    .txns
                    .get_mut(TryInto::<usize>::try_into(txn_index).unwrap())
                    .expect("ExecutedBlockBuilder TxnLog txn_index within bounds")
                    .as_mut()
                    .expect(
                        "ExecutedBlockBuilder TxnLog txn_index populated from preceding TxnStart",
                    );

                let txn_call_frames = txn_ref
                    .call_frames
                    .as_mut()
                    .expect("ExecutedBlockBuilder TxnReassemblyState call_frames set to Some");

                assert_eq!(txn_call_frames.len(), txn_call_frame.index as usize);

                txn_call_frames.push(ExecutedTxnCallFrame {
                    call_frame: txn_call_frame,
                    input: input_bytes,
                    r#return: return_bytes,
                });

                None
            }
            ExecEvent::TxnEnd => None,
            ExecEvent::EvmError(monad_exec_evm_error) => {
                let state = self.state.as_mut()?;

                unimplemented!("EvmError {monad_exec_evm_error:#?}");
            }
        }
    }
}

#[cfg(test)]
mod test {
    use monad_event_ring::{DecodedEventRing, EventNextResult};

    use crate::{block_builder::ExecutedBlockBuilder, BlockBuilderError, ExecSnapshotEventRing};

    fn run_block_builder(snapshot_name: &'static str, snapshot_zstd_bytes: &'static [u8]) {
        let snapshot =
            ExecSnapshotEventRing::new_from_zstd_bytes(snapshot_zstd_bytes, snapshot_name).unwrap();

        let mut event_reader = snapshot.create_reader();

        let mut block_builder = ExecutedBlockBuilder::default();

        loop {
            let event_descriptor = match event_reader.next_descriptor() {
                EventNextResult::NotReady => break,
                EventNextResult::Gap => panic!("snapshot cannot gap"),
                EventNextResult::Ready(event_descriptor) => event_descriptor,
            };

            let Some(result) = block_builder.process_event_descriptor(&event_descriptor) else {
                continue;
            };

            match result {
                Ok(executed_block) => {
                    eprintln!("{executed_block:#?}");
                }
                Err(BlockBuilderError::Rejected) => {
                    panic!("snapshot does not contain blocks that are rejected")
                }
                Err(BlockBuilderError::PayloadExpired) => panic!("payload expired on snapshot"),
                Err(BlockBuilderError::ImplicitDrop { .. }) => {
                    unreachable!()
                }
            }
        }
    }

    #[test]
    fn basic_test_ethereum_mainnet() {
        const SNAPSHOT_NAME: &str = "ETHEREUM_MAINNET_30B_15M";
        const SNAPSHOT_ZSTD_BYTES: &[u8] =
            include_bytes!("../../../test/data/exec-events-emn-30b-15m/snapshot.zst");

        run_block_builder(SNAPSHOT_NAME, SNAPSHOT_ZSTD_BYTES);
    }

    #[test]
    fn basic_test_monad_testnet() {
        const SNAPSHOT_NAME: &str = "MONAD_DEVNET_500B_GENESIS";
        const SNAPSHOT_ZSTD_BYTES: &[u8] =
            include_bytes!("../../../test/data/exec-events-mdn-500b-genesis/snapshot.zst");

        run_block_builder(SNAPSHOT_NAME, SNAPSHOT_ZSTD_BYTES);
    }
}
