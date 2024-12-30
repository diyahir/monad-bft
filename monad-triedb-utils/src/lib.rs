use std::{
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc,
    },
};

use alloy_consensus::Header;
use alloy_rlp::Decodable;
use futures::{channel::oneshot, executor::block_on, future::join_all, FutureExt};
use key::Version;
use monad_eth_types::{EthAccount, EthAddress};
use monad_state_backend::{StateBackend, StateBackendError};
use monad_triedb::TriedbHandle;
use monad_types::{BlockId, Hash, Round, SeqNum};
use tracing::{debug, trace, warn};

use crate::{
    decode::rlp_decode_account,
    key::{create_triedb_key, KeyInput},
};

pub mod decode;
pub mod key;
pub mod triedb_env;

const MAX_TRIEDB_ASYNC_POLLS: usize = 640_000;

#[derive(Clone)]
pub struct TriedbReader {
    handle: TriedbHandle,
}

impl TriedbReader {
    /// for debug only
    pub unsafe fn handle(&self) -> &TriedbHandle {
        &self.handle
    }

    pub fn try_new(triedb_path: &Path) -> Option<Self> {
        TriedbHandle::try_new(triedb_path).map(|handle| Self { handle })
    }

    pub fn get_latest_finalized_block(&self) -> Option<SeqNum> {
        let latest_finalized = self.handle.latest_finalized_block()?;
        Some(SeqNum(latest_finalized))
    }

    pub fn get_earliest_finalized_block(&self) -> Option<SeqNum> {
        let earliest_finalized = self.handle.earliest_finalized_block()?;
        Some(SeqNum(earliest_finalized))
    }

    pub fn get_account_finalized(
        &self,
        seq_num: &SeqNum,
        eth_address: &[u8; 20],
    ) -> Option<EthAccount> {
        self.get_account(seq_num, Version::Finalized, eth_address)
    }

    pub fn get_finalized_eth_header(&self, seq_num: &SeqNum) -> Option<Header> {
        if self
            .raw_read_latest_finalized_block()
            .is_some_and(|latest_finalized| seq_num <= &latest_finalized)
        {
            let (triedb_key, key_len_nibbles) =
                create_triedb_key(Version::Finalized, KeyInput::BlockHeader);
            let eth_header_bytes = self.handle.read(&triedb_key, key_len_nibbles, seq_num.0)?;
            let mut rlp_buf = eth_header_bytes.as_slice();
            let block_header = Header::decode(&mut rlp_buf).expect("invalid rlp eth header");

            Some(block_header)
        } else {
            None
        }
    }

    pub fn get_proposed_eth_header(
        &self,
        block_id: &BlockId,
        seq_num: &SeqNum,
        round: &Round,
    ) -> Option<Header> {
        let bft_id = self.get_bft_id(seq_num, round)?;
        if block_id != &bft_id {
            return None;
        }

        let (triedb_key, key_len_nibbles) =
            create_triedb_key(Version::Proposal(*round), KeyInput::BlockHeader);
        let eth_header_bytes = self.handle.read(&triedb_key, key_len_nibbles, seq_num.0)?;

        let bft_id = self.get_bft_id(seq_num, round)?;
        if block_id != &bft_id {
            return None;
        }

        let mut rlp_buf = eth_header_bytes.as_slice();
        let block_header = Header::decode(&mut rlp_buf).expect("invalid rlp eth header");

        Some(block_header)
    }

    // only guaranteed to work for proposals
    pub fn get_bft_id(&self, seq_num: &SeqNum, round: &Round) -> Option<BlockId> {
        let (triedb_key, key_len_nibbles) =
            create_triedb_key(Version::Proposal(*round), KeyInput::BftBlock);
        let bft_block_id = self.handle.read(&triedb_key, key_len_nibbles, seq_num.0)?;

        let mut rlp_buf = bft_block_id.as_slice();
        let block_id = <[u8; 32]>::decode(&mut rlp_buf).expect("failed to decode rlp BlockId");

        Some(BlockId(Hash(block_id)))
    }

    // for accessing Version::Proposed, bft_id MUST BE VERIFIED
    pub fn get_account(
        &self,
        seq_num: &SeqNum,
        version: Version,
        eth_address: &[u8; 20],
    ) -> Option<EthAccount> {
        let (triedb_key, key_len_nibbles) =
            create_triedb_key(version, KeyInput::Address(eth_address));

        let result = self.handle.read(&triedb_key, key_len_nibbles, seq_num.0);

        let Some(account_rlp) = result else {
            debug!(?seq_num, ?eth_address, "account not found");
            return None;
        };

        rlp_decode_account(account_rlp)
    }

    fn get_accounts_async<'a>(
        &self,
        seq_num: &SeqNum,
        version: Version,
        eth_addresses: impl Iterator<Item = &'a EthAddress>,
    ) -> Option<Vec<Option<EthAccount>>> {
        // Counter which is updated when TrieDB processes a single async read to completion
        let completed_counter = Arc::new(AtomicUsize::new(0));

        let mut num_accounts = 0;
        let eth_account_receivers = eth_addresses.map(|eth_address| {
            num_accounts += 1;
            let (triedb_key, key_len_nibbles) =
                create_triedb_key(version, KeyInput::Address(eth_address.as_ref()));
            let (sender, receiver) = oneshot::channel();
            self.handle.read_async(
                triedb_key.as_ref(),
                key_len_nibbles,
                seq_num.0,
                completed_counter.clone(),
                sender,
            );
            receiver.map(|receiver_result| {
                // Receiver should not fail
                let maybe_rlp_account = receiver_result.expect("receiver can't be canceled");
                // RLP decode the received account value
                maybe_rlp_account.and_then(rlp_decode_account)
            })
        });

        // Join all futures of receivers
        let eth_account_receivers = join_all(eth_account_receivers);

        let mut poll_count = 0;
        // Poll TrieDB until the completed_counter reaches num_accounts
        // TODO: if initiating the reads had an error, this call to triedb_poll will not terminate.
        //       wrap this loop in a timeout so consensus doesn't get stuck
        while completed_counter.load(SeqCst) < num_accounts && poll_count < MAX_TRIEDB_ASYNC_POLLS {
            // blocking = true => wait to do more work
            // max_completions = usize::MAX => process as many completions before returning
            poll_count += self.handle.triedb_poll(true, usize::MAX);
        }
        // TrieDB should have completed processing all the async callbacks at this point
        if completed_counter.load(SeqCst) != num_accounts {
            warn!(
                "TrieDB poll count went over limit for {} lookups",
                num_accounts
            );
            return None;
        }

        Some(block_on(eth_account_receivers))
    }
}

impl StateBackend for TriedbReader {
    fn get_account_statuses<'a>(
        &self,
        block_id: &BlockId,
        seq_num: &SeqNum,
        round: &Round,
        eth_addresses: impl Iterator<Item = &'a EthAddress>,
    ) -> Result<Vec<Option<EthAccount>>, StateBackendError> {
        if self
            .raw_read_latest_finalized_block()
            .is_some_and(|latest_finalized| seq_num <= &latest_finalized)
        {
            trace!(?seq_num, "triedb read finalized");
            // check finalized

            // block <= latest
            let Some(statuses) =
                self.get_accounts_async(seq_num, Version::Finalized, eth_addresses)
            else {
                // TODO: Use a more descriptive error
                return Err(StateBackendError::NotAvailableYet);
            };

            let earliest = self
                .raw_read_earliest_finalized_block()
                .expect("earliest must exist if latest does");
            if seq_num < &earliest {
                // block < earliest
                return Err(StateBackendError::NeverAvailable);
            }
            // block >= earliest
            Ok(statuses)
        } else {
            // check proposed, validate block_id
            trace!(?seq_num, ?round, "triedb read proposed");

            let Some(bft_id) = self.get_bft_id(seq_num, round) else {
                return Err(StateBackendError::NotAvailableYet);
            };
            if &bft_id != block_id {
                return Err(StateBackendError::NotAvailableYet);
            }

            let Some(statuses) =
                self.get_accounts_async(seq_num, Version::Proposal(*round), eth_addresses)
            else {
                // TODO: Use a more descriptive error
                return Err(StateBackendError::NotAvailableYet);
            };

            // check that block wasn't overrwritten
            let Some(bft_id) = self.get_bft_id(seq_num, round) else {
                return Err(StateBackendError::NotAvailableYet);
            };
            if &bft_id != block_id {
                return Err(StateBackendError::NotAvailableYet);
            }

            Ok(statuses)
        }
    }

    fn raw_read_earliest_finalized_block(&self) -> Option<SeqNum> {
        self.get_earliest_finalized_block()
    }

    fn raw_read_latest_finalized_block(&self) -> Option<SeqNum> {
        self.get_latest_finalized_block()
    }
}
