use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use itertools::Itertools;
use monad_eth_types::{EthAccount, EthAddress};
use monad_state_backend::{StateBackend, StateBackendError};
use monad_types::{BlockId, DropTimer, Round, SeqNum};
use tracing::{trace, warn};

#[derive(Debug)]
struct RoundCache {
    block_id: BlockId,
    seq_num: SeqNum,
    accounts: BTreeMap<EthAddress, Option<EthAccount>>,
}

#[derive(Debug)]
pub struct StateBackendCache<SBT> {
    // used so that StateBackendCache can maintain a logically immutable interface
    cache: Arc<Mutex<BTreeMap<Round, RoundCache>>>,
    state_backend: SBT,
    execution_delay: SeqNum,
}

impl<SBT> StateBackendCache<SBT>
where
    SBT: StateBackend,
{
    pub fn new(state_backend: SBT, execution_delay: SeqNum) -> Self {
        Self {
            cache: Default::default(),
            state_backend,
            execution_delay,
        }
    }
}

impl<SBT> StateBackend for StateBackendCache<SBT>
where
    SBT: StateBackend,
{
    fn get_account_statuses<'a>(
        &self,
        block_id: &BlockId,
        seq_num: &SeqNum,
        round: &Round,
        addresses: impl Iterator<Item = &'a EthAddress>,
    ) -> Result<Vec<Option<EthAccount>>, StateBackendError> {
        let addresses = addresses.collect_vec();
        if addresses.is_empty() {
            return Ok(Vec::new());
        }

        let mut cache = self.cache.lock().unwrap();

        // TODO consider removing this uniqueness filter... the callers we have so far already only
        // pass in a unique set of accounts
        let unique_addresses = addresses.iter().unique().copied();
        // find accounts that are missing from cache
        let cache_misses: Vec<_> = match cache.get(round) {
            None => unique_addresses.collect(),
            Some(round_cache) => {
                if &round_cache.block_id != block_id {
                    // drop cache, fetching new block_id for given round
                    cache.remove(round);
                    unique_addresses.collect()
                } else {
                    unique_addresses
                        .filter(|address| !round_cache.accounts.contains_key(address))
                        .collect()
                }
            }
        };

        if !cache_misses.is_empty() {
            // hydrate cache with missing accounts
            let cache_misses_data = {
                let _timer = DropTimer::start(Duration::from_millis(10), |elapsed| {
                    warn!(
                        ?elapsed,
                        lookups = cache_misses.len(),
                        "long get_account_statuses"
                    )
                });
                self.state_backend.get_account_statuses(
                    block_id,
                    seq_num,
                    round,
                    cache_misses.iter().copied(),
                )?
            };
            let hydrated_cache: Vec<_> = cache_misses
                .iter()
                .map(|&&address| address)
                .zip_eq(cache_misses_data)
                .collect();
            for (address, cache_miss) in &hydrated_cache {
                trace!(
                    ?block_id,
                    ?seq_num,
                    ?round,
                    ?address,
                    ?cache_miss,
                    "hydrated account"
                );
            }
            cache
                .entry(*round)
                .or_insert_with(|| RoundCache {
                    block_id: *block_id,
                    seq_num: *seq_num,
                    accounts: Default::default(),
                })
                .accounts
                .extend(hydrated_cache)
        }

        let round_cache = cache
            .get(round)
            .expect("cache must be populated... we asserted nonzero addresses at the start");

        assert_eq!(&round_cache.block_id, block_id);

        let accounts_data = addresses
            .iter()
            .map(|&address| {
                round_cache
                    .accounts
                    .get(address)
                    .expect("cache was hydrated")
            })
            .cloned()
            .collect();

        let last_finalized_block = self
            .raw_read_latest_finalized_block()
            .unwrap_or(SeqNum::MAX);

        while cache.first_entry().is_some_and(|entry| {
            (entry.get().seq_num + self.execution_delay) < last_finalized_block
        }) {
            let (evicted, _) = cache.pop_first().expect("nonempty");
            if &evicted == round {
                let (latest_round, _) = cache.last_key_value().expect("nonempty");
                tracing::warn!(
                    ?evicted,
                    ?round,
                    ?latest_round,
                    ?last_finalized_block,
                    "unexpected cache thrashing? only expect queries on the delay latest finalized blocks"
                );
            }
        }

        Ok(accounts_data)
    }

    fn raw_read_earliest_finalized_block(&self) -> Option<SeqNum> {
        self.state_backend.raw_read_earliest_finalized_block()
    }

    fn raw_read_latest_finalized_block(&self) -> Option<SeqNum> {
        self.state_backend.raw_read_latest_finalized_block()
    }
}
