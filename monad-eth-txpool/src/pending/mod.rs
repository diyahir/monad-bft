use indexmap::IndexMap;
use monad_consensus_types::txpool::TxPoolInsertionError;
use monad_eth_types::EthAddress;

pub use self::list::PendingTxList;
use crate::transaction::ValidEthTransaction;

mod list;

const MAX_ADDRESSES: usize = 16 * 1024;
const MAX_TXS: usize = 64 * 1024;
const PROMOTE_TXS_WATERMARK: usize = 48 * 1024;

/// Wrapper type to store byte-validated transactions and quickly query the total number of
/// transactions in the txs map.
#[derive(Clone, Debug, Default)]
pub struct PendingTxMap {
    txs: IndexMap<EthAddress, PendingTxList>,
    num_txs: usize,
}

impl PendingTxMap {
    pub fn is_empty(&self) -> bool {
        self.txs.is_empty()
    }

    pub fn num_txs(&self) -> usize {
        self.num_txs
    }

    pub fn is_at_promote_txs_watermark(&self) -> bool {
        self.num_txs >= PROMOTE_TXS_WATERMARK
    }

    pub fn try_add_tx(&mut self, tx: ValidEthTransaction) -> Result<(), TxPoolInsertionError> {
        if self.num_txs >= MAX_TXS {
            return Err(TxPoolInsertionError::PoolFull);
        }

        let num_addresses = self.txs.len();
        assert!(num_addresses <= MAX_ADDRESSES);

        match self.txs.entry(tx.sender()) {
            indexmap::map::Entry::Occupied(mut tx_list) => {
                if tx_list.get_mut().try_add(tx)? {
                    self.num_txs += 1;
                }
            }
            indexmap::map::Entry::Vacant(v) => {
                if num_addresses == MAX_ADDRESSES {
                    return Err(TxPoolInsertionError::PoolFull);
                }

                v.insert(PendingTxList::new(tx));
                self.num_txs += 1;
            }
        }

        Ok(())
    }

    pub fn remove(&mut self, address: &EthAddress) -> Option<PendingTxList> {
        self.txs.swap_remove(address)
    }

    pub fn split_off(&mut self, num_addresses: usize) -> IndexMap<EthAddress, PendingTxList> {
        if num_addresses >= self.txs.len() {
            self.num_txs = 0;
            return std::mem::take(&mut self.txs);
        }

        let mut split = self.txs.split_off(num_addresses);
        std::mem::swap(&mut split, &mut self.txs);

        self.num_txs = self
            .num_txs
            .checked_sub(split.values().map(PendingTxList::num_txs).sum())
            .expect("num txs does not underflow");

        split
    }
}
