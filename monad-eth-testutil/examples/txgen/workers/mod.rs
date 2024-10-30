use std::{
    collections::HashSet,
    sync::atomic::{AtomicUsize, Ordering::SeqCst},
};

use futures::{stream::FuturesUnordered, StreamExt};
use reth_primitives::TransactionSigned;

use crate::{
    prelude::*,
    shared::{erc20::ERC20, eth_json_rpc::EthJsonRpc},
};

pub mod committed_tx_watcher;
pub mod gen_harness;
pub mod metrics;
pub mod recipient_tracker;
pub mod refresher;
pub mod rpc_sender;

pub use committed_tx_watcher::*;
pub use gen_harness::*;
pub use metrics::*;
pub use recipient_tracker::*;
pub use refresher::*;
pub use rpc_sender::*;

pub const BATCH_SIZE: usize = 500;

#[derive(Default)]
pub struct SimpleAccount {
    pub nonce: u64,
    pub native_bal: U256,
    pub erc20_bal: U256,
    pub key: PrivateKey,
    pub addr: Address,
}

pub struct Accounts {
    pub accts: Vec<SimpleAccount>,
    pub root: Option<SimpleAccount>,
}

impl From<Vec<SimpleAccount>> for Accounts {
    fn from(accts: Vec<SimpleAccount>) -> Self {
        Self { accts, root: None }
    }
}

impl Accounts {
    pub fn iter_mut(&mut self) -> AccountsIterMut {
        AccountsIterMut {
            slice: self.accts.iter_mut(),
            root: self.root.as_mut(),
            done: false,
        }
    }
    pub fn iter(&self) -> AccountsIter {
        AccountsIter {
            slice: self.accts.iter(),
            root: &self.root,
            done: false,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        self.accts.len() + if self.root.is_some() { 1 } else { 0 }
    }
}

impl<'a> IntoIterator for &'a Accounts {
    type Item = &'a SimpleAccount;
    type IntoIter = AccountsIter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[derive(Clone)]
pub struct AccountsIter<'a> {
    slice: std::slice::Iter<'a, SimpleAccount>,
    root: &'a Option<SimpleAccount>,
    done: bool,
}

impl<'a> Iterator for AccountsIter<'a> {
    type Item = &'a SimpleAccount;

    fn next(&mut self) -> Option<Self::Item> {
        match (self.slice.next(), self.root) {
            (Some(x), _) => Some(x),
            (None, Some(x)) => {
                if self.done {
                    None
                } else {
                    self.done = true;
                    Some(x)
                }
            }
            (None, None) => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.slice.len() + if self.root.is_some() { 1 } else { 0 };
        (len, Some(len))
    }
}

pub struct AccountsIterMut<'a> {
    slice: std::slice::IterMut<'a, SimpleAccount>,
    root: Option<&'a mut SimpleAccount>,
    done: bool,
}

impl<'a> Iterator for AccountsIterMut<'a> {
    type Item = &'a mut SimpleAccount;

    fn next(&mut self) -> Option<Self::Item> {
        match self.slice.next() {
            Some(x) => Some(x),
            None => {
                if self.done {
                    None
                } else {
                    self.done = true;
                    std::mem::take(&mut self.root)
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.slice.len() + if self.root.is_some() { 1 } else { 0 };
        (len, Some(len))
    }
}

impl<'a> IntoIterator for &'a mut Accounts {
    type Item = &'a mut SimpleAccount;

    type IntoIter = AccountsIterMut<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

pub struct AccountsWithTime {
    pub accts: Accounts,
    pub sent: Instant,
}

pub struct AddrsWithTime {
    pub addrs: HashSet<Address>,
    pub sent: Instant,
}

pub struct AccountsWithTxs {
    pub accts: Accounts,
    pub txs: Vec<(TransactionSigned, Address)>,
}

impl<'a> ExactSizeIterator for AccountsIter<'a> {}
impl<'a> ExactSizeIterator for AccountsIterMut<'a> {}

impl std::fmt::Display for SimpleAccount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Account{{ nonce: {}, native: {:+.2e}, erc20: {:+.2e}, addr: {}}}",
            self.nonce,
            self.native_bal.to::<u128>(),
            self.erc20_bal.to::<u128>(),
            self.addr,
        )
    }
}

impl From<(Address, PrivateKey)> for SimpleAccount {
    fn from((addr, key): (Address, PrivateKey)) -> Self {
        SimpleAccount {
            key,
            addr,
            ..Default::default()
        }
    }
}
