use super::native_transfer_priority_fee;
use crate::prelude::*;

pub struct DuplicateTxGenerator {
    pub(crate) recipient_keys: SeededKeyPool,
    pub(crate) tx_per_sender: usize,
    pub random_priority_fee: bool,
}

impl Generator for DuplicateTxGenerator {
    fn handle_acct_group(
        &mut self,
        accts: &mut [SimpleAccount],
    ) -> Vec<(TransactionSigned, Address)> {
        let mut rng = SmallRng::from_entropy();
        let mut txs = Vec::with_capacity(self.tx_per_sender * accts.len());

        for sender in accts {
            let to = self.recipient_keys.next_addr(); // change sampling strategy?
            for _ in 0..self.tx_per_sender {
                let priority_fee = if self.random_priority_fee {
                    rng.gen_range(0..1000)
                } else {
                    0
                };
                let tx = native_transfer_priority_fee(sender, to, U256::from(10), priority_fee);
                txs.push((tx, to));
            }
        }

        txs
    }
}
