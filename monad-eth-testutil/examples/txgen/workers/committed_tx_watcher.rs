use alloy_sol_types::SolEvent;
use eyre::{Context, WrapErr};
use reth_rpc_types::{Block, FilterChanges, TransactionReceipt};
use serde_json::json;

use super::*;
use crate::shared::blockstream::BlockStream;

pub struct CommittedTxWatcher {
    sent_txs: Arc<DashMap<TxHash, Instant>>,
    metrics: Arc<Metrics>,
    delay: Duration,
    blockstream: BlockStream,
    client: ReqwestClient,

    // extra rpc flags
    use_receipts: bool,
    use_get_logs: bool,
    // use_by_hash: bool,
}

impl CommittedTxWatcher {
    pub async fn new(
        client: &ReqwestClient,
        sent_txs: &Arc<DashMap<TxHash, Instant>>,
        metrics: &Arc<Metrics>,
        delay: Duration,
        use_receipts: bool,
        use_get_logs: bool,
        // use_by_hash: bool,
    ) -> Self {
        Self {
            client: client.clone(),
            sent_txs: Arc::clone(sent_txs),
            metrics: Arc::clone(metrics),
            delay,
            blockstream: BlockStream::new(client.clone(), Duration::from_millis(50), false)
                .await
                .expect("Failed to fetch initial block number for blockstream"),

            use_receipts,
            use_get_logs,
            // use_by_hash,
        }
    }

    pub async fn run(mut self) {
        while let Some(block) = self.blockstream.next().await {
            let block = match block {
                Ok(b) => b,
                Err(e) => {
                    warn!("Blockstream returned error: {e}");
                    continue;
                }
            };

            let mut ours = 0;
            for hash in block.transactions.hashes() {
                if self.sent_txs.remove(hash).is_some() {
                    ours += 1;
                }
            }

            self.metrics.total_committed_txs.fetch_add(ours, SeqCst);

            let now = Instant::now();
            self.sent_txs.retain(|_, v| *v + self.delay > now);

            if self.use_receipts {
                if let Err(e) =
                    Self::receipts_for_block_slow(self.client.clone(), self.metrics.clone(), &block)
                        .await
                {
                    error!("Failed to get receipts for block: {e}");
                }
            }
            if self.use_get_logs {
                if let Err(e) =
                    Self::logs_for_block(self.client.clone(), self.metrics.clone(), &block).await
                {
                    error!("Failed to get logs for block: {e}");
                }
            }
        }
    }

    async fn logs_for_block(
        client: ReqwestClient,
        metrics: Arc<Metrics>,
        block: &reth_rpc_types::Block,
    ) -> Result<()> {
        let mut num_logs = 0;
        // let mut erc20_transfers = 0;
        // let mut erc20_value_transfered = U256::ZERO;

        let block_num = block
            .header
            .number
            .context("block number not present in header")?
            .to::<u32>();
        let params = serde_json::to_string(&json! {{
            "toBlock": block_num,
            "fromBlock": block_num,
        }})?;

        metrics.logs_rpc_calls.fetch_add(1, SeqCst);
        let rx: FilterChanges = client
            .request("eth_getLogs", [&params])
            .await
            .map_err(|e| {
                metrics.logs_rpc_calls_error.fetch_add(1, SeqCst);
                e
            })?;

        match rx {
            FilterChanges::Logs(logs) => {
                num_logs += logs.len();

                // todo: figure out how to parse logs into IERC20::Transfer event
                // for log in logs {
                //     if let Ok(transfer_data) = IERC20::Transfer::abi_decode_data(&log.data, false) {
                //         erc20_transfers += 1;
                //         erc20_value_transfered += transfer_data.0;
                //     }
                // }
            }
            FilterChanges::Hashes(_) | FilterChanges::Transactions(_) | FilterChanges::Empty => {
                warn!("Unexpected response from eth_getLogs")
            }
        }

        metrics.logs_total.fetch_add(num_logs, SeqCst);
        // metrics
        //     .logs_erc20_transfers
        //     .fetch_add(erc20_transfers, SeqCst);
        // if let Ok(mut val) = metrics.logs_erc20_total_value_transfered.write() {
        //     *val += erc20_value_transfered;
        // }
        Ok(())
    }

    async fn receipts_for_block(
        client: ReqwestClient,
        metrics: Arc<Metrics>,
        block: &Block,
    ) -> Result<()> {
        let mut tx_success = 0;
        let mut tx_failure = 0;
        let mut gas_consumed = U256::ZERO;
        let mut contract_addresses = Vec::new();

        let rxs: Vec<TransactionReceipt> = {
            let method = "eth_getBlockReceipts";
            let block_num = block.header.number.context("block number not found")?;
            let block_num = block_num.to::<u32>();

            metrics.receipts_rpc_calls.fetch_add(1, SeqCst);
            client.request(method, [block_num]).await.map_err(|e| {
                metrics.logs_rpc_calls_error.fetch_add(1, SeqCst);
                // todo: properly wrap error
                eyre::eyre!("Failed to get logs for block {block_num} {e}")
            })?
        };

        for rx in rxs {
            match rx.status_code {
                Some(status) if status.to::<u64>() == 1 => {
                    tx_success += 1;
                }
                _ => tx_failure += 1,
            }

            if let Some(gas_used) = rx.gas_used {
                gas_consumed += gas_used;
            }

            if let Some(contract_address) = rx.contract_address {
                contract_addresses.push(contract_address);
            }
        }

        metrics.receipts_tx_success.fetch_add(tx_success, SeqCst);
        metrics.receipts_tx_failure.fetch_add(tx_failure, SeqCst);
        if let Ok(mut x) = metrics.receipts_gas_consumed.write() {
            *x += gas_consumed;
        }
        Ok(())
    }

    async fn receipts_for_block_slow(
        client: ReqwestClient,
        metrics: Arc<Metrics>,
        block: &Block,
    ) -> Result<()> {
        let mut rpc_calls = 0;
        let mut rpc_calls_error = 0;
        let mut tx_success = 0;
        let mut tx_failure = 0;
        let mut gas_consumed = U256::ZERO;
        let mut contract_addresses = Vec::new();

        for hash in block.transactions.hashes() {
            rpc_calls += 1;
            let rx: TransactionReceipt =
                match client.request("eth_getTransactionReceipt", [hash]).await {
                    Ok(rx) => rx,
                    Err(e) => {
                        error!(tx_hash = hash.to_string(), "Failed to get rx for tx: {e}");
                        rpc_calls_error += 1;
                        continue;
                    }
                };
            tx_success += 1;

            match rx.status_code {
                Some(status) if status.to::<u64>() == 1 => {
                    tx_failure += 1;
                }
                _ => tx_failure += 1,
            }

            if let Some(gas_used) = rx.gas_used {
                gas_consumed += gas_used;
            }

            if let Some(contract_address) = rx.contract_address {
                contract_addresses.push(contract_address);
            }
        }

        metrics.receipts_rpc_calls.fetch_add(rpc_calls, SeqCst);
        metrics
            .receipts_rpc_calls_error
            .fetch_add(rpc_calls_error, SeqCst);
        metrics.receipts_tx_success.fetch_add(tx_success, SeqCst);
        metrics.receipts_tx_failure.fetch_add(tx_failure, SeqCst);
        if let Ok(mut x) = metrics.receipts_gas_consumed.write() {
            *x += gas_consumed;
        }
        Ok(())
    }
}
