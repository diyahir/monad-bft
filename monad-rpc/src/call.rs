use std::{cmp::min, path::Path};

use alloy_consensus::{Header, TxEip1559, TxLegacy};
use alloy_primitives::{Address, TxKind, Uint, U256, U64, U8};
use alloy_rpc_types::AccessList;
use monad_cxx::StateOverrideSet;
use monad_rpc_docs::rpc;
use monad_triedb_utils::triedb_env::{Triedb, TriedbPath};
use reth_primitives::Transaction;
use serde::{Deserialize, Serialize};

use crate::{
    block_handlers::get_block_num_from_tag,
    eth_json_types::BlockTags,
    hex,
    jsonrpc::{JsonRpcError, JsonRpcResult},
};

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CallRequest {
    pub from: Option<Address>,
    pub to: Option<Address>,
    pub gas: Option<U256>,
    #[serde(flatten)]
    pub gas_price_details: GasPriceDetails,
    pub value: Option<U256>,
    #[serde(flatten)]
    pub input: CallInput,
    pub nonce: Option<U64>,
    pub chain_id: Option<U64>,
    pub access_list: Option<Vec<u8>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_fee_per_blob_gas: Option<U256>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blob_versioned_hashes: Option<Vec<U256>>,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub transaction_type: Option<U8>,
}

impl schemars::JsonSchema for CallRequest {
    fn schema_name() -> String {
        "CallRequest".to_string()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed(concat!(module_path!(), "::NonGenericType"))
    }

    fn json_schema(_gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        let schema = schemars::schema_for_value!(CallRequest {
            from: None,
            to: None,
            gas: None,
            gas_price_details: GasPriceDetails::Eip1559 {
                max_fee_per_gas: Some(U256::default()),
                max_priority_fee_per_gas: Some(U256::default())
            },
            value: None,
            input: CallInput::default(),
            nonce: None,
            chain_id: None,
            access_list: None,
            max_fee_per_blob_gas: None,
            blob_versioned_hashes: None,
            transaction_type: None,
        });
        schema.schema.into()
    }
}

impl CallRequest {
    pub fn max_fee_per_gas(&self) -> Option<U256> {
        match self.gas_price_details {
            GasPriceDetails::Legacy { gas_price } => Some(gas_price),
            GasPriceDetails::Eip1559 {
                max_fee_per_gas: Some(max_fee_per_gas),
                ..
            } => Some(max_fee_per_gas),
            _ => None,
        }
    }

    pub fn fill_gas_prices(&mut self, base_fee: U256) -> Result<(), JsonRpcError> {
        match self.gas_price_details {
            GasPriceDetails::Legacy { mut gas_price } => {
                if gas_price < base_fee {
                    gas_price = base_fee;
                    self.gas_price_details = GasPriceDetails::Legacy { gas_price };
                }
            }
            GasPriceDetails::Eip1559 {
                max_fee_per_gas,
                max_priority_fee_per_gas,
            } => {
                let max_fee_per_gas = match max_fee_per_gas {
                    Some(mut max_fee_per_gas) => {
                        if max_fee_per_gas < base_fee {
                            max_fee_per_gas = base_fee;
                        }

                        if max_priority_fee_per_gas.is_some()
                            && max_fee_per_gas < max_priority_fee_per_gas.unwrap_or_default()
                        {
                            return Err(JsonRpcError::eth_call_error(
                                "priority fee greater than max".to_string(),
                                None,
                            ));
                        }

                        min(
                            max_fee_per_gas,
                            base_fee
                                .checked_add(max_priority_fee_per_gas.unwrap_or_default())
                                .ok_or_else(|| {
                                    JsonRpcError::eth_call_error("tip too high".to_string(), None)
                                })?,
                        )
                    }
                    None => base_fee
                        .checked_add(max_priority_fee_per_gas.unwrap_or_default())
                        .ok_or_else(|| {
                            JsonRpcError::eth_call_error("tip too high".to_string(), None)
                        })?,
                };

                self.gas_price_details = GasPriceDetails::Eip1559 {
                    max_fee_per_gas: Some(max_fee_per_gas),
                    max_priority_fee_per_gas,
                };
            }
        };
        Ok(())
    }
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CallInput {
    /// Transaction data
    pub input: Option<alloy_primitives::Bytes>,

    /// This is the same as `input` but is used for backwards compatibility:
    /// <https://github.com/ethereum/go-ethereum/issues/15628>
    pub data: Option<alloy_primitives::Bytes>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged, rename_all_fields = "camelCase")]
pub enum GasPriceDetails {
    Legacy {
        gas_price: U256,
    },
    Eip1559 {
        max_fee_per_gas: Option<U256>,
        max_priority_fee_per_gas: Option<U256>,
    },
}

impl Default for GasPriceDetails {
    fn default() -> Self {
        GasPriceDetails::Eip1559 {
            max_fee_per_gas: None,
            max_priority_fee_per_gas: None,
        }
    }
}

/// Optimistically create a typed Ethereum transaction from a CallRequest based on provided fields.
/// TODO: add support for other transaction types.
impl TryFrom<CallRequest> for Transaction {
    type Error = JsonRpcError;
    fn try_from(call_request: CallRequest) -> Result<Self, JsonRpcError> {
        match call_request {
            CallRequest {
                gas_price_details: GasPriceDetails::Legacy { gas_price },
                ..
            } => {
                // Legacy
                Ok(Transaction::Legacy(TxLegacy {
                    chain_id: call_request
                        .chain_id
                        .map(|id| id.try_into())
                        .transpose()
                        .map_err(|_| JsonRpcError::invalid_params())?,
                    nonce: call_request
                        .nonce
                        .unwrap_or_default()
                        .try_into()
                        .map_err(|_| JsonRpcError::invalid_params())?,
                    gas_price: gas_price
                        .try_into()
                        .map_err(|_| JsonRpcError::invalid_params())?,
                    gas_limit: call_request
                        .gas
                        .unwrap_or(Uint::from(u64::MAX))
                        .try_into()
                        .map_err(|_| JsonRpcError::invalid_params())?,
                    to: if let Some(to) = call_request.to {
                        TxKind::Call(to)
                    } else {
                        TxKind::Create
                    },
                    value: call_request.value.unwrap_or_default(),
                    input: call_request.input.input.unwrap_or_default(),
                }))
            }
            CallRequest {
                gas_price_details:
                    GasPriceDetails::Eip1559 {
                        max_fee_per_gas,
                        max_priority_fee_per_gas,
                    },
                ..
            } => {
                // EIP-1559
                Ok(Transaction::Eip1559(TxEip1559 {
                    chain_id: call_request
                        .chain_id
                        .unwrap_or_default()
                        .try_into()
                        .map_err(|_| JsonRpcError::invalid_params())?,
                    nonce: call_request
                        .nonce
                        .unwrap_or_default()
                        .try_into()
                        .map_err(|_| JsonRpcError::invalid_params())?,
                    max_fee_per_gas: max_fee_per_gas
                        .unwrap_or_default()
                        .try_into()
                        .map_err(|_| JsonRpcError::invalid_params())?,
                    max_priority_fee_per_gas: max_priority_fee_per_gas
                        .unwrap_or_default()
                        .try_into()
                        .map_err(|_| JsonRpcError::invalid_params())?,
                    gas_limit: call_request
                        .gas
                        .unwrap_or(Uint::from(u64::MAX))
                        .try_into()
                        .map_err(|_| JsonRpcError::invalid_params())?,
                    access_list: AccessList::default(),
                    to: if let Some(to) = call_request.to {
                        TxKind::Call(to)
                    } else {
                        // EIP-170
                        if let Some(code) = call_request.input.data.as_ref() {
                            if code.len() > 0x6000 {
                                return Err(JsonRpcError::code_size_too_large(code.len()));
                            }
                        }

                        TxKind::Create
                    },
                    value: call_request.value.unwrap_or_default(),
                    input: call_request.input.input.unwrap_or_default(),
                }))
            }
        }
    }
}

/// Subtract the effective gas price from the balance to get an accurate gas limit.
pub async fn sender_gas_allowance<T: Triedb>(
    triedb_env: &T,
    block: &Header,
    request: &CallRequest,
) -> Result<u64, JsonRpcError> {
    if let (Some(from), Some(gas_price)) = (request.from, request.max_fee_per_gas()) {
        if gas_price.is_zero() {
            return Ok(block.gas_limit);
        }

        let account = triedb_env
            .get_account(from.into(), block.number)
            .await
            .map_err(JsonRpcError::internal_error)?;

        let gas_limit = U256::from(account.balance)
            .checked_sub(request.value.unwrap_or_default())
            .ok_or_else(|| {
                JsonRpcError::eth_call_error(
                    "insufficient funds for gas * price + value".to_string(),
                    None,
                )
            })?
            .checked_div(gas_price)
            .ok_or_else(|| JsonRpcError::internal_error("zero gas price".into()))?;

        Ok(min(
            gas_limit.try_into().unwrap_or(block.gas_limit),
            block.gas_limit,
        ))
    } else {
        Ok(block.gas_limit)
    }
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct MonadEthCallParams {
    transaction: CallRequest,
    #[serde(default)]
    block: BlockTags,
    #[schemars(skip)] // TODO: move StateOverrideSet from monad-cxx
    #[serde(default)]
    state_overrides: StateOverrideSet, // empty = no state overrides
}

/// Executes a new message call immediately without creating a transaction on the block chain.
#[rpc(method = "eth_call", ignore = "chain_id")]
pub async fn monad_eth_call<T: Triedb + TriedbPath>(
    triedb_env: &T,
    execution_ledger_path: &Path,
    chain_id: u64,
    params: MonadEthCallParams,
) -> JsonRpcResult<String> {
    let mut params = params;
    params.transaction.input.input = match (
        params.transaction.input.input.take(),
        params.transaction.input.data.take(),
    ) {
        (Some(input), Some(data)) => {
            if input != data {
                return Err(JsonRpcError::invalid_params());
            }
            Some(input)
        }
        (None, data) | (data, None) => data,
    };

    let state_overrides = &params.state_overrides;

    // TODO: check duplicate address, duplicate storage key, etc.

    let block_num = get_block_num_from_tag(triedb_env, params.block).await?;
    let mut header = match triedb_env
        .get_block_header(block_num)
        .await
        .map_err(JsonRpcError::internal_error)?
    {
        Some(header) => header,
        None => {
            return Err(JsonRpcError::internal_error(
                "error getting block header".into(),
            ))
        }
    };

    let sender = params.transaction.from.unwrap_or_default();
    match sender {
        Address::ZERO => {
            // for eth_call from a zero address, we want to override the block base fee to be zero
            // so that reading from the smart contract does not require the zero address
            // to have any gas balance
            header.header.base_fee_per_gas = Some(0);
            params.transaction.fill_gas_prices(U256::from(0))?;

            if params.transaction.gas.is_none() {
                // eth_call from a zero address will default gas limit as block gas limit
                params.transaction.gas = Some(U256::from(header.header.gas_limit));
            }
        }
        _ => {
            params.transaction.fill_gas_prices(U256::from(
                header.header.base_fee_per_gas.unwrap_or_default(),
            ))?;

            if params.transaction.gas.is_none() {
                let allowance =
                    sender_gas_allowance(triedb_env, &header.header, &params.transaction).await?;
                params.transaction.gas = Some(U256::from(allowance));
            }
        }
    }

    if params.transaction.chain_id.is_none() {
        params.transaction.chain_id = Some(U64::from(chain_id));
    }

    let txn: Transaction = params.transaction.try_into()?;
    let block_number = header.header.number;
    match monad_cxx::eth_call(
        txn,
        header.header,
        sender,
        block_number,
        &triedb_env.path(),
        execution_ledger_path,
        state_overrides,
    ) {
        monad_cxx::CallResult::Success(monad_cxx::SuccessCallResult { output_data, .. }) => {
            Ok(hex::encode(&output_data))
        }
        monad_cxx::CallResult::Failure(error) => {
            Err(JsonRpcError::eth_call_error(error.message, error.data))
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::U256;
    use serde_json::json;

    use crate::{jsonrpc, tests::init_server};

    #[test]
    fn parse_call_request() {
        let payload = json!(
            {
                "from": "0xb60e8dd61c5d32be8058bb8eb970870f07233155",
                "to": "0xd46e8dd67c5d32be8058bb8eb970870f07244567",
                "gas": "0x76c0",
                "gasPrice": "0x9184e72a000",
                "value": "0x9184e72a",
                "data": "0xd46e8dd67c5d32be8d46e8dd67c5d32be8058bb8eb970870f072445675058bb8eb970870f072445675"
            }
        );
        let result = serde_json::from_value::<super::CallRequest>(payload).expect("parse failed");
        assert!(result.input.data.is_some());
        assert!(matches!(
            result.gas_price_details,
            super::GasPriceDetails::Legacy { gas_price: _ }
        ));
        assert_eq!(
            result.max_fee_per_gas(),
            Some(U256::from_str_radix("9184e72a000", 16).unwrap())
        );

        let payload = json!(
            {
                "from": "0xb60e8dd61c5d32be8058bb8eb970870f07233155",
                "to": "0xd46e8dd67c5d32be8058bb8eb970870f07244567",
                "gas": "0x76c0",
                "maxFeePerGas": "0x9184e72a000",
                "value": "0x9184e72a",
                "data": "0xd46e8dd67c5d32be8d46e8dd67c5d32be8058bb8eb970870f072445675058bb8eb970870f072445675"
            }
        );
        let result = serde_json::from_value::<super::CallRequest>(payload).expect("parse failed");
        assert!(matches!(
            result.gas_price_details,
            super::GasPriceDetails::Eip1559 {
                max_fee_per_gas: Some(_),
                ..
            }
        ));
        assert_eq!(
            result.max_fee_per_gas(),
            Some(U256::from_str_radix("9184e72a000", 16).unwrap())
        );
    }

    #[allow(non_snake_case)]
    #[actix_web::test]
    async fn test_monad_eth_call_sha256_precompile() {
        let (app, _monad) = init_server().await;
        let payload = json!({
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [
                {
                    "to": "0x0000000000000000000000000000000000000002",
                    "data": "0x68656c6c6f" // hex for "hello"
                },
                "latest"
            ],
            "id": 1
        });

        let req = actix_web::test::TestRequest::post()
            .uri("/")
            .set_payload(payload.to_string())
            .to_request();

        let resp: jsonrpc::Response = actix_test::call_and_read_body_json(&app, req).await;
        assert!(resp.result.is_none());
    }

    #[allow(non_snake_case)]
    #[actix_web::test]
    async fn test_monad_eth_call() {
        let (app, _monad) = init_server().await;
        let payload = json!({
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [
            {
                "from": "0xb60e8dd61c5d32be8058bb8eb970870f07233155",
                "to": "0xd46e8dd67c5d32be8058bb8eb970870f07244567",
                "gas": "0x76c0",
                "gasPrice": "0x9184e72a000",
                "value": "0x9184e72a",
                "data": "0xd46e8dd67c5d32be8d46e8dd67c5d32be8058bb8eb970870f072445675058bb8eb970870f072445675"
            },
            "latest"
            ],
            "id": 1
        });

        let req = actix_web::test::TestRequest::post()
            .uri("/")
            .set_payload(payload.to_string())
            .to_request();

        let resp: jsonrpc::Response = actix_test::call_and_read_body_json(&app, req).await;
        assert!(resp.result.is_none());
    }
}
