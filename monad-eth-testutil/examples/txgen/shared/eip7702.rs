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

use std::time::Duration;

use alloy_consensus::{SignableTransaction, TxEip1559, TxEnvelope};
use alloy_eips::{eip2718::Encodable2718, eip7702::Authorization};
use alloy_primitives::{
    hex::{self, FromHex},
    keccak256, Address, Bytes, TxKind, U256,
};
use alloy_rlp::Encodable;
use alloy_rpc_client::ReqwestClient;
use alloy_sol_macro::sol;
use eyre::Result;
use serde::Deserialize;
use tokio::time::sleep;
use tracing::info;

use crate::{
    shared::{eth_json_rpc::EthJsonRpc, private_key::PrivateKey},
    SimpleAccount,
};

const BYTECODE: &str = include_str!("7702batchcall_bytecode.txt");

#[derive(Deserialize, Debug, Clone, Copy)]
#[serde(transparent)]
pub struct EIP7702 {
    pub addr: Address,
}

pub async fn ensure_contract_deployed(client: &ReqwestClient, addr: Address) -> Result<()> {
    let mut timeout = Duration::from_millis(200);
    for _ in 0..10 {
        info!(
            "Waiting {}ms for contract to be deployed...",
            timeout.as_millis()
        );
        sleep(timeout).await;

        let code = client.get_code(&addr).await?;
        if code != "0x" {
            info!(addr = addr.to_string(), "Deployed contract");
            return Ok(());
        }

        // else exponential backoff
        timeout *= 2;
    }

    Err(eyre::eyre!(
        "Failed to deployed contract {}",
        addr.to_string()
    ))
}

impl EIP7702 {
    pub async fn deploy(
        deployer: &(Address, PrivateKey),
        client: &ReqwestClient,
        max_fee_per_gas: u128,
        chain_id: u64,
    ) -> Result<Self> {
        let nonce = client.get_transaction_count(&deployer.0).await?;
        let tx = Self::deploy_tx(nonce, &deployer.1, max_fee_per_gas, chain_id);
        let mut rlp_encoded_tx = Vec::new();
        tx.encode_2718(&mut rlp_encoded_tx);

        let _: String = client
            .request(
                "eth_sendRawTransaction",
                [format!("0x{}", hex::encode(rlp_encoded_tx))],
            )
            .await?;

        let addr = calculate_contract_addr(&deployer.0, nonce);
        ensure_contract_deployed(client, addr).await?;
        Ok(EIP7702 { addr })
    }

    pub fn deploy_tx(
        nonce: u64,
        deployer: &PrivateKey,
        max_fee_per_gas: u128,
        chain_id: u64,
    ) -> TxEnvelope {
        let input = Bytes::from_hex(BYTECODE).unwrap();
        let tx = TxEip1559 {
            chain_id,
            nonce,
            gas_limit: 2_000_000,
            max_fee_per_gas,
            max_priority_fee_per_gas: 10,
            to: TxKind::Create,
            value: U256::ZERO,
            access_list: Default::default(),
            input,
        };

        let sig = deployer.sign_transaction(&tx);
        TxEnvelope::Eip1559(tx.into_signed(sig))
    }

    pub fn create_authorization(
        &self,
        authority: &(Address, PrivateKey),
        nonce: u64,
        chain_id: u64,
    ) -> Result<alloy_eips::eip7702::SignedAuthorization> {
        let authorization = Authorization {
            chain_id,
            address: self.addr,
            nonce,
        };

        let signature = authority.1.sign_hash(&authorization.signature_hash());
        Ok(authorization.into_signed(signature))
    }

    pub fn create_eip7702_tx(
        &self,
        sender: &mut SimpleAccount,
        authorized_account: Address,
        authorization: Vec<alloy_eips::eip7702::SignedAuthorization>,
        calldata: Bytes,
        max_fee_per_gas: u128,
        chain_id: u64,
    ) -> TxEnvelope {
        use alloy_consensus::TxEip7702;

        let tx = TxEip7702 {
            chain_id,
            nonce: sender.nonce,
            gas_limit: 200_000,
            max_fee_per_gas,
            max_priority_fee_per_gas: 0,
            to: authorized_account,
            value: U256::ZERO,
            access_list: Default::default(),
            input: calldata,
            authorization_list: authorization,
        };

        let sig = sender.key.sign_transaction(&tx);

        // Update sender state
        sender.nonce += 1;
        sender.native_bal = sender
            .native_bal
            .checked_sub(U256::from(200_000 * max_fee_per_gas))
            .unwrap_or(U256::ZERO);

        TxEnvelope::Eip7702(tx.into_signed(sig))
    }

    pub fn create_simple_call_tx(
        &self,
        sender: &mut SimpleAccount,
        max_fee_per_gas: u128,
        chain_id: u64,
    ) -> TxEnvelope {
        let calldata = Bytes::from(vec![0u8; 100]);

        let tx = TxEip1559 {
            chain_id,
            nonce: sender.nonce,
            gas_limit: 200_000,
            max_fee_per_gas,
            max_priority_fee_per_gas: 0,
            to: TxKind::Call(self.addr),
            value: U256::ZERO,
            access_list: Default::default(),
            input: calldata,
        };

        let sig = sender.key.sign_transaction(&tx);

        sender.nonce += 1;
        sender.native_bal = sender
            .native_bal
            .checked_sub(U256::from(200_000 * max_fee_per_gas))
            .unwrap_or(U256::ZERO);

        TxEnvelope::Eip1559(tx.into_signed(sig))
    }

    pub fn create_authorization_usage_tx(
        &self,
        sender: &mut SimpleAccount,
        authorized_account: Address,
        max_fee_per_gas: u128,
        chain_id: u64,
    ) -> TxEnvelope {
        let execute_calldata = self.create_execute_calldata(authorized_account);

        let tx = TxEip1559 {
            chain_id,
            nonce: sender.nonce,
            gas_limit: 200_000,
            max_fee_per_gas,
            max_priority_fee_per_gas: 0,
            to: TxKind::Call(authorized_account),
            value: U256::ZERO,
            access_list: Default::default(),
            input: execute_calldata,
        };

        let sig = sender.key.sign_transaction(&tx);

        sender.nonce += 1;
        sender.native_bal = sender
            .native_bal
            .checked_sub(U256::from(200_000 * max_fee_per_gas))
            .unwrap_or(U256::ZERO);

        TxEnvelope::Eip1559(tx.into_signed(sig))
    }

    fn create_execute_calldata(&self, target_account: Address) -> Bytes {
        use alloy_sol_types::SolCall;

        let call = BatchCallAndSponsor::Call {
            to: target_account,
            value: U256::ZERO,
            data: Bytes::from(vec![0u8; 32]),
        };

        let calls = vec![call];
        let execute_call = BatchCallAndSponsor::execute_1Call { calls };

        execute_call.abi_encode().into()
    }
}

pub fn calculate_contract_addr(deployer: &Address, nonce: u64) -> Address {
    let mut out = Vec::new();
    let enc: [&dyn Encodable; 2] = [&deployer, &nonce];
    alloy_rlp::encode_list::<_, dyn Encodable>(&enc, &mut out);
    let hash = keccak256(out);
    let (_, contract_address) = hash.as_slice().split_at(12);
    Address::from_slice(contract_address)
}

sol! {
    pragma solidity ^0.8.20;

    contract BatchCallAndSponsor {
        uint256 public nonce;

        struct Call {
            address to;
            uint256 value;
            bytes data;
        }

        event CallExecuted(address indexed sender, address indexed to, uint256 value);
        event BatchExecuted(uint256 indexed nonce, Call[] calls);

        function execute(Call[] calldata calls, bytes calldata signature) external payable;
        function execute(Call[] calldata calls) external payable;
    }
}
