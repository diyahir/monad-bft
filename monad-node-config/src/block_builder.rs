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

use monad_crypto::certificate_signature::PubKey;
use monad_types::{deserialize_pubkey, serialize_pubkey};
use serde::{Deserialize, Serialize};

/// Configuration for external block builder transaction prioritization
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
#[serde(bound = "P: PubKey")]
pub struct BlockBuilderConfig<P: PubKey> {
    /// Enable block builder transaction prioritization
    #[serde(default)]
    pub enabled: bool,

    /// Maximum number of block builder transactions per block
    #[serde(default = "default_max_builder_txs")]
    pub max_builder_txs: usize,

    /// Maximum number of builder transactions to store in memory
    #[serde(default = "default_max_pool_size")]
    pub max_pool_size: usize,

    /// API endpoint for receiving builder transactions (optional)
    pub api_endpoint: Option<String>,

    /// Authorized block builder identities
    #[serde(default)]
    #[serde(bound = "P: PubKey")]
    pub authorized_builders: Vec<BlockBuilderIdentityConfig<P>>,

    /// Maximum age of builder bundles in seconds (replay protection)
    #[serde(default = "default_max_bundle_age_secs")]
    pub max_bundle_age_secs: u64,
}

/// Configuration for an authorized block builder identity
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct BlockBuilderIdentityConfig<P: PubKey> {
    /// Human-readable name for this builder (optional)
    pub name: Option<String>,

    /// Public key for this authorized builder
    #[serde(serialize_with = "serialize_pubkey::<_, P>")]
    #[serde(deserialize_with = "deserialize_pubkey::<_, P>")]
    #[serde(bound = "P: PubKey")]
    pub pubkey: P,
}

impl<P: PubKey> Default for BlockBuilderConfig<P> {
    fn default() -> Self {
        Self {
            enabled: false,
            max_builder_txs: default_max_builder_txs(),
            max_pool_size: default_max_pool_size(),
            api_endpoint: None,
            authorized_builders: Vec::new(),
            max_bundle_age_secs: default_max_bundle_age_secs(),
        }
    }
}

fn default_max_builder_txs() -> usize {
    1000
}

fn default_max_pool_size() -> usize {
    5000
}

fn default_max_bundle_age_secs() -> u64 {
    300 // 5 minutes
}
