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

use std::str::FromStr;

use aws_config::{
    meta::region::RegionProviderChain, retry::RetryConfig, timeout::TimeoutConfig, BehaviorVersion,
    Region, SdkConfig,
};
use eyre::{bail, OptionExt};
use futures::join;
use serde::{Deserialize, Serialize};

use crate::{
    kvstore::{mongo::MongoDbStorage, object_store::ObjectStoreKVStore},
    prelude::*,
};

const DEFAULT_BLOB_STORE_TIMEOUT: u64 = 30;
const DEFAULT_INDEX_STORE_TIMEOUT: u64 = 20;

pub fn set_source_and_sink_metrics(
    sink: &ArchiveArgs,
    source: &BlockDataReaderArgs,
    metrics: &Metrics,
) {
    match sink {
        ArchiveArgs::Aws(_) => {
            metrics.periodic_gauge_with_attrs(
                MetricNames::SINK_STORE_TYPE,
                1,
                vec![opentelemetry::KeyValue::new("sink_store_type", "aws")],
            );
        }
        ArchiveArgs::MongoDb(_) => {
            metrics.periodic_gauge_with_attrs(
                MetricNames::SINK_STORE_TYPE,
                2,
                vec![opentelemetry::KeyValue::new("sink_store_type", "mongodb")],
            );
        }
        ArchiveArgs::ObjectStore(_) => {
            metrics.periodic_gauge_with_attrs(
                MetricNames::SINK_STORE_TYPE,
                4, // 3 is for triedb
                vec![opentelemetry::KeyValue::new(
                    "sink_store_type",
                    "object_store",
                )],
            );
        }
    }

    match source {
        BlockDataReaderArgs::Aws(_) => {
            metrics.periodic_gauge_with_attrs(
                MetricNames::SOURCE_STORE_TYPE,
                1,
                vec![opentelemetry::KeyValue::new("source_store_type", "aws")],
            );
        }
        BlockDataReaderArgs::MongoDb(_) => {
            metrics.periodic_gauge_with_attrs(
                MetricNames::SOURCE_STORE_TYPE,
                2,
                vec![opentelemetry::KeyValue::new("source_store_type", "mongodb")],
            );
        }
        BlockDataReaderArgs::Triedb(_) => {
            metrics.periodic_gauge_with_attrs(
                MetricNames::SOURCE_STORE_TYPE,
                3,
                vec![opentelemetry::KeyValue::new("source_store_type", "triedb")],
            );
        }
        BlockDataReaderArgs::ObjectStore(_) => {
            metrics.periodic_gauge_with_attrs(
                MetricNames::SOURCE_STORE_TYPE,
                4,
                vec![opentelemetry::KeyValue::new(
                    "source_store_type",
                    "object_store",
                )],
            );
        }
    }
}

pub async fn get_aws_config(region: Option<String>, timeout_secs: u64) -> SdkConfig {
    let region_provider = RegionProviderChain::default_provider().or_else(
        region
            .map(Region::new)
            .unwrap_or_else(|| Region::new("us-east-2")),
    );

    info!(
        "Running in region: {}",
        region_provider
            .region()
            .await
            .map(|r| r.to_string())
            .unwrap_or("No region found".into())
    );

    aws_config::defaults(BehaviorVersion::latest())
        .region(region_provider)
        .timeout_config(
            TimeoutConfig::builder()
                .operation_timeout(Duration::from_secs(timeout_secs))
                .operation_attempt_timeout(Duration::from_secs(timeout_secs))
                .read_timeout(Duration::from_secs(timeout_secs))
                .build(),
        )
        .retry_config(RetryConfig::adaptive())
        .load()
        .await
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub enum BlockDataReaderArgs {
    Aws(AwsCliArgs),
    Triedb(TrieDbCliArgs),
    MongoDb(MongoDbCliArgs),
    ObjectStore(ObjectStoreCliArgs),
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub enum ArchiveArgs {
    Aws(AwsCliArgs),
    MongoDb(MongoDbCliArgs),
    ObjectStore(ObjectStoreCliArgs),
}

impl FromStr for BlockDataReaderArgs {
    type Err = eyre::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        use BlockDataReaderArgs::*;
        let mut words = s.split(' ');
        let Some(first) = words.next() else {
            bail!("Storage args string empty");
        };

        let next =
            |str: &'static str| -> Result<String> { Ok(words.next().ok_or_eyre(str)?.to_owned()) };

        Ok(match first.to_lowercase().as_str() {
            "aws" => Aws(AwsCliArgs::parse(next)?),
            "triedb" => Triedb(TrieDbCliArgs::parse(next)?),
            "mongodb" => MongoDb(MongoDbCliArgs::parse(next)?),
            "object-store" => ObjectStore(ObjectStoreCliArgs::parse(next)?),
            _ => {
                bail!("Unrecognized storage args variant: {first}");
            }
        })
    }
}

impl FromStr for ArchiveArgs {
    type Err = eyre::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        use ArchiveArgs::*;
        let mut words = s.split(' ');
        let Some(first) = words.next() else {
            bail!("Storage args string empty");
        };

        let next =
            |str: &'static str| -> Result<String> { Ok(words.next().ok_or_eyre(str)?.to_owned()) };

        Ok(match first.to_lowercase().as_str() {
            "aws" => Aws(AwsCliArgs::parse(next)?),
            "mongodb" => MongoDb(MongoDbCliArgs::parse(next)?),
            "object-store" => ObjectStore(ObjectStoreCliArgs::parse(next)?),
            _ => {
                bail!("Unrecognized storage args variant: {first}");
            }
        })
    }
}

impl BlockDataReaderArgs {
    pub async fn build(&self, metrics: &Metrics) -> Result<BlockDataReaderErased> {
        use BlockDataReaderArgs::*;
        Ok(match self {
            Triedb(args) => TriedbReader::new(args).into(),
            Aws(args) => BlockDataArchive::new(args.build_blob_store(metrics).await).into(),
            MongoDb(args) => BlockDataArchive::new(
                MongoDbStorage::new_block_store(&args.url, &args.db, metrics.clone()).await?,
            )
            .into(),
            ObjectStore(object_store_cli_args) => {
                let store = ObjectStoreKVStore::try_from(object_store_cli_args)?;
                BlockDataArchive::new(store).into()
            }
        })
    }

    pub fn replica_name(&self) -> String {
        use BlockDataReaderArgs::*;
        match self {
            Aws(aws_cli_args) => aws_cli_args.bucket.clone(),
            Triedb(trie_db_cli_args) => trie_db_cli_args.triedb_path.clone(),
            MongoDb(mongo_db_cli_args) => {
                format!("{}:{}", mongo_db_cli_args.url, mongo_db_cli_args.db)
            }
            ObjectStore(object_store_cli_args) => object_store_cli_args.url.clone(),
        }
    }
}

impl ArchiveArgs {
    pub async fn build_block_data_archive(&self, metrics: &Metrics) -> Result<BlockDataArchive> {
        let store = match self {
            ArchiveArgs::Aws(args) => args.build_blob_store(metrics).await,
            ArchiveArgs::MongoDb(args) => {
                MongoDbStorage::new_block_store(&args.url, &args.db, metrics.clone())
                    .await?
                    .into()
            }
            ArchiveArgs::ObjectStore(object_store_cli_args) => {
                ObjectStoreKVStore::try_from(object_store_cli_args)?.into()
            }
        };
        Ok(BlockDataArchive::new(store))
    }

    pub async fn build_index_archive(
        &self,
        metrics: &Metrics,
        max_inline_encoded_len: usize,
    ) -> Result<TxIndexArchiver> {
        let (blob, index) = match self {
            ArchiveArgs::Aws(args) => {
                join!(
                    args.build_blob_store(metrics),
                    args.build_index_store(metrics)
                )
            }
            ArchiveArgs::MongoDb(args) => (
                MongoDbStorage::new_block_store(&args.url, &args.db, metrics.clone())
                    .await?
                    .into(),
                MongoDbStorage::new_index_store(&args.url, &args.db, metrics.clone())
                    .await?
                    .into(),
            ),
            ArchiveArgs::ObjectStore(object_store_cli_args) => {
                warn!("Object store should not be used for index store.");
                (
                    ObjectStoreKVStore::try_from(object_store_cli_args)?.into(),
                    ObjectStoreKVStore::try_from(object_store_cli_args)?.into(),
                )
            }
        };
        Ok(TxIndexArchiver::new(
            index,
            BlockDataArchive::new(blob),
            max_inline_encoded_len,
        ))
    }

    pub async fn build_archive_reader(&self, metrics: &Metrics) -> Result<ArchiveReader> {
        let (blob, index) = match self {
            ArchiveArgs::Aws(args) => {
                join!(
                    args.build_blob_store(metrics),
                    args.build_index_store(metrics)
                )
            }
            ArchiveArgs::MongoDb(args) => (
                MongoDbStorage::new_block_store(&args.url, &args.db, metrics.clone())
                    .await?
                    .into(),
                MongoDbStorage::new_index_store(&args.url, &args.db, metrics.clone())
                    .await?
                    .into(),
            ),
            ArchiveArgs::ObjectStore(object_store_cli_args) => {
                warn!("Object store should not be used for index store.");
                (
                    ObjectStoreKVStore::try_from(object_store_cli_args)?.into(),
                    ObjectStoreKVStore::try_from(object_store_cli_args)?.into(),
                )
            }
        };
        let bdr = BlockDataReaderErased::from(BlockDataArchive::new(blob));
        Ok(ArchiveReader::new(
            bdr.clone(),
            IndexReaderImpl::new(index, bdr),
            None,
            None,
        ))
    }

    pub fn replica_name(&self) -> String {
        match self {
            ArchiveArgs::Aws(aws_cli_args) => aws_cli_args.bucket.clone(),
            ArchiveArgs::MongoDb(mongo_db_cli_args) => mongo_db_cli_args.db.clone(),
            ArchiveArgs::ObjectStore(args) => args.url.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct AwsCliArgs {
    pub bucket: String,
    pub concurrency: usize,
    pub region: Option<String>,
}

impl AwsCliArgs {
    pub fn parse(mut next: impl FnMut(&'static str) -> Result<String>) -> Result<Self> {
        Ok(Self {
            bucket: next("args missing bucket")?,
            concurrency: usize::from_str(&next("").unwrap_or_else(|_| "50".to_string()))?,
            region: next("").ok(),
        })
    }

    pub async fn build_blob_store(&self, metrics: &Metrics) -> KVStoreErased {
        S3Bucket::new(
            self.bucket.clone(),
            &get_aws_config(self.region.clone(), DEFAULT_BLOB_STORE_TIMEOUT).await,
            metrics.clone(),
        )
        .into()
    }

    pub async fn build_index_store(&self, metrics: &Metrics) -> KVStoreErased {
        let config = &get_aws_config(self.region.clone(), DEFAULT_INDEX_STORE_TIMEOUT).await;

        DynamoDBArchive::new(
            S3Bucket::new(self.bucket.clone(), config, metrics.clone()),
            self.bucket.clone(),
            config,
            self.concurrency,
            metrics.clone(),
        )
        .into()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct ObjectStoreCliArgs {
    pub url: String,
    pub access_secret_env_var_prefix: Option<String>,
}

impl ObjectStoreCliArgs {
    pub fn parse(mut next: impl FnMut(&'static str) -> Result<String>) -> Result<Self> {
        Ok(Self {
            url: next("storage args missing object store endpoint")?,
            access_secret_env_var_prefix: next("Prefix for access key and secret key env var. Used with minio or other s3 compatible object stores not directly supported by object_store crate").ok(),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct TrieDbCliArgs {
    pub triedb_path: String,
    pub max_buffered_read_requests: usize,
    pub max_triedb_async_read_concurrency: usize,
    pub max_buffered_traverse_requests: usize,
    pub max_triedb_async_traverse_concurrency: usize,
    pub max_finalized_block_cache_len: usize,
    pub max_voted_block_cache_len: usize,
}

impl TrieDbCliArgs {
    pub fn parse(mut next: impl FnMut(&'static str) -> Result<String>) -> Result<TrieDbCliArgs> {
        Ok(TrieDbCliArgs {
            triedb_path: next("storage args missing db path")?,
            max_buffered_read_requests: usize::from_str(&next(
                "args missing max_buffered_read_requests",
            )?)?,
            max_triedb_async_read_concurrency: 10000,
            max_buffered_traverse_requests: 200,
            max_triedb_async_traverse_concurrency: 20,
            max_finalized_block_cache_len: 200,
            max_voted_block_cache_len: 3,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct MongoDbCliArgs {
    pub url: String,
    pub db: String,
}

impl MongoDbCliArgs {
    pub fn parse(mut next: impl FnMut(&'static str) -> Result<String>) -> Result<Self> {
        let args = Self {
            url: next("storage args missing mongo url")?,
            db: next("storage args missing mongo db name")?,
        };
        let capped_size_gb = next("").ok().and_then(|s| u64::from_str(&s).ok());
        if capped_size_gb.is_some() {
            warn!("Capped size is deprecated, ignoring. You can remove this argument from the connection string");
        }
        Ok(args)
    }
}
