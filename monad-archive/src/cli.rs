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
use aws_sdk_s3::config::{Credentials, SharedCredentialsProvider};
use eyre::{bail, OptionExt};
use serde::{Deserialize, Serialize};

use crate::{kvstore::mongo::MongoDbStorage, prelude::*};

const DEFAULT_BUCKET_TIMEOUT: u64 = 10;

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

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub enum BlockDataReaderArgs {
    Aws(AwsCliArgs),
    Triedb(TrieDbCliArgs),
    MongoDb(MongoDbCliArgs),
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub enum ArchiveArgs {
    Aws(AwsCliArgs),
    MongoDb(MongoDbCliArgs),
}

impl FromStr for BlockDataReaderArgs {
    type Err = eyre::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        use BlockDataReaderArgs::*;
        let (storage_type, args) = s.split_once(' ').ok_or_eyre("Storage args string empty")?;

        Ok(match storage_type.to_lowercase().as_str() {
            "aws" => Aws(AwsCliArgs::parse(args)?),
            "triedb" => Triedb(TrieDbCliArgs::parse(args)?),
            "mongodb" => MongoDb(MongoDbCliArgs::parse(args)?),
            _ => {
                bail!("Unrecognized storage args variant: {storage_type}");
            }
        })
    }
}

impl FromStr for ArchiveArgs {
    type Err = eyre::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        use ArchiveArgs::*;
        let (storage_type, args) = s.split_once(' ').ok_or_eyre("Storage args string empty")?;

        Ok(match storage_type.to_lowercase().as_str() {
            "aws" => Aws(AwsCliArgs::parse(args)?),
            "mongodb" => MongoDb(MongoDbCliArgs::parse(args)?),
            _ => {
                bail!("Unrecognized storage args variant: {storage_type}");
            }
        })
    }
}

impl BlockDataReaderArgs {
    pub async fn build(&self, metrics: &Metrics) -> Result<BlockDataReaderErased> {
        use BlockDataReaderArgs::*;
        Ok(match self {
            Triedb(args) => TriedbReader::new(args).into(),
            Aws(args) => {
                let config = args.config().await;
                let bucket = Bucket::new(args.bucket.clone(), &config, metrics.clone());
                BlockDataArchive::new(bucket).into()
            }
            MongoDb(args) => BlockDataArchive::new(
                MongoDbStorage::new_block_store(&args.url, &args.db, metrics.clone()).await?,
            )
            .into(),
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
        }
    }
}

impl ArchiveArgs {
    pub async fn build_block_data_archive(&self, metrics: &Metrics) -> Result<BlockDataArchive> {
        let store: KVStoreErased = match self {
            ArchiveArgs::Aws(args) => {
                let config = args.config().await;
                Bucket::new(args.bucket.clone(), &config, metrics.clone()).into()
            }
            ArchiveArgs::MongoDb(args) => {
                MongoDbStorage::new_block_store(&args.url, &args.db, metrics.clone())
                    .await?
                    .into()
            }
        };
        Ok(BlockDataArchive::new(store))
    }

    pub async fn build_index_archive(
        &self,
        metrics: &Metrics,
        max_inline_encoded_len: usize,
    ) -> Result<TxIndexArchiver> {
        let (blob, index): (KVStoreErased, KVStoreErased) = match self {
            ArchiveArgs::Aws(args) => {
                let config = args.config().await;
                let bucket = Bucket::new(args.bucket.clone(), &config, metrics.clone());
                let index = DynamoDBArchive::new(
                    bucket.clone(),
                    args.bucket.clone(),
                    &config,
                    args.concurrency,
                    metrics.clone(),
                );
                (bucket.into(), index.into())
            }
            ArchiveArgs::MongoDb(args) => (
                MongoDbStorage::new_block_store(&args.url, &args.db, metrics.clone())
                    .await?
                    .into(),
                MongoDbStorage::new_index_store(&args.url, &args.db, metrics.clone())
                    .await?
                    .into(),
            ),
        };
        Ok(TxIndexArchiver::new(
            index,
            BlockDataArchive::new(blob),
            max_inline_encoded_len,
        ))
    }

    pub async fn build_archive_reader(&self, metrics: &Metrics) -> Result<ArchiveReader> {
        let (blob, index): (KVStoreErased, KVStoreErased) = match self {
            ArchiveArgs::Aws(args) => {
                let config = args.config().await;
                let bucket = Bucket::new(args.bucket.clone(), &config, metrics.clone());
                let index = DynamoDBArchive::new(
                    bucket.clone(),
                    args.bucket.clone(),
                    &config,
                    // TODO: remove me, concurrency should be handled elsewhere
                    args.concurrency,
                    metrics.clone(),
                );
                (bucket.into(), index.into())
            }
            ArchiveArgs::MongoDb(args) => (
                MongoDbStorage::new_block_store(&args.url, &args.db, metrics.clone())
                    .await?
                    .into(),
                MongoDbStorage::new_index_store(&args.url, &args.db, metrics.clone())
                    .await?
                    .into(),
            ),
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
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, Eq, PartialEq, Hash)]
pub struct AwsCliArgs {
    pub bucket: String,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    // TODO: remove me, concurrency should be handled elsewhere
    pub concurrency: usize,
    // If these are not provided, uses timeout_secs for all
    pub operation_timeout_secs: u64,
    pub operation_attempt_timeout_secs: u64,
    pub read_timeout_secs: u64,
}

impl AwsCliArgs {
    pub fn parse(s: &str) -> Result<Self> {
        let (mut positional, mut kv) = parse_str_positional_and_kv(s)?;

        let get_u64 = |kv: &HashMap<String, String>, key: &str, default: u64| -> u64 {
            kv.get(key)
                .and_then(|s| u64::from_str(s).ok())
                .unwrap_or(default)
        };

        let timeout_secs = get_u64(&kv, "timeout-secs", DEFAULT_BUCKET_TIMEOUT);
        info!("Using timeout_secs: {}", timeout_secs);

        Ok(Self {
            // prefer positional, fallback to kv
            bucket: positional
                .first_mut()
                .map(std::mem::take)
                .or_else(|| kv.remove("bucket"))
                .ok_or_eyre("storage args missing bucket")?,
            region: kv.remove("region"),
            endpoint: kv.remove("endpoint"),
            access_key_id: kv.remove("access-key-id"),
            secret_access_key: kv.remove("secret-access-key"),
            concurrency: kv
                .remove("concurrency")
                .and_then(|s| usize::from_str(&s).ok())
                // TODO: remove me, concurrency should be handled elsewhere
                .unwrap_or(200),
            // If these are not provided, uses timeout_secs for all
            operation_timeout_secs: get_u64(&kv, "operation-timeout-secs", timeout_secs),
            operation_attempt_timeout_secs: get_u64(
                &kv,
                "operation-attempt-timeout-secs",
                timeout_secs,
            ),
            read_timeout_secs: get_u64(&kv, "read-timeout-secs", timeout_secs),
        })
    }

    pub(crate) async fn config(&self) -> SdkConfig {
        let region = self
            .region
            .clone()
            .unwrap_or_else(|| "us-east-2".to_string());

        info!("Bucket {} running in region: {}", self.bucket, region);

        let mut config = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(region))
            .timeout_config(
                TimeoutConfig::builder()
                    .operation_timeout(Duration::from_secs(self.operation_timeout_secs))
                    .operation_attempt_timeout(Duration::from_secs(
                        self.operation_attempt_timeout_secs,
                    ))
                    .read_timeout(Duration::from_secs(self.read_timeout_secs))
                    .build(),
            )
            .retry_config(RetryConfig::adaptive());

        if let Some(endpoint) = &self.endpoint {
            config = config.endpoint_url(endpoint);
        }

        if let (Some(access_key_id), Some(secret_access_key)) =
            (&self.access_key_id, &self.secret_access_key)
        {
            config = config.credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                access_key_id, // fmt
                secret_access_key,
                None,
                None,
                "minio",
            )));
        }

        config.load().await
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
    pub fn parse(s: &str) -> Result<TrieDbCliArgs> {
        let (positional, kv) = parse_str_positional_and_kv(s)?;
        let triedb_path = positional
            .first()
            .ok_or_eyre("storage args missing db path")?
            .to_string();

        // get a usize from kv or use default value
        let get = |key: &str, default: usize| -> usize {
            kv.get(key)
                .and_then(|s| usize::from_str(s).ok())
                .unwrap_or(default)
        };

        // only this first one should be positional for backcompat
        let max_buffered_read_requests = positional
            .get(1)
            .and_then(|s| usize::from_str(s).ok())
            .unwrap_or_else(|| get("max-buffered-read-requests", 5000));

        Ok(TrieDbCliArgs {
            triedb_path,
            max_buffered_read_requests,
            max_triedb_async_read_concurrency: get("max-triedb-async-read-concurrency", 5000),
            max_buffered_traverse_requests: get("max-buffered-traverse-requests", 200),
            max_triedb_async_traverse_concurrency: get("max-triedb-async-traverse-concurrency", 20),
            max_finalized_block_cache_len: get("max-finalized-block-cache-len", 200),
            max_voted_block_cache_len: get("max-voted-block-cache-len", 3),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct MongoDbCliArgs {
    pub url: String,
    pub db: String,
}

impl MongoDbCliArgs {
    pub fn parse(s: &str) -> Result<Self> {
        let (positional, mut kv) = parse_str_positional_and_kv(s)?;
        Ok(Self {
            url: kv
                .remove("url")
                .or_else(|| positional.first().cloned())
                .ok_or_eyre("storage args missing mongo url")?,
            db: kv
                .remove("db")
                .or_else(|| positional.get(1).cloned())
                .ok_or_eyre("storage args missing mongo db name")?,
        })
    }
}

// Parse a string into a list of positional arguments and a map of key-value pairs.
// Example: "aws s3://bucket/path --concurrency 10" -> (["aws", "s3://bucket/path"], {"concurrency": "10"})
fn parse_str_positional_and_kv(s: &str) -> Result<(Vec<String>, HashMap<String, String>)> {
    let mut positional = Vec::new();
    let mut kv_pairs = HashMap::new();

    let mut parts = s.split_whitespace().peekable();

    while let Some(part) = parts.next() {
        if part.starts_with("--") {
            // This is a key-value flag
            let key = part.trim_start_matches("--");

            // Check if there's a value following this flag
            if let Some(next) = parts.peek() {
                if !next.starts_with("--") {
                    // The next item is the value for this flag
                    let value = parts.next().ok_or_eyre("Flag requires a value")?;
                    kv_pairs.insert(key.to_string(), value.to_string());
                } else {
                    // No value provided for this flag
                    bail!("Flag --{} requires a value", key)
                }
            } else {
                // Flag at the end with no value
                bail!("Flag --{} requires a value", key);
            }
        } else {
            // This is a positional argument
            positional.push(part.to_string());
        }
    }

    Ok((positional, kv_pairs))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn parse_str_positional_and_kv_basic() {
        let s = "object-store s3://bucket/path --env-prefix FOO";
        let (pos, kv) = parse_str_positional_and_kv(s).unwrap();
        assert_eq!(pos, vec!["object-store", "s3://bucket/path"]);
        assert_eq!(kv.get("env-prefix").map(String::as_str), Some("FOO"));
    }

    #[test]
    fn parse_str_positional_and_kv_many_flags_and_positional() {
        let s = "triedb /db/path \
                 --max-triedb-async-read-concurrency 100 \
                 --max-buffered-traverse-requests 300 \
                 --max-triedb-async-traverse-concurrency 30 \
                 --max-finalized-block-cache-len 250 \
                 --max-voted-block-cache-len 5";
        let (pos, kv) = parse_str_positional_and_kv(s).unwrap();
        assert_eq!(pos, vec!["triedb", "/db/path"]);
        assert_eq!(kv.get("max-triedb-async-read-concurrency").unwrap(), "100");
        assert_eq!(kv.get("max-buffered-traverse-requests").unwrap(), "300");
        assert_eq!(
            kv.get("max-triedb-async-traverse-concurrency").unwrap(),
            "30"
        );
        assert_eq!(kv.get("max-finalized-block-cache-len").unwrap(), "250");
        assert_eq!(kv.get("max-voted-block-cache-len").unwrap(), "5");
    }

    #[test]
    fn parse_str_positional_and_kv_missing_flag_value_errors() {
        let s = "aws s3://bucket --concurrency";
        let err = parse_str_positional_and_kv(s).unwrap_err().to_string();
        assert!(err.contains("requires a value"));
    }

    #[test]
    fn aws_fromstr_defaults() {
        let a = BlockDataReaderArgs::from_str("aws my-bucket").unwrap();
        match a {
            BlockDataReaderArgs::Aws(args) => {
                assert_eq!(args.bucket, "my-bucket");
                assert_eq!(args.concurrency, 200); // default
                assert_eq!(args.region, None);
            }
            _ => panic!("expected Aws variant"),
        }
    }

    #[test]
    fn aws_fromstr_overrides() {
        let a = BlockDataReaderArgs::from_str("aws my-bucket --concurrency 64 --region us-west-2")
            .unwrap();
        match a {
            BlockDataReaderArgs::Aws(args) => {
                assert_eq!(args.bucket, "my-bucket");
                assert_eq!(args.concurrency, 64);
                assert_eq!(args.region.as_deref(), Some("us-west-2"));
            }
            _ => panic!("expected Aws variant"),
        }

        let a = BlockDataReaderArgs::from_str("aws my-bucket 64 us-west-2").unwrap();
        match a {
            BlockDataReaderArgs::Aws(args) => {
                assert_eq!(args.bucket, "my-bucket");
                assert_eq!(args.concurrency, 200);
                assert_eq!(args.region.as_deref(), None);
            }
            _ => panic!("expected Aws variant"),
        }
    }

    #[test]
    fn mongodb_fromstr_basic() {
        let a = ArchiveArgs::from_str("mongodb mongodb://localhost:27017 mydb").unwrap();
        match a {
            ArchiveArgs::MongoDb(args) => {
                assert_eq!(args.url, "mongodb://localhost:27017");
                assert_eq!(args.db, "mydb");
            }
            _ => panic!("expected MongoDb variant"),
        }
    }

    #[test]
    fn mongodb_fromstr_ignores_deprecated_capped_size_arg() {
        // Third positional numeric should be ignored with a warning
        let a = BlockDataReaderArgs::from_str("mongodb mongodb://host:27017 mydb 10").unwrap();
        match a {
            BlockDataReaderArgs::MongoDb(args) => {
                assert_eq!(args.url, "mongodb://host:27017");
                assert_eq!(args.db, "mydb");
            }
            _ => panic!("expected MongoDb variant"),
        }
    }

    #[test]
    fn triedb_parse_minimal_defaults() {
        // Direct parser expects just the args string (no leading type token)
        let t = TrieDbCliArgs::parse("/data/triedb").unwrap();
        assert_eq!(t.triedb_path, "/data/triedb");
        assert_eq!(t.max_buffered_read_requests, 5000);
        assert_eq!(t.max_triedb_async_read_concurrency, 5000);
        assert_eq!(t.max_buffered_traverse_requests, 200);
        assert_eq!(t.max_triedb_async_traverse_concurrency, 20);
        assert_eq!(t.max_finalized_block_cache_len, 200);
        assert_eq!(t.max_voted_block_cache_len, 3);
    }

    #[test]
    fn triedb_parse_with_overrides() {
        let t = TrieDbCliArgs::parse(
            "/db 4000 \
             --max-triedb-async-read-concurrency 100 \
             --max-buffered-traverse-requests 300 \
             --max-triedb-async-traverse-concurrency 30 \
             --max-finalized-block-cache-len 250 \
             --max-voted-block-cache-len 5",
        )
        .unwrap();
        assert_eq!(t.triedb_path, "/db");
        assert_eq!(t.max_buffered_read_requests, 4000);
        assert_eq!(t.max_triedb_async_read_concurrency, 100);
        assert_eq!(t.max_buffered_traverse_requests, 300);
        assert_eq!(t.max_triedb_async_traverse_concurrency, 30);
        assert_eq!(t.max_finalized_block_cache_len, 250);
        assert_eq!(t.max_voted_block_cache_len, 5);
    }

    #[test]
    fn triedb_fromstr_roundtrip() {
        let r = BlockDataReaderArgs::from_str(
            "triedb /db/path \
             --max-triedb-async-read-concurrency 42",
        )
        .unwrap();
        match r {
            BlockDataReaderArgs::Triedb(t) => {
                assert_eq!(t.triedb_path, "/db/path");
                assert_eq!(t.max_triedb_async_read_concurrency, 42);
            }
            _ => panic!("expected Triedb variant"),
        }
    }

    #[test]
    fn unrecognized_variant_is_error() {
        assert!(BlockDataReaderArgs::from_str("foo bar baz").is_err());
        assert!(ArchiveArgs::from_str("nope something").is_err());
    }

    #[test]
    fn missing_args_is_error() {
        let err = BlockDataReaderArgs::from_str("aws")
            .unwrap_err()
            .to_string();
        assert!(err.contains("Storage args string empty"));
    }

    #[test]
    fn replica_name_roundtrip() {
        let aws = ArchiveArgs::from_str("aws my-bucket 10 us-west-1").unwrap();
        assert_eq!(aws.replica_name(), "my-bucket");

        let mongo = ArchiveArgs::from_str("mongodb mongodb://h:27017 mydb").unwrap();
        assert_eq!(mongo.replica_name(), "mydb");
    }
}
