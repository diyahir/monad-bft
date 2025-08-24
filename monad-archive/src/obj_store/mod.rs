use std::sync::OnceLock;

use chrono::{DateTime, Utc};
use object_store::{path::Path, ObjectStore, PutPayload};

use crate::prelude::*;

pub struct ObjectStoreTableConfig {
    bucket: String,
    table: String,
}

pub type Key = String;
pub type Checksum = String;
pub type Range = std::ops::Range<u64>;
pub type LastModified = DateTime<Utc>;
pub struct HeadObj {
    pub checksum: Checksum,
    pub last_modified: LastModified,
}

pub trait ObjectStoreTable {
    fn bucket_name(&self) -> &str;
    fn table_name(&self) -> &str;
    async fn get(&self, key: Key) -> Result<Option<Bytes>>;
    async fn get_range(&self, key: Key, range: Range) -> Result<Option<Bytes>>;
    async fn put(&self, key: Key, val: Bytes) -> Result<()>;
    async fn delete(&self, key: Key) -> Result<()>;

    // TODO: implement these later
    // async fn head(&self, key: Key) -> Result<Option<HeadObj>>;
    // async fn list(&self, prefix: Key) -> Result<Vec<(Key, HeadObj)>>;
    // async fn copy(
    //     &self,
    //     from: Key,
    //     to: Key,
    //     other_bucket: Option<impl ObjectStoreTable>,
    // ) -> Checksum;
}

async fn s3_store(bucket: &str) -> Result<Arc<dyn ObjectStore>> {
    use object_store::aws::AmazonS3Builder;
    let mut builder = AmazonS3Builder::from_env();

    let store = builder.build().wrap_err("Failed to build S3 store")?;
    Ok(Arc::new(store))
}

async fn gcp_store(bucket: &str) -> Result<Arc<dyn ObjectStore>> {
    use object_store::gcp::GoogleCloudStorageBuilder;
    let mut builder = GoogleCloudStorageBuilder::from_env();
    let store = builder.build().wrap_err("Failed to build GCP store")?;
    Ok(Arc::new(store))
}

/*
 * Idea: we use urls as connection strings.
 */

pub struct ObjectStoreTableImpl {
    inner: Arc<dyn ObjectStore>,
    config: ObjectStoreTableConfig,
}

impl ObjectStoreTableImpl {
    pub fn new(inner: Arc<dyn ObjectStore>, config: ObjectStoreTableConfig) -> Self {
        Self { inner, config }
    }

    fn key(&self, key: Key) -> Path {
        Path::from(format!("{}/{}", self.config.table, key))
    }
}

impl ObjectStoreTable for ObjectStoreTableImpl {
    fn bucket_name(&self) -> &str {
        &self.config.bucket
    }

    fn table_name(&self) -> &str {
        &self.config.table
    }

    async fn get(&self, key: Key) -> Result<Option<Bytes>> {
        match self.inner.get(&self.key(key)).await {
            Ok(obj) => {
                let bytes = obj.bytes().await?;
                Ok(Some(bytes))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn get_range(&self, key: Key, range: Range) -> Result<Option<Bytes>> {
        match self.inner.get_range(&self.key(key), range.into()).await {
            Ok(obj) => Ok(Some(obj)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn put(&self, key: Key, val: Bytes) -> Result<()> {
        self.inner
            .put(&self.key(key), PutPayload::from(val))
            .await?;
        Ok(())
    }

    async fn delete(&self, key: Key) -> Result<()> {
        self.inner.delete(&self.key(key)).await?;
        Ok(())
    }
}
