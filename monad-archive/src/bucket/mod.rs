use std::sync::OnceLock;

use chrono::{DateTime, Utc};
use enum_dispatch::enum_dispatch;
use object_store::{path::Path, ObjectStore, PutPayload};

use crate::prelude::*;

pub struct ObjectStoreTableConfig {
    bucket: String,
}

pub type Key = String;
pub type Checksum = String;
pub type Range = std::ops::Range<u64>;
pub type LastModified = DateTime<Utc>;
pub struct HeadObj {
    pub checksum: Checksum,
    pub last_modified: LastModified,
}

#[enum_dispatch]
pub trait BucketReader {
    fn bucket_name(&self) -> &str;
    async fn get(&self, key: impl Into<Path>) -> Result<Option<Bytes>>;
    async fn get_range(
        &self,
        key: impl Into<Path>,
        range: std::ops::Range<u64>,
    ) -> Result<Option<Bytes>>;

    // TODO: implement these later
    // async fn head(&self, key: impl Into<Path>) -> Result<Option<HeadObj>>;
    async fn list(&self, prefix: impl Into<Path>) -> Result<Vec<Path>>;
}

#[enum_dispatch]
pub trait Bucket: BucketReader {
    async fn put(&self, key: impl Into<Path>, val: Bytes) -> Result<()>;
    async fn delete(&self, key: impl Into<Path>) -> Result<()>;
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
}

impl Bucket for ObjectStoreTableImpl {
    async fn put(&self, key: impl Into<Path>, val: Bytes) -> Result<()> {
        self.inner.put(&key, PutPayload::from(val)).await?;
        Ok(())
    }

    async fn delete(&self, key: impl Into<Path>) -> Result<()> {
        self.inner.delete(&key).await?;
        Ok(())
    }
}

impl BucketReader for ObjectStoreTableImpl {
    fn bucket_name(&self) -> &str {
        &self.config.bucket
    }

    async fn get(&self, key: impl Into<Path>) -> Result<Option<Bytes>> {
        match self.inner.get(&key).await {
            Ok(obj) => {
                let bytes = obj.bytes().await?;
                Ok(Some(bytes))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn get_range(&self, key: impl Into<Path>, range: Range) -> Result<Option<Bytes>> {
        match self.inner.get_range(&key, range.into()).await {
            Ok(obj) => Ok(Some(obj)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn list(&self, prefix: impl Into<Path>) -> Result<Vec<Path>> {
        let resp = self.inner.list(Some(&prefix)).await?;
        Ok(resp.into_iter().map(|o| o.location).collect())
    }
}
