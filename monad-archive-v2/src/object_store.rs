use crate::prelude::*;

pub trait ObjectStore: Send + Sync + 'static {
    async fn get(&self, key: &str) -> Result<Bytes>;
    async fn get_range(&self, key: &str, range: RangeInclusive<u64>) -> Result<Bytes>;
    async fn put(&self, key: &str, value: &[u8]) -> Result<()>;
    async fn delete(&self, key: &str) -> Result<()>;
    async fn list(&self, prefix: &str) -> Result<impl Iterator<Item = String>>;
    async fn list_between(
        &self,
        prefix: &str,
        range: RangeInclusive<u64>,
    ) -> Result<impl Iterator<Item = String>>;
    async fn exists(&self, key: &str) -> Result<bool>;
    async fn is_stale(&self, key: &str, version: u64) -> Result<bool>;
}
