use std::{
    collections::BTreeMap,
    ops::RangeInclusive,
    sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use bytes::Bytes;

use crate::{
    errors::{KVError, NetworkError},
    object_store::{KVResult, ObjectStore},
};

pub struct MemoryStore {
    inner: Arc<RwLock<Inner>>,
}

type Inner = BTreeMap<String, Bytes>;
type Result<T> = KVResult<T>;

impl MemoryStore {
    fn read(&self) -> Result<RwLockReadGuard<Inner>> {
        self.inner.read().map_err(|_| {
            NetworkError::Other(eyre::eyre!("failed to read memory store").into()).into()
        })
    }

    fn write(&self) -> Result<RwLockWriteGuard<Inner>> {
        self.inner.write().map_err(|_| {
            NetworkError::Other(eyre::eyre!("failed to write memory store").into()).into()
        })
    }
}

impl ObjectStore for MemoryStore {
    async fn get(&self, key: &str) -> KVResult<Bytes> {
        self.read()?
            .get(key)
            .cloned()
            .ok_or_else(|| KVError::NotFound(key.to_string()))
    }

    async fn get_range(&self, key: &str, range: RangeInclusive<u32>) -> KVResult<Bytes> {
        let data = self.get(key).await?;
        let start = *range.start() as usize;
        let end = *range.end() as usize;
        let len = data.len();
        if start > len || end > len {
            return Err(KVError::RangeBounds {
                key: key.to_string(),
                start: *range.start(),
                end: *range.end(),
                len,
            });
        }

        Ok(data.slice(start..=end))
    }

    async fn put(&self, key: &str, value: Bytes) -> KVResult<()> {
        self.write()?.insert(key.to_string(), value);
        Ok(())
    }

    async fn delete(&self, key: &str) -> KVResult<()> {
        self.write()?.remove(key);
        Ok(())
    }

    async fn list(&self, prefix: &str) -> KVResult<Vec<String>> {
        let inner = self.read()?;
        // TODO: optimize with binary search on prefix
        let keys = inner.keys().filter(move |k| k.starts_with(prefix));
        Ok(keys.cloned().collect())
    }

    async fn list_range(&self, prefix: &str, range: RangeInclusive<u64>) -> KVResult<Vec<u64>> {
        let inner = self.read()?;
        let start = format!("{}{:20}", prefix, range.start());
        let end = format!("{}{:20}", prefix, range.end());

        let keys = inner.range(start..=end).map(|(k, _)| {
            k.strip_prefix(prefix)
                .expect("key should start with prefix")
                .parse::<u64>()
                .expect("key should be a valid u64")
        });
        Ok(keys.collect())
    }

    async fn exists(&self, key: &str) -> KVResult<bool> {
        Ok(self.read()?.contains_key(key))
    }
}
