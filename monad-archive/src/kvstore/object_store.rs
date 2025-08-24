use object_store::{
    aws::AmazonS3Builder, path::Path, Error as ObjError, ObjectStore, ObjectStoreScheme, PutPayload,
};
use url::Url;

use crate::{cli::ObjectStoreCliArgs, prelude::*};

impl KVStore for ObjectStoreKVStore {
    async fn put(&self, key: impl AsRef<str>, data: Vec<u8>) -> Result<()> {
        let path = self.path(key);
        self.inner.put(&path, PutPayload::from(data)).await?;
        Ok(())
    }

    async fn scan_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        let path = self.path(prefix);
        let resp = self
            .inner
            .list(Some(&path))
            .map_ok(|o| o.location.to_string())
            .try_collect::<Vec<_>>()
            .await
            .wrap_err_with(|| format!("Failed to list objects with prefix: {}", prefix))?;
        Ok(resp)
    }

    async fn delete(&self, key: impl AsRef<str>) -> Result<()> {
        let path = self.path(key);
        self.inner.delete(&path).await?;
        Ok(())
    }

    fn bucket_name(&self) -> &str {
        &self.url
    }
}

impl KVReader for ObjectStoreKVStore {
    async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        let path = self.path(key);
        let resp = self.inner.get(&path).await;
        match resp {
            Ok(resp) => Ok(Some(resp.bytes().await?)),
            Err(ObjError::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}

#[derive(Clone)]
pub struct ObjectStoreKVStore {
    inner: Arc<dyn ObjectStore>,
    path: Arc<String>,
    url: Arc<String>,
}

impl TryFrom<&ObjectStoreCliArgs> for ObjectStoreKVStore {
    type Error = eyre::Error;

    fn try_from(args: &ObjectStoreCliArgs) -> Result<Self> {
        use std::env::var;
        let mut options = vec![];
        if let Some(prefix) = &args.access_secret_env_var_prefix {
            info!(
                prefix,
                "Loading access key and secret key from environment variables"
            );
            options.push(("access_key", var(format!("{}_ACCESS_KEY", prefix))?));
            options.push((
                "secret_access_key",
                var(format!("{}_SECRET_ACCESS_KEY", prefix))?,
            ));
        };

        let url = Url::parse(&args.url)?;

        let (scheme, path) = ObjectStoreScheme::parse(&url) else {
            return Err(eyre!("Invalid object store URL: {}", args.url));
        };
        match scheme {
            ObjectStoreScheme::AmazonS3 => {
                let mut builder = AmazonS3Builder::from_env().with_url(url);

                if let Some((access_key, secret_key)) = parse_credentials_file() {
                    builder = builder
                        .with_access_key_id(access_key)
                        .with_secret_access_key(secret_key);
                }

                let inner: Arc<dyn ObjectStore> = Arc::new(builder.build()?);
                return Ok(Self::new(inner, args.url.clone(), path));
            }
            ObjectStoreScheme::Local | ObjectStoreScheme::Memory => {
                let (inner, path) = object_store::parse_url_opts(&url, options)?;
                info!("Object store path: {}", path.to_string());
                Ok(Self::new(inner, args.url.clone(), path))
            }
            ObjectStoreScheme::GoogleCloudStorage => {
                unimplemented!("Google Cloud Storage support is not implemented yet");
                // let builder = GoogleCloudStorageBuilder::from_env().with_url(url);
                // let inner: Arc<dyn ObjectStore> = Arc::new(builder.build()?);
                // return Ok(Self::new(inner, args.url.clone(), path));
            }
            ObjectStoreScheme::MicrosoftAzure => {
                unimplemented!("Microsoft Azure support is not implemented yet");
                // let builder = MicrosoftAzureBuilder::from_env().with_url(url);
                // let inner: Arc<dyn ObjectStore> = Arc::new(builder.build()?);
                // return Ok(Self::new(inner, args.url.clone(), path));
            }
            ObjectStoreScheme::Http => {
                unimplemented!("HTTP support is not implemented yet");
            }
            _ => {
                bail!("Unsupported object store scheme: {:?}", scheme);
            }
        }
    }
}

fn parse_credentials_file() -> Option<(String, String)> {
    let home = std::env::var("HOME").ok()?;
    let config = std::fs::read_to_string(format!("{}/.aws/credentials", home)).ok()?;
    let config = config.split("\n").collect::<Vec<&str>>();
    let access_key = config
        .iter()
        .find(|line| line.starts_with("aws_access_key_id"))?
        .split("=")
        .nth(1)?;
    let secret_key = config
        .iter()
        .find(|line| line.starts_with("aws_secret_access_key"))?
        .split("=")
        .nth(1)?;
    Some((access_key.to_string(), secret_key.to_string()))
}

impl ObjectStoreKVStore {
    pub fn new(inner: impl Into<Arc<dyn ObjectStore>>, url: String, path: Path) -> Self {
        Self {
            inner: inner.into(),
            url: Arc::new(url),
            path: Arc::new(path.to_string()),
        }
    }

    pub fn path(&self, key: impl AsRef<str>) -> Path {
        Path::from(format!("{}/{}", self.path, key.as_ref()))
    }
}
