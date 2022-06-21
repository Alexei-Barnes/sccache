// Copyright 2016 Mozilla Foundation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::cache::{Cache, CacheRead, CacheWrite, Storage};
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{Client, Region};
use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;

use crate::errors::*;

/// A cache that stores entries in Amazon S3.
pub struct S3Cache {
    /// The S3 client.
    client: Arc<Client>,
    /// The S3 bucket.
    bucket: String,
    /// Prefix to be used for bucket keys.
    key_prefix: String,
}

impl S3Cache {
    /// Create a new `S3Cache` storing data in `bucket`.
    pub fn new(
        bucket: &str,
        region: Option<String>,
        _endpoint: &str,
        _use_ssl: bool,
        key_prefix: &str,
    ) -> Result<S3Cache> {
        let runtime = Runtime::new()?;
        let bucket = bucket.to_owned();
        let region_provider = RegionProviderChain::first_try(region.map(Region::new))
            .or_default_provider()
            .or_else(Region::new("us-west-2"));

        let shared_config = runtime.block_on(aws_config::from_env().region(region_provider).load());
        let client = Arc::new(Client::new(&shared_config));

        Ok(S3Cache {
            client,
            bucket,
            key_prefix: key_prefix.to_owned(),
        })
    }

    fn normalize_key(&self, key: &str) -> String {
        format!(
            "{}{}/{}/{}/{}",
            &self.key_prefix,
            &key[0..1],
            &key[1..2],
            &key[2..3],
            &key
        )
    }
}

#[async_trait]
impl Storage for S3Cache {
    async fn get(&self, key: &str) -> Result<Cache> {
        let key = self.normalize_key(key);

        let result = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await;

        match result {
            Ok(data) => {
                let data: Vec<u8> = data
                    .body
                    .collect()
                    .await
                    .context("failed reading response from s3")?
                    .into_bytes()
                    .into_iter()
                    .collect();

                let hit = CacheRead::from(io::Cursor::new(data))?;
                Ok(Cache::Hit(hit))
            }
            Err(e) => {
                warn!("Got AWS error: {:?}", e);
                Ok(Cache::Miss)
            }
        }
    }

    async fn put(&self, key: &str, entry: CacheWrite) -> Result<Duration> {
        let key = self.normalize_key(key);
        let start = Instant::now();
        let data = entry.finish()?;

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(data.into())
            .send()
            .await
            .context("failed to put cache entry in s3")?;

        Ok(start.elapsed())
    }

    fn location(&self) -> String {
        format!("S3, bucket: {}", self.bucket)
    }

    async fn current_size(&self) -> Result<Option<u64>> {
        Ok(None)
    }
    async fn max_size(&self) -> Result<Option<u64>> {
        Ok(None)
    }
}
