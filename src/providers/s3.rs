//
// Copyright (c) 2022 Ryan Ciehanski <ryan@ciehanski.com>
//

use super::KipUploadOpts;
use crate::chunk::FileChunk;
use crate::providers::KipProvider;
use anyhow::{bail, Result};
use async_trait::async_trait;
use aws_sdk_s3::model::Object;
use aws_sdk_s3::types::ByteStream;
use aws_sdk_s3::{Client, Region};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;
use tracing::debug;
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct KipS3 {
    pub aws_bucket: String,
    pub aws_region: String,
}

impl KipS3 {
    // 3,500 API requests per second
    const _API_RATE_LIMIT: u64 = 3500;
    const _API_RATE_LIMIT_PERIOD: u64 = 1;

    pub fn new<S: Into<String>>(aws_bucket: S, aws_region: Region) -> Self {
        Self {
            aws_bucket: aws_bucket.into(),
            aws_region: aws_region.to_string(),
        }
    }
}

#[async_trait]
impl KipProvider for KipS3 {
    type Item = Object;

    async fn upload<'b>(
        &self,
        opts: KipUploadOpts,
        chunk: &FileChunk,
        chunk_bytes: &'b [u8],
    ) -> Result<(String, usize)> {
        // Create S3 client
        let s3_conf = aws_config::from_env()
            .region(Region::new(self.aws_region.clone()))
            .credentials_cache(aws_credential_types::cache::CredentialsCache::lazy())
            .load()
            .await;
        let s3_client = Client::new(&s3_conf);
        // Get chunk_bytes len
        let ce_bytes_len = chunk_bytes.len();
        let remote_path = format!("{}/chunks/{}.chunk", opts.job_id, chunk.hash);
        // Upload
        s3_client
            .put_object()
            .bucket(self.aws_bucket.clone())
            .key(remote_path.clone())
            .content_length(ce_bytes_len.try_into()?)
            .content_type("application/octet-stream")
            .body(ByteStream::from(Bytes::copy_from_slice(chunk_bytes)))
            .send()
            .await?;
        Ok((remote_path, ce_bytes_len))
    }

    async fn download(&self, file_name: &str) -> Result<Vec<u8>> {
        // Create S3 client
        let s3_conf = aws_config::from_env()
            .region(Region::new(self.aws_region.clone()))
            .credentials_cache(aws_credential_types::cache::CredentialsCache::lazy())
            .load()
            .await;
        let s3_client = Client::new(&s3_conf);
        let result = s3_client
            .get_object()
            .bucket(self.aws_bucket.clone())
            .key(file_name.to_string())
            .send()
            .await?;
        // Read result from S3 and convert to bytes
        let mut result_bytes = Vec::<u8>::new();
        result
            .body
            .into_async_read()
            .read_to_end(&mut result_bytes)
            .await?;
        // Return downloaded chunk bytes
        Ok(result_bytes)
    }

    async fn delete(&self, file_name: &str) -> Result<()> {
        let s3_conf = aws_config::from_env()
            .region(Region::new(self.aws_region.clone()))
            .credentials_cache(aws_credential_types::cache::CredentialsCache::lazy())
            .load()
            .await;
        let s3_client = Client::new(&s3_conf);
        // Delete
        s3_client
            .delete_object()
            .bucket(self.aws_bucket.clone())
            .key(file_name.to_string())
            .send()
            .await?;
        Ok(())
    }

    async fn contains(&self, job_id: Uuid, hash: &str) -> Result<bool> {
        // Check S3 for duplicates of chunk
        let s3_objs = self.list_all(job_id).await?;
        // If the S3 bucket is empty, no need to check for duplicate chunks
        if !s3_objs.is_empty() {
            for obj in s3_objs {
                if let Some(obj_key) = obj.key {
                    if obj_key.contains(hash) {
                        // Duplicate chunk found, return true
                        return Ok(true);
                    }
                } else {
                    debug!("unable to get chunk name from S3");
                }
            }
        }
        Ok(false)
    }

    async fn list_all(&self, job_id: Uuid) -> Result<Vec<Self::Item>> {
        let s3_conf = aws_config::from_env()
            .region(Region::new(self.aws_region.clone()))
            .credentials_cache(aws_credential_types::cache::CredentialsCache::lazy())
            .load()
            .await;
        let s3_client = Client::new(&s3_conf);
        let result = s3_client
            .list_objects_v2()
            .bucket(self.aws_bucket.clone())
            .send()
            .await?;
        // Convert S3 result into Vec<S3::Object> which can
        // be used to manipulate the list of files in S3
        let s3_contents = match result.contents {
            Some(rc) => {
                let mut filtered = rc
                    .into_iter()
                    .filter(|obj| filter_job_id(obj.key(), job_id))
                    .collect::<Vec<Object>>();
                // Handle pagination
                let mut cont_token = result.next_continuation_token;
                while let Some(token) = cont_token {
                    let paginated_result = s3_client
                        .list_objects_v2()
                        .bucket(self.aws_bucket.clone())
                        .continuation_token(token)
                        .send()
                        .await?;
                    match paginated_result.contents {
                        Some(prc) => {
                            filtered.extend(
                                prc.into_iter()
                                    .filter(|obj| filter_job_id(obj.key(), job_id)),
                            );
                        }
                        None => (),
                    };
                    cont_token = paginated_result.next_continuation_token;
                }
                filtered
            }
            None => {
                // S3 bucket was empty, return an empty Vec
                return Ok(vec![]);
            }
        };
        // Only check chunks that are within this job's
        // folder in S3
        // let mut job_contents = vec![];
        // for obj in s3_contents {
        //     if let Some(key) = obj.key.clone() {
        //         // We expect jid to be Some since key was not nil
        //         if let Some((jid, _)) = key.split_once('/') {
        //             if jid == job_id.to_string() {
        //                 job_contents.push(obj);
        //             };
        //         } else {
        //             // error splitting obj key returned from S3
        //             bail!("error splitting chunk name from S3")
        //         };
        //     } else {
        //         // error, no obj key returned from S3
        //         bail!("unable to get chunk name from S3")
        //     }
        // }
        Ok(s3_contents)
    }
}

/// Retrieves the hash from an S3 object name and returns
/// it as a String.
pub fn strip_hash_from_s3(s3_path: &str) -> Result<String> {
    // Pop hash off from S3 path
    let mut fp: Vec<&str> = s3_path.split('/').collect();
    if let Some(hdt) = fp.pop() {
        // Split the chunk. Ex: 902938470293847392033874592038473.chunk
        let hs: Vec<&str> = hdt.split('.').collect();
        // Just grab the first split, which is the hash
        let hash = hs[0].to_string();
        // Ship it
        Ok(hash)
    } else {
        bail!("failed to pop chunk's S3 path")
    }
}

fn filter_job_id(provider_path: Option<&str>, job_id: Uuid) -> bool {
    if let Some(key) = provider_path {
        if let Some((jid, _)) = key.split_once('/') {
            if jid == job_id.to_string() {
                return true;
            };
        } else {
            debug!("error splitting chunk name from S3");
        }
    } else {
        debug!("unable to get chunk name from S3");
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strip_hash_from_s3() {
        // Split the 902938470293847392033874592038473.chunk
        let hash = strip_hash_from_s3("f339aae7-e994-4fb4-b6aa-623681df99aa/chunks/001d46082763b930e5b9f0c52d16841b443bfbcd52af6cd475cb0182548da33a.chunk").unwrap();
        assert_eq!(
            hash,
            "001d46082763b930e5b9f0c52d16841b443bfbcd52af6cd475cb0182548da33a"
        )
    }
}
