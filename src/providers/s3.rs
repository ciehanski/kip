//
// Copyright (c) 2022 Ryan Ciehanski <ryan@ciehanski.com>
//

use crate::chunk::FileChunk;
use crate::crypto::{decrypt, encrypt};
use crate::providers::KipProvider;
use anyhow::{bail, Result};
use async_trait::async_trait;
use aws_sdk_s3::model::Object;
use aws_sdk_s3::types::ByteStream;
use aws_sdk_s3::{Client, Region};
use linya::{Bar, Progress};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct KipS3 {
    pub aws_bucket: String,
    pub aws_region: String,
}

impl KipS3 {
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

    async fn upload(
        &self,
        f: &Path,
        chunks_map: HashMap<FileChunk, &[u8]>,
        job_id: Uuid,
        secret: &str,
        progress: Arc<Mutex<Progress>>,
        bar: &Bar,
    ) -> Result<(Vec<FileChunk>, u64)> {
        // Create S3 client
        let s3_conf = aws_config::from_env()
            .region(Region::new(self.aws_region.clone()))
            .load()
            .await;
        let s3_client = Client::new(&s3_conf);
        // Upload each chunk
        let mut chunks = vec![];
        let mut bytes_uploaded: u64 = 0;
        for (mut chunk, chunk_bytes) in chunks_map {
            // Always compress before encryption
            let compressed = crate::run::compress(chunk_bytes).await?;
            // Encrypt chunk
            let encrypted = match encrypt(&compressed, secret) {
                Ok(ec) => ec,
                Err(e) => {
                    bail!("failed to encrypt chunk: {e}.")
                }
            };
            // Get amount of bytes uploaded in this chunk
            // after compression and encryption
            let ce_bytes_len = encrypted.len();
            // Upload
            s3_client
                .put_object()
                .bucket(self.aws_bucket.clone())
                .key(format!("{job_id}/chunks/{}.chunk", chunk.hash))
                .content_length(ce_bytes_len.try_into()?)
                .body(ByteStream::from(encrypted))
                .send()
                .await?;
            // Push chunk onto chunks hashmap for return
            chunk.local_path = f.canonicalize()?;
            chunks.push(chunk);
            // Increment progress bar for this file by one
            // since one chunk was uploaded
            progress.lock().await.inc_and_draw(bar, chunk_bytes.len());
            let ce_bytes_len_u64: u64 = ce_bytes_len.try_into()?;
            bytes_uploaded += ce_bytes_len_u64;
        }
        Ok((chunks, bytes_uploaded))
    }

    async fn download(&self, f: &str, secret: &str) -> Result<Vec<u8>> {
        let s3_conf = aws_config::from_env()
            .region(Region::new(self.aws_region.clone()))
            .load()
            .await;
        let s3_client = Client::new(&s3_conf);
        let result = s3_client
            .get_object()
            .bucket(self.aws_bucket.clone())
            .key(f.to_string())
            .send()
            .await?;
        // Read result from S3 and convert to bytes
        let mut result_bytes = Vec::<u8>::new();
        result
            .body
            .into_async_read()
            .read_to_end(&mut result_bytes)
            .await?;
        // Decrypt result_bytes
        let decrypted = match decrypt(&result_bytes, secret) {
            Ok(dc) => dc,
            Err(e) => {
                bail!("failed to decrypt file: {e}.")
            }
        };
        // Decompress decrypted bytes
        let decompressed = crate::run::decompress(&decrypted).await?;
        // Return downloaded & decrypted bytes
        Ok(decompressed)
    }

    async fn delete(&self, file_name: &str) -> Result<()> {
        let s3_conf = aws_config::from_env()
            .region(Region::new(self.aws_region.clone()))
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

    async fn contains(&self, job_id: Uuid, obj_name: &str) -> Result<bool> {
        // Check S3 for duplicates of chunk
        let s3_objs = self.list_all(job_id).await?;
        // If the S3 bucket is empty, no need to check for duplicate chunks
        if !s3_objs.is_empty() {
            for obj in s3_objs {
                if let Some(obj_key) = obj.key {
                    if obj_key.contains(obj_name) {
                        // Duplicate chunk found, return true
                        return Ok(true);
                    }
                } else {
                    bail!("unable to get chunk name from S3.")
                }
            }
        }
        Ok(false)
    }

    async fn list_all(&self, job_id: Uuid) -> Result<Vec<Self::Item>> {
        let s3_conf = aws_config::from_env()
            .region(Region::new(self.aws_region.clone()))
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
        let contents = match result.contents {
            Some(c) => c.into_iter().collect::<Vec<_>>(),
            _ => {
                // S3 bucket was empty, return an empty Vec
                let empty_bucket: Vec<Object> = vec![];
                return Ok(empty_bucket);
            }
        };
        // Only check chunks that are within this job's
        // folder in S3
        let mut corrected_contents = vec![];
        for obj in contents {
            if let Some(key) = obj.key.clone() {
                // We expect jid to be Some since key was not nil
                if let Some((jid, _)) = key.split_once('/') {
                    if jid == job_id.to_string() {
                        corrected_contents.push(obj);
                    };
                } else {
                    // error splitting obj key returned from S3
                    bail!("error splitting chunk name from S3.")
                };
            } else {
                // error, no obj key returned from S3
                bail!("unable to get chunk name from S3.")
            }
        }
        Ok(corrected_contents)
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
        bail!("failed to pop chunk's S3 path.")
    }
}
