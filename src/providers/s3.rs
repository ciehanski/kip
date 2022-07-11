//
// Copyright (c) 2022 Ryan Ciehanski <ryan@ciehanski.com>
//

use crate::chunk::FileChunk;
use crate::crypto::{decrypt, encrypt};
use crate::providers::KipProvider;
use anyhow::{bail, Result};
use async_trait::async_trait;
use linya::{Bar, Progress};
use rusoto_core::Region;
use rusoto_s3::{
    DeleteObjectRequest, GetObjectRequest, ListObjectsV2Request, PutObjectRequest, S3Client,
    StreamingBody, S3,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tokio::io::AsyncReadExt;
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct KipS3 {
    pub aws_bucket: String,
    pub aws_region: Region,
}

impl KipS3 {
    pub fn new<S: AsRef<str>>(aws_bucket: S, aws_region: Region) -> Self {
        KipS3 {
            aws_bucket: aws_bucket.as_ref().to_string(),
            aws_region,
        }
    }
}

#[async_trait]
impl KipProvider for KipS3 {
    async fn upload(
        &self,
        f: &Path,
        chunks_map: HashMap<FileChunk, &[u8]>,
        job_id: Uuid,
        secret: &str,
        progress: Arc<Mutex<Progress>>,
        bar: &Bar,
    ) -> Result<(Vec<FileChunk>, usize)> {
        // Create S3 client
        let s3_client = S3Client::new(self.aws_region.clone());
        // Upload each chunk
        let mut chunks = vec![];
        let mut bytes_uploaded = 0;
        for (mut chunk, chunk_bytes) in chunks_map {
            // Always compress before encryption
            let compressed = crate::run::compress(chunk_bytes).await?;
            // Encrypt chunk
            let encrypted = match encrypt(&compressed, secret) {
                Ok(ec) => ec,
                Err(e) => {
                    bail!("failed to encrypt chunk: {}.", e)
                }
            };
            // Get amount of bytes uploaded in this chunk
            // after compression and encryption
            let ce_bytes_len = encrypted.len();
            // Upload
            s3_client
                .put_object(PutObjectRequest {
                    bucket: self.aws_bucket.clone(),
                    key: format!("{}/chunks/{}.chunk", job_id, chunk.hash),
                    content_length: Some(encrypted.len() as i64),
                    body: Some(StreamingBody::from(encrypted)),
                    ..Default::default()
                })
                .await?;
            // Push chunk onto chunks hashmap for return
            chunk.local_path = f.canonicalize()?;
            chunks.push(chunk);
            // Increment progress bar for this file by one
            // since one chunk was uploaded
            progress
                .lock()
                .unwrap()
                .inc_and_draw(bar, chunk_bytes.len());
            bytes_uploaded += ce_bytes_len;
        }
        Ok((chunks, bytes_uploaded))
    }

    async fn download(&self, f: &str, secret: &str) -> Result<Vec<u8>> {
        let s3_client = S3Client::new(self.aws_region.clone());
        let result = s3_client
            .get_object(GetObjectRequest {
                bucket: self.aws_bucket.to_string(),
                key: f.to_string(),
                ..Default::default()
            })
            .await?;
        // Read result from S3 and convert to bytes
        let mut result_stream = match result.body {
            Some(b) => b.into_async_read(),
            _ => {
                bail!("unable to read response from S3.")
            }
        };
        // Read the downloaded S3 bytes into result_bytes
        let mut result_bytes = Vec::<u8>::new();
        result_stream.read_to_end(&mut result_bytes).await?;
        // Decrypt result_bytes
        let decrypted = match decrypt(&result_bytes, secret) {
            Ok(dc) => dc,
            Err(e) => {
                bail!("failed to decrypt file: {}.", e)
            }
        };
        // Decompress decrypted bytes
        let decompressed = crate::run::decompress(&decrypted).await?;
        // Return downloaded & decrypted bytes
        Ok(decompressed)
    }

    async fn delete(&self, file_name: &str) -> Result<()> {
        let s3_client = S3Client::new(self.aws_region.clone());
        s3_client
            .delete_object(DeleteObjectRequest {
                bucket: self.aws_bucket.to_string(),
                key: file_name.to_string(),
                ..DeleteObjectRequest::default()
            })
            .await?;
        Ok(())
    }

    async fn contains(&self, job_id: Uuid, obj_name: &str) -> Result<bool> {
        // Check S3 for duplicates of chunk
        let s3_objs = self.list_all(job_id).await?;
        // If the S3 bucket is empty, no need to check for duplicate chunks
        if !s3_objs.is_empty() {
            for obj in s3_objs {
                // let key = strip_hash_from_s3(&obj.key.unwrap())?;
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

    async fn list_all(&self, job_id: Uuid) -> Result<Vec<rusoto_s3::Object>> {
        let s3_client = S3Client::new(self.aws_region.clone());
        let result = s3_client
            .list_objects_v2(ListObjectsV2Request {
                bucket: self.aws_bucket.to_string(),
                ..ListObjectsV2Request::default()
            })
            .await?;
        // Convert S3 result into Vec<S3::Object> which can
        // be used to manipulate the list of files in S3
        let contents = match result.contents {
            Some(c) => c.into_iter().collect::<Vec<_>>(),
            _ => {
                // S3 bucket was empty, return an empty Vec
                let empty_bucket: Vec<rusoto_s3::Object> = vec![];
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
