//
// Copyright (c) 2023 Ryan Ciehanski <ryan@ciehanski.com>
//

use super::KipUploadOpts;
use crate::chunk::FileChunk;
use crate::compress::{
    decompress_brotli, decompress_gzip, decompress_lzma, decompress_zstd, KipCompressAlg,
    KipCompressOpts,
};
use crate::crypto::decrypt;
use crate::job::KipFile;
use crate::providers::KipProvider;
use crate::run::KipUploadMsg;
use anyhow::{bail, Result};
use async_trait::async_trait;
use linya::{Bar, Progress};
use memmap2::MmapOptions;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::create_dir_all;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Mutex;
use uuid::Uuid;
use walkdir::WalkDir;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct KipAzure {
    pub blob_name: String,
    pub container: String,
}

impl KipUsb {
    // 20,000 API requests per second
    // https://learn.microsoft.com/en-us/azure/azure-resource-manager/management/azure-subscription-service-limits#storage-limits
    const _API_RATE_LIMIT: u64 = 3500;
    const _API_RATE_LIMIT_PERIOD: u64 = 1;

    pub fn new<S: Into<String>, P: AsRef<Path>>(blob_name: S, container: S) -> Self {
        Self {
            blob_name: blob_name.into(),
            container: container.into(),
        }
    }
}

#[async_trait]
impl KipProvider for KipAzure {
    type Item = PutBlockBlobResponse;

    async fn upload<'b>(
        &mut self,
        opts: KipUploadOpts,
        chunks_map: HashMap<FileChunk, &'b [u8]>,
        msg_tx: UnboundedSender<KipUploadMsg>,
        progress: Arc<Mutex<Progress>>,
        bar: &Bar,
    ) -> Result<()> {
        // First we retrieve the account name and access key from environment variables.
        let account = std::env::var("STORAGE_ACCOUNT").expect("missing STORAGE_ACCOUNT");
        let access_key = std::env::var("STORAGE_ACCESS_KEY").expect("missing STORAGE_ACCOUNT_KEY");
        let storage_credentials = StorageCredentials::Key(account.clone(), access_key);
        let azure_client = ClientBuilder::new(account, storage_credentials)
            .blob_client(&self.container, self.blob_name);

        // Upload each chunk
        for (chunk, chunk_bytes) in chunks_map {
            // Get amount of bytes uploaded in this chunk
            // after compression and encryption
            let ce_bytes_len = chunk_bytes.len();
            // Upload
            azure_client
                .put_block_blob(chunk_bytes)
                .content_type("application/octet-stream")
                .await?;
            // Push chunk onto chunks hashmap for return
            msg_tx.send(KipUploadMsg::FileChunk(chunk))?;
            // Increment progress bar by chunk bytes len
            progress.lock().await.inc_and_draw(bar, ce_bytes_len);
            msg_tx.send(KipUploadMsg::BytesUploaded(ce_bytes_len.try_into()?))?;
        }
        Ok(())
    }

    async fn download(&self, f: &str, secret: &str, compress: KipCompressOpts) -> Result<Vec<u8>> {
        // Read result from S3 and convert to bytes
        let path = Path::new(f);
        let mut bytes = vec![];
        if path.metadata()?.len() > (500 * 1024 * 1024) {
            // SAFETY: unsafe used here for mmap
            let mmap = unsafe {
                MmapOptions::new()
                    .populate()
                    .map(&File::open(path).await?)?
            };
            bytes.extend_from_slice(&mmap[..]);
        } else {
            bytes.extend_from_slice(&tokio::fs::read(path).await?);
        }
        // Decrypt result_bytes
        let decrypted = match decrypt(&bytes, secret) {
            Ok(dc) => dc,
            Err(e) => {
                bail!("failed to decrypt file: {}.", e)
            }
        };
        // Decompress decrypted bytes
        let mut decompressed = Vec::<u8>::new();
        if compress.enabled {
            match compress.alg {
                KipCompressAlg::Zstd => decompressed = decompress_zstd(&decrypted).await?,
                KipCompressAlg::Lzma => decompressed = decompress_lzma(&decrypted).await?,
                KipCompressAlg::Gzip => decompressed = decompress_gzip(&decrypted).await?,
                KipCompressAlg::Brotli => decompressed = decompress_brotli(&decrypted).await?,
            }
        } else {
            decompressed.extend_from_slice(&decrypted);
        }
        // Return downloaded & decrypted bytes
        Ok(decompressed)
    }

    async fn delete(&self, file_name: &str) -> Result<()> {
        let path = Path::new(file_name);
        if path.is_dir() {
            tokio::fs::remove_dir_all(path).await?;
        } else {
            tokio::fs::remove_file(path).await?;
        }
        Ok(())
    }

    async fn contains(&self, job_id: Uuid, hash: &str) -> Result<bool> {
        // Check S3 for duplicates of chunk
        let file_objs = self.list_all(job_id).await?;
        // If the S3 bucket is empty, no need to check for duplicate chunks
        if !file_objs.is_empty() {
            for obj in file_objs {
                if obj.hash == hash {
                    // Duplicate chunk found, return true
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    async fn list_all(&self, job_id: Uuid) -> Result<Vec<Self::Item>> {
        let mut kfs = Vec::<KipFile>::new();
        let path_fmt = format!("{}/{}/chunks/", self.root_path.display(), job_id);
        let path = Path::new(&path_fmt).canonicalize()?;
        for entry in WalkDir::new(path).follow_links(true) {
            let entry = entry?;
            // If a directory, skip
            if entry.path().metadata()?.is_dir() {
                continue;
            }
            // Is a file, create KipFile and pusht to vec
            let entry_kf = KipFile::new(entry.path().canonicalize()?);
            kfs.push(entry_kf);
        }
        Ok(kfs)
    }
}
