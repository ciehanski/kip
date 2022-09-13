//
// Copyright (c) 2022 Ryan Ciehanski <ryan@ciehanski.com>
//

use crate::chunk::FileChunk;
use crate::crypto::{decrypt, encrypt};
use crate::job::KipFile;
use crate::providers::KipProvider;
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
use tokio::sync::Mutex;
use uuid::Uuid;
use walkdir::WalkDir;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct KipUsb {
    pub name: String,
    pub root_path: PathBuf,
    pub capacity: u64,
    pub used_capacity: u64,
}

impl KipUsb {
    pub fn new<S: Into<String>, P: AsRef<Path>>(
        name: S,
        root_path: P,
        capacity: u64,
        used_capacity: u64,
    ) -> Self {
        Self {
            name: name.into(),
            root_path: root_path.as_ref().to_path_buf(),
            capacity,
            used_capacity,
        }
    }
}

#[async_trait]
impl KipProvider for KipUsb {
    type Item = KipFile;

    async fn upload(
        &self,
        f: &Path,
        chunks_map: HashMap<FileChunk, &[u8]>,
        job_id: Uuid,
        secret: &str,
        progress: Arc<Mutex<Progress>>,
        bar: &Bar,
    ) -> Result<(Vec<FileChunk>, u64)> {
        // Create all parent dirs if missing
        create_dir_all(Path::new(&format!(
            "{}/{}/chunks/",
            self.root_path.display(),
            job_id
        )))?;
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
                    bail!("failed to encrypt chunk: {}.", e)
                }
            };
            // Get amount of bytes uploaded in this chunk
            // after compression and encryption
            let ce_bytes_len = encrypted.len();
            // Upload
            let usb_path = format!(
                "{}/{}/chunks/{}.chunk",
                self.root_path.display(),
                job_id,
                chunk.hash
            );
            // Create new file in the USB drive
            let mut cfile = File::create(usb_path).await?;
            // Copy encrypted and compressed chunk bytes into newly created
            // chunk file
            cfile.write_all(&encrypted).await?;
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
        let decompressed = crate::run::decompress(&decrypted).await?;
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
