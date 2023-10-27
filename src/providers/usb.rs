//
// Copyright (c) 2022 Ryan Ciehanski <ryan@ciehanski.com>
//

use super::KipUploadOpts;
use crate::chunk::FileChunk;
use crate::job::KipFile;
use crate::providers::KipProvider;
use anyhow::Result;
use async_trait::async_trait;
use memmap2::MmapOptions;
use serde::{Deserialize, Serialize};
use std::fs::create_dir_all;
use std::path::{Path, PathBuf};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tracing::debug;
use uuid::Uuid;
use walkdir::WalkDir;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct KipUsb {
    pub name: String,
    pub root_path: PathBuf,
    pub capacity: u64,
    pub used_capacity: u64,
    // file_system
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
    type Uploader = ();
    type Item = KipFile;

    async fn upload<'b>(
        &self,
        _client: Option<&Self::Uploader>,
        opts: KipUploadOpts,
        chunk: &FileChunk,
        chunk_bytes: &'b [u8],
    ) -> Result<usize> {
        // Create all parent dirs if missing
        create_dir_all(Path::new(&format!(
            "{}/{}/chunks/",
            self.root_path.display(),
            opts.job_id
        )))?;
        // Get amount of bytes uploaded in this chunk
        // after compression and encryption
        let ce_bytes_len = chunk_bytes.len();
        // Set chunk's remote path
        let usb_path = format!(
            "{}/{}/chunks/{}.chunk",
            self.root_path.display(),
            opts.job_id,
            chunk.hash
        );
        // Create new file in the USB drive
        let mut cfile = File::create(usb_path.clone()).await?;
        // Copy encrypted and compressed chunk bytes into newly created
        // chunk file
        cfile.write_all(chunk_bytes).await?;
        Ok(ce_bytes_len)
    }

    async fn download(&self, file_name: &str) -> Result<Vec<u8>> {
        // Read result from S3 and convert to bytes
        let path = Path::new(file_name);
        let bytes = if path.metadata()?.len() > crate::run::MAX_OPEN_FILE_LEN {
            debug!("opening {} with mmap", path.display());
            // SAFETY: unsafe used here for mmap
            let mmap = unsafe {
                MmapOptions::new()
                    .populate()
                    .map(&File::open(path).await?)?
            };
            mmap.to_owned()
        } else {
            debug!("opening {} with tokio", path.display());
            tokio::fs::read(path).await?
        };
        // Return downloaded chunk bytes
        Ok(bytes)
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
            let entry_kf = KipFile::new(entry.path().canonicalize()?)?;
            kfs.push(entry_kf);
        }
        Ok(kfs)
    }
}
